/*
** git0_backend: custom libgit2 ODB backend wrapping the storage layer.
**
** Creates a git_repository backed by storage_read_object/write_object,
** enabling all libgit2 algorithms (revwalk, diff, blame, merge-base)
** to operate on SQLite-stored objects without a .git directory.
**
** Usage from SQL:
**   SELECT git_log(git0_repo(), 'HEAD');
**   SELECT * FROM git_diff(git0_repo(), 'HEAD~1', 'HEAD');
**
** git0_repo() returns a pseudo-path that git0.c recognizes as the
** storage-backed repo handle.
*/

#include "git0.h"
#include "storage_git.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include "oidhex.h"
#include <git2/sys/odb_backend.h>
#include <git2/sys/refdb_backend.h>
#include <git2/sys/repository.h>
#include <git2/sys/refs.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ---- Custom ODB backend ---- */

struct storage_odb_backend {
  git_odb_backend parent;
};

static int storage_backend_read(void **data_p, size_t *len_p,
                                git_object_t *type_p,
                                git_odb_backend *_backend,
                                const git_oid *oid) {
  unsigned char *data;
  size_t size;
  char type_name[32];
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), oid);

  if (storage_read_object(hex, type_name, sizeof(type_name), &size, &data) < 0)
    return GIT_ENOTFOUND;

  /* libgit2 requires data allocated with git_odb_backend_data_alloc */
  void *buf = git_odb_backend_data_alloc(_backend, size);
  if (!buf) { free(data); return GIT_ERROR; }
  memcpy(buf, data, size);
  free(data);

  *data_p = buf;
  *len_p = size;
  *type_p = git_object_string2type(type_name);
  return 0;
}

static int storage_backend_read_header(size_t *len_p, git_object_t *type_p,
                                       git_odb_backend *_backend,
                                       const git_oid *oid) {
  (void)_backend;
  size_t size;
  char type_name[32];
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), oid);

  /* Header-only: no inflate or delta resolve (the point of a read_header
   * path), unlike a full read whose bytes we'd discard. */
  if (storage_read_header(hex, type_name, sizeof(type_name), &size) < 0)
    return GIT_ENOTFOUND;

  *len_p = size;
  *type_p = git_object_string2type(type_name);
  return 0;
}

static int storage_backend_write(git_odb_backend *_backend,
                                 const git_oid *oid,
                                 const void *data, size_t len,
                                 git_object_t type) {
  (void)_backend;
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), oid);
  if (storage_write_object(hex, git_object_type2string(type), data, len, 0) != 0)
    return GIT_ERROR;
  return 0;
}

static int storage_backend_exists(git_odb_backend *_backend,
                                  const git_oid *oid) {
  (void)_backend;
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), oid);
  return storage_object_exists(hex);
}

/* Bridge storage's object-iteration callback to libgit2's git_oid one. Reuses
 * storage_obj_list (the hex enumeration the helper face serves to git's
 * cat-file --batch-all-objects / count-objects / fsck, covered by git core's
 * t0620) instead of a private oid-only query, so the libgit2 foreach shares the
 * single enumeration path and cannot drift from it. type/size are unused here. */
struct foreach_bridge { git_odb_foreach_cb cb; void *payload; };
static int foreach_bridge_cb(const char *oid_hex, const char *type,
                             size_t size, void *data) {
  (void)type; (void)size;
  struct foreach_bridge *b = data;
  git_oid oid;
  if (oid_from_hex(&oid, oid_hex) != 0) return 0;  /* skip an oid this build can't represent */
  return b->cb(&oid, b->payload);
}

static int storage_backend_foreach(git_odb_backend *_backend,
                                   git_odb_foreach_cb cb,
                                   void *payload) {
  (void)_backend;
  struct foreach_bridge b = { cb, payload };
  return storage_obj_list(foreach_bridge_cb, &b);
}

/* read_prefix / exists_prefix: resolve an abbreviated oid through the indexed
 * prefix scan (storage_obj_list_filtered scopes server-side by hex prefix), so
 * libgit2 abbreviation/disambiguation does not fall back to a full foreach. The
 * short oid has its trailing bits zeroed (the read_prefix contract), so its full
 * hex truncated to `len` nibbles is the prefix to match. */
struct prefix_match {
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  int count;
};
static int prefix_match_cb(const char *oid_hex, const char *type,
                           size_t size, void *data) {
  (void)type; (void)size;
  struct prefix_match *m = data;
  if (m->count == 0)
    snprintf(m->hex, sizeof(m->hex), "%s", oid_hex);
  m->count++;
  return 0;
}

static int prefix_resolve(git_oid *out, const git_oid *short_oid, size_t len) {
  char full[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(full, sizeof(full), short_oid);
  if (len > GIT_OID_MAX_HEXSIZE) len = GIT_OID_MAX_HEXSIZE;
  char prefix[GIT_OID_MAX_HEXSIZE + 1];
  snprintf(prefix, sizeof(prefix), "%.*s", (int)len, full);
  struct prefix_match m = { .count = 0 };
  storage_obj_list_filtered(0, prefix, prefix_match_cb, &m);
  if (m.count == 0) return GIT_ENOTFOUND;
  if (m.count > 1) return GIT_EAMBIGUOUS;
  return oid_from_hex(out, m.hex) == 0 ? 0 : GIT_ENOTFOUND;
}

static int storage_backend_read_prefix(git_oid *out_oid, void **data_p, size_t *len_p,
                                       git_object_t *type_p, git_odb_backend *_backend,
                                       const git_oid *short_oid, size_t len) {
  int rc = prefix_resolve(out_oid, short_oid, len);
  if (rc) return rc;
  return storage_backend_read(data_p, len_p, type_p, _backend, out_oid);
}

static int storage_backend_exists_prefix(git_oid *out_full, git_odb_backend *_backend,
                                         const git_oid *short_oid, size_t len) {
  (void)_backend;
  return prefix_resolve(out_full, short_oid, len);
}

static void storage_backend_free(git_odb_backend *_backend) {
  free(_backend);
}

/*
 * Deliberately-absent odb_backend methods (libgit2 handles their absence):
 *   writestream/readstream: storage holds whole objects, so the streaming
 *     interface would only buffer then call the same whole-object path libgit2
 *     already falls back to via read/write.
 *   writepack/writemidx: a per-object store has no packfiles or midx.
 *   freshen: last-used timestamps exist to protect objects from prune, which
 *     is the helper's gc domain, not the in-process extension's.
 *   refresh: no out-of-process cache to re-scan.
 */
static git_odb_backend *create_storage_backend(void) {
  struct storage_odb_backend *backend = calloc(1, sizeof(*backend));
  if (!backend) return NULL;

  backend->parent.version = GIT_ODB_BACKEND_VERSION;
  backend->parent.read = storage_backend_read;
  backend->parent.read_prefix = storage_backend_read_prefix;
  backend->parent.read_header = storage_backend_read_header;
  backend->parent.write = storage_backend_write;
  backend->parent.exists = storage_backend_exists;
  backend->parent.exists_prefix = storage_backend_exists_prefix;
  backend->parent.foreach = storage_backend_foreach;
  backend->parent.free = storage_backend_free;

  return &backend->parent;
}

/* ---- Custom refdb backend ---- */

struct storage_refdb_backend {
  git_refdb_backend parent;
};

static int storage_refdb_exists(int *exists, git_refdb_backend *_be,
                                const char *ref_name) {
  (void)_be;
  char oid[GIT_OID_MAX_HEXSIZE + 1]; char symref[4096];
  *exists = (storage_ref_read(ref_name, oid, sizeof(oid), symref, sizeof(symref)) == 0);
  return 0;
}

static int storage_refdb_lookup(git_reference **out, git_refdb_backend *_be,
                                const char *ref_name) {
  (void)_be;
  char oid_hex[GIT_OID_MAX_HEXSIZE + 1] = ""; char symref[4096] = "";
  if (storage_ref_read(ref_name, oid_hex, sizeof(oid_hex), symref, sizeof(symref)) < 0)
    return GIT_ENOTFOUND;
  if (symref[0])
    *out = git_reference__alloc_symbolic(ref_name, symref);
  else {
    git_oid oid;
    if (oid_from_hex(&oid, oid_hex) != 0)
      return GIT_ENOTFOUND;
    *out = git_reference__alloc(ref_name, &oid, NULL);
  }
  return *out ? 0 : -1;
}

struct storage_ref_iter {
  git_reference_iterator parent;
  struct { char *name; git_oid oid; char *symref; } *entries;
  size_t nr, alloc, pos;
};

static int collect_ref(const char *refname, const char *oid,
                       const char *symref, void *data) {
  struct storage_ref_iter *iter = data;
  if (iter->nr >= iter->alloc) {
    size_t new_alloc = iter->alloc ? iter->alloc * 2 : 64;
    void *p = realloc(iter->entries, new_alloc * sizeof(*iter->entries));
    if (!p) return -1;
    iter->entries = p;
    iter->alloc = new_alloc;
  }
  iter->entries[iter->nr].name = strdup(refname);
  if (!iter->entries[iter->nr].name) return -1;
  if (!oid || !*oid || oid_from_hex(&iter->entries[iter->nr].oid, oid) != 0)
    memset(&iter->entries[iter->nr].oid, 0, sizeof(git_oid));
  if (symref && *symref) {
    iter->entries[iter->nr].symref = strdup(symref);
    if (!iter->entries[iter->nr].symref) { free(iter->entries[iter->nr].name); return -1; }
  } else {
    iter->entries[iter->nr].symref = NULL;
  }
  iter->nr++;
  return 0;
}

static int storage_ref_iter_next(git_reference **out,
                                 git_reference_iterator *_iter) {
  struct storage_ref_iter *iter = (struct storage_ref_iter *)_iter;
  if (iter->pos >= iter->nr) return GIT_ITEROVER;
  size_t i = iter->pos++;
  if (iter->entries[i].symref)
    *out = git_reference__alloc_symbolic(iter->entries[i].name, iter->entries[i].symref);
  else
    *out = git_reference__alloc(iter->entries[i].name, &iter->entries[i].oid, NULL);
  return *out ? 0 : -1;
}

static int storage_ref_iter_next_name(const char **out,
                                      git_reference_iterator *_iter) {
  struct storage_ref_iter *iter = (struct storage_ref_iter *)_iter;
  if (iter->pos >= iter->nr) return GIT_ITEROVER;
  *out = iter->entries[iter->pos++].name;
  return 0;
}

static void storage_ref_iter_free(git_reference_iterator *_iter) {
  struct storage_ref_iter *iter = (struct storage_ref_iter *)_iter;
  for (size_t i = 0; i < iter->nr; i++) {
    free(iter->entries[i].name);
    free(iter->entries[i].symref);
  }
  free(iter->entries);
  free(iter);
}

static int storage_refdb_iterator(git_reference_iterator **out,
                                  struct git_refdb_backend *_be,
                                  const char *glob) {
  (void)_be;
  struct storage_ref_iter *iter = calloc(1, sizeof(*iter));
  if (!iter) return -1;
  iter->parent.next = storage_ref_iter_next;
  iter->parent.next_name = storage_ref_iter_next_name;
  iter->parent.free = storage_ref_iter_free;
  storage_ref_list(glob, collect_ref, iter);
  *out = &iter->parent;
  return 0;
}

/* Verify a ref's current value against the expected old (the compare in CAS).
 * Returns 0 on match, GIT_EMODIFIED on mismatch. old_target expects a symref,
 * old_id a direct oid. */
static int refdb_check_old(const char *name, const git_oid *old_id,
                           const char *old_target) {
  char cur_oid[GIT_OID_MAX_HEXSIZE + 1] = {0};
  char cur_sym[4096] = {0};
  int exists = storage_ref_read(name, cur_oid, sizeof(cur_oid),
                                cur_sym, sizeof(cur_sym)) >= 0;
  if (old_target) {
    if (!exists || !cur_sym[0] || strcmp(cur_sym, old_target) != 0)
      return GIT_EMODIFIED;
  } else {
    char old_hex[GIT_OID_MAX_HEXSIZE + 1];
    git_oid_tostr(old_hex, sizeof(old_hex), old_id);
    if (!exists || cur_sym[0] || strcmp(cur_oid, old_hex) != 0)
      return GIT_EMODIFIED;
  }
  return 0;
}

static int storage_refdb_write(git_refdb_backend *_be,
                               const git_reference *ref, int force,
                               const git_signature *who, const char *message,
                               const git_oid *old, const char *old_target) {
  (void)_be;
  const char *name = git_reference_name(ref);

  /*
   * Compare-and-swap: unless forced, an update carrying an expected old value
   * must match the ref's current value. libgit2's git_reference_create_matching
   * / symbolic_set_target contract is GIT_EMODIFIED on mismatch, NOT a clobber of
   * a ref that moved underneath. (The helper face enforces the same in
   * cmd_txn_update.)
   */
  if (!force && (old || old_target)) {
    int rc = refdb_check_old(name, old, old_target);
    if (rc) return rc;
  }

  if (git_reference_type(ref) == GIT_REFERENCE_SYMBOLIC) {
    if (storage_ref_write(name, NULL, git_reference_symbolic_target(ref)) != 0)
      return GIT_ERROR;
    return 0;
  }

  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), git_reference_target(ref));
  if (storage_ref_write(name, hex, NULL) != 0)
    return GIT_ERROR;

  /*
   * Persist a reflog entry when libgit2 requests one (who != NULL): a logged
   * direct-ref update (e.g. git_commit_create with an update_ref, or a
   * git_reference API call passing a log message) records old -> new with the
   * committer identity, timestamp and message, exactly as the files backend
   * appends to logs/<ref>. who == NULL is an unlogged update, so no entry is
   * written. The old value is the CAS old oid when given, otherwise the zero oid
   * of the matching width (a ref creation). It runs in the same enclosing SQLite
   * statement transaction as the ref write, so a reflog failure rolls the ref
   * write back with it. (Symbolic-ref updates above are not logged here; their
   * reflog is the resolved-oid history the helper face records, and the
   * extension's symref writes carry no who.)
   */
  if (who) {
    char old_hex[GIT_OID_MAX_HEXSIZE + 1], committer[512];
    if (old)
      git_oid_tostr(old_hex, sizeof(old_hex), old);
    else
      /* a ref creation: the zero oid as hex's own width of '0' digits */
      snprintf(old_hex, sizeof(old_hex), "%0*d", (int)strlen(hex), 0);
    snprintf(committer, sizeof(committer), "%s <%s>", who->name, who->email);
    if (storage_reflog_append(name, old_hex, hex, committer,
                              (long long)who->when.time, who->when.offset,
                              message ? message : "") != 0)
      return GIT_ERROR;
  }
  return 0;
}

static int storage_refdb_del(git_refdb_backend *_be, const char *ref_name,
                             const git_oid *old_id, const char *old_target) {
  (void)_be;
  /* Compare-and-swap delete: an expected old value must match before removing. */
  if (old_id || old_target) {
    int rc = refdb_check_old(ref_name, old_id, old_target);
    if (rc) return rc;
  }
  if (storage_ref_delete(ref_name) != 0)
    return GIT_ERROR;
  return 0;
}

static int storage_refdb_has_log(git_refdb_backend *_be, const char *refname) {
  (void)_be;
  return storage_reflog_exists(refname);
}

static int storage_refdb_ensure_log(git_refdb_backend *_be, const char *refname) {
  (void)_be; (void)refname;
  return 0;
}

static int storage_refdb_reflog_read(git_reflog **out, git_refdb_backend *_be,
                                     const char *name) {
  (void)_be; (void)out; (void)name;
  /*
   * Documented exception to "implement every libgit2 interface": libgit2 (1.9.0)
   * exposes no public constructor for a git_reflog, so a custom refdb backend
   * cannot build the git_reflog this callback must return. The fix is upstream
   * PR libgit2#7041 ("Ref DB: Expose method to allocate a new reflog"), which
   * exposes the internal reflog_alloc; once it lands we implement this with that
   * allocator plus git_reflog_append. Until then reflog_read stays stubbed.
   * Reflogs ARE available to the extension via storage_reflog_read directly (the
   * git_stash TVF uses it); only the libgit2 git_reflog_read() path is affected.
   */
  return GIT_ENOTFOUND;
}

/*
 * reflog_write is a deliberate no-op: libgit2 exposes no public accessor for a
 * git_reflog's ref name (the callback receives only the opaque reflog), so a
 * custom backend cannot tell which ref's log to persist; and the path is
 * unreachable anyway, since reflog_read is stubbed (GIT_ENOTFOUND), so libgit2
 * never hands the caller a reflog to write back. Reflog ENTRIES are instead
 * persisted at the ref write itself (storage_refdb_write, when who != NULL),
 * which has the name + old + new + committer + message.
 */
static int storage_refdb_reflog_write(git_refdb_backend *_be, git_reflog *reflog) {
  (void)_be; (void)reflog;
  return 0;
}

static int storage_refdb_reflog_rename(git_refdb_backend *_be,
                                       const char *old_name, const char *new_name) {
  (void)_be;
  /*
   * Carry the ref's reflog to the new name on rename (delete_old), mirroring the
   * files backend moving logs/<old> to logs/<new> and reftable's write_copy_table.
   * Both names are given here (unlike reflog_write), so this IS implementable.
   */
  if (storage_reflog_copy(old_name, new_name, 1) != 0)
    return GIT_ERROR;
  return 0;
}

static int storage_refdb_reflog_delete(git_refdb_backend *_be, const char *name) {
  (void)_be;
  storage_reflog_delete(name);
  return 0;
}

static int storage_refdb_rename(git_reference **out, git_refdb_backend *_be,
                                const char *old_name, const char *new_name,
                                int force, const git_signature *who,
                                const char *message) {
  (void)force; (void)who; (void)message;
  char oid[GIT_OID_MAX_HEXSIZE + 1] = ""; char symref[4096] = "";
  if (storage_ref_read(old_name, oid, sizeof(oid), symref, sizeof(symref)) < 0)
    return GIT_ENOTFOUND;
  if (symref[0])
    storage_ref_write(new_name, NULL, symref);
  else
    storage_ref_write(new_name, oid, NULL);
  storage_ref_delete(old_name);
  return storage_refdb_lookup(out, _be, new_name);
}

static void storage_refdb_free(git_refdb_backend *_be) { free(_be); }

static git_refdb_backend *create_storage_refdb(void) {
  struct storage_refdb_backend *be = calloc(1, sizeof(*be));
  if (!be) return NULL;
  git_refdb_init_backend(&be->parent, GIT_REFDB_BACKEND_VERSION);
  be->parent.exists = storage_refdb_exists;
  be->parent.lookup = storage_refdb_lookup;
  be->parent.iterator = storage_refdb_iterator;
  be->parent.write = storage_refdb_write;
  be->parent.del = storage_refdb_del;
  be->parent.has_log = storage_refdb_has_log;
  be->parent.ensure_log = storage_refdb_ensure_log;
  be->parent.reflog_read = storage_refdb_reflog_read;
  be->parent.reflog_write = storage_refdb_reflog_write;
  be->parent.reflog_rename = storage_refdb_reflog_rename;
  be->parent.reflog_delete = storage_refdb_reflog_delete;
  be->parent.rename = storage_refdb_rename;
  be->parent.free = storage_refdb_free;
  return &be->parent;
}

/* ---- Cached repo handle ---- */

static git_repository *s_repo;
static git_odb *s_odb;
static git_refdb *s_refdb;
static sqlite3 *s_ext_db; /* extension db handle for lazy storage init */

static void ensure_repo(void) {
  if (s_repo) return;

  /* Lazily initialize storage on first use */
  if (!storage_db() && s_ext_db)
    storage_open_db(s_ext_db);
  if (!storage_db()) return;
  storage_git_init();  /* ensure the git schema exists on this connection */

  git_odb_backend *odb_be = create_storage_backend();
  if (!odb_be) return;

#ifdef GIT_EXPERIMENTAL_SHA256
  /* The odb's oid_type is the repo's hash, taken from the persisted object format
     git0_init recorded; git_repository_wrap_odb carries it to the repo, so
     git_index_write_tree and git_repository_oid_type all observe the same hash. */
  git_odb_options odb_opts = GIT_ODB_OPTIONS_INIT;
  odb_opts.oid_type = storage_object_format() == 1 ? GIT_OID_SHA256 : GIT_OID_SHA1;
  git_odb_new(&s_odb, &odb_opts);
#else
  git_odb_new(&s_odb);
#endif
  git_odb_add_backend(s_odb, odb_be, 1);

  /* Create a repo from ODB, then attach storage-backed refdb */
  git_repository_wrap_odb(&s_repo, s_odb);

  git_refdb_backend *refdb_be = create_storage_refdb();
  if (refdb_be) {
    git_refdb_new(&s_refdb, s_repo);
    if (s_refdb) {
      git_refdb_set_backend(s_refdb, refdb_be);
      git_repository_set_refdb(s_repo, s_refdb);
    }
  }
}

/* ---- git0_repo() scalar function ---- */

static const char STORAGE_REPO_PATH[] = ":storage:";

static void fn_git0_repo(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc; (void)argv;
  ensure_repo();
  if (!s_repo) {
    sqlite3_result_error(ctx, "storage layer not initialized", -1);
    return;
  }
  sqlite3_result_text(ctx, STORAGE_REPO_PATH, -1, SQLITE_STATIC);
}

/*
 * Get the storage-backed repo. Returns NULL if not initialized.
 * Called by git0.c when repo path is ":storage:".
 */
git_repository *git0_storage_repo(void) {
  ensure_repo();
  return s_repo;
}

/* ---- Registration ---- */

int git0_register_backend(sqlite3 *db) {
  s_ext_db = db;
  sqlite3_create_function(db, "git0_repo", 0, SQLITE_UTF8, 0, fn_git0_repo, 0, 0);
  return SQLITE_OK;
}
