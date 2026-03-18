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
#include "storage.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include <git2.h>
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
  git_object_t type;

  if (storage_read_object(oid, &type, &size, &data) < 0)
    return GIT_ENOTFOUND;

  /* libgit2 requires data allocated with git_odb_backend_data_alloc */
  void *buf = git_odb_backend_data_alloc(_backend, size);
  if (!buf) { free(data); return GIT_ERROR; }
  memcpy(buf, data, size);
  free(data);

  *data_p = buf;
  *len_p = size;
  *type_p = type;
  return 0;
}

static int storage_backend_read_header(size_t *len_p, git_object_t *type_p,
                                       git_odb_backend *_backend,
                                       const git_oid *oid) {
  (void)_backend;
  unsigned char *data;
  size_t size;
  git_object_t type;

  if (storage_read_object(oid, &type, &size, &data) < 0)
    return GIT_ENOTFOUND;

  free(data);
  *len_p = size;
  *type_p = type;
  return 0;
}

static int storage_backend_write(git_odb_backend *_backend,
                                 const git_oid *oid,
                                 const void *data, size_t len,
                                 git_object_t type) {
  (void)_backend;
  storage_write_object(oid, type, data, len);
  return 0;
}

static int storage_backend_exists(git_odb_backend *_backend,
                                  const git_oid *oid) {
  (void)_backend;
  return storage_object_exists(oid);
}

static int storage_backend_foreach(git_odb_backend *_backend,
                                   git_odb_foreach_cb cb,
                                   void *payload) {
  (void)_backend;
  /* Not implemented; returning 0 is acceptable (optional callback) */
  (void)cb; (void)payload;
  return 0;
}

static void storage_backend_free(git_odb_backend *_backend) {
  free(_backend);
}

static git_odb_backend *create_storage_backend(void) {
  struct storage_odb_backend *backend = calloc(1, sizeof(*backend));
  if (!backend) return NULL;

  backend->parent.version = GIT_ODB_BACKEND_VERSION;
  backend->parent.read = storage_backend_read;
  backend->parent.read_header = storage_backend_read_header;
  backend->parent.write = storage_backend_write;
  backend->parent.exists = storage_backend_exists;
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
  git_oid oid; char symref[4096];
  *exists = (storage_ref_read(ref_name, &oid, symref, sizeof(symref)) == 0);
  return 0;
}

static int storage_refdb_lookup(git_reference **out, git_refdb_backend *_be,
                                const char *ref_name) {
  (void)_be;
  git_oid oid; char symref[4096] = "";
  if (storage_ref_read(ref_name, &oid, symref, sizeof(symref)) < 0)
    return GIT_ENOTFOUND;
  if (symref[0])
    *out = git_reference__alloc_symbolic(ref_name, symref);
  else
    *out = git_reference__alloc(ref_name, &oid, NULL);
  return *out ? 0 : -1;
}

struct storage_ref_iter {
  git_reference_iterator parent;
  struct { char *name; git_oid oid; char *symref; } *entries;
  size_t nr, alloc, pos;
};

static int collect_ref(const char *refname, const git_oid *oid,
                       const char *symref, void *data) {
  struct storage_ref_iter *iter = data;
  if (iter->nr >= iter->alloc) {
    iter->alloc = iter->alloc ? iter->alloc * 2 : 64;
    iter->entries = realloc(iter->entries, iter->alloc * sizeof(*iter->entries));
  }
  iter->entries[iter->nr].name = strdup(refname);
  if (oid) iter->entries[iter->nr].oid = *oid;
  else memset(&iter->entries[iter->nr].oid, 0, sizeof(git_oid));
  iter->entries[iter->nr].symref = symref && *symref ? strdup(symref) : NULL;
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

static int storage_refdb_write(git_refdb_backend *_be,
                               const git_reference *ref, int force,
                               const git_signature *who, const char *message,
                               const git_oid *old, const char *old_target) {
  (void)_be; (void)force; (void)who; (void)message;
  (void)old; (void)old_target;
  const char *name = git_reference_name(ref);
  if (git_reference_type(ref) == GIT_REFERENCE_SYMBOLIC)
    storage_ref_write(name, NULL, git_reference_symbolic_target(ref));
  else
    storage_ref_write(name, git_reference_target(ref), NULL);
  return 0;
}

static int storage_refdb_del(git_refdb_backend *_be, const char *ref_name,
                             const git_oid *old_id, const char *old_target) {
  (void)_be; (void)old_id; (void)old_target;
  storage_ref_delete(ref_name);
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
  /* Reflog reading through libgit2 not yet implemented */
  return GIT_ENOTFOUND;
}

static int storage_refdb_reflog_write(git_refdb_backend *_be, git_reflog *reflog) {
  (void)_be; (void)reflog;
  return 0;
}

static int storage_refdb_reflog_rename(git_refdb_backend *_be,
                                       const char *old_name, const char *new_name) {
  (void)_be; (void)old_name; (void)new_name;
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
  git_oid oid; char symref[4096] = "";
  if (storage_ref_read(old_name, &oid, symref, sizeof(symref)) < 0)
    return GIT_ENOTFOUND;
  if (symref[0])
    storage_ref_write(new_name, NULL, symref);
  else
    storage_ref_write(new_name, &oid, NULL);
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

  git_odb_backend *odb_be = create_storage_backend();
  if (!odb_be) return;

  git_odb_new(&s_odb);
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

/* ---- Cleanup ---- */

void git0_backend_cleanup(void) {
  if (s_repo) { git_repository_free(s_repo); s_repo = NULL; }
  if (s_refdb) { git_refdb_free(s_refdb); s_refdb = NULL; }
  if (s_odb) { git_odb_free(s_odb); s_odb = NULL; }
  s_ext_db = NULL;
  storage_close();
}

/* ---- Registration ---- */

int git0_register_backend(sqlite3 *db) {
  s_ext_db = db;
  sqlite3_create_function(db, "git0_repo", 0, SQLITE_UTF8, 0, fn_git0_repo, 0, 0);
  return SQLITE_OK;
}
