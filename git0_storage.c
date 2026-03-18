/*
** git0_storage: SQL functions that operate directly on the storage layer.
**
** These are storage-native equivalents of the git_* functions in git0.c.
** Where git_blob(repo, oid) opens a .git repo via libgit2, git0_blob(oid)
** reads directly from the SQLite objects table via storage_read_object().
**
** Commit and tree parsing uses the raw git binary format:
**   Commit: "tree <hex>\n[parent <hex>\n]*author ...\ncommitter ...\n\n<msg>"
**   Tree: "<mode> <name>\0<20-byte-oid>" entries concatenated
**
** Functions:
**   git0_blob(oid)              -> blob content
**   git0_blob(rev, path)        -> blob at rev:path (tree walk)
**   git0_type(oid)              -> 'blob'|'tree'|'commit'|'tag'
**   git0_size(oid)              -> object size
**   git0_exists(oid)            -> 1 or 0
**   git0_cat(oid)               -> raw object content (any type)
**   git0_ref(name)              -> resolved oid
**   git0_ref_create(name, oid)  -> void
**   git0_ref_delete(name)       -> void
**   git0_commit_message(oid)    -> message text
**   git0_commit_summary(oid)    -> first line of message
**   git0_commit_tree(oid)       -> tree oid hex
**   git0_commit_author(oid)     -> author line
**   git0_commit_parent(oid, n)  -> nth parent oid hex
**   git0_commit_parents(oid)    -> parent count
*/

#include "git0.h"
#include "storage.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include <git2.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Parse a hex OID from text, return 0 on success */
static int parse_oid(sqlite3_context *ctx, sqlite3_value *arg, git_oid *oid) {
  const char *hex = (const char *)sqlite3_value_text(arg);
  if (!hex || git_oid_fromstr(oid, hex) < 0) {
    sqlite3_result_null(ctx);
    return -1;
  }
  return 0;
}

/* Read an object via the storage layer. Caller frees *out. */
static int read_obj(sqlite3_context *ctx, const git_oid *oid,
                    git_object_t *type, size_t *size, unsigned char **out) {
  (void)ctx;
  if (storage_read_object(oid, type, size, out) < 0) {
    sqlite3_result_null(ctx);
    return -1;
  }
  return 0;
}

/* Resolve a ref name to OID, following symrefs via the storage layer. */
static int resolve_ref(const char *name, git_oid *oid) {
  char symref[4096];
  int depth = 0;
  char current[4096];
  snprintf(current, sizeof(current), "%s", name);

  while (depth++ < 10) {
    symref[0] = '\0';
    if (storage_ref_read(current, oid, symref, sizeof(symref)) < 0)
      return -1;
    if (!symref[0])
      return 0;
    snprintf(current, sizeof(current), "%s", symref);
  }
  return -1;
}

/* Find a field in commit text: "tree <hex>\n" returns pointer to hex */
static const char *commit_field(const char *data, size_t size,
                                const char *field, int fieldlen) {
  const char *p = data;
  const char *end = data + size;
  while (p < end) {
    if (p + fieldlen < end && !strncmp(p, field, fieldlen))
      return p + fieldlen;
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
    if (p < end && *p == '\n') return NULL; /* header/body separator */
  }
  return NULL;
}

/* ---- git0_type(oid) ---- */

static void fn_s_type(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_oid oid;
  if (parse_oid(ctx, argv[0], &oid)) return;
  git_object_t type; size_t size; unsigned char *data;
  if (read_obj(ctx, &oid, &type, &size, &data)) return;
  free(data);
  sqlite3_result_text(ctx, git_object_type2string(type), -1, SQLITE_STATIC);
}

/* ---- git0_size(oid) ---- */

static void fn_s_size(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_oid oid;
  if (parse_oid(ctx, argv[0], &oid)) return;
  git_object_t type; size_t size; unsigned char *data;
  if (read_obj(ctx, &oid, &type, &size, &data)) return;
  free(data);
  sqlite3_result_int64(ctx, (sqlite3_int64)size);
}

/* ---- git0_exists(oid) ---- */

static void fn_s_exists(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_oid oid;
  if (parse_oid(ctx, argv[0], &oid)) return;
  sqlite3_result_int(ctx, storage_object_exists(&oid));
}

/* ---- git0_cat(oid) ---- */

static void fn_s_cat(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_oid oid;
  if (parse_oid(ctx, argv[0], &oid)) return;
  git_object_t type; size_t size; unsigned char *data;
  if (read_obj(ctx, &oid, &type, &size, &data)) return;
  sqlite3_result_blob64(ctx, data, size, free);
}

/* ---- git0_blob(oid) and git0_blob(rev_or_oid, path) ---- */

static void fn_s_blob(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  git_oid oid;

  if (argc == 1) {
    /* git0_blob(oid) */
    if (parse_oid(ctx, argv[0], &oid)) return;
    git_object_t type; size_t size; unsigned char *data;
    if (read_obj(ctx, &oid, &type, &size, &data)) return;
    if (type != GIT_OBJECT_BLOB) { free(data); sqlite3_result_null(ctx); return; }
    sqlite3_result_blob64(ctx, data, size, free);
    return;
  }

  /* git0_blob(commit_oid, path) - resolve through tree */
  const char *path = (const char *)sqlite3_value_text(argv[1]);
  if (!path) { sqlite3_result_null(ctx); return; }

  /* Try as ref first, then as raw OID */
  const char *rev = (const char *)sqlite3_value_text(argv[0]);
  if (!rev) { sqlite3_result_null(ctx); return; }
  if (resolve_ref(rev, &oid) < 0 && git_oid_fromstr(&oid, rev) < 0) {
    sqlite3_result_null(ctx); return;
  }

  /* Read commit, extract tree OID */
  git_object_t type; size_t size; unsigned char *data;
  if (read_obj(ctx, &oid, &type, &size, &data)) return;
  if (type != GIT_OBJECT_COMMIT) { free(data); sqlite3_result_null(ctx); return; }

  const char *tree_hex = commit_field((const char *)data, size, "tree ", 5);
  if (!tree_hex) { free(data); sqlite3_result_null(ctx); return; }

  git_oid tree_oid;
  char hex_buf[GIT_OID_SHA1_HEXSIZE + 1];
  memcpy(hex_buf, tree_hex, GIT_OID_SHA1_HEXSIZE);
  hex_buf[GIT_OID_SHA1_HEXSIZE] = '\0';
  free(data);

  if (git_oid_fromstr(&tree_oid, hex_buf) < 0) { sqlite3_result_null(ctx); return; }

  /* Walk path components through tree objects */
  const char *seg = path;
  git_oid current = tree_oid;

  while (*seg) {
    /* Extract next path component */
    const char *slash = strchr(seg, '/');
    size_t seglen = slash ? (size_t)(slash - seg) : strlen(seg);

    if (read_obj(ctx, &current, &type, &size, &data)) return;
    if (type != GIT_OBJECT_TREE) { free(data); sqlite3_result_null(ctx); return; }

    /* Search tree entries: "<mode> <name>\0<20-byte-oid>" */
    const unsigned char *p = data;
    const unsigned char *end = data + size;
    int found = 0;

    while (p < end) {
      /* Skip mode */
      while (p < end && *p != ' ') p++;
      if (p >= end) break;
      p++; /* skip space */

      /* Name */
      const unsigned char *name = p;
      while (p < end && *p != 0) p++;
      if (p >= end) break;
      size_t namelen = p - name;
      p++; /* skip null */

      if (p + GIT_OID_SHA1_SIZE > end) break;

      if (namelen == seglen && !memcmp(name, seg, seglen)) {
        memcpy(current.id, p, GIT_OID_SHA1_SIZE);
        found = 1;
        break;
      }
      p += GIT_OID_SHA1_SIZE;
    }
    free(data);
    if (!found) { sqlite3_result_null(ctx); return; }

    seg += seglen;
    if (*seg == '/') seg++;
  }

  /* current now points to the blob OID */
  if (read_obj(ctx, &current, &type, &size, &data)) return;
  if (type != GIT_OBJECT_BLOB) { free(data); sqlite3_result_null(ctx); return; }
  sqlite3_result_blob64(ctx, data, size, free);
}

/* ---- git0_ref(name) ---- */

static void fn_s_ref(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  if (!name) { sqlite3_result_null(ctx); return; }
  git_oid oid;
  if (resolve_ref(name, &oid) < 0) { sqlite3_result_null(ctx); return; }
  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_ref_create(name, oid) ---- */

static void fn_s_ref_create(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  const char *hex = (const char *)sqlite3_value_text(argv[1]);
  if (!name || !hex) { sqlite3_result_null(ctx); return; }
  git_oid oid;
  if (git_oid_fromstr(&oid, hex) < 0) {
    sqlite3_result_error(ctx, "invalid oid", -1); return;
  }
  storage_ref_write(name, &oid, NULL);
  sqlite3_result_text(ctx, "ok", -1, SQLITE_STATIC);
}

/* ---- git0_ref_delete(name) ---- */

static void fn_s_ref_delete(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  if (!name) return;
  storage_ref_delete(name);
  sqlite3_result_text(ctx, "ok", -1, SQLITE_STATIC);
}

/* ---- Commit field accessors ---- */

static int read_commit(sqlite3_context *ctx, sqlite3_value *arg,
                       unsigned char **data, size_t *size) {
  git_oid oid;
  const char *spec = (const char *)sqlite3_value_text(arg);
  if (!spec) { sqlite3_result_null(ctx); return -1; }

  /* Try as ref, then as raw OID */
  if (resolve_ref(spec, &oid) < 0 && git_oid_fromstr(&oid, spec) < 0) {
    sqlite3_result_null(ctx); return -1;
  }

  git_object_t type;
  if (read_obj(ctx, &oid, &type, size, data)) return -1;
  if (type != GIT_OBJECT_COMMIT) { free(*data); sqlite3_result_null(ctx); return -1; }
  return 0;
}

/* git0_commit_tree(rev) */
static void fn_s_commit_tree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  unsigned char *data; size_t size;
  if (read_commit(ctx, argv[0], &data, &size)) return;
  const char *tree_hex = commit_field((const char *)data, size, "tree ", 5);
  if (tree_hex) {
    sqlite3_result_text(ctx, tree_hex, GIT_OID_SHA1_HEXSIZE, SQLITE_TRANSIENT);
  } else {
    sqlite3_result_null(ctx);
  }
  free(data);
}

/* git0_commit_message(rev) */
static void fn_s_commit_message(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  unsigned char *data; size_t size;
  if (read_commit(ctx, argv[0], &data, &size)) return;
  /* Message starts after "\n\n" */
  const char *p = (const char *)data;
  const char *end = p + size;
  const char *msg = NULL;
  while (p + 1 < end) {
    if (p[0] == '\n' && p[1] == '\n') { msg = p + 2; break; }
    p++;
  }
  if (msg && msg < end)
    sqlite3_result_text(ctx, msg, (int)(end - msg), SQLITE_TRANSIENT);
  else
    sqlite3_result_null(ctx);
  free(data);
}

/* git0_commit_summary(rev) */
static void fn_s_commit_summary(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  unsigned char *data; size_t size;
  if (read_commit(ctx, argv[0], &data, &size)) return;
  const char *p = (const char *)data;
  const char *end = p + size;
  const char *msg = NULL;
  while (p + 1 < end) {
    if (p[0] == '\n' && p[1] == '\n') { msg = p + 2; break; }
    p++;
  }
  if (msg && msg < end) {
    const char *eol = memchr(msg, '\n', end - msg);
    int len = eol ? (int)(eol - msg) : (int)(end - msg);
    sqlite3_result_text(ctx, msg, len, SQLITE_TRANSIENT);
  } else {
    sqlite3_result_null(ctx);
  }
  free(data);
}

/* git0_commit_author(rev) */
static void fn_s_commit_author(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  unsigned char *data; size_t size;
  if (read_commit(ctx, argv[0], &data, &size)) return;
  const char *author = commit_field((const char *)data, size, "author ", 7);
  if (author) {
    const char *eol = memchr(author, '\n', size - (author - (const char *)data));
    int len = eol ? (int)(eol - author) : (int)(size - (author - (const char *)data));
    sqlite3_result_text(ctx, author, len, SQLITE_TRANSIENT);
  } else {
    sqlite3_result_null(ctx);
  }
  free(data);
}

/* git0_commit_parent(rev, n) */
static void fn_s_commit_parent(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  unsigned char *data; size_t size;
  if (read_commit(ctx, argv[0], &data, &size)) return;
  int n = sqlite3_value_int(argv[1]);
  const char *p = (const char *)data;
  const char *end = p + size;
  int count = 0;
  while (p < end) {
    if (!strncmp(p, "parent ", 7)) {
      if (count == n) {
        sqlite3_result_text(ctx, p + 7, GIT_OID_SHA1_HEXSIZE, SQLITE_TRANSIENT);
        free(data);
        return;
      }
      count++;
    }
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
    if (p < end && *p == '\n') break;
  }
  sqlite3_result_null(ctx);
  free(data);
}

/* git0_commit_parents(rev) */
static void fn_s_commit_parents(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  unsigned char *data; size_t size;
  if (read_commit(ctx, argv[0], &data, &size)) return;
  const char *p = (const char *)data;
  const char *end = p + size;
  int count = 0;
  while (p < end) {
    if (!strncmp(p, "parent ", 7)) count++;
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
    if (p < end && *p == '\n') break;
  }
  sqlite3_result_int(ctx, count);
  free(data);
}

/* git0_refs_list is just a view on git0_refs virtual table
 * which already uses the storage layer. No separate TVF needed.
 * Users can: SELECT * FROM git0_refs; or CREATE VIRTUAL TABLE r USING git0_refs; */

/* ---- Registration ---- */

int git0_register_storage(sqlite3 *db) {
  sqlite3_create_function(db, "git0_type", 1, SQLITE_UTF8, 0, fn_s_type, 0, 0);
  sqlite3_create_function(db, "git0_size", 1, SQLITE_UTF8, 0, fn_s_size, 0, 0);
  sqlite3_create_function(db, "git0_exists", 1, SQLITE_UTF8, 0, fn_s_exists, 0, 0);
  sqlite3_create_function(db, "git0_cat", 1, SQLITE_UTF8, 0, fn_s_cat, 0, 0);
  sqlite3_create_function(db, "git0_blob", 1, SQLITE_UTF8, 0, fn_s_blob, 0, 0);
  sqlite3_create_function(db, "git0_blob", 2, SQLITE_UTF8, 0, fn_s_blob, 0, 0);
  sqlite3_create_function(db, "git0_ref", 1, SQLITE_UTF8, 0, fn_s_ref, 0, 0);
  sqlite3_create_function(db, "git0_ref_create", 2, SQLITE_UTF8, 0, fn_s_ref_create, 0, 0);
  sqlite3_create_function(db, "git0_ref_delete", 1, SQLITE_UTF8, 0, fn_s_ref_delete, 0, 0);
  sqlite3_create_function(db, "git0_commit_tree", 1, SQLITE_UTF8, 0, fn_s_commit_tree, 0, 0);
  sqlite3_create_function(db, "git0_commit_message", 1, SQLITE_UTF8, 0, fn_s_commit_message, 0, 0);
  sqlite3_create_function(db, "git0_commit_summary", 1, SQLITE_UTF8, 0, fn_s_commit_summary, 0, 0);
  sqlite3_create_function(db, "git0_commit_author", 1, SQLITE_UTF8, 0, fn_s_commit_author, 0, 0);
  sqlite3_create_function(db, "git0_commit_parent", 2, SQLITE_UTF8, 0, fn_s_commit_parent, 0, 0);
  sqlite3_create_function(db, "git0_commit_parents", 1, SQLITE_UTF8, 0, fn_s_commit_parents, 0, 0);
  return SQLITE_OK;
}
