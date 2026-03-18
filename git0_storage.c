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
#include <zlib.h>

/* Parse a hex OID from text, return 0 on success */
static int parse_oid(sqlite3_context *ctx, sqlite3_value *arg, git_oid *oid) {
  const char *hex = (const char *)sqlite3_value_text(arg);
  if (!hex || git_oid_fromstr(oid, hex) < 0) {
    sqlite3_result_null(ctx);
    return -1;
  }
  return 0;
}

/* Read an object, return data and size. Caller frees *out.
 * Uses storage_read_object when storage is open (helper binary),
 * otherwise queries the objects table directly (extension mode). */
static int read_obj(sqlite3_context *ctx, const git_oid *oid,
                    git_object_t *type, size_t *size, unsigned char **out) {
  /* Try storage layer first (handles delta resolution + decompression) */
  if (storage_db() && storage_read_object(oid, type, size, out) == 0)
    return 0;

  /* Fallback: direct SQL on extension db (no compression/delta) */
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  sqlite3_stmt *st = NULL;
  sqlite3_prepare_v2(db,
    "SELECT type, size, data FROM objects WHERE oid = ?", -1, &st, 0);
  if (!st) { sqlite3_result_null(ctx); return -1; }
  sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
  if (sqlite3_step(st) != SQLITE_ROW) {
    sqlite3_finalize(st);
    sqlite3_result_null(ctx);
    return -1;
  }
  *type = (git_object_t)sqlite3_column_int(st, 0);
  *size = (size_t)sqlite3_column_int64(st, 1);
  const void *blob = sqlite3_column_blob(st, 2);
  int blob_len = sqlite3_column_bytes(st, 2);

  /* Data might be compressed or a delta. Try zlib decompression. */
  unsigned char *decompressed = malloc(*size ? *size : 1);
  if (!decompressed) { sqlite3_finalize(st); sqlite3_result_null(ctx); return -1; }
  unsigned long dstlen = *size;
  if (uncompress(decompressed, &dstlen, blob, blob_len) == Z_OK) {
    *out = decompressed;
  } else {
    /* Raw data (already decompressed or empty) */
    free(decompressed);
    *out = malloc(blob_len ? blob_len : 1);
    if (!*out) { sqlite3_finalize(st); sqlite3_result_null(ctx); return -1; }
    memcpy(*out, blob, blob_len);
    *size = blob_len;
  }
  sqlite3_finalize(st);
  return 0;
}

/* Resolve a ref name to OID, following symrefs */
static int resolve_ref_db(sqlite3 *db, const char *name, git_oid *oid) {
  int depth = 0;
  char current[4096];
  snprintf(current, sizeof(current), "%s", name);

  while (depth++ < 10) {
    /* Try storage layer first */
    if (storage_db()) {
      char symref[4096] = "";
      if (storage_ref_read(current, oid, symref, sizeof(symref)) < 0)
        return -1;
      if (!symref[0]) return 0;
      snprintf(current, sizeof(current), "%s", symref);
      continue;
    }

    /* Direct SQL fallback */
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
      "SELECT oid, symref FROM refs WHERE refname = ?", -1, &st, 0);
    if (!st) return -1;
    sqlite3_bind_text(st, 1, current, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return -1; }
    const void *blob = sqlite3_column_blob(st, 0);
    const char *sym = (const char *)sqlite3_column_text(st, 1);
    if (sym && *sym) {
      snprintf(current, sizeof(current), "%s", sym);
      sqlite3_finalize(st);
      continue;
    }
    if (blob) memcpy(oid->id, blob, GIT_OID_SHA1_SIZE);
    sqlite3_finalize(st);
    return blob ? 0 : -1;
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
  if (resolve_ref_db(sqlite3_context_db_handle(ctx),rev, &oid) < 0 && git_oid_fromstr(&oid, rev) < 0) {
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
  if (resolve_ref_db(sqlite3_context_db_handle(ctx),name, &oid) < 0) { sqlite3_result_null(ctx); return; }
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
  if (resolve_ref_db(sqlite3_context_db_handle(ctx),spec, &oid) < 0 && git_oid_fromstr(&oid, spec) < 0) {
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

/* ---- git0_refs_list TVF: storage-native ref enumeration ---- */

typedef struct {
  sqlite3_vtab base;
} git0_srl_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *st;
  int eof;
  sqlite3_int64 rowid;
} git0_srl_cursor;

static int srl_connect(sqlite3 *db, void *pAux, int argc,
                       const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr) {
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(name TEXT, oid TEXT, symref TEXT)");
  if (rc != SQLITE_OK) return rc;
  git0_srl_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int srl_disconnect(sqlite3_vtab *pVtab) {
  sqlite3_free(pVtab); return SQLITE_OK;
}

static int srl_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  (void)pVtab;
  pInfo->estimatedCost = 100;
  return SQLITE_OK;
}

static int srl_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  (void)pVtab;
  git0_srl_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int srl_close(sqlite3_vtab_cursor *pCursor) {
  git0_srl_cursor *cur = (git0_srl_cursor *)pCursor;
  if (cur->st) sqlite3_finalize(cur->st);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int srl_filter(sqlite3_vtab_cursor *pCursor, int idxNum,
                      const char *idxStr, int argc, sqlite3_value **argv) {
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  git0_srl_cursor *cur = (git0_srl_cursor *)pCursor;
  if (cur->st) { sqlite3_finalize(cur->st); cur->st = NULL; }
  cur->rowid = 0;
  sqlite3 *db = storage_db();
  if (!db) { cur->eof = 1; return SQLITE_OK; }
  sqlite3_prepare_v2(db,
    "SELECT refname, oid, symref FROM refs ORDER BY refname",
    -1, &cur->st, 0);
  cur->eof = (sqlite3_step(cur->st) != SQLITE_ROW);
  return SQLITE_OK;
}

static int srl_next(sqlite3_vtab_cursor *pCursor) {
  git0_srl_cursor *cur = (git0_srl_cursor *)pCursor;
  cur->eof = (sqlite3_step(cur->st) != SQLITE_ROW);
  cur->rowid++;
  return SQLITE_OK;
}

static int srl_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git0_srl_cursor *)pCursor)->eof;
}

static int srl_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git0_srl_cursor *cur = (git0_srl_cursor *)pCursor;
  switch (col) {
    case 0: /* name */
      sqlite3_result_value(ctx, sqlite3_column_value(cur->st, 0));
      break;
    case 1: { /* oid as hex */
      const void *blob = sqlite3_column_blob(cur->st, 1);
      if (blob) {
        char hex[GIT_OID_SHA1_HEXSIZE + 1];
        git_oid oid; memcpy(oid.id, blob, GIT_OID_SHA1_SIZE);
        git_oid_tostr(hex, sizeof(hex), &oid);
        sqlite3_result_text(ctx, hex, GIT_OID_SHA1_HEXSIZE, SQLITE_TRANSIENT);
      } else {
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 2: /* symref */
      sqlite3_result_value(ctx, sqlite3_column_value(cur->st, 2));
      break;
  }
  return SQLITE_OK;
}

static int srl_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git0_srl_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

static sqlite3_module git0_refs_list_module = {
  .iVersion = 0,
  .xCreate = srl_connect,
  .xConnect = srl_connect,
  .xBestIndex = srl_bestindex,
  .xDisconnect = srl_disconnect,
  .xDestroy = srl_disconnect,
  .xOpen = srl_open,
  .xClose = srl_close,
  .xFilter = srl_filter,
  .xNext = srl_next,
  .xEof = srl_eof,
  .xColumn = srl_column,
  .xRowid = srl_rowid,
};

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
  sqlite3_create_module(db, "git0_refs_list", &git0_refs_list_module, 0);
  return SQLITE_OK;
}
