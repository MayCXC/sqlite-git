/*
** git0_objects: virtual table storing git objects in SQLite.
**
** CREATE VIRTUAL TABLE repo USING git0_objects;
**
** Columns: oid TEXT, type TEXT, size INT, data BLOB
** Hidden:  encoding TEXT (for future delta support)
**
** INSERT INTO repo(type, data) VALUES('blob', 'hello world');
**   -> computes oid automatically, stores in loose table
**
** SELECT * FROM repo WHERE oid = 'abc123...';
**   -> searches loose table (pack index in future)
**
** Shadow tables:
**   {name}_loose(oid TEXT PRIMARY KEY, type INT, size INT, data BLOB)
**   {name}_pack(id INTEGER PRIMARY KEY, data BLOB)     -- future
**   {name}_idx(pack_id INT, oid TEXT, offset INT, size INT) -- future
*/

#include "git0.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include <git2.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

extern void git0_oid_to_hex(const git_oid *oid, char *out);

/* Object type conversions */
static git_object_t type_from_string(const char *s) {
  if (!s) return GIT_OBJECT_INVALID;
  if (strcmp(s, "blob") == 0) return GIT_OBJECT_BLOB;
  if (strcmp(s, "tree") == 0) return GIT_OBJECT_TREE;
  if (strcmp(s, "commit") == 0) return GIT_OBJECT_COMMIT;
  if (strcmp(s, "tag") == 0) return GIT_OBJECT_TAG;
  return GIT_OBJECT_INVALID;
}

static const char *type_to_string(int t) {
  switch (t) {
    case GIT_OBJECT_BLOB: return "blob";
    case GIT_OBJECT_TREE: return "tree";
    case GIT_OBJECT_COMMIT: return "commit";
    case GIT_OBJECT_TAG: return "tag";
    default: return "unknown";
  }
}

/* ---- Virtual table structure ---- */

typedef struct {
  sqlite3_vtab base;
  sqlite3 *db;
  char *table_name;
  /* Prepared statements for shadow table ops */
  sqlite3_stmt *st_insert;
  sqlite3_stmt *st_lookup;
  sqlite3_stmt *st_scan;
  sqlite3_stmt *st_count;
  sqlite3_stmt *st_exists;
} git0_obj_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *st;       /* current query statement */
  int eof;
  int is_scan;            /* 1 = full scan, 0 = oid lookup */
  sqlite3_int64 rowid;
} git0_obj_cursor;

/* ---- Lifecycle ---- */

static int obj_create_or_connect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr, int is_create) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(oid TEXT, type TEXT, size INT, data BLOB)");
  if (rc != SQLITE_OK) return rc;

  git0_obj_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  vtab->db = db;

  /* Table name for shadow tables */
  const char *name = argv[2]; /* module argument = table name */
  vtab->table_name = sqlite3_mprintf("%s", name);

  if (is_create) {
    /* Create shadow table for loose objects */
    char *sql = sqlite3_mprintf(
      "CREATE TABLE IF NOT EXISTS \"%w_loose\"("
      "  oid TEXT PRIMARY KEY,"
      "  type INTEGER NOT NULL,"
      "  size INTEGER NOT NULL,"
      "  data BLOB NOT NULL"
      ")", name);
    rc = sqlite3_exec(db, sql, 0, 0, pzErr);
    sqlite3_free(sql);
    if (rc != SQLITE_OK) { sqlite3_free(vtab->table_name); sqlite3_free(vtab); return rc; }
  }

  /* Prepare statements */
  char *sql;

  sql = sqlite3_mprintf("INSERT OR IGNORE INTO \"%w_loose\"(oid, type, size, data) VALUES(?,?,?,?)", name);
  sqlite3_prepare_v2(db, sql, -1, &vtab->st_insert, 0);
  sqlite3_free(sql);

  sql = sqlite3_mprintf("SELECT oid, type, size, data FROM \"%w_loose\" WHERE oid = ?", name);
  sqlite3_prepare_v2(db, sql, -1, &vtab->st_lookup, 0);
  sqlite3_free(sql);

  sql = sqlite3_mprintf("SELECT oid, type, size, data FROM \"%w_loose\" ORDER BY oid", name);
  sqlite3_prepare_v2(db, sql, -1, &vtab->st_scan, 0);
  sqlite3_free(sql);

  sql = sqlite3_mprintf("SELECT oid FROM \"%w_loose\" WHERE oid = ?", name);
  sqlite3_prepare_v2(db, sql, -1, &vtab->st_exists, 0);
  sqlite3_free(sql);

  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int obj_create(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                      sqlite3_vtab **ppVtab, char **pzErr) {
  return obj_create_or_connect(db, pAux, argc, argv, ppVtab, pzErr, 1);
}

static int obj_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                       sqlite3_vtab **ppVtab, char **pzErr) {
  return obj_create_or_connect(db, pAux, argc, argv, ppVtab, pzErr, 0);
}

static int obj_disconnect(sqlite3_vtab *pVtab) {
  git0_obj_vtab *vtab = (git0_obj_vtab *)pVtab;
  if (vtab->st_insert) sqlite3_finalize(vtab->st_insert);
  if (vtab->st_lookup) sqlite3_finalize(vtab->st_lookup);
  if (vtab->st_scan) sqlite3_finalize(vtab->st_scan);
  if (vtab->st_count) sqlite3_finalize(vtab->st_count);
  if (vtab->st_exists) sqlite3_finalize(vtab->st_exists);
  sqlite3_free(vtab->table_name);
  sqlite3_free(vtab);
  return SQLITE_OK;
}

static int obj_destroy(sqlite3_vtab *pVtab) {
  git0_obj_vtab *vtab = (git0_obj_vtab *)pVtab;
  char *sql = sqlite3_mprintf("DROP TABLE IF EXISTS \"%w_loose\"", vtab->table_name);
  sqlite3_exec(vtab->db, sql, 0, 0, 0);
  sqlite3_free(sql);
  return obj_disconnect(pVtab);
}

/* ---- Cursor ---- */

static int obj_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git0_obj_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int obj_close(sqlite3_vtab_cursor *pCursor) {
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;
  if (cur->st) sqlite3_reset(cur->st);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/* ---- Best index: optimize oid = X lookups ---- */

static int obj_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].iColumn == 0 /* oid */ &&
        pInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
      pInfo->aConstraintUsage[i].argvIndex = 1;
      pInfo->aConstraintUsage[i].omit = 1;
      pInfo->estimatedCost = 1;
      pInfo->idxNum = 1; /* lookup mode */
      return SQLITE_OK;
    }
  }
  pInfo->estimatedCost = 1000;
  pInfo->idxNum = 0; /* scan mode */
  return SQLITE_OK;
}

/* ---- Filter / Next / Eof ---- */

static int obj_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                      int argc, sqlite3_value **argv) {
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;
  git0_obj_vtab *vtab = (git0_obj_vtab *)pCursor->pVtab;

  cur->eof = 0;
  cur->rowid = 0;

  if (idxNum == 1 && argc > 0) {
    /* OID lookup */
    cur->st = vtab->st_lookup;
    cur->is_scan = 0;
    sqlite3_reset(cur->st);
    sqlite3_bind_text(cur->st, 1, (const char *)sqlite3_value_text(argv[0]), -1, SQLITE_TRANSIENT);
  } else {
    /* Full scan */
    cur->st = vtab->st_scan;
    cur->is_scan = 1;
    sqlite3_reset(cur->st);
  }

  int rc = sqlite3_step(cur->st);
  if (rc == SQLITE_DONE) cur->eof = 1;
  else if (rc != SQLITE_ROW) {
    cur->eof = 1;
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int obj_next(sqlite3_vtab_cursor *pCursor) {
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;
  cur->rowid++;
  int rc = sqlite3_step(cur->st);
  if (rc != SQLITE_ROW) cur->eof = 1;
  return SQLITE_OK;
}

static int obj_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git0_obj_cursor *)pCursor)->eof;
}

/* ---- Column ---- */

static int obj_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;
  switch (col) {
    case 0: /* oid */
      sqlite3_result_value(ctx, sqlite3_column_value(cur->st, 0));
      break;
    case 1: /* type */
      sqlite3_result_text(ctx, type_to_string(sqlite3_column_int(cur->st, 1)), -1, SQLITE_STATIC);
      break;
    case 2: /* size */
      sqlite3_result_value(ctx, sqlite3_column_value(cur->st, 2));
      break;
    case 3: /* data */
      sqlite3_result_value(ctx, sqlite3_column_value(cur->st, 3));
      break;
    default:
      sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int obj_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git0_obj_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

/* ---- Update (INSERT) ---- */

static int obj_update(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid) {
  git0_obj_vtab *vtab = (git0_obj_vtab *)pVtab;

  /* argc == 1: DELETE (content-addressed, ignore) */
  if (argc == 1) return SQLITE_OK;

  /* argc > 1 && argv[0] == NULL: INSERT */
  if (sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    /* UPDATE: not meaningful for content-addressed store */
    return SQLITE_OK;
  }

  /* argv[2] = oid (or NULL to auto-compute)
   * argv[3] = type
   * argv[4] = size (ignored, computed)
   * argv[5] = data
   */
  const char *type_str = (const char *)sqlite3_value_text(argv[3]);
  const void *data = sqlite3_value_blob(argv[5]);
  int data_len = sqlite3_value_bytes(argv[5]);
  git_object_t type = type_from_string(type_str);

  if (type == GIT_OBJECT_INVALID) {
    vtab->base.zErrMsg = sqlite3_mprintf("invalid object type: %s", type_str);
    return SQLITE_ERROR;
  }

  /* Compute OID */
  char oid_hex[GIT_OID_MAX_HEXSIZE + 1];
  if (sqlite3_value_type(argv[2]) != SQLITE_NULL) {
    /* OID provided */
    const char *provided = (const char *)sqlite3_value_text(argv[2]);
    strncpy(oid_hex, provided, GIT_OID_MAX_HEXSIZE);
    oid_hex[GIT_OID_MAX_HEXSIZE] = 0;
  } else {
    /* Auto-compute OID from content */
    git_oid oid;
    git_odb_hash(&oid, data, data_len, type);
    git0_oid_to_hex(&oid, oid_hex);
  }

  /* Insert into loose table */
  sqlite3_reset(vtab->st_insert);
  sqlite3_bind_text(vtab->st_insert, 1, oid_hex, -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(vtab->st_insert, 2, (int)type);
  sqlite3_bind_int(vtab->st_insert, 3, data_len);
  sqlite3_bind_blob(vtab->st_insert, 4, data, data_len, SQLITE_TRANSIENT);
  int rc = sqlite3_step(vtab->st_insert);
  sqlite3_reset(vtab->st_insert);

  if (rc != SQLITE_DONE) {
    vtab->base.zErrMsg = sqlite3_mprintf("insert failed: %s", sqlite3_errmsg(vtab->db));
    return SQLITE_ERROR;
  }

  return SQLITE_OK;
}

/* ---- Module definition ---- */

static sqlite3_module git0_objects_module = {
  .iVersion = 1,  /* need iVersion >= 1 for xUpdate */
  .xCreate = obj_create,
  .xConnect = obj_connect,
  .xBestIndex = obj_bestindex,
  .xDisconnect = obj_disconnect,
  .xDestroy = obj_destroy,
  .xOpen = obj_open,
  .xClose = obj_close,
  .xFilter = obj_filter,
  .xNext = obj_next,
  .xEof = obj_eof,
  .xColumn = obj_column,
  .xRowid = obj_rowid,
  .xUpdate = obj_update,
};

/* ---- Registration ---- */

int git0_register_objects(sqlite3 *db) {
  return sqlite3_create_module(db, "git0_objects", &git0_objects_module, 0);
}
