/*
** git0_objects: virtual table storing git objects in SQLite.
**
** CREATE VIRTUAL TABLE repo USING git0_objects;
**
** Columns: oid TEXT, type TEXT, size INT, data BLOB
**
** INSERT INTO repo(type, data) VALUES('blob', 'hello world');
**   -> computes oid, stores compressed with delta encoding
**
** SELECT * FROM repo WHERE oid = 'abc123...';
**   -> decompresses and resolves delta chains transparently
**
** Uses the shared storage layer (storage.h) for all I/O.
** Schema: objects(oid BLOB PK, type INT, size INT, data BLOB, base BLOB)
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

extern void git0_oid_to_hex(const git_oid *oid, char *out);

/* Object type conversions (git_object_t to storage int) */
static int git_type_to_storage(git_object_t t) {
  switch (t) {
    case GIT_OBJECT_COMMIT: return 1;
    case GIT_OBJECT_TREE: return 2;
    case GIT_OBJECT_BLOB: return 3;
    case GIT_OBJECT_TAG: return 4;
    default: return 0;
  }
}

static git_object_t storage_to_git_type(int t) {
  switch (t) {
    case 1: return GIT_OBJECT_COMMIT;
    case 2: return GIT_OBJECT_TREE;
    case 3: return GIT_OBJECT_BLOB;
    case 4: return GIT_OBJECT_TAG;
    default: return GIT_OBJECT_INVALID;
  }
}

/* ---- Virtual table structure ---- */

typedef struct {
  sqlite3_vtab base;
  sqlite3 *db;
} git0_obj_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *st;
  int eof;
  int is_scan;
  sqlite3_int64 rowid;
} git0_obj_cursor;

/* ---- Lifecycle ---- */

static int obj_create_or_connect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr, int is_create) {
  (void)pAux; (void)argc; (void)argv; (void)pzErr;

  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(oid TEXT, type TEXT, size INT, data BLOB)");
  if (rc != SQLITE_OK) return rc;

  git0_obj_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  vtab->db = db;

  if (is_create) {
    /* Create the objects table using storage schema */
    rc = sqlite3_exec(db,
      "CREATE TABLE IF NOT EXISTS objects("
      "  oid BLOB PRIMARY KEY,"
      "  type INTEGER NOT NULL,"
      "  size INTEGER NOT NULL,"
      "  data BLOB NOT NULL,"
      "  base BLOB"
      ") WITHOUT ROWID",
      0, 0, pzErr);
    if (rc != SQLITE_OK) { sqlite3_free(vtab); return rc; }
  }

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
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int obj_destroy(sqlite3_vtab *pVtab) {
  git0_obj_vtab *vtab = (git0_obj_vtab *)pVtab;
  sqlite3_exec(vtab->db, "DROP TABLE IF EXISTS objects", 0, 0, 0);
  return obj_disconnect(pVtab);
}

/* ---- Best index ---- */

static int obj_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo) {
  (void)pVtab;
  for (int i = 0; i < pIdxInfo->nConstraint; i++) {
    if (pIdxInfo->aConstraint[i].iColumn == 0 &&
        pIdxInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ &&
        pIdxInfo->aConstraint[i].usable) {
      pIdxInfo->aConstraintUsage[i].argvIndex = 1;
      pIdxInfo->aConstraintUsage[i].omit = 1;
      pIdxInfo->idxNum = 1;
      pIdxInfo->estimatedCost = 1.0;
      return SQLITE_OK;
    }
  }
  pIdxInfo->idxNum = 0;
  pIdxInfo->estimatedCost = 1000000.0;
  return SQLITE_OK;
}

/* ---- Cursor ---- */

static int obj_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  (void)pVtab;
  git0_obj_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int obj_close(sqlite3_vtab_cursor *pCursor) {
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;
  if (cur->st) sqlite3_finalize(cur->st);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int obj_filter(sqlite3_vtab_cursor *pCursor, int idxNum,
                      const char *idxStr, int argc, sqlite3_value **argv) {
  (void)idxStr;
  git0_obj_vtab *vtab = (git0_obj_vtab *)pCursor->pVtab;
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;

  if (cur->st) { sqlite3_finalize(cur->st); cur->st = NULL; }
  cur->rowid = 0;

  if (idxNum == 1 && argc >= 1) {
    /* OID lookup */
    const char *hex = (const char *)sqlite3_value_text(argv[0]);
    unsigned char oid_bin[OID_RAWSZ];
    if (!hex || hex2bin(hex, oid_bin, OID_RAWSZ) < 0) {
      cur->eof = 1;
      return SQLITE_OK;
    }
    sqlite3_prepare_v2(vtab->db,
      "SELECT oid, type, size, data, base FROM objects WHERE oid = ?",
      -1, &cur->st, 0);
    sqlite3_bind_blob(cur->st, 1, oid_bin, OID_RAWSZ, SQLITE_TRANSIENT);
    cur->is_scan = 0;
  } else {
    /* Full scan */
    sqlite3_prepare_v2(vtab->db,
      "SELECT oid, type, size, data, base FROM objects ORDER BY oid",
      -1, &cur->st, 0);
    cur->is_scan = 1;
  }

  cur->eof = (sqlite3_step(cur->st) != SQLITE_ROW);
  return SQLITE_OK;
}

static int obj_next(sqlite3_vtab_cursor *pCursor) {
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;
  cur->eof = (sqlite3_step(cur->st) != SQLITE_ROW);
  cur->rowid++;
  return SQLITE_OK;
}

static int obj_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git0_obj_cursor *)pCursor)->eof;
}

static int obj_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git0_obj_cursor *cur = (git0_obj_cursor *)pCursor;

  switch (col) {
    case 0: { /* oid TEXT */
      const unsigned char *oid_bin = sqlite3_column_blob(cur->st, 0);
      if (oid_bin) {
        char hex[OID_HEXSZ + 1];
        bin2hex(oid_bin, OID_RAWSZ, hex);
        sqlite3_result_text(ctx, hex, OID_HEXSZ, SQLITE_TRANSIENT);
      } else {
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 1: /* type TEXT */
      sqlite3_result_text(ctx, type_name(sqlite3_column_int(cur->st, 1)),
                          -1, SQLITE_STATIC);
      break;
    case 2: /* size */
      sqlite3_result_int(ctx, sqlite3_column_int(cur->st, 2));
      break;
    case 3: { /* data BLOB (decompressed, delta-resolved) */
      const unsigned char *oid_bin = sqlite3_column_blob(cur->st, 0);
      if (oid_bin) {
        int type; unsigned long size; unsigned char *data;
        if (storage_read_object(oid_bin, &type, &size, &data) == 0) {
          sqlite3_result_blob(ctx, data, size, free);
        } else {
          sqlite3_result_null(ctx);
        }
      }
      break;
    }
  }
  return SQLITE_OK;
}

static int obj_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git0_obj_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

/* ---- INSERT ---- */

static int obj_update(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv,
                      sqlite3_int64 *pRowid) {
  (void)pVtab; (void)pRowid;
  if (argc < 2) return SQLITE_OK; /* DELETE not supported */

  /* INSERT: argv[2]=oid, argv[3]=type, argv[4]=size, argv[5]=data */
  const char *type_str = (const char *)sqlite3_value_text(argv[3]);
  const void *data = sqlite3_value_blob(argv[5]);
  int data_len = sqlite3_value_bytes(argv[5]);

  if (!type_str || !data) return SQLITE_ERROR;

  int type = type_from_name(type_str);
  if (!type) return SQLITE_ERROR;

  /* Compute OID using git's format: "<type> <size>\0<data>" */
  git_oid oid;
  git_odb_hash(&oid, data, data_len, storage_to_git_type(type));

  /* Write via storage layer (handles compression + delta) */
  storage_write_object(oid.id, type, data, data_len);

  return SQLITE_OK;
}

/* ---- Module definition ---- */

static sqlite3_module git0_objects_module = {
  .iVersion = 0,
  .xCreate = obj_create,
  .xConnect = obj_connect,
  .xBestIndex = obj_best_index,
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

void git0_register_objects(sqlite3 *db) {
  sqlite3_create_module(db, "git0_objects", &git0_objects_module, 0);
}
