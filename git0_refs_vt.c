/*
** git0_refs: virtual table storing git references in SQLite.
**
** CREATE VIRTUAL TABLE refs USING git0_refs;
**
** Columns: name TEXT, type TEXT, target TEXT, symref TEXT
**
** Uses the shared storage schema: refs(refname TEXT PK, oid BLOB, symref TEXT)
** The 'target' column presents oid as hex text to SQL users.
*/

#include "git0.h"
#include "storage.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include <string.h>

/* ---- Virtual table structure ---- */

typedef struct {
  sqlite3_vtab base;
  sqlite3 *db;
  sqlite3_stmt *st_insert;
  sqlite3_stmt *st_update;
  sqlite3_stmt *st_delete;
  sqlite3_stmt *st_lookup;
  sqlite3_stmt *st_scan;
} git0_refs_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *st;
  int eof;
  sqlite3_int64 rowid;
} git0_refs_cursor;

/* ---- Lifecycle ---- */

static int refs_create_or_connect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr, int is_create) {
  (void)pAux; (void)argc; (void)argv;

  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(name TEXT, type TEXT, target TEXT, symref TEXT)");
  if (rc != SQLITE_OK) return rc;

  git0_refs_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  vtab->db = db;

  if (is_create) {
    rc = sqlite3_exec(db,
      "CREATE TABLE IF NOT EXISTS refs("
      "  refname TEXT PRIMARY KEY,"
      "  oid BLOB,"
      "  symref TEXT"
      ") WITHOUT ROWID",
      0, 0, pzErr);
    if (rc != SQLITE_OK) { sqlite3_free(vtab); return rc; }
  }

  sqlite3_prepare_v2(db,
    "INSERT OR REPLACE INTO refs(refname, oid, symref) VALUES(?,?,?)",
    -1, &vtab->st_insert, 0);
  sqlite3_prepare_v2(db,
    "UPDATE refs SET oid = ?, symref = ? WHERE refname = ?",
    -1, &vtab->st_update, 0);
  sqlite3_prepare_v2(db,
    "DELETE FROM refs WHERE refname = ?",
    -1, &vtab->st_delete, 0);
  sqlite3_prepare_v2(db,
    "SELECT refname, oid, symref FROM refs WHERE refname = ?",
    -1, &vtab->st_lookup, 0);
  sqlite3_prepare_v2(db,
    "SELECT refname, oid, symref FROM refs ORDER BY refname",
    -1, &vtab->st_scan, 0);

  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int refs_create(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                       sqlite3_vtab **ppVtab, char **pzErr) {
  return refs_create_or_connect(db, pAux, argc, argv, ppVtab, pzErr, 1);
}

static int refs_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                        sqlite3_vtab **ppVtab, char **pzErr) {
  return refs_create_or_connect(db, pAux, argc, argv, ppVtab, pzErr, 0);
}

static int refs_disconnect(sqlite3_vtab *pVtab) {
  git0_refs_vtab *vtab = (git0_refs_vtab *)pVtab;
  if (vtab->st_insert) sqlite3_finalize(vtab->st_insert);
  if (vtab->st_update) sqlite3_finalize(vtab->st_update);
  if (vtab->st_delete) sqlite3_finalize(vtab->st_delete);
  if (vtab->st_lookup) sqlite3_finalize(vtab->st_lookup);
  if (vtab->st_scan) sqlite3_finalize(vtab->st_scan);
  sqlite3_free(vtab);
  return SQLITE_OK;
}

static int refs_destroy(sqlite3_vtab *pVtab) {
  git0_refs_vtab *vtab = (git0_refs_vtab *)pVtab;
  sqlite3_exec(vtab->db, "DROP TABLE IF EXISTS refs", 0, 0, 0);
  return refs_disconnect(pVtab);
}

/* ---- Cursor ---- */

static int refs_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  (void)pVtab;
  git0_refs_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int refs_close(sqlite3_vtab_cursor *pCursor) {
  git0_refs_cursor *cur = (git0_refs_cursor *)pCursor;
  if (cur->st) sqlite3_reset(cur->st);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int refs_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  (void)pVtab;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].iColumn == 0 &&
        pInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
      pInfo->aConstraintUsage[i].argvIndex = 1;
      pInfo->aConstraintUsage[i].omit = 1;
      pInfo->estimatedCost = 1;
      pInfo->idxNum = 1;
      return SQLITE_OK;
    }
  }
  pInfo->estimatedCost = 100;
  pInfo->idxNum = 0;
  return SQLITE_OK;
}

static int refs_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                       int argc, sqlite3_value **argv) {
  (void)idxStr;
  git0_refs_cursor *cur = (git0_refs_cursor *)pCursor;
  git0_refs_vtab *vtab = (git0_refs_vtab *)pCursor->pVtab;
  cur->eof = 0; cur->rowid = 0;

  if (idxNum == 1 && argc > 0) {
    cur->st = vtab->st_lookup;
    sqlite3_reset(cur->st);
    sqlite3_bind_text(cur->st, 1,
      (const char *)sqlite3_value_text(argv[0]), -1, SQLITE_TRANSIENT);
  } else {
    cur->st = vtab->st_scan;
    sqlite3_reset(cur->st);
  }

  if (sqlite3_step(cur->st) != SQLITE_ROW) cur->eof = 1;
  return SQLITE_OK;
}

static int refs_next(sqlite3_vtab_cursor *pCursor) {
  git0_refs_cursor *cur = (git0_refs_cursor *)pCursor;
  cur->rowid++;
  if (sqlite3_step(cur->st) != SQLITE_ROW) cur->eof = 1;
  return SQLITE_OK;
}

static int refs_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git0_refs_cursor *)pCursor)->eof;
}

static int refs_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git0_refs_cursor *cur = (git0_refs_cursor *)pCursor;
  switch (col) {
    case 0: /* name TEXT */
      sqlite3_result_value(ctx, sqlite3_column_value(cur->st, 0));
      break;
    case 1: { /* type TEXT (derived from name) */
      const char *name = (const char *)sqlite3_column_text(cur->st, 0);
      if (strncmp(name, "refs/heads/", 11) == 0) sqlite3_result_text(ctx, "branch", -1, SQLITE_STATIC);
      else if (strncmp(name, "refs/tags/", 10) == 0) sqlite3_result_text(ctx, "tag", -1, SQLITE_STATIC);
      else if (strncmp(name, "refs/remotes/", 13) == 0) sqlite3_result_text(ctx, "remote", -1, SQLITE_STATIC);
      else if (strcmp(name, "HEAD") == 0) sqlite3_result_text(ctx, "head", -1, SQLITE_STATIC);
      else sqlite3_result_text(ctx, "other", -1, SQLITE_STATIC);
      break;
    }
    case 2: { /* target TEXT (hex oid) */
      const void *oid_blob = sqlite3_column_blob(cur->st, 1);
      if (oid_blob) {
        char hex[GIT_OID_SHA1_HEXSIZE + 1];
        git_oid oid_r; memcpy(oid_r.id, oid_blob, GIT_OID_SHA1_SIZE); git_oid_tostr(hex, sizeof(hex), &oid_r);
        sqlite3_result_text(ctx, hex, GIT_OID_SHA1_HEXSIZE, SQLITE_TRANSIENT);
      } else {
        sqlite3_result_null(ctx);
      }
      break;
    }
    case 3: /* symref TEXT */
      sqlite3_result_value(ctx, sqlite3_column_value(cur->st, 2));
      break;
    default:
      sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int refs_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git0_refs_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

/* ---- Update (INSERT, UPDATE, DELETE) ---- */

static int refs_update(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid) {
  git0_refs_vtab *vtab = (git0_refs_vtab *)pVtab;
  (void)pRowid;

  if (argc == 1) {
    /* DELETE by rowid (not well supported, skip) */
    return SQLITE_OK;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    /* INSERT */
    const char *name = (const char *)sqlite3_value_text(argv[2]);
    const char *target_hex = (const char *)sqlite3_value_text(argv[4]);
    const char *symref = argc > 5 && sqlite3_value_type(argv[5]) != SQLITE_NULL
      ? (const char *)sqlite3_value_text(argv[5]) : NULL;

    sqlite3_reset(vtab->st_insert);
    sqlite3_bind_text(vtab->st_insert, 1, name, -1, SQLITE_TRANSIENT);

    if (target_hex && *target_hex) {
      git_oid oid_val;
      if (git_oid_fromstr(&oid_val, target_hex) < 0) return SQLITE_ERROR;
      sqlite3_bind_blob(vtab->st_insert, 2, oid_val.id, GIT_OID_SHA1_SIZE, SQLITE_TRANSIENT);
    } else {
      sqlite3_bind_null(vtab->st_insert, 2);
    }

    if (symref) sqlite3_bind_text(vtab->st_insert, 3, symref, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(vtab->st_insert, 3);
    sqlite3_step(vtab->st_insert);
  } else {
    /* UPDATE */
    const char *target_hex = (const char *)sqlite3_value_text(argv[4]);
    const char *name = (const char *)sqlite3_value_text(argv[2]);
    const char *symref = argc > 5 && sqlite3_value_type(argv[5]) != SQLITE_NULL
      ? (const char *)sqlite3_value_text(argv[5]) : NULL;

    sqlite3_reset(vtab->st_update);
    if (target_hex && *target_hex) {
      git_oid oid_val;
      if (git_oid_fromstr(&oid_val, target_hex) < 0) return SQLITE_ERROR;
      sqlite3_bind_blob(vtab->st_update, 1, oid_val.id, GIT_OID_SHA1_SIZE, SQLITE_TRANSIENT);
    } else {
      sqlite3_bind_null(vtab->st_update, 1);
    }
    if (symref) sqlite3_bind_text(vtab->st_update, 2, symref, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(vtab->st_update, 2);
    sqlite3_bind_text(vtab->st_update, 3, name, -1, SQLITE_TRANSIENT);
    sqlite3_step(vtab->st_update);
  }

  return SQLITE_OK;
}

/* ---- Module ---- */

static sqlite3_module git0_refs_vt_module = {
  .iVersion = 1,
  .xCreate = refs_create,
  .xConnect = refs_connect,
  .xBestIndex = refs_bestindex,
  .xDisconnect = refs_disconnect,
  .xDestroy = refs_destroy,
  .xOpen = refs_open,
  .xClose = refs_close,
  .xFilter = refs_filter,
  .xNext = refs_next,
  .xEof = refs_eof,
  .xColumn = refs_column,
  .xRowid = refs_rowid,
  .xUpdate = refs_update,
};

int git0_register_refs_vt(sqlite3 *db) {
  return sqlite3_create_module(db, "git0_refs", &git0_refs_vt_module, 0);
}
