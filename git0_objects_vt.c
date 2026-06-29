/*
** git0_objects_vt: a writable virtual table over the storage-backed object store.
**
**   CREATE VIRTUAL TABLE git_objects USING git0_objects;
**   SELECT oid, type, size FROM git_objects;
**   SELECT data FROM git_objects WHERE oid = '<oid-hex>';
**   INSERT INTO git_objects(type, data) VALUES('blob', 'hello world');
**
** Columns: oid TEXT (hex), type TEXT, size INT, data BLOB.
**
** Writes go through libgit2: an INSERT is git_odb_write (content-addressed, the
** oid is computed from the type + data under the repository's hash), so any oid
** the caller supplies is ignored. There is NO UPDATE (an object's bytes ARE its
** identity) and NO DELETE (libgit2's odb exposes no delete; object removal is
** gc/prune on the helper side), so both are rejected, matching libgit2's surface.
** The data column reads through libgit2 (git_odb_read).
**
** The enumeration is the one read kept on storage rather than libgit2:
** git_odb_foreach's callback yields only the oid, so a typed listing through it
** would re-read type/size per object (N+1 queries) where storage_obj_list returns
** all three in a single scan. Per the project rule (fit libgit2 wherever it is
** performant, never at an asymptotic loss), the typed enumeration uses
** storage_obj_list / storage_obj_list_filtered; type/size then come free from the
** scan and only the heavy data column hits libgit2.
*/

#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT3
#else
  #include <sqlite3.h>
#endif

#include "git0.h"
#include "git0_internal.h"
#include "oidhex.h"
#include "storage_git.h"
#include <stdlib.h>
#include <string.h>

typedef struct { sqlite3_vtab base; } obj_vtab;

struct obj_row { char *oid; char *type; sqlite3_int64 size; };

typedef struct {
  sqlite3_vtab_cursor base;
  struct obj_row *rows;
  int nr, alloc, pos;
} obj_cursor;

static int obj_connect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                       sqlite3_vtab **ppVtab, char **pzErr) {
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(oid TEXT, type TEXT, size INT, data BLOB)");
  if (rc != SQLITE_OK) return rc;
  /* The typed enumeration reads storage_obj_list directly, so bind storage to
   * this connection and ensure the git schema (idempotent; the same connection
   * git0_storage_repo uses for the libgit2 odb writes/reads). */
  if (!storage_db()) storage_open_db(db);
  storage_git_init();
  obj_vtab *vt = sqlite3_malloc(sizeof(*vt));
  if (!vt) return SQLITE_NOMEM;
  memset(vt, 0, sizeof(*vt));
  *ppVtab = &vt->base;
  return SQLITE_OK;
}

static int obj_disconnect(sqlite3_vtab *pVtab) {
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int obj_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  (void)pVtab;
  obj_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static void obj_rows_free(obj_cursor *cur) {
  for (int i = 0; i < cur->nr; i++) {
    free(cur->rows[i].oid);
    free(cur->rows[i].type);
  }
  cur->nr = 0;
}

static int obj_close(sqlite3_vtab_cursor *pCursor) {
  obj_cursor *cur = (obj_cursor *)pCursor;
  obj_rows_free(cur);
  free(cur->rows);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int obj_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *info) {
  (void)pVtab;
  for (int i = 0; i < info->nConstraint; i++) {
    if (info->aConstraint[i].usable &&
        info->aConstraint[i].iColumn == 0 &&	/* oid */
        info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ) {
      info->aConstraintUsage[i].argvIndex = 1;
      info->aConstraintUsage[i].omit = 1;
      info->idxNum = 1;
      info->estimatedCost = 1;
      return SQLITE_OK;
    }
  }
  info->idxNum = 0;
  info->estimatedCost = 1e6;
  return SQLITE_OK;
}

static int obj_collect(const char *oid, const char *type, size_t size, void *data) {
  obj_cursor *cur = data;
  if (cur->nr >= cur->alloc) {
    int na = cur->alloc ? cur->alloc * 2 : 64;
    struct obj_row *p = realloc(cur->rows, (size_t)na * sizeof(*p));
    if (!p) return 1;
    cur->rows = p;
    cur->alloc = na;
  }
  struct obj_row *r = &cur->rows[cur->nr];
  r->oid = strdup(oid ? oid : "");
  r->type = strdup(type ? type : "");
  r->size = (sqlite3_int64)size;
  if (!r->oid || !r->type) { free(r->oid); free(r->type); return 1; }
  cur->nr++;
  return 0;
}

static int obj_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                      int argc, sqlite3_value **argv) {
  (void)idxStr;
  obj_cursor *cur = (obj_cursor *)pCursor;
  obj_rows_free(cur);
  cur->pos = 0;

  /* Typed enumeration in one scan (oid + type + size); a full oid is its own
   * range, so the point lookup returns just that object. */
  if (idxNum == 1 && argc > 0) {
    const char *oid = (const char *)sqlite3_value_text(argv[0]);
    if (oid && *oid)
      storage_obj_list_filtered(0, oid, obj_collect, cur);
  } else {
    storage_obj_list(obj_collect, cur);
  }
  return SQLITE_OK;
}

static int obj_next(sqlite3_vtab_cursor *pCursor) {
  ((obj_cursor *)pCursor)->pos++;
  return SQLITE_OK;
}

static int obj_eof(sqlite3_vtab_cursor *pCursor) {
  obj_cursor *cur = (obj_cursor *)pCursor;
  return cur->pos >= cur->nr;
}

static int obj_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  obj_cursor *cur = (obj_cursor *)pCursor;
  struct obj_row *r = &cur->rows[cur->pos];
  switch (col) {
  case 0:
    sqlite3_result_text(ctx, r->oid, -1, SQLITE_TRANSIENT);
    break;
  case 1:
    sqlite3_result_text(ctx, r->type, -1, SQLITE_TRANSIENT);
    break;
  case 2:
    sqlite3_result_int64(ctx, r->size);
    break;
  case 3: {	/* data: read the full object through libgit2, only when selected */
    git_oid oid;
    git_repository *repo = git0_storage_repo();
    git_odb *odb = NULL;
    if (!repo || oid_from_hex(&oid, r->oid) != 0 ||
        git_repository_odb(&odb, repo) != 0) {
      sqlite3_result_null(ctx);
      break;
    }
    git_odb_object *o = NULL;
    if (git_odb_read(&o, odb, &oid) == 0) {
      sqlite3_result_blob64(ctx, git_odb_object_data(o), git_odb_object_size(o), SQLITE_TRANSIENT);
      git_odb_object_free(o);
    } else {
      sqlite3_result_null(ctx);
    }
    git_odb_free(odb);
    break;
  }
  default:
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int obj_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((obj_cursor *)pCursor)->pos;
  return SQLITE_OK;
}

static int obj_update(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv,
                      sqlite3_int64 *pRowid) {
  /* DELETE: libgit2's odb has no object delete (objects are immutable; removal
   * is gc/prune on the helper side), so reject rather than invent one. */
  if (argc == 1) {
    pVtab->zErrMsg = sqlite3_mprintf(
      "git_objects: objects are immutable; removal is gc/prune's domain, not a SQL DELETE");
    return SQLITE_ERROR;
  }
  /* UPDATE: an object's bytes ARE its identity (a change is a different object). */
  if (sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    pVtab->zErrMsg = sqlite3_mprintf(
      "git_objects: objects are content-addressed and immutable; INSERT a new object");
    return SQLITE_ERROR;
  }

  /* INSERT: type = argv[3], data = argv[5]; git_odb_write computes the oid. */
  const char *type_str = (const char *)sqlite3_value_text(argv[3]);
  const void *data = sqlite3_value_blob(argv[5]);
  int len = sqlite3_value_bytes(argv[5]);
  if (!type_str) {
    pVtab->zErrMsg = sqlite3_mprintf("git_objects: a type is required");
    return SQLITE_ERROR;
  }
  git_object_t type = git_object_string2type(type_str);
  if (type == GIT_OBJECT_INVALID) {
    pVtab->zErrMsg = sqlite3_mprintf("git_objects: invalid type '%s'", type_str);
    return SQLITE_ERROR;
  }
  git_repository *repo = git0_storage_repo();
  git_odb *odb = NULL;
  if (!repo || git_repository_odb(&odb, repo) != 0) {
    pVtab->zErrMsg = sqlite3_mprintf("git_objects: storage repo unavailable");
    return SQLITE_ERROR;
  }
  git_oid oid;
  int rc = git_odb_write(&oid, odb, data ? data : "", (size_t)len, type);
  git_odb_free(odb);
  if (rc != 0) {
    const git_error *e = git_error_last();
    pVtab->zErrMsg = sqlite3_mprintf("git_objects: %s", e ? e->message : "write failed");
    return SQLITE_ERROR;
  }
  if (pRowid) *pRowid = 0;	/* rowids are per-scan, not persistent */
  return SQLITE_OK;
}

static sqlite3_module git0_objects_module = {
  .iVersion = 1,
  .xCreate = obj_connect,
  .xConnect = obj_connect,
  .xBestIndex = obj_bestindex,
  .xDisconnect = obj_disconnect,
  .xDestroy = obj_disconnect,
  .xOpen = obj_open,
  .xClose = obj_close,
  .xFilter = obj_filter,
  .xNext = obj_next,
  .xEof = obj_eof,
  .xColumn = obj_column,
  .xRowid = obj_rowid,
  .xUpdate = obj_update,
};

int git0_register_objects_vt(sqlite3 *db) {
  return sqlite3_create_module(db, "git0_objects", &git0_objects_module, 0);
}
