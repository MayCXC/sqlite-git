/*
** git0_refs_vt: a WRITABLE virtual table over the libgit2 reference database.
**
**   CREATE VIRTUAL TABLE git_refs USING git0_refs;
**   SELECT * FROM git_refs;
**   SELECT target FROM git_refs WHERE name = 'refs/heads/master';
**   INSERT INTO git_refs(name, target) VALUES('refs/heads/x', '<oid-hex>');
**   INSERT INTO git_refs(name, symref) VALUES('HEAD', 'refs/heads/x');
**   UPDATE git_refs SET target = '<oid-hex>' WHERE name = 'refs/heads/x';
**   DELETE FROM git_refs WHERE name = 'refs/heads/x';
**
** Columns: name TEXT, type TEXT (derived), target TEXT (oid hex), symref TEXT.
**
** Every read and write goes through the storage-backed repo's libgit2 refdb
** (git_reference_iterator / git_reference_lookup / git_reference_create /
** git_reference_symbolic_create / git_reference_remove), the same refdb the
** git_* scalars and TVFs use, so there is one ref store and no direct storage
** SQL. The libgit2 write path also enforces compare-and-swap and records the
** reflog through the backend, exactly as the files backend would.
**
** SQLite identifies an UPDATE/DELETE row by the integer rowid xRowid hands back.
** refs are name-keyed, so a per-scan rowid->refname map (del_keys, rebuilt each
** xFilter) translates that rowid to the name git_reference_remove/_lookup needs.
** This assumes one writable-vtab statement scans at a time, which is how
** DELETE/UPDATE drive it.
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
#include <stdlib.h>
#include <string.h>

typedef struct {
  sqlite3_vtab base;
  char **del_keys;	/* rowid -> refname for the in-flight scan */
  int del_n, del_alloc;
} refs_vtab;

struct ref_row { char *name; char *target; char *symref; };

typedef struct {
  sqlite3_vtab_cursor base;
  struct ref_row *rows;
  int nr, alloc, pos;
} refs_cursor;

static int refs_connect(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                        sqlite3_vtab **ppVtab, char **pzErr) {
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(name TEXT, type TEXT, target TEXT, symref TEXT)");
  if (rc != SQLITE_OK) return rc;
  refs_vtab *vt = sqlite3_malloc(sizeof(*vt));
  if (!vt) return SQLITE_NOMEM;
  memset(vt, 0, sizeof(*vt));
  *ppVtab = &vt->base;
  return SQLITE_OK;
}

static void refs_del_keys_clear(refs_vtab *vt) {
  for (int i = 0; i < vt->del_n; i++) free(vt->del_keys[i]);
  vt->del_n = 0;
}

static int refs_disconnect(sqlite3_vtab *pVtab) {
  refs_vtab *vt = (refs_vtab *)pVtab;
  refs_del_keys_clear(vt);
  free(vt->del_keys);
  sqlite3_free(vt);
  return SQLITE_OK;
}

static int refs_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  (void)pVtab;
  refs_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static void refs_rows_free(refs_cursor *cur) {
  for (int i = 0; i < cur->nr; i++) {
    free(cur->rows[i].name);
    free(cur->rows[i].target);
    free(cur->rows[i].symref);
  }
  cur->nr = 0;
}

static int refs_close(sqlite3_vtab_cursor *pCursor) {
  refs_cursor *cur = (refs_cursor *)pCursor;
  refs_rows_free(cur);
  free(cur->rows);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int refs_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *info) {
  (void)pVtab;
  for (int i = 0; i < info->nConstraint; i++) {
    if (info->aConstraint[i].usable &&
        info->aConstraint[i].iColumn == 0 &&	/* name */
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

/* Collect one libgit2 git_reference into the cursor's row array. A direct ref
 * carries the target oid (symref NULL); a symbolic ref carries the symref
 * target (target NULL). */
static int refs_collect(refs_cursor *cur, const git_reference *ref) {
  if (cur->nr >= cur->alloc) {
    int na = cur->alloc ? cur->alloc * 2 : 16;
    struct ref_row *p = realloc(cur->rows, (size_t)na * sizeof(*p));
    if (!p) return -1;
    cur->rows = p;
    cur->alloc = na;
  }
  struct ref_row *r = &cur->rows[cur->nr];
  r->name = strdup(git_reference_name(ref));
  r->target = NULL;
  r->symref = NULL;
  if (!r->name) return -1;
  if (git_reference_type(ref) == GIT_REFERENCE_SYMBOLIC) {
    const char *sym = git_reference_symbolic_target(ref);
    if (sym) r->symref = strdup(sym);
  } else {
    const git_oid *oid = git_reference_target(ref);
    if (oid) {
      char hex[GIT_OID_MAX_HEXSIZE + 1];
      git_oid_tostr(hex, sizeof(hex), oid);
      r->target = strdup(hex);
    }
  }
  cur->nr++;
  return 0;
}

static int refs_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                       int argc, sqlite3_value **argv) {
  (void)idxStr;
  refs_cursor *cur = (refs_cursor *)pCursor;
  refs_vtab *vt = (refs_vtab *)pCursor->pVtab;
  refs_rows_free(cur);
  cur->pos = 0;

  git_repository *repo = git0_storage_repo();
  if (repo) {
    if (idxNum == 1 && argc > 0) {
      const char *name = (const char *)sqlite3_value_text(argv[0]);
      git_reference *ref = NULL;
      if (name && git_reference_lookup(&ref, repo, name) == 0) {
        refs_collect(cur, ref);
        git_reference_free(ref);
      }
    } else {
      git_reference_iterator *it = NULL;
      if (git_reference_iterator_new(&it, repo) == 0) {
        git_reference *ref;
        while (git_reference_next(&ref, it) == 0) {
          refs_collect(cur, ref);
          git_reference_free(ref);
        }
        git_reference_iterator_free(it);
      }
    }
  }

  /* Rebuild the rowid->refname map for this scan so a following DELETE/UPDATE
   * resolves the rowid SQLite returns back to a refname. */
  refs_del_keys_clear(vt);
  if (cur->nr > vt->del_alloc) {
    char **p = realloc(vt->del_keys, (size_t)cur->nr * sizeof(*p));
    if (!p) return SQLITE_NOMEM;
    vt->del_keys = p;
    vt->del_alloc = cur->nr;
  }
  for (int i = 0; i < cur->nr; i++) {
    vt->del_keys[i] = strdup(cur->rows[i].name);
    if (!vt->del_keys[i]) { vt->del_n = i; return SQLITE_NOMEM; }
  }
  vt->del_n = cur->nr;
  return SQLITE_OK;
}

static int refs_next(sqlite3_vtab_cursor *pCursor) {
  ((refs_cursor *)pCursor)->pos++;
  return SQLITE_OK;
}

static int refs_eof(sqlite3_vtab_cursor *pCursor) {
  refs_cursor *cur = (refs_cursor *)pCursor;
  return cur->pos >= cur->nr;
}

static int refs_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  refs_cursor *cur = (refs_cursor *)pCursor;
  struct ref_row *r = &cur->rows[cur->pos];
  switch (col) {
  case 0:
    sqlite3_result_text(ctx, r->name, -1, SQLITE_TRANSIENT);
    break;
  case 1: {	/* type, derived from the refname */
    const char *t = "other";
    if (!strncmp(r->name, "refs/heads/", 11)) t = "branch";
    else if (!strncmp(r->name, "refs/tags/", 10)) t = "tag";
    else if (!strncmp(r->name, "refs/remotes/", 13)) t = "remote";
    else if (!strcmp(r->name, "HEAD")) t = "head";
    sqlite3_result_text(ctx, t, -1, SQLITE_STATIC);
    break;
  }
  case 2:
    if (r->target) sqlite3_result_text(ctx, r->target, -1, SQLITE_TRANSIENT);
    else sqlite3_result_null(ctx);
    break;
  case 3:
    if (r->symref) sqlite3_result_text(ctx, r->symref, -1, SQLITE_TRANSIENT);
    else sqlite3_result_null(ctx);
    break;
  default:
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int refs_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((refs_cursor *)pCursor)->pos;	/* index into del_keys for this scan */
  return SQLITE_OK;
}

static int refs_update(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv,
                       sqlite3_int64 *pRowid) {
  refs_vtab *vt = (refs_vtab *)pVtab;
  (void)pRowid;
  git_repository *repo = git0_storage_repo();
  if (!repo) {
    pVtab->zErrMsg = sqlite3_mprintf("git_refs: storage repo unavailable");
    return SQLITE_ERROR;
  }

  /* DELETE: argv[0] is the scan rowid -> refname -> git_reference_remove. */
  if (argc == 1) {
    sqlite3_int64 rid = sqlite3_value_int64(argv[0]);
    if (rid < 0 || rid >= vt->del_n) return SQLITE_OK;	/* stale, nothing to do */
    if (git_reference_remove(repo, vt->del_keys[rid]) != 0) {
      const git_error *e = git_error_last();
      pVtab->zErrMsg = sqlite3_mprintf("git_refs: %s", e ? e->message : "delete failed");
      return SQLITE_ERROR;
    }
    return SQLITE_OK;
  }

  /* INSERT / UPDATE share the column layout argv[2..]: name, type, target,
   * symref. A direct ref carries target (hex) + NULL symref; a symbolic ref
   * carries symref + NULL target. force=1 keeps an upsert. */
  const char *name = (const char *)sqlite3_value_text(argv[2]);
  if (!name || !*name) {
    pVtab->zErrMsg = sqlite3_mprintf("git_refs: a ref name is required");
    return SQLITE_ERROR;
  }
  const char *target = sqlite3_value_type(argv[4]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[4]) : NULL;
  const char *symref = sqlite3_value_type(argv[5]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[5]) : NULL;

  /* An UPDATE that renames (the new name differs from the scan rowid's name)
   * removes the old name first, so the row moves rather than duplicating. */
  if (sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    sqlite3_int64 rid = sqlite3_value_int64(argv[0]);
    if (rid >= 0 && rid < vt->del_n && strcmp(vt->del_keys[rid], name) != 0)
      git_reference_remove(repo, vt->del_keys[rid]);
  }

  git_reference *ref = NULL;
  int rc;
  if (symref && *symref) {
    rc = git_reference_symbolic_create(&ref, repo, name, symref, 1, NULL);
  } else if (target && *target) {
    git_oid oid;
    if (oid_from_hex(&oid, target) != 0) {
      pVtab->zErrMsg = sqlite3_mprintf("git_refs: invalid target oid '%s'", target);
      return SQLITE_ERROR;
    }
    rc = git_reference_create(&ref, repo, name, &oid, 1, NULL);
  } else {
    pVtab->zErrMsg = sqlite3_mprintf("git_refs: a target oid or symref is required");
    return SQLITE_ERROR;
  }
  if (rc != 0) {
    const git_error *e = git_error_last();
    pVtab->zErrMsg = sqlite3_mprintf("git_refs: %s", e ? e->message : "write failed");
    return SQLITE_ERROR;
  }
  git_reference_free(ref);
  return SQLITE_OK;
}

static sqlite3_module git0_refs_module = {
  .iVersion = 1,
  .xCreate = refs_connect,
  .xConnect = refs_connect,
  .xBestIndex = refs_bestindex,
  .xDisconnect = refs_disconnect,
  .xDestroy = refs_disconnect,
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
  return sqlite3_create_module(db, "git0_refs", &git0_refs_module, 0);
}
