/*
** Table-valued functions for git0.
**
** git_log(repo, from?, path?)        -> oid, message, author_name, author_email, author_when, committer_name, committer_email, committer_when, parents
** git_tree(repo, rev, path?)         -> name, mode, type, oid, size
** git_diff(repo, old_rev, new_rev)   -> status, path, old_oid, new_oid, old_mode, new_mode
** git_diff_stat(repo, old_rev, new_rev) -> path, additions, deletions
** git_refs(repo, pattern?)           -> name, type, oid
** git_status(repo)                   -> path, status, head_to_index, index_to_workdir
** git_blame(repo, path, rev?)        -> line_start, line_count, oid, orig_path, author_name, author_email, author_when
** git_index(repo)                    -> path, mode, oid, stage, flags
** git_config_list(repo)              -> key, value
** git_ancestors(repo, oid)           -> oid
**
** Each TVF uses the eponymous virtual table pattern.
*/

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

#include <git2.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Shared repo cache (defined in git0.c, declared here) */
extern git_repository *g_repo;
extern char g_repo_path[4096];

static git_repository *get_repo(const char *path, char **err) {
  if (g_repo && strcmp(g_repo_path, path) == 0) return g_repo;
  if (g_repo) { git_repository_free(g_repo); g_repo = NULL; }
  int rc = git_repository_open(&g_repo, path);
  if (rc != 0) { *err = (char *)git_error_last()->message; return NULL; }
  strncpy(g_repo_path, path, sizeof(g_repo_path) - 1);
  return g_repo;
}

static void oid_to_hex(const git_oid *oid, char *out) {
  git_oid_tostr(out, GIT_OID_MAX_HEXSIZE + 1, oid);
}

/*
** ========================================================
** git_log(repo, from?, path?) TVF
** ========================================================
*/

typedef struct {
  sqlite3_vtab base;
} git_log_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_repository *repo;
  git_revwalk *walk;
  git_pathspec *pathspec;
  git_commit *current;
  int eof;
  int rowid;
} git_log_cursor;

static int git_log_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                           sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(oid TEXT, message TEXT, author_name TEXT, author_email TEXT, "
    "author_when TEXT, committer_name TEXT, committer_email TEXT, committer_when TEXT, "
    "parents INT, repo TEXT HIDDEN, rev TEXT HIDDEN, path TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_log_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_log_disconnect(sqlite3_vtab *pVtab) {
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int git_log_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_log_cursor *cur = sqlite3_malloc(sizeof(*cur));
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_log_close(sqlite3_vtab_cursor *pCursor) {
  git_log_cursor *cur = (git_log_cursor *)pCursor;
  if (cur->current) git_commit_free(cur->current);
  if (cur->walk) git_revwalk_free(cur->walk);
  if (cur->pathspec) git_pathspec_free(cur->pathspec);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_log_advance(git_log_cursor *cur) {
  if (cur->current) { git_commit_free(cur->current); cur->current = NULL; }
  git_oid oid;
  while (git_revwalk_next(&oid, cur->walk) == 0) {
    git_commit *c = NULL;
    if (git_commit_lookup(&c, cur->repo, &oid) != 0) continue;

    /* Path filter: check if commit touches the path */
    if (cur->pathspec) {
      git_tree *tree = NULL, *parent_tree = NULL;
      git_commit_tree(&tree, c);
      if (git_commit_parentcount(c) > 0) {
        git_commit *parent = NULL;
        git_commit_parent(&parent, c, 0);
        git_commit_tree(&parent_tree, parent);
        git_commit_free(parent);
      }
      git_diff *diff = NULL;
      git_diff_tree_to_tree(&diff, cur->repo, parent_tree, tree, NULL);
      if (parent_tree) git_tree_free(parent_tree);
      git_tree_free(tree);

      int dominated = 0;
      if (diff) {
        size_t ndeltas = git_diff_num_deltas(diff);
        for (size_t i = 0; i < ndeltas; i++) {
          const git_diff_delta *delta = git_diff_get_delta(diff, i);
          if (git_pathspec_matches_path(cur->pathspec, 0, delta->new_file.path) ||
              git_pathspec_matches_path(cur->pathspec, 0, delta->old_file.path)) {
            dominated = 1;
            break;
          }
        }
        git_diff_free(diff);
      } else {
        dominated = 1; /* Root commit with pathspec: include */
      }
      if (!dominated) { git_commit_free(c); continue; }
    }

    cur->current = c;
    cur->rowid++;
    return 0;
  }
  cur->eof = 1;
  return 0;
}

static int git_log_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                          int argc, sqlite3_value **argv) {
  git_log_cursor *cur = (git_log_cursor *)pCursor;
  if (cur->walk) git_revwalk_free(cur->walk);
  if (cur->pathspec) { git_pathspec_free(cur->pathspec); cur->pathspec = NULL; }
  cur->eof = 0;
  cur->rowid = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  const char *rev = argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[1]) : "HEAD";
  const char *path = argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[2]) : NULL;

  char *err = NULL;
  cur->repo = get_repo(repo_path, &err);
  if (!cur->repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  git_revwalk_new(&cur->walk, cur->repo);
  git_revwalk_sorting(cur->walk, GIT_SORT_TIME);

  git_object *obj = NULL;
  if (git_revparse_single(&obj, cur->repo, rev) == 0) {
    git_revwalk_push(cur->walk, git_object_id(obj));
    git_object_free(obj);
  } else {
    git_revwalk_push_head(cur->walk);
  }

  if (path) {
    git_strarray arr = { (char **)&path, 1 };
    git_pathspec_new(&cur->pathspec, &arr);
  }

  return git_log_advance(cur);
}

static int git_log_next(sqlite3_vtab_cursor *pCursor) {
  return git_log_advance((git_log_cursor *)pCursor);
}

static int git_log_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git_log_cursor *)pCursor)->eof;
}

static int git_log_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_log_cursor *cur = (git_log_cursor *)pCursor;
  git_commit *c = cur->current;
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  char timebuf[64];
  const git_signature *sig;
  struct tm tm;
  time_t t;

  switch (col) {
    case 0: /* oid */
      oid_to_hex(git_commit_id(c), hex);
      sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
      break;
    case 1: /* message */
      sqlite3_result_text(ctx, git_commit_message(c), -1, SQLITE_TRANSIENT);
      break;
    case 2: /* author_name */
      sqlite3_result_text(ctx, git_commit_author(c)->name, -1, SQLITE_TRANSIENT);
      break;
    case 3: /* author_email */
      sqlite3_result_text(ctx, git_commit_author(c)->email, -1, SQLITE_TRANSIENT);
      break;
    case 4: /* author_when */
      sig = git_commit_author(c);
      t = (time_t)sig->when.time;
      gmtime_r(&t, &tm);
      strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", &tm);
      sqlite3_result_text(ctx, timebuf, -1, SQLITE_TRANSIENT);
      break;
    case 5: /* committer_name */
      sqlite3_result_text(ctx, git_commit_committer(c)->name, -1, SQLITE_TRANSIENT);
      break;
    case 6: /* committer_email */
      sqlite3_result_text(ctx, git_commit_committer(c)->email, -1, SQLITE_TRANSIENT);
      break;
    case 7: /* committer_when */
      sig = git_commit_committer(c);
      t = (time_t)sig->when.time;
      gmtime_r(&t, &tm);
      strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", &tm);
      sqlite3_result_text(ctx, timebuf, -1, SQLITE_TRANSIENT);
      break;
    case 8: /* parents */
      sqlite3_result_int(ctx, (int)git_commit_parentcount(c));
      break;
    default:
      sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_log_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git_log_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

static int git_log_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    int col = pInfo->aConstraint[i].iColumn;
    if (col == 9 || col == 10 || col == 11) { /* repo, rev, path */
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
    }
  }
  pInfo->estimatedCost = 1000;
  return SQLITE_OK;
}

static sqlite3_module git_log_module = {
  0,                    /* iVersion */
  0,                    /* xCreate */
  git_log_connect,      /* xConnect */
  git_log_bestindex,    /* xBestIndex */
  git_log_disconnect,   /* xDisconnect */
  0,                    /* xDestroy */
  git_log_open,         /* xOpen */
  git_log_close,        /* xClose */
  git_log_filter,       /* xFilter */
  git_log_next,         /* xNext */
  git_log_eof,          /* xEof */
  git_log_column,       /* xColumn */
  git_log_rowid,        /* xRowid */
};

/*
** ========================================================
** git_tree(repo, rev, path?) TVF
** ========================================================
*/

typedef struct { sqlite3_vtab base; } git_tree_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_repository *repo;
  git_tree *tree;
  size_t count;
  size_t idx;
} git_tree_cursor;

static int git_tree_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                            sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(name TEXT, mode INT, type TEXT, oid TEXT, size INT, "
    "repo TEXT HIDDEN, rev TEXT HIDDEN, path TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_tree_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_tree_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_tree_cursor *cur = sqlite3_malloc(sizeof(*cur));
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_tree_close(sqlite3_vtab_cursor *pCursor) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  if (cur->tree) git_tree_free(cur->tree);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_tree_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                           int argc, sqlite3_value **argv) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  if (cur->tree) { git_tree_free(cur->tree); cur->tree = NULL; }
  cur->idx = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  const char *rev = argc > 1 ? (const char *)sqlite3_value_text(argv[1]) : "HEAD";
  const char *path = argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[2]) : NULL;

  char *err = NULL;
  cur->repo = get_repo(repo_path, &err);
  if (!cur->repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  git_object *obj = NULL;
  git_revparse_single(&obj, cur->repo, rev);
  if (!obj) return SQLITE_OK;
  git_commit *commit = NULL;
  git_object_peel((git_object **)&commit, obj, GIT_OBJECT_COMMIT);
  git_object_free(obj);
  if (!commit) return SQLITE_OK;

  git_tree *root = NULL;
  git_commit_tree(&root, commit);
  git_commit_free(commit);

  if (path) {
    git_tree_entry *entry = NULL;
    if (git_tree_entry_bypath(&entry, root, path) == 0) {
      if (git_tree_entry_type(entry) == GIT_OBJECT_TREE) {
        git_tree_lookup(&cur->tree, cur->repo, git_tree_entry_id(entry));
      }
      git_tree_entry_free(entry);
    }
    git_tree_free(root);
  } else {
    cur->tree = root;
  }

  cur->count = cur->tree ? git_tree_entrycount(cur->tree) : 0;
  return SQLITE_OK;
}

static int git_tree_next(sqlite3_vtab_cursor *pCursor) {
  ((git_tree_cursor *)pCursor)->idx++;
  return SQLITE_OK;
}

static int git_tree_eof(sqlite3_vtab_cursor *pCursor) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  return cur->idx >= cur->count;
}

static int git_tree_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  const git_tree_entry *entry = git_tree_entry_byindex(cur->tree, cur->idx);
  char hex[GIT_OID_MAX_HEXSIZE + 1];

  switch (col) {
    case 0: /* name */
      sqlite3_result_text(ctx, git_tree_entry_name(entry), -1, SQLITE_TRANSIENT);
      break;
    case 1: /* mode */
      sqlite3_result_int(ctx, (int)git_tree_entry_filemode(entry));
      break;
    case 2: /* type */
      sqlite3_result_text(ctx, git_object_type2string(git_tree_entry_type(entry)), -1, SQLITE_STATIC);
      break;
    case 3: /* oid */
      oid_to_hex(git_tree_entry_id(entry), hex);
      sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
      break;
    case 4: /* size */
      if (git_tree_entry_type(entry) == GIT_OBJECT_BLOB) {
        git_blob *blob = NULL;
        if (git_blob_lookup(&blob, cur->repo, git_tree_entry_id(entry)) == 0) {
          sqlite3_result_int64(ctx, (sqlite3_int64)git_blob_rawsize(blob));
          git_blob_free(blob);
        } else {
          sqlite3_result_null(ctx);
        }
      } else {
        sqlite3_result_null(ctx);
      }
      break;
    default:
      sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_tree_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = (sqlite3_int64)((git_tree_cursor *)pCursor)->idx;
  return SQLITE_OK;
}

static int git_tree_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    int col = pInfo->aConstraint[i].iColumn;
    if (col == 5 || col == 6 || col == 7) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
    }
  }
  pInfo->estimatedCost = 100;
  return SQLITE_OK;
}

static sqlite3_module git_tree_module = {
  0, 0, git_tree_connect, git_tree_bestindex, git_log_disconnect, 0,
  git_tree_open, git_tree_close, git_tree_filter, git_tree_next, git_tree_eof,
  git_tree_column, git_tree_rowid,
};

/*
** ========================================================
** git_refs(repo, pattern?) TVF
** ========================================================
*/

typedef struct { sqlite3_vtab base; } git_refs_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_repository *repo;
  git_reference_iterator *iter;
  git_reference *current;
  int eof;
  int rowid;
} git_refs_cursor;

static int git_refs_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                            sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(name TEXT, type TEXT, oid TEXT, "
    "repo TEXT HIDDEN, pattern TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_refs_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_refs_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_refs_cursor *cur = sqlite3_malloc(sizeof(*cur));
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_refs_close(sqlite3_vtab_cursor *pCursor) {
  git_refs_cursor *cur = (git_refs_cursor *)pCursor;
  if (cur->current) git_reference_free(cur->current);
  if (cur->iter) git_reference_iterator_free(cur->iter);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_refs_advance(git_refs_cursor *cur) {
  if (cur->current) { git_reference_free(cur->current); cur->current = NULL; }
  if (git_reference_next(&cur->current, cur->iter) != 0) {
    cur->eof = 1;
  } else {
    cur->rowid++;
  }
  return SQLITE_OK;
}

static int git_refs_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                           int argc, sqlite3_value **argv) {
  git_refs_cursor *cur = (git_refs_cursor *)pCursor;
  if (cur->iter) { git_reference_iterator_free(cur->iter); cur->iter = NULL; }
  cur->eof = 0; cur->rowid = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  const char *pattern = argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[1]) : NULL;

  char *err = NULL;
  cur->repo = get_repo(repo_path, &err);
  if (!cur->repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  if (pattern) {
    git_reference_iterator_glob_new(&cur->iter, cur->repo, pattern);
  } else {
    git_reference_iterator_new(&cur->iter, cur->repo);
  }

  return git_refs_advance(cur);
}

static int git_refs_next(sqlite3_vtab_cursor *pCursor) {
  return git_refs_advance((git_refs_cursor *)pCursor);
}

static int git_refs_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git_refs_cursor *)pCursor)->eof;
}

static int git_refs_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_refs_cursor *cur = (git_refs_cursor *)pCursor;
  git_reference *ref = cur->current;
  char hex[GIT_OID_MAX_HEXSIZE + 1];

  switch (col) {
    case 0: /* name */
      sqlite3_result_text(ctx, git_reference_shorthand(ref), -1, SQLITE_TRANSIENT);
      break;
    case 1: /* type */
      if (git_reference_is_branch(ref)) sqlite3_result_text(ctx, "branch", -1, SQLITE_STATIC);
      else if (git_reference_is_tag(ref)) sqlite3_result_text(ctx, "tag", -1, SQLITE_STATIC);
      else if (git_reference_is_remote(ref)) sqlite3_result_text(ctx, "remote", -1, SQLITE_STATIC);
      else sqlite3_result_text(ctx, "other", -1, SQLITE_STATIC);
      break;
    case 2: { /* oid */
      git_reference *resolved = NULL;
      if (git_reference_resolve(&resolved, ref) == 0) {
        oid_to_hex(git_reference_target(resolved), hex);
        git_reference_free(resolved);
      } else if (git_reference_target(ref)) {
        oid_to_hex(git_reference_target(ref), hex);
      } else {
        sqlite3_result_null(ctx);
        break;
      }
      sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
      break;
    }
    default:
      sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_refs_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git_refs_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

static int git_refs_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    int col = pInfo->aConstraint[i].iColumn;
    if (col == 3 || col == 4) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
    }
  }
  pInfo->estimatedCost = 100;
  return SQLITE_OK;
}

static sqlite3_module git_refs_module = {
  0, 0, git_refs_connect, git_refs_bestindex, git_log_disconnect, 0,
  git_refs_open, git_refs_close, git_refs_filter, git_refs_next, git_refs_eof,
  git_refs_column, git_refs_rowid,
};

/*
** ========================================================
** Register all TVFs
** ========================================================
*/

int git0_register_vtabs(sqlite3 *db) {
  sqlite3_create_module(db, "git_log", &git_log_module, 0);
  sqlite3_create_module(db, "git_tree", &git_tree_module, 0);
  sqlite3_create_module(db, "git_refs", &git_refs_module, 0);
  return SQLITE_OK;
}
