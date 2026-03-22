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

/* Shared functions (defined in git0.c) */
extern git_repository *git0_repo_open(const char *path, char **err);

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
  if (!vtab) return SQLITE_NOMEM;
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
  if (!cur) return SQLITE_NOMEM;
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
  cur->repo = git0_repo_open(repo_path, &err);
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
      git_oid_tostr(hex, GIT_OID_MAX_HEXSIZE + 1, git_commit_id(c));
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
      pInfo->aConstraintUsage[i].omit = 1;
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

#define TREE_DEPTH_MAX 32

typedef struct {
  sqlite3_vtab_cursor base;
  git_repository *repo;
  git_tree *tree;
  size_t count;
  size_t idx;
  int recursive;
  int depth;
  struct { git_tree *tree; size_t idx; } stack[TREE_DEPTH_MAX];
} git_tree_cursor;

static int git_tree_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                            sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(name TEXT, mode INT, type TEXT, oid TEXT, size INT, "
    "repo TEXT HIDDEN, rev TEXT HIDDEN, path TEXT HIDDEN, recursive INT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_tree_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_tree_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_tree_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_tree_close(sqlite3_vtab_cursor *pCursor) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  if (cur->tree) git_tree_free(cur->tree);
  for (int i = 0; i < cur->depth; i++)
    git_tree_free(cur->stack[i].tree);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/* Descend into subtree at current position */
static int tree_push(git_tree_cursor *cur) {
  const git_tree_entry *entry = git_tree_entry_byindex(cur->tree, cur->idx);
  if (!entry || git_tree_entry_type(entry) != GIT_OBJECT_TREE) return 0;
  if (cur->depth >= TREE_DEPTH_MAX) return 0;
  git_tree *sub = NULL;
  if (git_tree_lookup(&sub, cur->repo, git_tree_entry_id(entry)) != 0) return 0;
  cur->stack[cur->depth].tree = cur->tree;
  cur->stack[cur->depth].idx = cur->idx;
  cur->depth++;
  cur->tree = sub;
  cur->count = git_tree_entrycount(sub);
  cur->idx = 0;
  return 1;
}

/* Advance cursor, descending into trees and popping exhausted levels */
static void tree_advance(git_tree_cursor *cur) {
  cur->idx++;
  while (cur->idx >= cur->count && cur->depth > 0) {
    git_tree_free(cur->tree);
    cur->depth--;
    cur->tree = cur->stack[cur->depth].tree;
    cur->count = git_tree_entrycount(cur->tree);
    cur->idx = cur->stack[cur->depth].idx + 1;
  }
  /* Descend into tree entries */
  while (cur->idx < cur->count) {
    const git_tree_entry *e = git_tree_entry_byindex(cur->tree, cur->idx);
    if (e && git_tree_entry_type(e) == GIT_OBJECT_TREE && tree_push(cur))
      continue;
    break;
  }
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
  cur->recursive = argc > 3 && sqlite3_value_int(argv[3]);
  cur->depth = 0;

  char *err = NULL;
  cur->repo = git0_repo_open(repo_path, &err);
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

  if (!cur->tree) { cur->count = 0; return SQLITE_OK; }
  cur->count = git_tree_entrycount(cur->tree);

  /* In recursive mode, descend into initial tree entries */
  if (cur->recursive && cur->count > 0) {
    const git_tree_entry *e = git_tree_entry_byindex(cur->tree, 0);
    if (e && git_tree_entry_type(e) == GIT_OBJECT_TREE)
      while (cur->idx < cur->count) {
        e = git_tree_entry_byindex(cur->tree, cur->idx);
        if (e && git_tree_entry_type(e) == GIT_OBJECT_TREE && tree_push(cur))
          continue;
        break;
      }
  }
  return SQLITE_OK;
}

static int git_tree_next(sqlite3_vtab_cursor *pCursor) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  if (cur->recursive)
    tree_advance(cur);
  else
    cur->idx++;
  return SQLITE_OK;
}

static int git_tree_eof(sqlite3_vtab_cursor *pCursor) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  return cur->idx >= cur->count;
}

static int git_tree_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_tree_cursor *cur = (git_tree_cursor *)pCursor;
  char hex[GIT_OID_MAX_HEXSIZE + 1];

  const git_tree_entry *entry = git_tree_entry_byindex(cur->tree, cur->idx);
  switch (col) {
    case 0: /* name */
      if (cur->recursive && cur->depth > 0) {
        char buf[4096];
        int pos = 0;
        for (int i = 0; i < cur->depth; i++) {
          const git_tree_entry *se = git_tree_entry_byindex(
            cur->stack[i].tree, cur->stack[i].idx);
          if (se) pos += snprintf(buf + pos, sizeof(buf) - pos,
            "%s%s", pos ? "/" : "", git_tree_entry_name(se));
        }
        snprintf(buf + pos, sizeof(buf) - pos, "%s%s",
          pos ? "/" : "", git_tree_entry_name(entry));
        sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
      } else {
        sqlite3_result_text(ctx, git_tree_entry_name(entry), -1, SQLITE_TRANSIENT);
      }
      break;
    case 1: /* mode */
      sqlite3_result_int(ctx, (int)git_tree_entry_filemode(entry));
      break;
    case 2: /* type */
      sqlite3_result_text(ctx, git_object_type2string(git_tree_entry_type(entry)), -1, SQLITE_STATIC);
      break;
    case 3: /* oid */
      git_oid_tostr(hex, sizeof(hex), git_tree_entry_id(entry));
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
    if (col == 5 || col == 6 || col == 7 || col == 8) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
      pInfo->aConstraintUsage[i].omit = 1;
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
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_refs_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_refs_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
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
  cur->repo = git0_repo_open(repo_path, &err);
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
        git_oid_tostr(hex, GIT_OID_MAX_HEXSIZE + 1, git_reference_target(resolved));
        git_reference_free(resolved);
      } else if (git_reference_target(ref)) {
        git_oid_tostr(hex, GIT_OID_MAX_HEXSIZE + 1, git_reference_target(ref));
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
      pInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pInfo->estimatedCost = 100;
  return SQLITE_OK;
}

static sqlite3_module git_refs_module = {
  .iVersion = 0,
  .xConnect = git_refs_connect,
  .xBestIndex = git_refs_bestindex,
  .xDisconnect = git_log_disconnect,
  .xOpen = git_refs_open,
  .xClose = git_refs_close,
  .xFilter = git_refs_filter,
  .xNext = git_refs_next,
  .xEof = git_refs_eof,
  .xColumn = git_refs_column,
  .xRowid = git_refs_rowid,
};

/*
** ========================================================
** git_diff(repo, old_rev, new_rev) TVF
** ========================================================
*/

typedef struct { sqlite3_vtab base; } git_diff_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_diff *diff;
  size_t count;
  size_t idx;
} git_diff_cursor;

static int git_diff_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                            sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(status TEXT, path TEXT, old_oid TEXT, new_oid TEXT, "
    "old_mode INT, new_mode INT, "
    "repo TEXT HIDDEN, old_rev TEXT HIDDEN, new_rev TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_diff_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_diff_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_diff_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_diff_close(sqlite3_vtab_cursor *pCursor) {
  git_diff_cursor *cur = (git_diff_cursor *)pCursor;
  if (cur->diff) git_diff_free(cur->diff);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_diff_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                           int argc, sqlite3_value **argv) {
  git_diff_cursor *cur = (git_diff_cursor *)pCursor;
  if (cur->diff) { git_diff_free(cur->diff); cur->diff = NULL; }
  cur->idx = 0; cur->count = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  const char *old_rev = argc > 1 ? (const char *)sqlite3_value_text(argv[1]) : "HEAD~1";
  const char *new_rev = argc > 2 ? (const char *)sqlite3_value_text(argv[2]) : "HEAD";

  char *err = NULL;
  git_repository *repo = git0_repo_open(repo_path, &err);
  if (!repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  git_object *old_obj = NULL, *new_obj = NULL;
  git_revparse_single(&old_obj, repo, old_rev);
  git_revparse_single(&new_obj, repo, new_rev);
  if (!old_obj || !new_obj) {
    if (old_obj) git_object_free(old_obj);
    if (new_obj) git_object_free(new_obj);
    return SQLITE_OK;
  }

  git_tree *old_tree = NULL, *new_tree = NULL;
  git_commit *c1 = NULL, *c2 = NULL;
  git_object_peel((git_object **)&c1, old_obj, GIT_OBJECT_COMMIT);
  git_object_peel((git_object **)&c2, new_obj, GIT_OBJECT_COMMIT);
  git_object_free(old_obj);
  git_object_free(new_obj);
  if (c1) git_commit_tree(&old_tree, c1);
  if (c2) git_commit_tree(&new_tree, c2);
  if (c1) git_commit_free(c1);
  if (c2) git_commit_free(c2);

  if (old_tree && new_tree) {
    git_diff_tree_to_tree(&cur->diff, repo, old_tree, new_tree, NULL);
    if (cur->diff) {
      git_diff_find_similar(cur->diff, NULL);
      cur->count = git_diff_num_deltas(cur->diff);
    }
  }
  if (old_tree) git_tree_free(old_tree);
  if (new_tree) git_tree_free(new_tree);

  return SQLITE_OK;
}

static int git_diff_next(sqlite3_vtab_cursor *pCursor) {
  ((git_diff_cursor *)pCursor)->idx++;
  return SQLITE_OK;
}

static int git_diff_eof(sqlite3_vtab_cursor *pCursor) {
  git_diff_cursor *cur = (git_diff_cursor *)pCursor;
  return cur->idx >= cur->count;
}

static const char *delta_status_str(git_delta_t s) {
  switch (s) {
    case GIT_DELTA_ADDED: return "A";
    case GIT_DELTA_DELETED: return "D";
    case GIT_DELTA_MODIFIED: return "M";
    case GIT_DELTA_RENAMED: return "R";
    case GIT_DELTA_COPIED: return "C";
    case GIT_DELTA_TYPECHANGE: return "T";
    default: return "?";
  }
}

static int git_diff_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_diff_cursor *cur = (git_diff_cursor *)pCursor;
  const git_diff_delta *delta = git_diff_get_delta(cur->diff, cur->idx);
  char hex[GIT_OID_MAX_HEXSIZE + 1];

  switch (col) {
    case 0: sqlite3_result_text(ctx, delta_status_str(delta->status), -1, SQLITE_STATIC); break;
    case 1: sqlite3_result_text(ctx, delta->new_file.path, -1, SQLITE_TRANSIENT); break;
    case 2: git_oid_tostr(hex, GIT_OID_MAX_HEXSIZE + 1, &delta->old_file.id); sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT); break;
    case 3: git_oid_tostr(hex, GIT_OID_MAX_HEXSIZE + 1, &delta->new_file.id); sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT); break;
    case 4: sqlite3_result_int(ctx, (int)delta->old_file.mode); break;
    case 5: sqlite3_result_int(ctx, (int)delta->new_file.mode); break;
    default: sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_diff_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = (sqlite3_int64)((git_diff_cursor *)pCursor)->idx;
  return SQLITE_OK;
}

static int git_diff_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    int col = pInfo->aConstraint[i].iColumn;
    if (col == 6 || col == 7 || col == 8) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
      pInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pInfo->estimatedCost = 500;
  return SQLITE_OK;
}

static sqlite3_module git_diff_module = {
  .xConnect = git_diff_connect,
  .xBestIndex = git_diff_bestindex,
  .xDisconnect = git_log_disconnect,
  .xOpen = git_diff_open,
  .xClose = git_diff_close,
  .xFilter = git_diff_filter,
  .xNext = git_diff_next,
  .xEof = git_diff_eof,
  .xColumn = git_diff_column,
  .xRowid = git_diff_rowid,
};

/*
** ========================================================
** git_ancestors(repo, oid) TVF  (just oids, fast rev-list)
** ========================================================
*/

typedef struct { sqlite3_vtab base; } git_anc_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_revwalk *walk;
  git_oid current;
  int eof;
  int rowid;
} git_anc_cursor;

static int git_anc_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                           sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(oid TEXT, repo TEXT HIDDEN, rev TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_anc_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_anc_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_anc_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_anc_close(sqlite3_vtab_cursor *pCursor) {
  git_anc_cursor *cur = (git_anc_cursor *)pCursor;
  if (cur->walk) git_revwalk_free(cur->walk);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_anc_advance(git_anc_cursor *cur) {
  if (git_revwalk_next(&cur->current, cur->walk) != 0) {
    cur->eof = 1;
  } else {
    cur->rowid++;
  }
  return SQLITE_OK;
}

static int git_anc_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                          int argc, sqlite3_value **argv) {
  git_anc_cursor *cur = (git_anc_cursor *)pCursor;
  if (cur->walk) { git_revwalk_free(cur->walk); cur->walk = NULL; }
  cur->eof = 0; cur->rowid = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  const char *rev = argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[1]) : "HEAD";

  char *err = NULL;
  git_repository *repo = git0_repo_open(repo_path, &err);
  if (!repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  git_revwalk_new(&cur->walk, repo);
  git_revwalk_sorting(cur->walk, GIT_SORT_TIME);

  git_object *obj = NULL;
  if (git_revparse_single(&obj, repo, rev) == 0) {
    git_revwalk_push(cur->walk, git_object_id(obj));
    git_object_free(obj);
  }

  return git_anc_advance(cur);
}

static int git_anc_next(sqlite3_vtab_cursor *pCursor) {
  return git_anc_advance((git_anc_cursor *)pCursor);
}

static int git_anc_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git_anc_cursor *)pCursor)->eof;
}

static int git_anc_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_anc_cursor *cur = (git_anc_cursor *)pCursor;
  if (col == 0) {
    char hex[GIT_OID_MAX_HEXSIZE + 1];
    git_oid_tostr(hex, GIT_OID_MAX_HEXSIZE + 1, &cur->current);
    sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  } else {
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_anc_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git_anc_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

static int git_anc_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    int col = pInfo->aConstraint[i].iColumn;
    if (col == 1 || col == 2) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
      pInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pInfo->estimatedCost = 1000;
  return SQLITE_OK;
}

static sqlite3_module git_anc_module = {
  .xConnect = git_anc_connect,
  .xBestIndex = git_anc_bestindex,
  .xDisconnect = git_log_disconnect,
  .xOpen = git_anc_open,
  .xClose = git_anc_close,
  .xFilter = git_anc_filter,
  .xNext = git_anc_next,
  .xEof = git_anc_eof,
  .xColumn = git_anc_column,
  .xRowid = git_anc_rowid,
};

/*
** ========================================================
** git_config_list(repo) TVF
** ========================================================
*/

typedef struct { sqlite3_vtab base; } git_cfgl_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_config_iterator *iter;
  git_config_entry *current;
  int eof;
  int rowid;
} git_cfgl_cursor;

static int git_cfgl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                            sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db, "CREATE TABLE x(key TEXT, value TEXT, repo TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_cfgl_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_cfgl_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_cfgl_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_cfgl_close(sqlite3_vtab_cursor *pCursor) {
  git_cfgl_cursor *cur = (git_cfgl_cursor *)pCursor;
  if (cur->iter) git_config_iterator_free(cur->iter);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_cfgl_advance(git_cfgl_cursor *cur) {
  if (git_config_next(&cur->current, cur->iter) != 0) {
    cur->eof = 1;
  } else {
    cur->rowid++;
  }
  return SQLITE_OK;
}

static int git_cfgl_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                           int argc, sqlite3_value **argv) {
  git_cfgl_cursor *cur = (git_cfgl_cursor *)pCursor;
  if (cur->iter) { git_config_iterator_free(cur->iter); cur->iter = NULL; }
  cur->eof = 0; cur->rowid = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  char *err = NULL;
  git_repository *repo = git0_repo_open(repo_path, &err);
  if (!repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  git_config *cfg = NULL;
  git_repository_config(&cfg, repo);
  git_config_iterator_new(&cur->iter, cfg);
  git_config_free(cfg);

  return git_cfgl_advance(cur);
}

static int git_cfgl_next(sqlite3_vtab_cursor *pCursor) {
  return git_cfgl_advance((git_cfgl_cursor *)pCursor);
}

static int git_cfgl_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git_cfgl_cursor *)pCursor)->eof;
}

static int git_cfgl_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_cfgl_cursor *cur = (git_cfgl_cursor *)pCursor;
  switch (col) {
    case 0: sqlite3_result_text(ctx, cur->current->name, -1, SQLITE_TRANSIENT); break;
    case 1: sqlite3_result_text(ctx, cur->current->value, -1, SQLITE_TRANSIENT); break;
    default: sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_cfgl_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = ((git_cfgl_cursor *)pCursor)->rowid;
  return SQLITE_OK;
}

static int git_cfgl_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    if (pInfo->aConstraint[i].iColumn == 2) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
      pInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pInfo->estimatedCost = 100;
  return SQLITE_OK;
}

static sqlite3_module git_cfgl_module = {
  .xConnect = git_cfgl_connect,
  .xBestIndex = git_cfgl_bestindex,
  .xDisconnect = git_log_disconnect,
  .xOpen = git_cfgl_open,
  .xClose = git_cfgl_close,
  .xFilter = git_cfgl_filter,
  .xNext = git_cfgl_next,
  .xEof = git_cfgl_eof,
  .xColumn = git_cfgl_column,
  .xRowid = git_cfgl_rowid,
};

/*
** ========================================================
** git_status(repo) TVF
** ========================================================
*/

typedef struct { sqlite3_vtab base; } git_stat_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_status_list *list;
  size_t count;
  size_t idx;
} git_stat_cursor;

static int git_stat_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                            sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(path TEXT, status TEXT, repo TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_stat_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_stat_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_stat_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_stat_close(sqlite3_vtab_cursor *pCursor) {
  git_stat_cursor *cur = (git_stat_cursor *)pCursor;
  if (cur->list) git_status_list_free(cur->list);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_stat_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                           int argc, sqlite3_value **argv) {
  git_stat_cursor *cur = (git_stat_cursor *)pCursor;
  if (cur->list) { git_status_list_free(cur->list); cur->list = NULL; }
  cur->idx = 0; cur->count = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  char *err = NULL;
  git_repository *repo = git0_repo_open(repo_path, &err);
  if (!repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  git_status_options opts = GIT_STATUS_OPTIONS_INIT;
  opts.show = GIT_STATUS_SHOW_INDEX_AND_WORKDIR;
  opts.flags = GIT_STATUS_OPT_INCLUDE_UNTRACKED | GIT_STATUS_OPT_RENAMES_HEAD_TO_INDEX;
  git_status_list_new(&cur->list, repo, &opts);
  if (cur->list) cur->count = git_status_list_entrycount(cur->list);

  return SQLITE_OK;
}

static int git_stat_next(sqlite3_vtab_cursor *pCursor) {
  ((git_stat_cursor *)pCursor)->idx++;
  return SQLITE_OK;
}

static int git_stat_eof(sqlite3_vtab_cursor *pCursor) {
  git_stat_cursor *cur = (git_stat_cursor *)pCursor;
  return cur->idx >= cur->count;
}

static const char *status_flags_str(unsigned int flags) {
  if (flags & GIT_STATUS_INDEX_NEW) return "new";
  if (flags & GIT_STATUS_INDEX_MODIFIED) return "modified";
  if (flags & GIT_STATUS_INDEX_DELETED) return "deleted";
  if (flags & GIT_STATUS_INDEX_RENAMED) return "renamed";
  if (flags & GIT_STATUS_WT_NEW) return "untracked";
  if (flags & GIT_STATUS_WT_MODIFIED) return "modified";
  if (flags & GIT_STATUS_WT_DELETED) return "deleted";
  if (flags & GIT_STATUS_IGNORED) return "ignored";
  return "clean";
}

static int git_stat_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_stat_cursor *cur = (git_stat_cursor *)pCursor;
  const git_status_entry *entry = git_status_byindex(cur->list, cur->idx);
  switch (col) {
    case 0: {
      const char *path = entry->head_to_index
        ? entry->head_to_index->new_file.path
        : (entry->index_to_workdir ? entry->index_to_workdir->new_file.path : NULL);
      sqlite3_result_text(ctx, path ? path : "", -1, SQLITE_TRANSIENT);
      break;
    }
    case 1: sqlite3_result_text(ctx, status_flags_str(entry->status), -1, SQLITE_STATIC); break;
    default: sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_stat_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = (sqlite3_int64)((git_stat_cursor *)pCursor)->idx;
  return SQLITE_OK;
}

static int git_stat_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    if (pInfo->aConstraint[i].iColumn == 2) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
      pInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pInfo->estimatedCost = 200;
  return SQLITE_OK;
}

static sqlite3_module git_stat_module = {
  .xConnect = git_stat_connect,
  .xBestIndex = git_stat_bestindex,
  .xDisconnect = git_log_disconnect,
  .xOpen = git_stat_open,
  .xClose = git_stat_close,
  .xFilter = git_stat_filter,
  .xNext = git_stat_next,
  .xEof = git_stat_eof,
  .xColumn = git_stat_column,
  .xRowid = git_stat_rowid,
};

/*
** ========================================================
** git_blame(repo, path, rev?) TVF
** ========================================================
*/

typedef struct { sqlite3_vtab base; } git_bl_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_blame *blame;
  uint32_t count;
  uint32_t idx;
} git_bl_cursor;

static int git_bl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                          sqlite3_vtab **ppVtab, char **pzErr) {
  int rc = sqlite3_declare_vtab(db,
    "CREATE TABLE x(line_start INT, line_count INT, oid TEXT, orig_path TEXT, "
    "author_name TEXT, author_email TEXT, author_when TEXT, "
    "repo TEXT HIDDEN, path TEXT HIDDEN, rev TEXT HIDDEN)");
  if (rc != SQLITE_OK) return rc;
  git_bl_vtab *vtab = sqlite3_malloc(sizeof(*vtab));
  if (!vtab) return SQLITE_NOMEM;
  memset(vtab, 0, sizeof(*vtab));
  *ppVtab = &vtab->base;
  return SQLITE_OK;
}

static int git_bl_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  git_bl_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_bl_close(sqlite3_vtab_cursor *pCursor) {
  git_bl_cursor *cur = (git_bl_cursor *)pCursor;
  if (cur->blame) git_blame_free(cur->blame);
  sqlite3_free(cur);
  return SQLITE_OK;
}

static int git_bl_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
                         int argc, sqlite3_value **argv) {
  git_bl_cursor *cur = (git_bl_cursor *)pCursor;
  if (cur->blame) { git_blame_free(cur->blame); cur->blame = NULL; }
  cur->idx = 0; cur->count = 0;

  const char *repo_path = argc > 0 ? (const char *)sqlite3_value_text(argv[0]) : ".";
  const char *path = argc > 1 ? (const char *)sqlite3_value_text(argv[1]) : NULL;
  const char *rev = argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[2]) : NULL;

  if (!path) return SQLITE_OK;

  char *err = NULL;
  git_repository *repo = git0_repo_open(repo_path, &err);
  if (!repo) { pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err); return SQLITE_ERROR; }

  git_blame_options opts = GIT_BLAME_OPTIONS_INIT;
  if (rev) {
    git_object *obj = NULL;
    if (git_revparse_single(&obj, repo, rev) == 0) {
      git_oid_cpy(&opts.newest_commit, git_object_id(obj));
      git_object_free(obj);
    }
  }

  if (git_blame_file(&cur->blame, repo, path, &opts) == 0) {
    cur->count = git_blame_get_hunk_count(cur->blame);
  }

  return SQLITE_OK;
}

static int git_bl_next(sqlite3_vtab_cursor *pCursor) {
  ((git_bl_cursor *)pCursor)->idx++;
  return SQLITE_OK;
}

static int git_bl_eof(sqlite3_vtab_cursor *pCursor) {
  git_bl_cursor *cur = (git_bl_cursor *)pCursor;
  return cur->idx >= cur->count;
}

static int git_bl_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_bl_cursor *cur = (git_bl_cursor *)pCursor;
  const git_blame_hunk *hunk = git_blame_get_hunk_byindex(cur->blame, cur->idx);
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  char timebuf[64];
  time_t t;
  struct tm tm;

  switch (col) {
    case 0: sqlite3_result_int(ctx, (int)hunk->final_start_line_number); break;
    case 1: sqlite3_result_int(ctx, (int)hunk->lines_in_hunk); break;
    case 2: git_oid_tostr(hex, GIT_OID_MAX_HEXSIZE + 1, &hunk->final_commit_id); sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT); break;
    case 3: sqlite3_result_text(ctx, hunk->orig_path, -1, SQLITE_TRANSIENT); break;
    case 4: sqlite3_result_text(ctx, hunk->final_signature ? hunk->final_signature->name : "", -1, SQLITE_TRANSIENT); break;
    case 5: sqlite3_result_text(ctx, hunk->final_signature ? hunk->final_signature->email : "", -1, SQLITE_TRANSIENT); break;
    case 6:
      if (hunk->final_signature) {
        t = (time_t)hunk->final_signature->when.time;
        gmtime_r(&t, &tm);
        strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", &tm);
        sqlite3_result_text(ctx, timebuf, -1, SQLITE_TRANSIENT);
      } else { sqlite3_result_null(ctx); }
      break;
    default: sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

static int git_bl_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = (sqlite3_int64)((git_bl_cursor *)pCursor)->idx;
  return SQLITE_OK;
}

static int git_bl_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    int col = pInfo->aConstraint[i].iColumn;
    if (col == 7 || col == 8 || col == 9) {
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
      pInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pInfo->estimatedCost = 500;
  return SQLITE_OK;
}

static sqlite3_module git_bl_module = {
  .xConnect = git_bl_connect,
  .xBestIndex = git_bl_bestindex,
  .xDisconnect = git_log_disconnect,
  .xOpen = git_bl_open,
  .xClose = git_bl_close,
  .xFilter = git_bl_filter,
  .xNext = git_bl_next,
  .xEof = git_bl_eof,
  .xColumn = git_bl_column,
  .xRowid = git_bl_rowid,
};

/*
** ========================================================
** git_attr(repo, path) TVF
** Returns all attributes for a given path via git_attr_foreach.
** ========================================================
*/

typedef struct {
  sqlite3_vtab base;
} git_attr_vtab;

typedef struct {
  sqlite3_vtab_cursor base;
  git_repository *repo;
  int n;
  int idx;
  char **names;
  char **values;
  int eof;
} git_attr_cursor;

static int git_attr_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                            sqlite3_vtab **ppVtab, char **pzErr) {
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  sqlite3_declare_vtab(db,
    "CREATE TABLE x(name TEXT, value TEXT, "
    "repo TEXT HIDDEN, path TEXT HIDDEN)");
  git_attr_vtab *vt = sqlite3_malloc(sizeof(*vt));
  if (!vt) return SQLITE_NOMEM;
  memset(vt, 0, sizeof(*vt));
  *ppVtab = &vt->base;
  return SQLITE_OK;
}

static int git_attr_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
  (void)pVtab;
  git_attr_cursor *cur = sqlite3_malloc(sizeof(*cur));
  if (!cur) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCursor = &cur->base;
  return SQLITE_OK;
}

static int git_attr_close(sqlite3_vtab_cursor *pCursor) {
  git_attr_cursor *cur = (git_attr_cursor *)pCursor;
  for (int i = 0; i < cur->n; i++) {
    free(cur->names[i]);
    free(cur->values[i]);
  }
  free(cur->names);
  free(cur->values);
  sqlite3_free(cur);
  return SQLITE_OK;
}

struct attr_collect {
  int n, cap;
  char **names;
  char **values;
};

static int attr_foreach_cb(const char *name, const char *value, void *payload) {
  struct attr_collect *ac = (struct attr_collect *)payload;
  git_attr_value_t vtype = git_attr_value(value);
  if (vtype == GIT_ATTR_VALUE_UNSPECIFIED) return 0;
  if (ac->n >= ac->cap) {
    ac->cap = ac->cap ? ac->cap * 2 : 16;
    ac->names = realloc(ac->names, ac->cap * sizeof(char *));
    ac->values = realloc(ac->values, ac->cap * sizeof(char *));
  }
  ac->names[ac->n] = strdup(name);
  if (vtype == GIT_ATTR_VALUE_TRUE)
    ac->values[ac->n] = strdup("true");
  else if (vtype == GIT_ATTR_VALUE_FALSE)
    ac->values[ac->n] = strdup("false");
  else
    ac->values[ac->n] = strdup(value ? value : "");
  ac->n++;
  return 0;
}

static int git_attr_filter(sqlite3_vtab_cursor *pCursor, int idxNum,
                           const char *idxStr, int argc, sqlite3_value **argv) {
  (void)idxNum; (void)idxStr;
  git_attr_cursor *cur = (git_attr_cursor *)pCursor;
  /* Clean up previous iteration */
  for (int i = 0; i < cur->n; i++) {
    free(cur->names[i]);
    free(cur->values[i]);
  }
  free(cur->names);
  free(cur->values);
  cur->names = NULL;
  cur->values = NULL;
  cur->n = 0;
  cur->idx = 0;
  cur->eof = 1;

  if (argc < 2) return SQLITE_OK;
  const char *repo_path = (const char *)sqlite3_value_text(argv[0]);
  const char *path = (const char *)sqlite3_value_text(argv[1]);
  if (!repo_path || !path) return SQLITE_OK;

  char *err = NULL;
  cur->repo = git0_repo_open(repo_path, &err);
  if (!cur->repo) {
    if (err) pCursor->pVtab->zErrMsg = sqlite3_mprintf("%s", err);
    return SQLITE_ERROR;
  }

  struct attr_collect ac = {0, 0, NULL, NULL};
  uint32_t flags = GIT_ATTR_CHECK_INDEX_ONLY | GIT_ATTR_CHECK_INCLUDE_HEAD;
  git_attr_foreach(cur->repo, flags, path, attr_foreach_cb, &ac);
  cur->n = ac.n;
  cur->names = ac.names;
  cur->values = ac.values;
  cur->eof = (ac.n == 0);
  return SQLITE_OK;
}

static int git_attr_next(sqlite3_vtab_cursor *pCursor) {
  git_attr_cursor *cur = (git_attr_cursor *)pCursor;
  cur->idx++;
  if (cur->idx >= cur->n) cur->eof = 1;
  return SQLITE_OK;
}

static int git_attr_eof(sqlite3_vtab_cursor *pCursor) {
  return ((git_attr_cursor *)pCursor)->eof;
}

static int git_attr_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int col) {
  git_attr_cursor *cur = (git_attr_cursor *)pCursor;
  switch (col) {
    case 0: /* name */
      sqlite3_result_text(ctx, cur->names[cur->idx], -1, SQLITE_TRANSIENT);
      break;
    case 1: /* value */
      sqlite3_result_text(ctx, cur->values[cur->idx], -1, SQLITE_TRANSIENT);
      break;
  }
  return SQLITE_OK;
}

static int git_attr_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
  *pRowid = (sqlite3_int64)((git_attr_cursor *)pCursor)->idx;
  return SQLITE_OK;
}

static int git_attr_bestindex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo) {
  (void)pVtab;
  int argIdx = 0;
  for (int i = 0; i < pInfo->nConstraint; i++) {
    if (!pInfo->aConstraint[i].usable) continue;
    if (pInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) continue;
    int col = pInfo->aConstraint[i].iColumn;
    if (col == 2 || col == 3) { /* repo, path */
      pInfo->aConstraintUsage[i].argvIndex = ++argIdx;
      pInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pInfo->estimatedCost = 10;
  return SQLITE_OK;
}

static sqlite3_module git_attr_module = {
  .xConnect = git_attr_connect,
  .xBestIndex = git_attr_bestindex,
  .xDisconnect = git_log_disconnect,
  .xOpen = git_attr_open,
  .xClose = git_attr_close,
  .xFilter = git_attr_filter,
  .xNext = git_attr_next,
  .xEof = git_attr_eof,
  .xColumn = git_attr_column,
  .xRowid = git_attr_rowid,
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
  sqlite3_create_module(db, "git_diff", &git_diff_module, 0);
  sqlite3_create_module(db, "git_ancestors", &git_anc_module, 0);
  sqlite3_create_module(db, "git_config_list", &git_cfgl_module, 0);
  sqlite3_create_module(db, "git_status", &git_stat_module, 0);
  sqlite3_create_module(db, "git_blame", &git_bl_module, 0);
  sqlite3_create_module(db, "git_attr", &git_attr_module, 0);
  return SQLITE_OK;
}
