/*
** git0_storage: convenience SQL functions for storage-backed repos.
**
** These are wrappers around the git_* functions in git0.c that
** implicitly use git0_repo() (the storage-backed libgit2 repo)
** so callers don't need to pass a repo path.
**
**   git0_type(oid)     = git_type(git0_repo(), oid)
**   git0_blob(oid)     = git_blob(git0_repo(), oid)
**   git0_ref(name)     = git_ref(git0_repo(), name)
**   ... etc
**
** Also provides git0_cat(oid) for raw object content (no libgit2
** equivalent) and git0_exists(oid) via storage_object_exists.
*/

#include "git0.h"
#include "git0_internal.h"
#include "storage.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include <git2.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Get the storage-backed repo, or set error and return NULL */
static git_repository *get_repo(sqlite3_context *ctx) {
  git_repository *repo = git0_storage_repo();
  if (!repo)
    sqlite3_result_error(ctx, "storage not initialized (call git0_init first)", -1);
  return repo;
}

/* Parse hex OID from argument */
static int get_oid(sqlite3_context *ctx, sqlite3_value *arg, git_oid *oid) {
  const char *hex = (const char *)sqlite3_value_text(arg);
  if (!hex || git_oid_fromstr(oid, hex) < 0) {
    sqlite3_result_null(ctx);
    return -1;
  }
  return 0;
}

/* Resolve a revspec (ref name or OID) to a git_object */
static git_object *resolve_rev(git_repository *repo, sqlite3_context *ctx,
                                sqlite3_value *arg) {
  const char *spec = (const char *)sqlite3_value_text(arg);
  if (!spec) { sqlite3_result_null(ctx); return NULL; }
  git_object *obj = NULL;
  if (git_revparse_single(&obj, repo, spec) != 0) {
    sqlite3_result_null(ctx);
    return NULL;
  }
  return obj;
}

static git_commit *resolve_commit(git_repository *repo, sqlite3_context *ctx,
                                   sqlite3_value *arg) {
  git_object *obj = resolve_rev(repo, ctx, arg);
  if (!obj) return NULL;
  git_commit *commit = NULL;
  if (git_object_peel((git_object **)&commit, obj, GIT_OBJECT_COMMIT) != 0) {
    git_object_free(obj);
    sqlite3_result_null(ctx);
    return NULL;
  }
  git_object_free(obj);
  return commit;
}

/* ---- git0_exists(oid) ---- */

static void fn_s_exists(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_oid oid;
  if (get_oid(ctx, argv[0], &oid)) return;
  sqlite3_result_int(ctx, storage_object_exists(&oid));
}

/* ---- git0_cat(oid) - raw object content ---- */

static void fn_s_cat(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_oid oid;
  if (get_oid(ctx, argv[0], &oid)) return;
  git_object_t type; size_t size; unsigned char *data;
  if (storage_read_object(&oid, &type, &size, &data) < 0) {
    sqlite3_result_null(ctx); return;
  }
  sqlite3_result_blob64(ctx, data, size, free);
}

/* ---- Wrappers using libgit2 via storage-backed repo ---- */

static void fn_s_type(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_oid oid;
  if (get_oid(ctx, argv[0], &oid)) return;
  size_t size; git_object_t type;
  git_odb *odb; git_repository_odb(&odb, repo);
  if (git_odb_read_header(&size, &type, odb, &oid) == 0)
    sqlite3_result_text(ctx, git_object_type2string(type), -1, SQLITE_STATIC);
  else
    sqlite3_result_null(ctx);
  git_odb_free(odb);
}

static void fn_s_size(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_oid oid;
  if (get_oid(ctx, argv[0], &oid)) return;
  size_t size; git_object_t type;
  git_odb *odb; git_repository_odb(&odb, repo);
  if (git_odb_read_header(&size, &type, odb, &oid) == 0)
    sqlite3_result_int64(ctx, (sqlite3_int64)size);
  else
    sqlite3_result_null(ctx);
  git_odb_free(odb);
}

static void fn_s_blob(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  git_repository *repo = get_repo(ctx); if (!repo) return;

  if (argc == 1) {
    git_oid oid;
    if (get_oid(ctx, argv[0], &oid)) return;
    git_blob *blob = NULL;
    if (git_blob_lookup(&blob, repo, &oid) != 0) { sqlite3_result_null(ctx); return; }
    sqlite3_result_blob64(ctx, git_blob_rawcontent(blob),
                          git_blob_rawsize(blob), SQLITE_TRANSIENT);
    git_blob_free(blob);
    return;
  }

  /* git0_blob(rev, path) */
  const char *path = (const char *)sqlite3_value_text(argv[1]);
  if (!path) { sqlite3_result_null(ctx); return; }
  git_commit *commit = resolve_commit(repo, ctx, argv[0]);
  if (!commit) return;
  git_tree *tree = NULL;
  if (git_commit_tree(&tree, commit) != 0) {
    git_commit_free(commit); sqlite3_result_null(ctx); return;
  }
  git_commit_free(commit);
  git_tree_entry *entry = NULL;
  if (git_tree_entry_bypath(&entry, tree, path) != 0) {
    git_tree_free(tree); sqlite3_result_null(ctx); return;
  }
  git_blob *blob = NULL;
  if (git_blob_lookup(&blob, repo, git_tree_entry_id(entry)) != 0) {
    git_tree_entry_free(entry); git_tree_free(tree);
    sqlite3_result_null(ctx); return;
  }
  sqlite3_result_blob64(ctx, git_blob_rawcontent(blob),
                        git_blob_rawsize(blob), SQLITE_TRANSIENT);
  git_blob_free(blob);
  git_tree_entry_free(entry);
  git_tree_free(tree);
}

static void fn_s_ref(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  if (!name) { sqlite3_result_null(ctx); return; }
  git_oid oid;
  if (git_reference_name_to_id(&oid, repo, name) != 0) {
    sqlite3_result_null(ctx); return;
  }
  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

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

static void fn_s_ref_delete(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  if (!name) return;
  storage_ref_delete(name);
  sqlite3_result_text(ctx, "ok", -1, SQLITE_STATIC);
}

static void fn_s_commit_tree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_commit *c = resolve_commit(repo, ctx, argv[0]); if (!c) return;
  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), git_commit_tree_id(c));
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_s_commit_message(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_commit *c = resolve_commit(repo, ctx, argv[0]); if (!c) return;
  sqlite3_result_text(ctx, git_commit_message(c), -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_s_commit_summary(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_commit *c = resolve_commit(repo, ctx, argv[0]); if (!c) return;
  sqlite3_result_text(ctx, git_commit_summary(c), -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_s_commit_author(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_commit *c = resolve_commit(repo, ctx, argv[0]); if (!c) return;
  const git_signature *a = git_commit_author(c);
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s <%s> %lld %+03d%02d",
           a->name, a->email, (long long)a->when.time,
           a->when.offset / 60, abs(a->when.offset) % 60);
  sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_s_commit_parent(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_commit *c = resolve_commit(repo, ctx, argv[0]); if (!c) return;
  unsigned int n = (unsigned int)sqlite3_value_int(argv[1]);
  if (n >= git_commit_parentcount(c)) {
    git_commit_free(c); sqlite3_result_null(ctx); return;
  }
  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), git_commit_parent_id(c, n));
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_s_commit_parents(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = get_repo(ctx); if (!repo) return;
  git_commit *c = resolve_commit(repo, ctx, argv[0]); if (!c) return;
  sqlite3_result_int(ctx, (int)git_commit_parentcount(c));
  git_commit_free(c);
}

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
