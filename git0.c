/*
** git0: SQLite extension exposing git plumbing via libgit2.
**
** Scalar functions:
**   git_version()                    -> version string
**   git_blob(repo, oid)             -> blob content
**   git_blob(repo, rev, path)       -> blob content at rev:path
**   git_type(repo, oid)             -> 'blob'|'tree'|'commit'|'tag'
**   git_size(repo, oid)             -> object size in bytes
**   git_exists(repo, oid)           -> 1 or 0
**   git_hash(content, type)         -> oid (no write)
**   git_write(repo, content, type)  -> oid (writes object)
**   git_rev_parse(repo, spec)       -> oid
**   git_describe(repo, rev?)        -> descriptive string
**   git_commit_message(repo, rev)   -> message text
**   git_commit_summary(repo, rev)   -> first line
**   git_commit_tree(repo, rev)      -> tree oid
**   git_commit_author(repo, rev)    -> 'name <email> timestamp tz'
**   git_commit_parent(repo, rev, n) -> nth parent oid
**   git_commit_parents(repo, rev)   -> parent count
**   git_ref(repo, name)             -> resolved oid
**   git_ref_create(repo, name, oid, force?) -> void
**   git_ref_delete(repo, name)      -> void
**   git_merge_base(repo, oid1, oid2) -> oid
**   git_config(repo, key)           -> value
**   git_config_set(repo, key, val)  -> void
**
** Build: gcc -shared -fPIC -o git0.so git0.c git0_vtab.c -lgit2
** Load:  .load ./git0
*/

#include "git0.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT1
#endif

#include <git2.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ---- Per-connection repo cache ---- */

typedef struct git0_ctx {
  git_repository *repo;
  char repo_path[4096];
} git0_ctx;

static void git0_ctx_free(void *p) {
  git0_ctx *ctx = (git0_ctx *)p;
  if (ctx->repo) git_repository_free(ctx->repo);
  sqlite3_free(ctx);
}

/* Get or create per-connection context, open repo if path changed */
static git_repository *git0_repo(sqlite3_context *ctx, const char *path) {
  git0_ctx *g = (git0_ctx *)sqlite3_get_auxdata(ctx, 0);
  if (!g) {
    g = sqlite3_malloc(sizeof(*g));
    if (!g) { sqlite3_result_error_nomem(ctx); return NULL; }
    memset(g, 0, sizeof(*g));
    sqlite3_set_auxdata(ctx, 0, g, git0_ctx_free);
    /* re-fetch in case sqlite3_set_auxdata replaced the old one */
    g = (git0_ctx *)sqlite3_get_auxdata(ctx, 0);
    if (!g) { sqlite3_result_error_nomem(ctx); return NULL; }
  }
  if (g->repo && strcmp(g->repo_path, path) == 0) return g->repo;
  if (g->repo) { git_repository_free(g->repo); g->repo = NULL; }
  int rc = git_repository_open(&g->repo, path);
  if (rc != 0) {
    const git_error *e = git_error_last();
    sqlite3_result_error(ctx, e ? e->message : "failed to open repository", -1);
    return NULL;
  }
  strncpy(g->repo_path, path, sizeof(g->repo_path) - 1);
  return g->repo;
}

/* Get repo for TVFs (no sqlite3_context, uses raw path) */
git_repository *git0_repo_open(const char *path, char **err) {
  /* TVFs manage their own repo handles via cursor state.
   * This is a simple open without caching for TVF use. */
  static git_repository *tvf_repo = NULL;
  static char tvf_path[4096] = {0};
  if (tvf_repo && strcmp(tvf_path, path) == 0) return tvf_repo;
  if (tvf_repo) { git_repository_free(tvf_repo); tvf_repo = NULL; }
  int rc = git_repository_open(&tvf_repo, path);
  if (rc != 0) {
    const git_error *e = git_error_last();
    *err = (char *)(e ? e->message : "failed to open repository");
    return NULL;
  }
  strncpy(tvf_path, path, sizeof(tvf_path) - 1);
  return tvf_repo;
}

void git0_oid_to_hex(const git_oid *oid, char *out) {
  git_oid_tostr(out, GIT_OID_MAX_HEXSIZE + 1, oid);
}

static git_object *resolve_rev(git_repository *repo, const char *spec, sqlite3_context *ctx) {
  git_object *obj = NULL;
  if (git_revparse_single(&obj, repo, spec) != 0) {
    const git_error *e = git_error_last();
    sqlite3_result_error(ctx, e ? e->message : "revision not found", -1);
    return NULL;
  }
  return obj;
}

static git_commit *resolve_commit(git_repository *repo, const char *rev, sqlite3_context *ctx) {
  git_object *obj = resolve_rev(repo, rev, ctx);
  if (!obj) return NULL;
  if (git_object_type(obj) != GIT_OBJECT_COMMIT) {
    git_object *peeled = NULL;
    int rc = git_object_peel(&peeled, obj, GIT_OBJECT_COMMIT);
    git_object_free(obj);
    if (rc != 0) {
      sqlite3_result_error(ctx, "not a commit", -1);
      return NULL;
    }
    return (git_commit *)peeled;
  }
  return (git_commit *)obj;
}

/* ---- git_version() ---- */

static void fn_git_version(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc; (void)argv;
  sqlite3_result_text(ctx, GIT0_VERSION, -1, SQLITE_STATIC);
}

/* ---- git_blob(repo, oid) or git_blob(repo, rev, path) ---- */

static void fn_git_blob(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  const char *repo_path = (const char *)sqlite3_value_text(argv[0]);
  git_repository *repo = git0_repo(ctx, repo_path);
  if (!repo) return;

  if (argc == 2) {
    const char *spec = (const char *)sqlite3_value_text(argv[1]);
    git_oid oid;
    if (git_oid_fromstr(&oid, spec) != 0) {
      git_object *obj = resolve_rev(repo, spec, ctx);
      if (!obj) return;
      if (git_object_type(obj) != GIT_OBJECT_BLOB) {
        git_object_free(obj);
        sqlite3_result_error(ctx, "not a blob", -1);
        return;
      }
      git_blob *blob = (git_blob *)obj;
      sqlite3_result_blob(ctx, git_blob_rawcontent(blob),
                          (int)git_blob_rawsize(blob), SQLITE_TRANSIENT);
      git_blob_free(blob);
      return;
    }
    git_blob *blob = NULL;
    if (git_blob_lookup(&blob, repo, &oid) != 0) {
      const git_error *e = git_error_last();
      sqlite3_result_error(ctx, e ? e->message : "blob lookup failed", -1);
      return;
    }
    sqlite3_result_blob(ctx, git_blob_rawcontent(blob),
                        (int)git_blob_rawsize(blob), SQLITE_TRANSIENT);
    git_blob_free(blob);
  } else {
    const char *rev = (const char *)sqlite3_value_text(argv[1]);
    const char *path = (const char *)sqlite3_value_text(argv[2]);
    git_commit *commit = resolve_commit(repo, rev, ctx);
    if (!commit) return;
    git_tree *tree = NULL;
    git_commit_tree(&tree, commit);
    git_commit_free(commit);
    git_tree_entry *entry = NULL;
    if (git_tree_entry_bypath(&entry, tree, path) != 0) {
      git_tree_free(tree);
      sqlite3_result_null(ctx);
      return;
    }
    git_blob *blob = NULL;
    if (git_blob_lookup(&blob, repo, git_tree_entry_id(entry)) != 0) {
      git_tree_entry_free(entry);
      git_tree_free(tree);
      const git_error *e = git_error_last();
      sqlite3_result_error(ctx, e ? e->message : "blob lookup failed", -1);
      return;
    }
    sqlite3_result_blob(ctx, git_blob_rawcontent(blob),
                        (int)git_blob_rawsize(blob), SQLITE_TRANSIENT);
    git_blob_free(blob);
    git_tree_entry_free(entry);
    git_tree_free(tree);
  }
}

/* ---- git_type(repo, oid) ---- */

static void fn_git_type(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!obj) return;
  sqlite3_result_text(ctx, git_object_type2string(git_object_type(obj)), -1, SQLITE_STATIC);
  git_object_free(obj);
}

/* ---- git_size(repo, oid) ---- */

static void fn_git_size(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!obj) return;
  git_odb *odb = NULL;
  git_repository_odb(&odb, repo);
  size_t len; git_object_t type;
  git_odb_read_header(&len, &type, odb, git_object_id(obj));
  sqlite3_result_int64(ctx, (sqlite3_int64)len);
  git_odb_free(odb);
  git_object_free(obj);
}

/* ---- git_exists(repo, oid) ---- */

static void fn_git_exists(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_object *obj = NULL;
  int rc = git_revparse_single(&obj, repo, (const char *)sqlite3_value_text(argv[1]));
  sqlite3_result_int(ctx, rc == 0 ? 1 : 0);
  if (obj) git_object_free(obj);
}

/* ---- git_hash(content, type) ---- */

static void fn_git_hash(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const void *data = sqlite3_value_blob(argv[0]);
  int len = sqlite3_value_bytes(argv[0]);
  const char *type_str = (const char *)sqlite3_value_text(argv[1]);
  git_object_t type = git_object_string2type(type_str);
  if (type == GIT_OBJECT_INVALID) {
    sqlite3_result_error(ctx, "invalid object type", -1);
    return;
  }
  git_oid oid;
  git_odb_hash(&oid, data, len, type);
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&oid, hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git_write(repo, content, type) ---- */

static void fn_git_write(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  const void *data = sqlite3_value_blob(argv[1]);
  int len = sqlite3_value_bytes(argv[1]);
  git_object_t type = git_object_string2type((const char *)sqlite3_value_text(argv[2]));
  git_odb *odb = NULL;
  git_repository_odb(&odb, repo);
  git_oid oid;
  int rc = git_odb_write(&oid, odb, data, len, type);
  git_odb_free(odb);
  if (rc != 0) {
    const git_error *e = git_error_last();
    sqlite3_result_error(ctx, e ? e->message : "write failed", -1);
    return;
  }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&oid, hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git_rev_parse(repo, spec) ---- */

static void fn_git_rev_parse(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!obj) return;
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(git_object_id(obj), hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_object_free(obj);
}

/* ---- git_describe(repo, rev?) ---- */

static void fn_git_describe(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_describe_options opts = GIT_DESCRIBE_OPTIONS_INIT;
  opts.describe_strategy = GIT_DESCRIBE_ALL;
  git_describe_result *result = NULL;
  int rc;
  if (argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL) {
    git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
    if (!obj) return;
    rc = git_describe_commit(&result, obj, &opts);
    git_object_free(obj);
  } else {
    rc = git_describe_workdir(&result, repo, &opts);
  }
  if (rc != 0) { sqlite3_result_null(ctx); return; }
  git_describe_format_options fmt = GIT_DESCRIBE_FORMAT_OPTIONS_INIT;
  git_buf buf = GIT_BUF_INIT;
  git_describe_format(&buf, result, &fmt);
  sqlite3_result_text(ctx, buf.ptr, (int)buf.size, SQLITE_TRANSIENT);
  git_buf_dispose(&buf);
  git_describe_result_free(result);
}

/* ---- git_commit_* scalar functions ---- */

static void fn_git_commit_message(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!c) return;
  sqlite3_result_text(ctx, git_commit_message(c), -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_summary(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!c) return;
  sqlite3_result_text(ctx, git_commit_summary(c), -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_tree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!c) return;
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(git_commit_tree_id(c), hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_author(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!c) return;
  const git_signature *a = git_commit_author(c);
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s <%s> %lld %+03d%02d",
           a->name, a->email, (long long)a->when.time,
           a->when.offset / 60, abs(a->when.offset) % 60);
  sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_parent(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!c) return;
  unsigned int n = (unsigned int)sqlite3_value_int(argv[2]);
  if (n >= git_commit_parentcount(c)) { sqlite3_result_null(ctx); git_commit_free(c); return; }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(git_commit_parent_id(c, n), hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_parents(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!c) return;
  sqlite3_result_int(ctx, (int)git_commit_parentcount(c));
  git_commit_free(c);
}

/* ---- git_ref(repo, name) ---- */

static void fn_git_ref(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_reference *ref = NULL;
  if (git_reference_dwim(&ref, repo, (const char *)sqlite3_value_text(argv[1])) != 0) {
    sqlite3_result_null(ctx);
    return;
  }
  git_reference *resolved = NULL;
  const git_oid *target;
  if (git_reference_resolve(&resolved, ref) == 0) {
    target = git_reference_target(resolved);
  } else {
    target = git_reference_target(ref);
  }
  if (target) {
    char hex[GIT_OID_MAX_HEXSIZE + 1];
    git0_oid_to_hex(target, hex);
    sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  } else {
    sqlite3_result_null(ctx);
  }
  if (resolved) git_reference_free(resolved);
  git_reference_free(ref);
}

/* ---- git_ref_create(repo, name, oid, force?) ---- */

static void fn_git_ref_create(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  const char *name = (const char *)sqlite3_value_text(argv[1]);
  const char *oid_str = (const char *)sqlite3_value_text(argv[2]);
  int force = argc > 3 ? sqlite3_value_int(argv[3]) : 0;
  git_oid oid;
  if (git_oid_fromstr(&oid, oid_str) != 0) {
    sqlite3_result_error(ctx, "invalid oid", -1); return;
  }
  git_reference *ref = NULL;
  if (git_reference_create(&ref, repo, name, &oid, force, NULL) != 0) {
    const git_error *e = git_error_last();
    sqlite3_result_error(ctx, e ? e->message : "ref create failed", -1);
    return;
  }
  git_reference_free(ref);
  sqlite3_result_null(ctx);
}

/* ---- git_ref_delete(repo, name) ---- */

static void fn_git_ref_delete(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_reference *ref = NULL;
  if (git_reference_dwim(&ref, repo, (const char *)sqlite3_value_text(argv[1])) != 0) {
    const git_error *e = git_error_last();
    sqlite3_result_error(ctx, e ? e->message : "ref not found", -1);
    return;
  }
  git_reference_delete(ref);
  git_reference_free(ref);
  sqlite3_result_null(ctx);
}

/* ---- git_merge_base(repo, oid1, oid2) ---- */

static void fn_git_merge_base(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_object *o1 = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), ctx);
  if (!o1) return;
  git_object *o2 = resolve_rev(repo, (const char *)sqlite3_value_text(argv[2]), ctx);
  if (!o2) { git_object_free(o1); return; }
  git_oid base;
  int rc = git_merge_base(&base, repo, git_object_id(o1), git_object_id(o2));
  git_object_free(o1);
  git_object_free(o2);
  if (rc != 0) { sqlite3_result_null(ctx); return; }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&base, hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git_config(repo, key) ---- */

static void fn_git_config(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_config *cfg = NULL;
  git_repository_config(&cfg, repo);
  git_buf val = GIT_BUF_INIT;
  if (git_config_get_string_buf(&val, cfg, (const char *)sqlite3_value_text(argv[1])) != 0) {
    sqlite3_result_null(ctx);
  } else {
    sqlite3_result_text(ctx, val.ptr, (int)val.size, SQLITE_TRANSIENT);
  }
  git_buf_dispose(&val);
  git_config_free(cfg);
}

/* ---- git_config_set(repo, key, value) ---- */

static void fn_git_config_set(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_repo(ctx, (const char *)sqlite3_value_text(argv[0]));
  if (!repo) return;
  git_config *cfg = NULL;
  git_repository_config(&cfg, repo);
  int rc = git_config_set_string(cfg, (const char *)sqlite3_value_text(argv[1]),
                                 (const char *)sqlite3_value_text(argv[2]));
  git_config_free(cfg);
  if (rc != 0) {
    const git_error *e = git_error_last();
    sqlite3_result_error(ctx, e ? e->message : "config set failed", -1);
  } else {
    sqlite3_result_null(ctx);
  }
}

/* ---- Extension entry point ---- */

extern int git0_register_vtabs(sqlite3 *db);
extern int git0_register_objects(sqlite3 *db);
extern int git0_register_refs_vt(sqlite3 *db);

GIT0_API int sqlite3_git_init(sqlite3 *db, char **pzErrMsg,
                               const sqlite3_api_routines *pApi) {
  (void)pzErrMsg;
#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT2(pApi);
#endif
  git_libgit2_init();

#define F(name, narg, func) \
  sqlite3_create_function(db, name, narg, SQLITE_UTF8, 0, func, 0, 0)

  F("git_version",        0, fn_git_version);
  F("git_blob",           2, fn_git_blob);
  F("git_blob",           3, fn_git_blob);
  F("git_type",           2, fn_git_type);
  F("git_size",           2, fn_git_size);
  F("git_exists",         2, fn_git_exists);
  F("git_hash",           2, fn_git_hash);
  F("git_write",          3, fn_git_write);
  F("git_rev_parse",      2, fn_git_rev_parse);
  F("git_describe",       1, fn_git_describe);
  F("git_describe",       2, fn_git_describe);
  F("git_commit_message", 2, fn_git_commit_message);
  F("git_commit_summary", 2, fn_git_commit_summary);
  F("git_commit_tree",    2, fn_git_commit_tree);
  F("git_commit_author",  2, fn_git_commit_author);
  F("git_commit_parent",  3, fn_git_commit_parent);
  F("git_commit_parents", 2, fn_git_commit_parents);
  F("git_ref",            2, fn_git_ref);
  F("git_ref_create",     3, fn_git_ref_create);
  F("git_ref_create",     4, fn_git_ref_create);
  F("git_ref_delete",     2, fn_git_ref_delete);
  F("git_merge_base",     3, fn_git_merge_base);
  F("git_config",         2, fn_git_config);
  F("git_config_set",     3, fn_git_config_set);

#undef F

  git0_register_vtabs(db);
  git0_register_objects(db);
  git0_register_refs_vt(db);

  return SQLITE_OK;
}
