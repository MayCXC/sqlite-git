/*
** sqlite-git: SQLite extension exposing git plumbing via libgit2.
**
** Scalar functions:
**   git_blob(repo, oid)              -> blob content
**   git_blob(repo, rev, path)        -> blob content at rev:path
**   git_type(repo, oid)              -> 'blob'|'tree'|'commit'|'tag'
**   git_size(repo, oid)              -> object size in bytes
**   git_exists(repo, oid)            -> 1 or 0
**   git_hash(content, type)          -> oid (no write)
**   git_write(repo, content, type)   -> oid (writes object)
**   git_rev_parse(repo, spec)        -> oid
**   git_describe(repo, rev?)         -> descriptive string
**   git_commit_message(repo, rev)    -> message text
**   git_commit_summary(repo, rev)    -> first line
**   git_commit_tree(repo, rev)       -> tree oid
**   git_commit_author(repo, rev)     -> 'name <email> timestamp'
**   git_commit_parent(repo, rev, n)  -> nth parent oid
**   git_commit_parents(repo, rev)    -> parent count
**   git_ref(repo, name)              -> resolved oid
**   git_ref_create(repo, name, oid, force?) -> void
**   git_ref_delete(repo, name)       -> void
**   git_merge_base(repo, oid1, oid2) -> oid
**   git_config(repo, key)            -> value
**   git_config_set(repo, key, val)   -> void
**
** Build: gcc -shared -fPIC -o git0.so git0.c -lgit2
** Load:  .load ./git0
*/

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <git2.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Repo cache: keep last-used repo open to avoid repeated open/close */
git_repository *g_repo = NULL;
char g_repo_path[4096] = {0};

/* Forward declaration for TVF registration */
extern int git0_register_vtabs(sqlite3 *db);

static git_repository *get_repo(const char *path, char **err) {
  if (g_repo && strcmp(g_repo_path, path) == 0) return g_repo;
  if (g_repo) { git_repository_free(g_repo); g_repo = NULL; }
  int rc = git_repository_open(&g_repo, path);
  if (rc != 0) {
    *err = (char *)git_error_last()->message;
    return NULL;
  }
  strncpy(g_repo_path, path, sizeof(g_repo_path) - 1);
  return g_repo;
}

static void oid_to_hex(const git_oid *oid, char *out) {
  git_oid_tostr(out, GIT_OID_MAX_HEXSIZE + 1, oid);
}

/* Resolve a revision spec to an object */
static git_object *resolve_rev(git_repository *repo, const char *spec, char **err) {
  git_object *obj = NULL;
  int rc = git_revparse_single(&obj, repo, spec);
  if (rc != 0) {
    *err = (char *)git_error_last()->message;
    return NULL;
  }
  return obj;
}

/* Resolve rev to a commit */
static git_commit *resolve_commit(git_repository *repo, const char *rev, char **err) {
  git_object *obj = resolve_rev(repo, rev, err);
  if (!obj) return NULL;
  if (git_object_type(obj) != GIT_OBJECT_COMMIT) {
    git_object *peeled = NULL;
    int rc = git_object_peel(&peeled, obj, GIT_OBJECT_COMMIT);
    git_object_free(obj);
    if (rc != 0) {
      *err = "not a commit";
      return NULL;
    }
    return (git_commit *)peeled;
  }
  return (git_commit *)obj;
}

/* ---- git_blob(repo, oid) or git_blob(repo, rev, path) ---- */

static void fn_git_blob(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  const char *repo_path = (const char *)sqlite3_value_text(argv[0]);
  git_repository *repo = get_repo(repo_path, &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }

  if (argc == 2) {
    /* git_blob(repo, oid) */
    const char *oid_str = (const char *)sqlite3_value_text(argv[1]);
    git_oid oid;
    if (git_oid_fromstr(&oid, oid_str) != 0) {
      /* Try as revspec */
      git_object *obj = resolve_rev(repo, oid_str, &err);
      if (!obj) { sqlite3_result_error(ctx, err, -1); return; }
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
      sqlite3_result_error(ctx, git_error_last()->message, -1);
      return;
    }
    sqlite3_result_blob(ctx, git_blob_rawcontent(blob),
                        (int)git_blob_rawsize(blob), SQLITE_TRANSIENT);
    git_blob_free(blob);
  } else {
    /* git_blob(repo, rev, path) */
    const char *rev = (const char *)sqlite3_value_text(argv[1]);
    const char *path = (const char *)sqlite3_value_text(argv[2]);
    git_commit *commit = resolve_commit(repo, rev, &err);
    if (!commit) { sqlite3_result_error(ctx, err, -1); return; }
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
      sqlite3_result_error(ctx, git_error_last()->message, -1);
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
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!obj) { sqlite3_result_error(ctx, err, -1); return; }
  sqlite3_result_text(ctx, git_object_type2string(git_object_type(obj)), -1, SQLITE_STATIC);
  git_object_free(obj);
}

/* ---- git_size(repo, oid) ---- */

static void fn_git_size(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!obj) { sqlite3_result_error(ctx, err, -1); return; }
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
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  sqlite3_result_int(ctx, obj ? 1 : 0);
  if (obj) git_object_free(obj);
}

/* ---- git_hash(content, type) ---- */

static void fn_git_hash(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
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
  oid_to_hex(&oid, hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git_write(repo, content, type) ---- */

static void fn_git_write(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  const void *data = sqlite3_value_blob(argv[1]);
  int len = sqlite3_value_bytes(argv[1]);
  const char *type_str = (const char *)sqlite3_value_text(argv[2]);
  git_object_t type = git_object_string2type(type_str);
  git_odb *odb = NULL;
  git_repository_odb(&odb, repo);
  git_oid oid;
  int rc = git_odb_write(&oid, odb, data, len, type);
  git_odb_free(odb);
  if (rc != 0) { sqlite3_result_error(ctx, git_error_last()->message, -1); return; }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  oid_to_hex(&oid, hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git_rev_parse(repo, spec) ---- */

static void fn_git_rev_parse(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!obj) { sqlite3_result_error(ctx, err, -1); return; }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  oid_to_hex(git_object_id(obj), hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_object_free(obj);
}

/* ---- git_describe(repo, rev?) ---- */

static void fn_git_describe(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_describe_options opts = GIT_DESCRIBE_OPTIONS_INIT;
  opts.describe_strategy = GIT_DESCRIBE_ALL;
  git_describe_result *result = NULL;
  int rc;
  if (argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL) {
    git_object *obj = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), &err);
    if (!obj) { sqlite3_result_error(ctx, err, -1); return; }
    git_commit *commit = NULL;
    git_object_peel((git_object **)&commit, obj, GIT_OBJECT_COMMIT);
    git_object_free(obj);
    rc = git_describe_commit(&result, (git_object *)commit, &opts);
    git_commit_free(commit);
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
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!c) { sqlite3_result_error(ctx, err, -1); return; }
  sqlite3_result_text(ctx, git_commit_message(c), -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_summary(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!c) { sqlite3_result_error(ctx, err, -1); return; }
  sqlite3_result_text(ctx, git_commit_summary(c), -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_tree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!c) { sqlite3_result_error(ctx, err, -1); return; }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  oid_to_hex(git_commit_tree_id(c), hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_author(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!c) { sqlite3_result_error(ctx, err, -1); return; }
  const git_signature *a = git_commit_author(c);
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s <%s> %lld %+03d%02d",
           a->name, a->email, (long long)a->when.time,
           a->when.offset / 60, abs(a->when.offset) % 60);
  sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_parent(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!c) { sqlite3_result_error(ctx, err, -1); return; }
  unsigned int n = (unsigned int)sqlite3_value_int(argv[2]);
  if (n >= git_commit_parentcount(c)) { sqlite3_result_null(ctx); git_commit_free(c); return; }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  oid_to_hex(git_commit_parent_id(c, n), hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_commit_free(c);
}

static void fn_git_commit_parents(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_commit *c = resolve_commit(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!c) { sqlite3_result_error(ctx, err, -1); return; }
  sqlite3_result_int(ctx, (int)git_commit_parentcount(c));
  git_commit_free(c);
}

/* ---- git_ref(repo, name) ---- */

static void fn_git_ref(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  const char *name = (const char *)sqlite3_value_text(argv[1]);
  git_reference *ref = NULL;
  if (git_reference_dwim(&ref, repo, name) != 0) {
    sqlite3_result_null(ctx);
    return;
  }
  git_reference *resolved = NULL;
  if (git_reference_resolve(&resolved, ref) != 0) {
    char hex[GIT_OID_MAX_HEXSIZE + 1];
    oid_to_hex(git_reference_target(ref), hex);
    sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
    git_reference_free(ref);
    return;
  }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  oid_to_hex(git_reference_target(resolved), hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
  git_reference_free(resolved);
  git_reference_free(ref);
}

/* ---- git_ref_create(repo, name, oid, force?) ---- */

static void fn_git_ref_create(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  const char *name = (const char *)sqlite3_value_text(argv[1]);
  const char *oid_str = (const char *)sqlite3_value_text(argv[2]);
  int force = argc > 3 ? sqlite3_value_int(argv[3]) : 0;
  git_oid oid;
  if (git_oid_fromstr(&oid, oid_str) != 0) {
    sqlite3_result_error(ctx, "invalid oid", -1); return;
  }
  git_reference *ref = NULL;
  if (git_reference_create(&ref, repo, name, &oid, force, NULL) != 0) {
    sqlite3_result_error(ctx, git_error_last()->message, -1); return;
  }
  git_reference_free(ref);
  sqlite3_result_null(ctx);
}

/* ---- git_ref_delete(repo, name) ---- */

static void fn_git_ref_delete(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_reference *ref = NULL;
  if (git_reference_dwim(&ref, repo, (const char *)sqlite3_value_text(argv[1])) != 0) {
    sqlite3_result_error(ctx, git_error_last()->message, -1); return;
  }
  git_reference_delete(ref);
  git_reference_free(ref);
  sqlite3_result_null(ctx);
}

/* ---- git_merge_base(repo, oid1, oid2) ---- */

static void fn_git_merge_base(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_object *o1 = resolve_rev(repo, (const char *)sqlite3_value_text(argv[1]), &err);
  if (!o1) { sqlite3_result_error(ctx, err, -1); return; }
  git_object *o2 = resolve_rev(repo, (const char *)sqlite3_value_text(argv[2]), &err);
  if (!o2) { git_object_free(o1); sqlite3_result_error(ctx, err, -1); return; }
  git_oid base;
  int rc = git_merge_base(&base, repo, git_object_id(o1), git_object_id(o2));
  git_object_free(o1);
  git_object_free(o2);
  if (rc != 0) { sqlite3_result_null(ctx); return; }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  oid_to_hex(&base, hex);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git_config(repo, key) ---- */

static void fn_git_config(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
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
  char *err = NULL;
  git_repository *repo = get_repo((const char *)sqlite3_value_text(argv[0]), &err);
  if (!repo) { sqlite3_result_error(ctx, err, -1); return; }
  git_config *cfg = NULL;
  git_repository_config(&cfg, repo);
  int rc = git_config_set_string(cfg, (const char *)sqlite3_value_text(argv[1]),
                                 (const char *)sqlite3_value_text(argv[2]));
  git_config_free(cfg);
  if (rc != 0) sqlite3_result_error(ctx, git_error_last()->message, -1);
  else sqlite3_result_null(ctx);
}

/* ---- Extension entry point ---- */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_git_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
  SQLITE_EXTENSION_INIT2(pApi);
  git_libgit2_init();

  /* Scalar functions */
  sqlite3_create_function(db, "git_blob", 2, SQLITE_UTF8, 0, fn_git_blob, 0, 0);
  sqlite3_create_function(db, "git_blob", 3, SQLITE_UTF8, 0, fn_git_blob, 0, 0);
  sqlite3_create_function(db, "git_type", 2, SQLITE_UTF8, 0, fn_git_type, 0, 0);
  sqlite3_create_function(db, "git_size", 2, SQLITE_UTF8, 0, fn_git_size, 0, 0);
  sqlite3_create_function(db, "git_exists", 2, SQLITE_UTF8, 0, fn_git_exists, 0, 0);
  sqlite3_create_function(db, "git_hash", 2, SQLITE_UTF8, 0, fn_git_hash, 0, 0);
  sqlite3_create_function(db, "git_write", 3, SQLITE_UTF8, 0, fn_git_write, 0, 0);
  sqlite3_create_function(db, "git_rev_parse", 2, SQLITE_UTF8, 0, fn_git_rev_parse, 0, 0);
  sqlite3_create_function(db, "git_describe", 1, SQLITE_UTF8, 0, fn_git_describe, 0, 0);
  sqlite3_create_function(db, "git_describe", 2, SQLITE_UTF8, 0, fn_git_describe, 0, 0);
  sqlite3_create_function(db, "git_commit_message", 2, SQLITE_UTF8, 0, fn_git_commit_message, 0, 0);
  sqlite3_create_function(db, "git_commit_summary", 2, SQLITE_UTF8, 0, fn_git_commit_summary, 0, 0);
  sqlite3_create_function(db, "git_commit_tree", 2, SQLITE_UTF8, 0, fn_git_commit_tree, 0, 0);
  sqlite3_create_function(db, "git_commit_author", 2, SQLITE_UTF8, 0, fn_git_commit_author, 0, 0);
  sqlite3_create_function(db, "git_commit_parent", 3, SQLITE_UTF8, 0, fn_git_commit_parent, 0, 0);
  sqlite3_create_function(db, "git_commit_parents", 2, SQLITE_UTF8, 0, fn_git_commit_parents, 0, 0);
  sqlite3_create_function(db, "git_ref", 2, SQLITE_UTF8, 0, fn_git_ref, 0, 0);
  sqlite3_create_function(db, "git_ref_create", 3, SQLITE_UTF8, 0, fn_git_ref_create, 0, 0);
  sqlite3_create_function(db, "git_ref_create", 4, SQLITE_UTF8, 0, fn_git_ref_create, 0, 0);
  sqlite3_create_function(db, "git_ref_delete", 2, SQLITE_UTF8, 0, fn_git_ref_delete, 0, 0);
  sqlite3_create_function(db, "git_merge_base", 3, SQLITE_UTF8, 0, fn_git_merge_base, 0, 0);
  sqlite3_create_function(db, "git_config", 2, SQLITE_UTF8, 0, fn_git_config, 0, 0);
  sqlite3_create_function(db, "git_config_set", 3, SQLITE_UTF8, 0, fn_git_config_set, 0, 0);

  /* Table-valued functions */
  git0_register_vtabs(db);

  return SQLITE_OK;
}
