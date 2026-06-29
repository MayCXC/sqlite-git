/*
** git0_repo: self-contained git repo in SQLite.
**
** git0_init()           -> create tables, empty tree, initial commit, HEAD
** git0_add(path, data)  -> store blob, return hex oid
** git0_mktree(entries)  -> build tree object from "mode name oid\n" entries
** git0_mkcommit(tree, parent, msg, author?) -> build commit object
**
** Object and ref construction goes through libgit2 (git_odb_write,
** git_treebuilder, git_commit_create, git_reference_create) against the
** storage-backed odb/refdb in git0_backend.c, so libgit2 owns serialization
** and hashing; sqlite is only the storage leaf.
*/

#include "git0.h"
#include "git0_internal.h"
#include "storage_git.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include "oidhex.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>



/* ---- git0_init() ---- */

/* The git-init options that apply to a sqlite-backed odb repo: object-format
 * picks the hash (sha1 default; sha256 only on the experimental build) and
 * initial-branch names the first ref (default master). The on-disk options (bare,
 * ref-format, template, separate-git-dir, shared) have no meaning here. */
static void fn_git0_init(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  /* Lazily initialize storage if not already open */
  if (!storage_db())
    storage_open_db(sqlite3_context_db_handle(ctx));
  storage_git_init();	/* ensure the git schema exists (idempotent) */

  /* object-format: choose the repo's hash, like git init --object-format. */
  int fmt = 0;
  if (argc > 0 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    const char *of = (const char *)sqlite3_value_text(argv[0]);
    if (of && !strcmp(of, "sha256")) {
#ifdef GIT_EXPERIMENTAL_SHA256
      fmt = 1;
#else
      sqlite3_result_error(ctx, "init: object-format sha256 requires the experimental build", -1);
      return;
#endif
    } else if (of && strcmp(of, "sha1")) {
      sqlite3_result_error(ctx, "init: unknown object-format (use sha1 or sha256)", -1);
      return;
    }
  }

  /* initial-branch: name the first branch, like git init -b. Defaults to master,
   * matching git core's compiled default; pass a name to override. */
  const char *branch = "master";
  if (argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL) {
    const char *b = (const char *)sqlite3_value_text(argv[1]);
    if (b && *b) branch = b;
  }

  /* Check if already initialized. HEAD is normally a symref; follow it to the
   * branch's commit so the result is HEAD's oid rather than an empty value. */
  char head_hex[GIT_OID_MAX_HEXSIZE + 1] = "";
  char symref[256] = "";
  if (storage_ref_read("HEAD", head_hex, sizeof(head_hex), symref, sizeof(symref)) == 0) {
    if (symref[0]) {
      char tgt_sym[256] = "";
      storage_ref_read(symref, head_hex, sizeof(head_hex), tgt_sym, sizeof(tgt_sym));
    }
    sqlite3_result_text(ctx, head_hex, -1, SQLITE_TRANSIENT);
    return;
  }

  /* Record the object format BEFORE the storage-backed repo is built, so its
   * odb is wrapped with the matching hash (ensure_repo reads it). */
  if (storage_set_object_format(fmt) != 0) {
    sqlite3_result_error(ctx, "init: failed to record object format", -1);
    return;
  }

  git_repository *repo = git0_storage_repo();
  if (!repo) {
    sqlite3_result_error(ctx, "init: storage repo unavailable", -1);
    return;
  }

  /* Build the empty tree, the initial commit, and the branch + HEAD through
   * libgit2 against the storage-backed odb/refdb, so libgit2 owns the object
   * serialization, hashing, and ref writes (no hand-rolled object bytes). */
  git_treebuilder *bld = NULL;
  git_oid tree_oid;
  if (git_treebuilder_new(&bld, repo, NULL) != 0 ||
      git_treebuilder_write(&tree_oid, bld) != 0) {
    git_treebuilder_free(bld);
    sqlite3_result_error(ctx, "init: empty tree write failed", -1);
    return;
  }
  git_treebuilder_free(bld);

  git_tree *tree = NULL;
  git_signature *sig = NULL;
  if (git_tree_lookup(&tree, repo, &tree_oid) != 0 ||
      git_signature_now(&sig, "git0", "git0@sqlite") != 0) {
    git_tree_free(tree);
    sqlite3_result_error(ctx, "init: tree lookup / signature failed", -1);
    return;
  }

  git_oid commit_oid;
  int crc = git_commit_create(&commit_oid, repo, NULL, sig, sig, NULL,
                              "initial commit\n", tree, 0, NULL);
  git_tree_free(tree);
  git_signature_free(sig);
  if (crc != 0) {
    sqlite3_result_error(ctx, "init: initial commit failed", -1);
    return;
  }

  char branch_ref[256];
  snprintf(branch_ref, sizeof(branch_ref), "refs/heads/%s", branch);
  git_reference *bref = NULL, *href = NULL;
  if (git_reference_create(&bref, repo, branch_ref, &commit_oid, 0, NULL) != 0 ||
      git_reference_symbolic_create(&href, repo, "HEAD", branch_ref, 0, NULL) != 0) {
    git_reference_free(bref);
    git_reference_free(href);
    sqlite3_result_error(ctx, "init: branch/HEAD create failed", -1);
    return;
  }
  git_reference_free(bref);
  git_reference_free(href);

  char commit_hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(commit_hex, sizeof(commit_hex), &commit_oid);
  sqlite3_result_text(ctx, commit_hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_add(path, data, type?) ---- */

static void fn_git0_add(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  git_repository *repo = git0_storage_repo();
  if (!repo) {
    sqlite3_result_error(ctx, "storage not initialized (call git0_init first)", -1);
    return;
  }
  const void *data = sqlite3_value_blob(argv[1]);
  int data_len = sqlite3_value_bytes(argv[1]);
  git_object_t type = GIT_OBJECT_BLOB;

  if (argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL) {
    type = git_object_string2type((const char *)sqlite3_value_text(argv[2]));
    if (type == GIT_OBJECT_INVALID) type = GIT_OBJECT_BLOB;
  }

  /*
   * argv[0] is the conventional name being "added" (so the SQL reads like
   * `git add <path>`), but a git object is content-addressed: the path->oid
   * mapping belongs in a tree (git0_mktree), not the object row, so the blob is
   * stored by content alone. git_odb_write hashes under the repo's odb (the
   * hash git0_init recorded) and writes through the storage backend.
   */
  git_odb *odb = NULL;
  if (git_repository_odb(&odb, repo) != 0) {
    sqlite3_result_error(ctx, "odb unavailable", -1);
    return;
  }
  git_oid oid;
  int rc = git_odb_write(&oid, odb, data ? data : "", (size_t)data_len, type);
  git_odb_free(odb);
  if (rc != 0) {
    sqlite3_result_error(ctx, "object write failed", -1);
    return;
  }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_mktree(entries_text) ---- */
/* entries: "mode path oid\n" per line. Paths with '/' create nested subtrees.
 * Built through an in-memory git_index: git_index_add records each entry and
 * git_index_write_tree_to writes the (possibly nested) tree objects into the
 * storage-backed odb, so libgit2 owns the sorting, subtree creation, and the
 * hash algorithm (inherited from the repository). */

static void fn_git0_mktree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *text = (const char *)sqlite3_value_text(argv[0]);
  if (!text) { sqlite3_result_null(ctx); return; }

  git_repository *repo = git0_storage_repo();
  if (!repo) {
    sqlite3_result_error(ctx, "storage not initialized (call git0_init first)", -1);
    return;
  }

  git_index *idx = NULL;
#ifdef GIT_EXPERIMENTAL_SHA256
  /* The experimental API carries the oid type; match the repository's so the
   * written tree hashes under the same algorithm (mirrors ensure_repo's odb). */
  git_index_options opts = GIT_INDEX_OPTIONS_INIT;
  opts.oid_type = git_repository_oid_type(repo);
  int irc = git_index_new(&idx, &opts);
#else
  int irc = git_index_new(&idx);
#endif
  if (irc < 0) { sqlite3_result_error(ctx, "failed to create index", -1); return; }

  const char *p = text;
  while (*p) {
    /* oid_hex must hold the longest git oid hex (sha256: 64) + NUL to match the
     * %64s field width. GIT_OID_MAX_HEXSIZE is the wrong size here: a libgit2
     * built without GIT_EXPERIMENTAL_SHA256 defines it as 40 (sha1), so %64s
     * would overflow the buffer (ASan-confirmed stack-buffer-overflow). */
    char mode[16], path[256], oid_hex[65];
    if (sscanf(p, "%15s %255s %64s", mode, path, oid_hex) != 3) break;
    git_index_entry e;
    memset(&e, 0, sizeof(e));
    if (oid_from_hex(&e.id, oid_hex) == 0) {
      e.path = path;
      e.mode = (unsigned int)strtol(mode, NULL, 8);
      git_index_add(idx, &e);
    }
    while (*p && *p != '\n') p++;
    if (*p == '\n') p++;
  }

  git_oid tree_oid;
  int rc = git_index_write_tree_to(&tree_oid, idx, repo);
  git_index_free(idx);
  if (rc < 0) {
    sqlite3_result_error(ctx, "failed to build tree", -1);
    return;
  }

  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &tree_oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_mkcommit(tree_oid, parent_oid, message, author?) ---- */

static void fn_git0_mkcommit(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  git_repository *repo = git0_storage_repo();
  if (!repo) {
    sqlite3_result_error(ctx, "storage not initialized (call git0_init first)", -1);
    return;
  }
  const char *tree_hex = (const char *)sqlite3_value_text(argv[0]);
  if (!tree_hex) { sqlite3_result_null(ctx); return; }
  const char *parent_hex = (argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL)
    ? (const char *)sqlite3_value_text(argv[1]) : NULL;
  const char *message = (argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL)
    ? (const char *)sqlite3_value_text(argv[2]) : "commit";
  const char *author_name = (argc > 3 && sqlite3_value_type(argv[3]) != SQLITE_NULL)
    ? (const char *)sqlite3_value_text(argv[3]) : "git0 <git0@sqlite>";

  /* Resolve the tree and optional parent as real objects, then let libgit2
   * build and write the commit (it owns the serialization + hash). */
  git_oid tree_oid;
  git_tree *tree = NULL;
  if (oid_from_hex(&tree_oid, tree_hex) != 0 ||
      git_tree_lookup(&tree, repo, &tree_oid) != 0) {
    sqlite3_result_error(ctx, "mkcommit: tree not found", -1);
    return;
  }

  git_commit *parent = NULL;
  const git_commit *parents[1] = { NULL };
  size_t nparents = 0;
  if (parent_hex) {
    git_oid parent_oid;
    if (oid_from_hex(&parent_oid, parent_hex) != 0 ||
        git_commit_lookup(&parent, repo, &parent_oid) != 0) {
      git_tree_free(tree);
      sqlite3_result_error(ctx, "mkcommit: parent not found", -1);
      return;
    }
    parents[0] = parent;
    nparents = 1;
  }

  /* The author arg is a free-form "Name <email>" identity; format it with the
   * current time and let git_signature_from_buffer parse it (no hand-split). */
  time_t now = time(NULL);
  char sigbuf[600];
  snprintf(sigbuf, sizeof(sigbuf), "%s %lld +0000", author_name, (long long)now);
  git_signature *sig = NULL;
  if (git_signature_from_buffer(&sig, sigbuf) != 0) {
    git_tree_free(tree);
    git_commit_free(parent);
    sqlite3_result_error(ctx, "mkcommit: invalid author identity", -1);
    return;
  }

  char *msg = sqlite3_mprintf("%s\n", message);
  git_oid oid;
  int rc = git_commit_create(&oid, repo, NULL, sig, sig, NULL,
                             msg ? msg : message, tree, nparents, parents);
  sqlite3_free(msg);
  git_signature_free(sig);
  git_tree_free(tree);
  git_commit_free(parent);
  if (rc != 0) {
    sqlite3_result_error(ctx, "mkcommit: commit create failed", -1);
    return;
  }
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- Registration ---- */

int git0_register_repo(sqlite3 *db) {
  sqlite3_create_function(db, "git0_init", 0, SQLITE_UTF8, 0, fn_git0_init, 0, 0);
  sqlite3_create_function(db, "git0_init", 1, SQLITE_UTF8, 0, fn_git0_init, 0, 0);
  sqlite3_create_function(db, "git0_init", 2, SQLITE_UTF8, 0, fn_git0_init, 0, 0);
  sqlite3_create_function(db, "git0_add", 2, SQLITE_UTF8, 0, fn_git0_add, 0, 0);
  sqlite3_create_function(db, "git0_add", 3, SQLITE_UTF8, 0, fn_git0_add, 0, 0);
  sqlite3_create_function(db, "git0_mktree", 1, SQLITE_UTF8, 0, fn_git0_mktree, 0, 0);
  sqlite3_create_function(db, "git0_mkcommit", 3, SQLITE_UTF8, 0, fn_git0_mkcommit, 0, 0);
  sqlite3_create_function(db, "git0_mkcommit", 4, SQLITE_UTF8, 0, fn_git0_mkcommit, 0, 0);
  return SQLITE_OK;
}
