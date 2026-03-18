/*
** git0_repo: self-contained git repo in SQLite.
**
** git0_init()           -> create tables, empty tree, initial commit, HEAD
** git0_add(path, data)  -> store blob, return hex oid
** git0_mktree(entries)  -> build tree object from "mode name oid\n" entries
** git0_mkcommit(tree, parent, msg, author?) -> build commit object
**
** All operations go through the storage layer (storage.h).
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
#include <time.h>



/* ---- git0_init() ---- */

static void fn_git0_init(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc; (void)argv;

  /* Lazily initialize storage if not already open */
  if (!storage_db())
    storage_open_db(sqlite3_context_db_handle(ctx), 0);

  /* Check if already initialized */
  git_oid head_oid;
  char symref[256];
  if (storage_ref_read("HEAD", &head_oid, symref, sizeof(symref)) == 0) {
    char hex[GIT_OID_SHA1_HEXSIZE + 1];
    git_oid_tostr(hex, sizeof(hex), &head_oid);
    sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
    return;
  }

  /* Create empty tree */
  git_oid empty_tree_oid;
  git_odb_hash(&empty_tree_oid, NULL, 0, GIT_OBJECT_TREE);
  storage_write_object(&empty_tree_oid, GIT_OBJECT_TREE, "", 0);

  /* Build initial commit */
  time_t now = time(NULL);
  char tree_hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(tree_hex, sizeof(tree_hex), &empty_tree_oid);

  char author[256];
  snprintf(author, sizeof(author), "git0 <git0@sqlite> %lld +0000", (long long)now);

  char commit_buf[2048];
  int commit_len = snprintf(commit_buf, sizeof(commit_buf),
    "tree %s\nauthor %s\ncommitter %s\n\ninitial commit\n",
    tree_hex, author, author);

  git_oid commit_oid;
  git_odb_hash(&commit_oid, commit_buf, commit_len, GIT_OBJECT_COMMIT);
  storage_write_object(&commit_oid, GIT_OBJECT_COMMIT, commit_buf, commit_len);

  /* Set refs */
  storage_ref_write("refs/heads/main", &commit_oid, NULL);
  storage_ref_write("HEAD", NULL, "refs/heads/main");

  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &commit_oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_add(path, data, type?) ---- */

static void fn_git0_add(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const void *data = sqlite3_value_blob(argv[1]);
  int data_len = sqlite3_value_bytes(argv[1]);
  git_object_t type = GIT_OBJECT_BLOB;

  if (argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL) {
    type = git_object_string2type((const char *)sqlite3_value_text(argv[2]));
    if (type == GIT_OBJECT_INVALID) type = GIT_OBJECT_BLOB;
  }

  const char *path = (const char *)sqlite3_value_text(argv[0]);

  git_oid oid;
  git_odb_hash(&oid, data, data_len, type);
  storage_write_object_named(&oid, type, data, data_len, path);

  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_mktree(entries_text) ---- */
/* entries: "mode path oid\n" per line
 * Paths with '/' create nested subtrees automatically.
 * Uses git_treebuilder from libgit2 for correct sorting and format. */

struct parsed_entry {
  char mode[16];
  char path[256];
  char oid_hex[GIT_OID_SHA1_HEXSIZE + 1];
};

static int mktree_recursive(git_repository *repo,
                             struct parsed_entry *entries, int count,
                             git_oid *out) {
  git_treebuilder *bld = NULL;
  if (git_treebuilder_new(&bld, repo, NULL) < 0) return -1;

  /* Collect unique top-level directories */
  char dirs[256][256];
  int dir_count = 0;

  for (int i = 0; i < count; i++) {
    char *slash = strchr(entries[i].path, '/');
    if (!slash) {
      /* Direct entry: insert into treebuilder */
      git_oid oid;
      if (git_oid_fromstr(&oid, entries[i].oid_hex) < 0) continue;
      unsigned int mode = (unsigned int)strtol(entries[i].mode, NULL, 8);
      git_treebuilder_insert(NULL, bld, entries[i].path, &oid, (git_filemode_t)mode);
    } else {
      /* Collect unique directory names */
      int dlen = (int)(slash - entries[i].path);
      char dir[256];
      snprintf(dir, sizeof(dir), "%.*s", dlen, entries[i].path);
      int found = 0;
      for (int j = 0; j < dir_count; j++) {
        if (!strcmp(dirs[j], dir)) { found = 1; break; }
      }
      if (!found && dir_count < 256)
        strcpy(dirs[dir_count++], dir);
    }
  }

  /* Recurse for each subdirectory */
  for (int d = 0; d < dir_count; d++) {
    struct parsed_entry sub[1024];
    int sub_count = 0;
    int dlen = strlen(dirs[d]);

    for (int i = 0; i < count; i++) {
      if (strncmp(entries[i].path, dirs[d], dlen) != 0 ||
          entries[i].path[dlen] != '/') continue;
      strcpy(sub[sub_count].mode, entries[i].mode);
      strcpy(sub[sub_count].path, entries[i].path + dlen + 1);
      strcpy(sub[sub_count].oid_hex, entries[i].oid_hex);
      sub_count++;
    }

    git_oid subtree_oid;
    if (mktree_recursive(repo, sub, sub_count, &subtree_oid) < 0) {
      git_treebuilder_free(bld);
      return -1;
    }
    git_treebuilder_insert(NULL, bld, dirs[d], &subtree_oid, GIT_FILEMODE_TREE);
  }

  int rc = git_treebuilder_write(out, bld);
  git_treebuilder_free(bld);
  return rc;
}

static void fn_git0_mktree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *text = (const char *)sqlite3_value_text(argv[0]);
  if (!text) { sqlite3_result_null(ctx); return; }

  git_repository *repo = git0_storage_repo();
  if (!repo) {
    sqlite3_result_error(ctx, "storage not initialized (call git0_init first)", -1);
    return;
  }

  /* Parse entries */
  struct parsed_entry entries[1024];
  int count = 0;
  const char *p = text;
  while (*p && count < 1024) {
    if (sscanf(p, "%15s %255s %40s",
               entries[count].mode, entries[count].path,
               entries[count].oid_hex) != 3) break;
    count++;
    while (*p && *p != '\n') p++;
    if (*p == '\n') p++;
  }

  git_oid tree_oid;
  if (mktree_recursive(repo, entries, count, &tree_oid) < 0) {
    sqlite3_result_error(ctx, "failed to build tree", -1);
    return;
  }

  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &tree_oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_mkcommit(tree_oid, parent_oid, message, author?) ---- */

static void fn_git0_mkcommit(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  const char *tree_hex = (const char *)sqlite3_value_text(argv[0]);
  const char *parent_hex = (argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL)
    ? (const char *)sqlite3_value_text(argv[1]) : NULL;
  const char *message = (argc > 2) ? (const char *)sqlite3_value_text(argv[2]) : "commit";
  const char *author_name = (argc > 3 && sqlite3_value_type(argv[3]) != SQLITE_NULL)
    ? (const char *)sqlite3_value_text(argv[3]) : "git0 <git0@sqlite>";

  if (!tree_hex) { sqlite3_result_null(ctx); return; }

  time_t now = time(NULL);
  char author_line[512];
  snprintf(author_line, sizeof(author_line), "%s %lld +0000", author_name, (long long)now);

  char commit_buf[65536];
  int len = snprintf(commit_buf, sizeof(commit_buf), "tree %s\n", tree_hex);
  if (parent_hex)
    len += snprintf(commit_buf + len, sizeof(commit_buf) - len, "parent %s\n", parent_hex);
  len += snprintf(commit_buf + len, sizeof(commit_buf) - len,
    "author %s\ncommitter %s\n\n%s\n", author_line, author_line, message);

  git_oid oid;
  git_odb_hash(&oid, commit_buf, len, GIT_OBJECT_COMMIT);
  storage_write_object(&oid, GIT_OBJECT_COMMIT, commit_buf, len);

  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- Registration ---- */

int git0_register_repo(sqlite3 *db) {
  sqlite3_create_function(db, "git0_init", 0, SQLITE_UTF8, 0, fn_git0_init, 0, 0);
  sqlite3_create_function(db, "git0_add", 2, SQLITE_UTF8, 0, fn_git0_add, 0, 0);
  sqlite3_create_function(db, "git0_add", 3, SQLITE_UTF8, 0, fn_git0_add, 0, 0);
  sqlite3_create_function(db, "git0_mktree", 1, SQLITE_UTF8, 0, fn_git0_mktree, 0, 0);
  sqlite3_create_function(db, "git0_mkcommit", 3, SQLITE_UTF8, 0, fn_git0_mkcommit, 0, 0);
  sqlite3_create_function(db, "git0_mkcommit", 4, SQLITE_UTF8, 0, fn_git0_mkcommit, 0, 0);
  return SQLITE_OK;
}
