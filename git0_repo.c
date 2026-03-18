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

  git_oid oid;
  git_odb_hash(&oid, data, data_len, type);
  storage_write_object(&oid, type, data, data_len);

  char hex[GIT_OID_SHA1_HEXSIZE + 1];
  git_oid_tostr(hex, sizeof(hex), &oid);
  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_mktree(entries_text) ---- */
/* entries: "mode name oid\n" per line (sorted by name) */

static void fn_git0_mktree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *entries = (const char *)sqlite3_value_text(argv[0]);
  if (!entries) { sqlite3_result_null(ctx); return; }

  /* Build binary tree format: "mode name\0<20-byte-oid>" per entry */
  unsigned char tree_buf[65536];
  int pos = 0;
  const char *p = entries;

  while (*p) {
    char mode[16], name[256], oid_hex[GIT_OID_SHA1_HEXSIZE + 1];
    if (sscanf(p, "%15s %255s %40s", mode, name, oid_hex) != 3) break;

    int mode_len = strlen(mode);
    int name_len = strlen(name);
    int need = mode_len + 1 + name_len + 1 + GIT_OID_SHA1_SIZE;
    if (pos + need > (int)sizeof(tree_buf)) break;

    memcpy(tree_buf + pos, mode, mode_len); pos += mode_len;
    tree_buf[pos++] = ' ';
    memcpy(tree_buf + pos, name, name_len); pos += name_len;
    tree_buf[pos++] = '\0';

    git_oid entry_oid;
    git_oid_fromstr(&entry_oid, oid_hex);
    memcpy(tree_buf + pos, entry_oid.id, GIT_OID_SHA1_SIZE); pos += GIT_OID_SHA1_SIZE;

    /* Advance to next line */
    while (*p && *p != '\n') p++;
    if (*p == '\n') p++;
  }

  git_oid tree_oid;
  git_odb_hash(&tree_oid, tree_buf, pos, GIT_OBJECT_TREE);
  storage_write_object(&tree_oid, GIT_OBJECT_TREE, tree_buf, pos);

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
