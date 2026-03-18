/*
** git0_repo: self-contained git repo in SQLite.
**
** git0_init(db_path?)  -> creates objects + refs tables, empty tree, initial commit, HEAD
** git0_commit(db_path, message, author?)  -> creates a commit from staged objects
** git0_add(db_path, path, data)  -> stages a blob and tree entry
**
** Also provides bridge functions to read from SQLite-backed repos
** using the same interface as the libgit2-backed scalar functions.
*/

#include "git0.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include <git2.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

extern void git0_oid_to_hex(const git_oid *oid, char *out);

/* ---- Helper: execute SQL on a database ---- */

static int exec_sql(sqlite3 *db, const char *sql, char **err) {
  return sqlite3_exec(db, sql, 0, 0, err);
}

/* ---- Helper: query single text result ---- */

static const char *query_text(sqlite3 *db, const char *sql) {
  sqlite3_stmt *st;
  const char *result = NULL;
  if (sqlite3_prepare_v2(db, sql, -1, &st, 0) == SQLITE_OK) {
    if (sqlite3_step(st) == SQLITE_ROW) {
      const char *t = (const char *)sqlite3_column_text(st, 0);
      if (t) result = sqlite3_mprintf("%s", t);
    }
    sqlite3_finalize(st);
  }
  return result;
}

/* ---- git0_init(db_path?) ---- */

static void fn_git0_init(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  sqlite3 *db;
  int own_db = 0;

  if (argc > 0 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
    /* Open external database */
    const char *path = (const char *)sqlite3_value_text(argv[0]);
    if (sqlite3_open(path, &db) != SQLITE_OK) {
      sqlite3_result_error(ctx, "failed to open database", -1);
      return;
    }
    own_db = 1;
  } else {
    /* Use current database */
    db = sqlite3_context_db_handle(ctx);
  }

  char *err = NULL;
  int rc;

  /* Create tables */
  rc = exec_sql(db,
    "CREATE VIRTUAL TABLE IF NOT EXISTS objects USING git0_objects;"
    "CREATE VIRTUAL TABLE IF NOT EXISTS refs USING git0_refs;",
    &err);

  if (rc != SQLITE_OK) {
    sqlite3_result_error(ctx, err ? err : "failed to create tables", -1);
    if (err) sqlite3_free(err);
    if (own_db) sqlite3_close(db);
    return;
  }

  /* Check if already initialized (HEAD exists) */
  const char *head = query_text(db, "SELECT oid FROM refs WHERE refname = 'HEAD'");
  if (head) {
    sqlite3_free((void *)head);
    sqlite3_result_text(ctx, "already initialized", -1, SQLITE_STATIC);
    if (own_db) sqlite3_close(db);
    return;
  }

  /* Create empty tree (zero-length tree object) */
  git_oid empty_tree_oid;
  git_odb_hash(&empty_tree_oid, NULL, 0, GIT_OBJECT_TREE);
  char tree_hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&empty_tree_oid, tree_hex);

  /* Insert empty tree object */
  sqlite3_stmt *st;
  sqlite3_prepare_v2(db,
    "INSERT OR IGNORE INTO objects(oid, type, size, data) VALUES(?, ?, 0, X'')", -1, &st, 0);
  sqlite3_bind_blob(st, 1, empty_tree_oid.id, 20, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 2, GIT_OBJECT_TREE);
  sqlite3_step(st);
  sqlite3_finalize(st);

  /* Create initial commit object */
  time_t now = time(NULL);
  char author_line[256];
  snprintf(author_line, sizeof(author_line), "git0 <git0@sqlite> %lld +0000", (long long)now);

  /* Build raw commit content */
  char commit_buf[2048];
  int commit_len = snprintf(commit_buf, sizeof(commit_buf),
    "tree %s\n"
    "author %s\n"
    "committer %s\n"
    "\n"
    "initial commit\n",
    tree_hex, author_line, author_line);

  git_oid commit_oid;
  git_odb_hash(&commit_oid, commit_buf, commit_len, GIT_OBJECT_COMMIT);
  char commit_hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&commit_oid, commit_hex);

  /* Insert commit object */
  sqlite3_prepare_v2(db,
    "INSERT OR IGNORE INTO objects(oid, type, size, data) VALUES(?, ?, ?, ?)", -1, &st, 0);
  sqlite3_bind_blob(st, 1, commit_oid.id, 20, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 2, GIT_OBJECT_COMMIT);
  sqlite3_bind_int(st, 3, commit_len);
  sqlite3_bind_blob(st, 4, commit_buf, commit_len, SQLITE_TRANSIENT);
  sqlite3_step(st);
  sqlite3_finalize(st);

  /* Set HEAD and main branch */
  sqlite3_prepare_v2(db,
    "INSERT INTO refs(refname, oid) VALUES(?, ?)", -1, &st, 0);

  sqlite3_bind_text(st, 1, "refs/heads/main", -1, SQLITE_STATIC);
  sqlite3_bind_text(st, 2, commit_hex, -1, SQLITE_TRANSIENT);
  sqlite3_step(st);
  sqlite3_reset(st);

  sqlite3_bind_text(st, 1, "HEAD", -1, SQLITE_STATIC);
  sqlite3_bind_text(st, 2, commit_hex, -1, SQLITE_TRANSIENT);
  sqlite3_step(st);
  sqlite3_finalize(st);

  sqlite3_result_text(ctx, commit_hex, -1, SQLITE_TRANSIENT);
  if (own_db) sqlite3_close(db);
}

/* ---- git0_add(path, data, type?) ---- */
/* Adds a blob to the objects table and returns the oid.
   Works on the current database connection. */

static void fn_git0_add(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  sqlite3 *db = sqlite3_context_db_handle(ctx);

  const void *data = sqlite3_value_blob(argv[1]);
  int data_len = sqlite3_value_bytes(argv[1]);
  const char *type_str = argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[2]) : "blob";
  int type = GIT_OBJECT_BLOB;
  if (strcmp(type_str, "tree") == 0) type = GIT_OBJECT_TREE;
  else if (strcmp(type_str, "commit") == 0) type = GIT_OBJECT_COMMIT;
  else if (strcmp(type_str, "tag") == 0) type = GIT_OBJECT_TAG;

  git_oid oid;
  git_odb_hash(&oid, data, data_len, type);
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&oid, hex);

  sqlite3_stmt *st;
  sqlite3_prepare_v2(db,
    "INSERT OR IGNORE INTO objects(oid, type, size, data) VALUES(?, ?, ?, ?)", -1, &st, 0);
  sqlite3_bind_blob(st, 1, oid.id, 20, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 2, type);
  sqlite3_bind_int(st, 3, data_len);
  sqlite3_bind_blob(st, 4, data, data_len, SQLITE_TRANSIENT);
  int rc = sqlite3_step(st);
  sqlite3_finalize(st);

  if (rc != SQLITE_DONE) {
    sqlite3_result_error(ctx, "failed to add object", -1);
    return;
  }

  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_tree(json_entries) ---- */
/* Creates a tree object from a JSON array of entries.
   Each entry: {"name":"file.txt", "mode":"100644", "oid":"abc123..."}
   Returns the tree oid. */

static void fn_git0_mktree(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *json = (const char *)sqlite3_value_text(argv[0]);

  /* Build tree object directly in binary format.
     Git tree format: repeated entries of "mode name\0oid_binary" */
  /* Input: tab-separated lines "mode\toid_hex\tname\n..." */
  unsigned char tree_buf[65536];
  int tree_len = 0;

  char *input = sqlite3_mprintf("%s", json);
  char *line = input;
  while (line && *line) {
    char *nl = strchr(line, '\n');
    if (nl) *nl = 0;

    char *tab1 = strchr(line, '\t');
    if (!tab1) { line = nl ? nl + 1 : NULL; continue; }
    *tab1 = 0;
    char *tab2 = strchr(tab1 + 1, '\t');
    if (!tab2) { line = nl ? nl + 1 : NULL; continue; }
    *tab2 = 0;

    const char *mode_str = line;
    const char *oid_str = tab1 + 1;
    const char *name = tab2 + 1;

    /* Write: "mode name\0" */
    int n = snprintf((char *)tree_buf + tree_len, sizeof(tree_buf) - tree_len,
                     "%s %s", mode_str, name);
    tree_len += n + 1; /* include null byte */

    /* Write: 20-byte binary OID */
    git_oid oid;
    git_oid_fromstr(&oid, oid_str);
#if LIBGIT2_VER_MAJOR > 1 || (LIBGIT2_VER_MAJOR == 1 && LIBGIT2_VER_MINOR >= 8)
    memcpy(tree_buf + tree_len, oid.id, GIT_OID_SHA1_SIZE);
    tree_len += GIT_OID_SHA1_SIZE;
#else
    memcpy(tree_buf + tree_len, oid.id, 20);
    tree_len += 20;
#endif

    line = nl ? nl + 1 : NULL;
  }
  sqlite3_free(input);

  /* Hash the tree object */
  git_oid tree_oid;
  git_odb_hash(&tree_oid, tree_buf, tree_len, GIT_OBJECT_TREE);
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&tree_oid, hex);

  /* Insert tree into objects */
  sqlite3_stmt *st_tree;
  sqlite3_prepare_v2(db,
    "INSERT OR IGNORE INTO objects(oid, type, size, data) VALUES(?, ?, ?, ?)", -1, &st_tree, 0);
  sqlite3_bind_text(st_tree, 1, hex, -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(st_tree, 2, GIT_OBJECT_TREE);
  sqlite3_bind_int(st_tree, 3, tree_len);
  sqlite3_bind_blob(st_tree, 4, tree_buf, tree_len, SQLITE_TRANSIENT);
  sqlite3_step(st_tree);
  sqlite3_finalize(st_tree);

  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- git0_mkcommit(tree_oid, parent_oid, message, author?, committer?) ---- */
/* Creates a commit object and inserts it. Returns the commit oid. */

static void fn_git0_mkcommit(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  sqlite3 *db = sqlite3_context_db_handle(ctx);

  const char *tree_oid = (const char *)sqlite3_value_text(argv[0]);
  const char *parent_oid = argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[1]) : NULL;
  const char *message = argc > 2 ? (const char *)sqlite3_value_text(argv[2]) : "commit";
  const char *author = argc > 3 && sqlite3_value_type(argv[3]) != SQLITE_NULL
    ? (const char *)sqlite3_value_text(argv[3]) : NULL;

  time_t now = time(NULL);
  char default_author[256];
  if (!author) {
    snprintf(default_author, sizeof(default_author), "git0 <git0@sqlite> %lld +0000", (long long)now);
    author = default_author;
  }

  /* Build raw commit */
  char commit_buf[65536];
  int len = 0;
  len += snprintf(commit_buf + len, sizeof(commit_buf) - len, "tree %s\n", tree_oid);
  if (parent_oid) {
    len += snprintf(commit_buf + len, sizeof(commit_buf) - len, "parent %s\n", parent_oid);
  }
  len += snprintf(commit_buf + len, sizeof(commit_buf) - len,
    "author %s\n"
    "committer %s\n"
    "\n"
    "%s\n",
    author, author, message);

  git_oid oid;
  git_odb_hash(&oid, commit_buf, len, GIT_OBJECT_COMMIT);
  char hex[GIT_OID_MAX_HEXSIZE + 1];
  git0_oid_to_hex(&oid, hex);

  sqlite3_stmt *st;
  sqlite3_prepare_v2(db,
    "INSERT OR IGNORE INTO objects(oid, type, size, data) VALUES(?, ?, ?, ?)", -1, &st, 0);
  sqlite3_bind_blob(st, 1, oid.id, 20, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 2, GIT_OBJECT_COMMIT);
  sqlite3_bind_int(st, 3, len);
  sqlite3_bind_blob(st, 4, commit_buf, len, SQLITE_TRANSIENT);
  sqlite3_step(st);
  sqlite3_finalize(st);

  sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
}

/* ---- Registration ---- */

int git0_register_repo(sqlite3 *db) {
  sqlite3_create_function(db, "git0_init", 0, SQLITE_UTF8, 0, fn_git0_init, 0, 0);
  sqlite3_create_function(db, "git0_init", 1, SQLITE_UTF8, 0, fn_git0_init, 0, 0);
  sqlite3_create_function(db, "git0_add", 2, SQLITE_UTF8, 0, fn_git0_add, 0, 0);
  sqlite3_create_function(db, "git0_add", 3, SQLITE_UTF8, 0, fn_git0_add, 0, 0);
  sqlite3_create_function(db, "git0_mktree", 1, SQLITE_UTF8, 0, fn_git0_mktree, 0, 0);
  sqlite3_create_function(db, "git0_mkcommit", 3, SQLITE_UTF8, 0, fn_git0_mkcommit, 0, 0);
  sqlite3_create_function(db, "git0_mkcommit", 4, SQLITE_UTF8, 0, fn_git0_mkcommit, 0, 0);
  sqlite3_create_function(db, "git0_mkcommit", 5, SQLITE_UTF8, 0, fn_git0_mkcommit, 0, 0);
  return SQLITE_OK;
}
