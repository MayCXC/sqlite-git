/*
** git-remote-sqlite: git remote helper for SQLite-backed repos.
**
** Git invokes this as: git-remote-sqlite <remote-name> <url>
** URL format: sqlite:///path/to/repo.db
**
** Implements the git remote helper protocol:
**   capabilities -> list supported operations
**   list         -> list refs
**   fetch <sha> <ref> -> fetch objects
**   push <refspec>    -> push objects
**
** Requires git0 extension loaded for the SQLite database.
** Objects are transferred via pack format (git's native wire format).
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <git2.h>
#include <sqlite3.h>

/* ---- Helpers ---- */

static sqlite3 *open_db(const char *url) {
  /* Parse sqlite:///path/to/db -> /path/to/db */
  const char *path = url;
  if (strncmp(url, "sqlite://", 9) == 0) path = url + 9;
  /* Handle sqlite:/// (three slashes = absolute) */
  /* sqlite://relative also works */

  sqlite3 *db = NULL;
  if (sqlite3_open(path, &db) != SQLITE_OK) {
    fprintf(stderr, "git-remote-sqlite: cannot open %s: %s\n", path, sqlite3_errmsg(db));
    return NULL;
  }

  /* Ensure tables exist */
  sqlite3_exec(db,
    "CREATE TABLE IF NOT EXISTS objects_loose("
    "  oid TEXT PRIMARY KEY, type INTEGER NOT NULL, size INTEGER NOT NULL, data BLOB NOT NULL);"
    "CREATE TABLE IF NOT EXISTS refs_data("
    "  name TEXT PRIMARY KEY, target TEXT NOT NULL, symref TEXT);",
    0, 0, 0);

  return db;
}

static void oid_to_hex(const git_oid *oid, char *out) {
  git_oid_tostr(out, GIT_OID_MAX_HEXSIZE + 1, oid);
}

/* ---- List refs ---- */

static void cmd_list(sqlite3 *db) {
  sqlite3_stmt *st;
  sqlite3_prepare_v2(db,
    "SELECT target, name FROM refs_data ORDER BY name", -1, &st, 0);
  while (sqlite3_step(st) == SQLITE_ROW) {
    const char *target = (const char *)sqlite3_column_text(st, 0);
    const char *name = (const char *)sqlite3_column_text(st, 1);
    if (strcmp(name, "HEAD") == 0) {
      /* Check if HEAD is a symref */
      sqlite3_stmt *st2;
      sqlite3_prepare_v2(db,
        "SELECT symref FROM refs_data WHERE name = 'HEAD' AND symref IS NOT NULL", -1, &st2, 0);
      if (sqlite3_step(st2) == SQLITE_ROW) {
        printf("@%s HEAD\n", sqlite3_column_text(st2, 0));
      } else {
        printf("%s HEAD\n", target);
      }
      sqlite3_finalize(st2);
    } else {
      printf("%s %s\n", target, name);
    }
  }
  sqlite3_finalize(st);
  printf("\n");
  fflush(stdout);
}

/* ---- Fetch: send objects from SQLite to git ---- */

static int object_exists_locally(const char *oid_hex) {
  /* Check if object exists in the local git repo */
  git_oid oid;
  if (git_oid_fromstr(&oid, oid_hex) != 0) return 0;

  git_repository *repo = NULL;
  if (git_repository_open(&repo, ".") != 0) return 0;
  git_odb *odb = NULL;
  git_repository_odb(&odb, repo);
  int exists = git_odb_exists(odb, &oid);
  git_odb_free(odb);
  git_repository_free(repo);
  return exists;
}

static void cmd_fetch(sqlite3 *db, const char *sha, const char *ref) {
  /* Walk the object graph from sha, collect all objects not in local repo,
     write them as a pack to stdout.

     For simplicity, we write each object individually using
     git's fast-import or direct odb writes. */

  git_repository *repo = NULL;
  if (git_repository_open(&repo, ".") != 0) {
    fprintf(stderr, "git-remote-sqlite: cannot open local repo\n");
    return;
  }
  git_odb *odb = NULL;
  git_repository_odb(&odb, repo);

  /* Recursively fetch objects from SQLite and write to local odb */
  /* Start with the requested sha and walk its tree */
  sqlite3_stmt *st;
  sqlite3_prepare_v2(db,
    "SELECT oid, type, data FROM objects_loose WHERE oid = ?", -1, &st, 0);

  /* Simple BFS: queue of oids to process */
  /* For now, just fetch the requested object and let git handle the rest */
  const char *queue[65536];
  int queue_head = 0, queue_tail = 0;
  queue[queue_tail++] = sha;

  while (queue_head < queue_tail) {
    const char *current = queue[queue_head++];

    /* Skip if already exists locally */
    if (object_exists_locally(current)) continue;

    sqlite3_reset(st);
    sqlite3_bind_text(st, 1, current, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) continue;

    int type = sqlite3_column_int(st, 1);
    const void *data = sqlite3_column_blob(st, 2);
    int size = sqlite3_column_bytes(st, 2);

    /* Write to local odb */
    git_oid oid;
    git_odb_write(&oid, odb, data, size, (git_object_t)type);

    /* Parse object to find referenced oids */
    if (type == GIT_OBJECT_COMMIT) {
      /* Parse commit to find tree and parent oids */
      const char *text = (const char *)data;
      const char *p = text;
      while (p < text + size) {
        if (strncmp(p, "tree ", 5) == 0) {
          char tree_oid[41];
          strncpy(tree_oid, p + 5, 40);
          tree_oid[40] = 0;
          if (queue_tail < 65536) queue[queue_tail++] = strdup(tree_oid);
        } else if (strncmp(p, "parent ", 7) == 0) {
          char parent_oid[41];
          strncpy(parent_oid, p + 7, 40);
          parent_oid[40] = 0;
          if (queue_tail < 65536) queue[queue_tail++] = strdup(parent_oid);
        }
        /* Advance to next line */
        while (p < text + size && *p != '\n') p++;
        if (p < text + size) p++;
        /* Stop at empty line (end of headers) */
        if (p < text + size && *p == '\n') break;
      }
    } else if (type == GIT_OBJECT_TREE) {
      /* Parse tree entries to find referenced oids */
      const unsigned char *p = (const unsigned char *)data;
      const unsigned char *end = p + size;
      while (p < end) {
        /* Skip mode */
        while (p < end && *p != ' ') p++;
        if (p >= end) break;
        p++; /* skip space */
        /* Skip name */
        while (p < end && *p != 0) p++;
        if (p >= end) break;
        p++; /* skip null */
        /* Read 20-byte oid */
        if (p + 20 > end) break;
        git_oid entry_oid;
        memcpy(entry_oid.id, p, 20);
        p += 20;
        char entry_hex[41];
        oid_to_hex(&entry_oid, entry_hex);
        if (queue_tail < 65536) queue[queue_tail++] = strdup(entry_hex);
      }
    }
  }

  sqlite3_finalize(st);
  git_odb_free(odb);
  git_repository_free(repo);

  /* Free duplicated strings */
  for (int i = 1; i < queue_tail; i++) {
    free((void *)queue[i]);
  }
}

/* ---- Push: send objects from git to SQLite ---- */

static void cmd_push(sqlite3 *db, const char *refspec) {
  /* Parse refspec: src:dst */
  char src[256] = {0}, dst[256] = {0};
  const char *colon = strchr(refspec, ':');
  if (colon) {
    strncpy(src, refspec, colon - refspec);
    strncpy(dst, colon + 1, sizeof(dst) - 1);
  } else {
    strncpy(src, refspec, sizeof(src) - 1);
    strncpy(dst, refspec, sizeof(dst) - 1);
  }

  /* Resolve src to oid in local repo */
  git_repository *repo = NULL;
  if (git_repository_open(&repo, ".") != 0) {
    printf("error %s failed to open local repo\n", dst);
    return;
  }

  git_object *obj = NULL;
  if (git_revparse_single(&obj, repo, src) != 0) {
    printf("error %s cannot resolve %s\n", dst, src);
    git_repository_free(repo);
    return;
  }

  git_oid target_oid = *git_object_id(obj);
  char target_hex[41];
  oid_to_hex(&target_oid, target_hex);
  git_object_free(obj);

  git_odb *odb = NULL;
  git_repository_odb(&odb, repo);

  /* Walk all reachable objects from target and push to SQLite */
  git_revwalk *walk = NULL;
  git_revwalk_new(&walk, repo);
  git_revwalk_push(walk, &target_oid);
  git_revwalk_sorting(walk, GIT_SORT_TOPOLOGICAL);

  sqlite3_stmt *st_insert;
  sqlite3_prepare_v2(db,
    "INSERT OR IGNORE INTO objects_loose(oid, type, size, data) VALUES(?,?,?,?)",
    -1, &st_insert, 0);

  sqlite3_stmt *st_exists;
  sqlite3_prepare_v2(db, "SELECT 1 FROM objects_loose WHERE oid = ?", -1, &st_exists, 0);

  /* Push commits and their trees/blobs */
  git_oid oid;
  while (git_revwalk_next(&oid, walk) == 0) {
    char hex[41];
    oid_to_hex(&oid, hex);

    /* Check if already in SQLite */
    sqlite3_reset(st_exists);
    sqlite3_bind_text(st_exists, 1, hex, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st_exists) == SQLITE_ROW) continue; /* already there */

    /* Read object from local repo */
    git_odb_object *obj_data = NULL;
    if (git_odb_read(&obj_data, odb, &oid) != 0) continue;

    /* Insert into SQLite */
    sqlite3_reset(st_insert);
    sqlite3_bind_text(st_insert, 1, hex, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st_insert, 2, (int)git_odb_object_type(obj_data));
    sqlite3_bind_int64(st_insert, 3, (sqlite3_int64)git_odb_object_size(obj_data));
    sqlite3_bind_blob(st_insert, 4, git_odb_object_data(obj_data),
                      (int)git_odb_object_size(obj_data), SQLITE_TRANSIENT);
    sqlite3_step(st_insert);

    /* Also push the tree and its blobs */
    if (git_odb_object_type(obj_data) == GIT_OBJECT_COMMIT) {
      git_commit *commit = NULL;
      git_commit_lookup(&commit, repo, &oid);
      if (commit) {
        git_tree *tree = NULL;
        git_commit_tree(&tree, commit);
        if (tree) {
          /* Push tree object */
          git_odb_object *tree_obj = NULL;
          if (git_odb_read(&tree_obj, odb, git_tree_id(tree)) == 0) {
            char tree_hex[41];
            oid_to_hex(git_tree_id(tree), tree_hex);
            sqlite3_reset(st_insert);
            sqlite3_bind_text(st_insert, 1, tree_hex, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st_insert, 2, GIT_OBJECT_TREE);
            sqlite3_bind_int64(st_insert, 3, (sqlite3_int64)git_odb_object_size(tree_obj));
            sqlite3_bind_blob(st_insert, 4, git_odb_object_data(tree_obj),
                              (int)git_odb_object_size(tree_obj), SQLITE_TRANSIENT);
            sqlite3_step(st_insert);
            git_odb_object_free(tree_obj);
          }

          /* Push blobs in tree */
          size_t count = git_tree_entrycount(tree);
          for (size_t i = 0; i < count; i++) {
            const git_tree_entry *entry = git_tree_entry_byindex(tree, i);
            git_odb_object *entry_obj = NULL;
            if (git_odb_read(&entry_obj, odb, git_tree_entry_id(entry)) == 0) {
              char entry_hex[41];
              oid_to_hex(git_tree_entry_id(entry), entry_hex);
              sqlite3_reset(st_insert);
              sqlite3_bind_text(st_insert, 1, entry_hex, -1, SQLITE_TRANSIENT);
              sqlite3_bind_int(st_insert, 2, (int)git_odb_object_type(entry_obj));
              sqlite3_bind_int64(st_insert, 3, (sqlite3_int64)git_odb_object_size(entry_obj));
              sqlite3_bind_blob(st_insert, 4, git_odb_object_data(entry_obj),
                                (int)git_odb_object_size(entry_obj), SQLITE_TRANSIENT);
              sqlite3_step(st_insert);
              git_odb_object_free(entry_obj);
            }
          }
          git_tree_free(tree);
        }
        git_commit_free(commit);
      }
    }

    git_odb_object_free(obj_data);
  }

  sqlite3_finalize(st_insert);
  sqlite3_finalize(st_exists);
  git_revwalk_free(walk);
  git_odb_free(odb);
  git_repository_free(repo);

  /* Update ref in SQLite */
  sqlite3_stmt *st_ref;
  sqlite3_prepare_v2(db,
    "INSERT OR REPLACE INTO refs_data(name, target) VALUES(?, ?)", -1, &st_ref, 0);
  sqlite3_bind_text(st_ref, 1, dst, -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st_ref, 2, target_hex, -1, SQLITE_TRANSIENT);
  sqlite3_step(st_ref);
  sqlite3_finalize(st_ref);

  printf("ok %s\n", dst);
}

/* ---- Main: remote helper protocol loop ---- */

int main(int argc, char **argv) {
  if (argc < 3) {
    fprintf(stderr, "Usage: git-remote-sqlite <remote> <url>\n");
    return 1;
  }

  const char *url = argv[2];
  git_libgit2_init();

  sqlite3 *db = open_db(url);
  if (!db) return 1;

  /* Protocol loop: read commands from stdin */
  char line[4096];
  while (fgets(line, sizeof(line), stdin)) {
    /* Strip newline */
    size_t len = strlen(line);
    if (len > 0 && line[len - 1] == '\n') line[len - 1] = 0;

    if (strcmp(line, "capabilities") == 0) {
      printf("fetch\n");
      printf("push\n");
      printf("\n");
      fflush(stdout);
    } else if (strcmp(line, "list") == 0 || strcmp(line, "list for-push") == 0) {
      cmd_list(db);
    } else if (strncmp(line, "fetch ", 6) == 0) {
      /* May receive multiple fetch lines, process until blank line */
      do {
        char sha[41] = {0}, ref[256] = {0};
        sscanf(line + 6, "%40s %255s", sha, ref);
        cmd_fetch(db, sha, ref);

        if (!fgets(line, sizeof(line), stdin)) break;
        len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') line[len - 1] = 0;
      } while (line[0] != 0);
      printf("\n");
      fflush(stdout);
    } else if (strncmp(line, "push ", 5) == 0) {
      do {
        cmd_push(db, line + 5);

        if (!fgets(line, sizeof(line), stdin)) break;
        len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') line[len - 1] = 0;
      } while (line[0] != 0);
      printf("\n");
      fflush(stdout);
    } else if (line[0] == 0) {
      /* Empty line = end of batch */
    } else {
      fprintf(stderr, "git-remote-sqlite: unknown command: %s\n", line);
    }
  }

  sqlite3_close(db);
  git_libgit2_shutdown();
  return 0;
}
