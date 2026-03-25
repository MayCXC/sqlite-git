/*
 * Shared storage layer for sqlite-git.
 *
 * All operations use git_oid (binary 20-byte) and git_object_t
 * from libgit2. No hex conversion in the storage API; that happens
 * at the protocol boundary in the helpers.
 */
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT3
#else
  #include <sqlite3.h>
#endif
#include <git2.h>
#include "storage.h"
#include "git0_internal.h"
#include "vendor/delta.h"
#include "vendor/sha256.h"

/* Guard against circular delta chains in corrupt databases.
 * Git enforces no depth limit on read; this is our safety net.
 * 4095 matches pack-objects.h structural max (12-bit depth field). */
#define MAX_DELTA_DEPTH 4095

static sqlite3 *sdb;
static int sdb_owned; /* 1 if we opened the db, 0 if borrowed */

static sqlite3_stmt *st_obj_read, *st_obj_write, *st_obj_exists, *st_obj_list;
static sqlite3_stmt *st_find_base, *st_find_base_named;
static sqlite3_stmt *st_ref_read, *st_ref_write, *st_ref_delete, *st_ref_list;
static sqlite3_stmt *st_reflog_read, *st_reflog_read_rev, *st_reflog_append;
static sqlite3_stmt *st_reflog_exists, *st_reflog_delete, *st_reflog_list;
static sqlite3_stmt *st_lfs_read, *st_lfs_write, *st_lfs_exists;
static sqlite3_stmt *st_mark_kept, *st_clear_kept, *st_have_kept;
static sqlite3_stmt *st_mark_promisor;
static sqlite3_stmt *st_obj_oids;
static sqlite3_stmt *st_obj_list_promisor, *st_obj_list_skip_kept;
static int batch_kept = 0, batch_promisor = 0;

/* Feature 7: Alternates linked list */
struct alternate {
	sqlite3 *db;
	sqlite3_stmt *read_stmt;
	char *path;
	struct alternate *next;
};
static struct alternate *alt_list;

/* Forward declarations for alternates */
static void load_alternates(void);
static int read_from_alternate(struct alternate *alt, const git_oid *oid,
			       git_object_t *out_type, size_t *out_size,
			       unsigned char **out_data, int depth);

/*
 * Statement lifecycle: in owned mode (helper binary), statements are
 * prepared once at open and cached in st_* globals. In borrowed mode
 * (extension), st_* are NULL and each function prepares/finalizes
 * per call to avoid keeping statements alive past sqlite3_close(v1).
 *
 * stmt_acquire: returns cached statement (reset) or prepares fresh.
 * stmt_release: finalizes if freshly prepared, no-op if cached.
 */
static sqlite3_stmt *stmt_acquire(sqlite3_stmt *cached, const char *sql) {
	if (cached) {
		sqlite3_reset(cached);
		return cached;
	}
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb, sql, -1, &st, 0);
	return st;
}

static void stmt_release(sqlite3_stmt *cached, sqlite3_stmt *used) {
	if (used != cached)
		sqlite3_finalize(used);
}

/* ---- Zlib ---- */

static unsigned char *zcompress(const void *src, unsigned long srclen,
				unsigned long *outlen) {
	unsigned long bound = compressBound(srclen);
	unsigned char *dst = malloc(bound);
	if (!dst) return NULL;
	if (compress2(dst, &bound, src, srclen, Z_DEFAULT_COMPRESSION) != Z_OK) {
		free(dst);
		return NULL;
	}
	*outlen = bound;
	return dst;
}

static unsigned char *zdecompress(const void *src, unsigned long srclen,
				  unsigned long orig) {
	unsigned char *dst = malloc(orig);
	unsigned long dstlen = orig;
	if (!dst) return NULL;
	if (uncompress(dst, &dstlen, src, srclen) != Z_OK) {
		free(dst);
		return NULL;
	}
	return dst;
}

/* ---- Delta base selection ---- */

static int find_delta_base(git_object_t type, size_t target_size,
			   const char *path_hint,
			   git_oid *base_oid,
			   unsigned char **base_data, unsigned long *base_len) {
	/*
	 * Name-aware delta base selection (matching git and fossil heuristics).
	 * If a path hint is provided, prefer a same-name object first.
	 * Fall back to closest size if no same-name match exists.
	 */
	sqlite3_stmt *st;
	sqlite3_stmt *cached;
	if (path_hint && *path_hint) {
		cached = st_find_base_named;
		st = stmt_acquire(st_find_base_named,
			"SELECT oid, size, data, base FROM objects"
			" WHERE type = ? AND base IS NULL AND path = ?"
			" ORDER BY ABS(size - ?) LIMIT 1");
		sqlite3_bind_int(st, 1, (int)type);
		sqlite3_bind_text(st, 2, path_hint, -1, SQLITE_STATIC);
		sqlite3_bind_int64(st, 3, (sqlite3_int64)target_size);
		if (sqlite3_step(st) != SQLITE_ROW) {
			/* No same-name match, fall back to size-only */
			stmt_release(cached, st);
			path_hint = NULL; /* fall through to size-only below */
		}
	}

	if (!path_hint || !*path_hint) {
		cached = st_find_base;
		st = stmt_acquire(st_find_base,
			"SELECT oid, size, data, base FROM objects"
			" WHERE type = ? AND base IS NULL"
			" ORDER BY ABS(size - ?) LIMIT 1");
		sqlite3_bind_int(st, 1, (int)type);
		sqlite3_bind_int64(st, 2, (sqlite3_int64)target_size);
		if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(cached, st); return -1; }
	}

	const void *oid_blob = sqlite3_column_blob(st, 0);
	if (!oid_blob) { stmt_release(cached, st); return -1; }

	memcpy(base_oid->id, oid_blob, GIT_OID_SHA1_SIZE);
	size_t sz = (size_t)sqlite3_column_int64(st, 1);

	/* Size filter from git-core: skip if target < base/32 */
	if (target_size < sz / 32) { stmt_release(cached, st); return -1; }

	const void *comp = sqlite3_column_blob(st, 2);
	int comp_len = sqlite3_column_bytes(st, 2);
	if (!comp) { stmt_release(cached, st); return -1; }

	if (sqlite3_column_blob(st, 3) != NULL) { stmt_release(cached, st); return -1; }

	*base_data = zdecompress(comp, comp_len, sz);
	if (!*base_data) { stmt_release(cached, st); return -1; }
	*base_len = sz;
	stmt_release(cached, st);
	return 0;
}

/* ---- Database ---- */

static int storage_init_db(sqlite3 *db);

int storage_open(const char *path_arg) {
	size_t len = strlen(path_arg);
	char *gitdir = sqlite3_mprintf("%s", path_arg);
	if (!gitdir) return -1;

	if (len > 8 && !strcmp(gitdir + len - 8, "/objects"))
		gitdir[len - 8] = '\0';

	mkdir(gitdir, 0755);
	char *path = sqlite3_mprintf("%s/sqlite.db", gitdir);
	sqlite3_free(gitdir);
	if (!path) return -1;

	int rc = sqlite3_open(path, &sdb);
	sqlite3_free(path);
	if (rc != SQLITE_OK)
		return -1;
	sdb_owned = 1;

	return storage_init_db(sdb);
}

int storage_open_db(sqlite3 *db, int persistent) {
	if (sdb) return 0;
	sdb = db;
	if (persistent) {
		/* Caller manages cleanup (e.g. calls storage_close or uses
		 * sqlite3_close_v2). Cache statements for performance. */
		sdb_owned = 0;
		return storage_init_db(sdb);
	}
	/* Non-persistent: create schema only, statements prepared per call.
	 * Safe with sqlite3_close (v1) since no statements survive calls. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS objects("
		"  oid BLOB PRIMARY KEY, type INTEGER NOT NULL,"
		"  size INTEGER NOT NULL, data BLOB NOT NULL,"
		"  base BLOB, path TEXT,"
		"  kept INTEGER NOT NULL DEFAULT 0,"
		"  promisor INTEGER NOT NULL DEFAULT 0"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS refs("
		"  refname TEXT PRIMARY KEY, oid BLOB, symref TEXT"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS reflog("
		"  refname TEXT NOT NULL, idx INTEGER NOT NULL,"
		"  old_oid BLOB NOT NULL, new_oid BLOB NOT NULL,"
		"  committer TEXT NOT NULL, timestamp INTEGER NOT NULL,"
		"  tz INTEGER NOT NULL, msg TEXT NOT NULL DEFAULT '',"
		"  PRIMARY KEY(refname, idx)) WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS lfs("
		"  oid BLOB PRIMARY KEY,"
		"  size INTEGER NOT NULL,"
		"  data BLOB NOT NULL"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS oid_map("
		"  src BLOB NOT NULL, dest BLOB NOT NULL,"
		"  algo TEXT NOT NULL,"
		"  PRIMARY KEY(src, algo)"
		") WITHOUT ROWID;"
		"CREATE INDEX IF NOT EXISTS idx_objects_type_base"
		"  ON objects(type, size) WHERE base IS NULL;"
		"CREATE INDEX IF NOT EXISTS idx_objects_type"
		"  ON objects(type);"
		"CREATE INDEX IF NOT EXISTS idx_objects_path"
		"  ON objects(path) WHERE path IS NOT NULL;"
		"CREATE INDEX IF NOT EXISTS idx_reflog_ts"
		"  ON reflog(timestamp);"
		"CREATE TABLE IF NOT EXISTS promised("
		"  oid BLOB PRIMARY KEY, remote TEXT NOT NULL"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS promisor_remotes("
		"  name TEXT PRIMARY KEY, url TEXT NOT NULL"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS commit_graph("
		"  oid BLOB PRIMARY KEY, generation INT NOT NULL,"
		"  commit_time INT NOT NULL"
		") WITHOUT ROWID;"
		"CREATE INDEX IF NOT EXISTS idx_commit_graph_gen"
		"  ON commit_graph(generation);"
		"CREATE TABLE IF NOT EXISTS worktrees("
		"  name TEXT PRIMARY KEY, path TEXT NOT NULL,"
		"  head_ref TEXT NOT NULL DEFAULT 'refs/heads/main'"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS alternates("
		"  path TEXT PRIMARY KEY"
		") WITHOUT ROWID;",
		0, 0, 0);
	return 0;
}

static int storage_init_db(sqlite3 *db) {
	sqlite3_exec(db,
		"PRAGMA busy_timeout = 5000;"
		"PRAGMA synchronous = NORMAL;"
		"PRAGMA journal_mode = WAL;"
		"PRAGMA page_size = 8192;"
		"CREATE TABLE IF NOT EXISTS objects("
		"  oid BLOB PRIMARY KEY, type INTEGER NOT NULL,"
		"  size INTEGER NOT NULL, data BLOB NOT NULL,"
		"  base BLOB, path TEXT,"
		"  kept INTEGER NOT NULL DEFAULT 0,"
		"  promisor INTEGER NOT NULL DEFAULT 0"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS refs("
		"  refname TEXT PRIMARY KEY, oid BLOB, symref TEXT"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS reflog("
		"  refname TEXT NOT NULL, idx INTEGER NOT NULL,"
		"  old_oid BLOB NOT NULL, new_oid BLOB NOT NULL,"
		"  committer TEXT NOT NULL, timestamp INTEGER NOT NULL,"
		"  tz INTEGER NOT NULL, msg TEXT NOT NULL DEFAULT '',"
		"  PRIMARY KEY(refname, idx)) WITHOUT ROWID;"

		"CREATE TABLE IF NOT EXISTS lfs("
		"  oid BLOB PRIMARY KEY,"
		"  size INTEGER NOT NULL,"
		"  data BLOB NOT NULL"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS oid_map("
		"  src BLOB NOT NULL, dest BLOB NOT NULL,"
		"  algo TEXT NOT NULL,"
		"  PRIMARY KEY(src, algo)"
		") WITHOUT ROWID;",
		0, 0, 0);

	/* Feature 3: Partial clone tables */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS promised("
		"  oid BLOB PRIMARY KEY, remote TEXT NOT NULL"
		") WITHOUT ROWID;"
		"CREATE TABLE IF NOT EXISTS promisor_remotes("
		"  name TEXT PRIMARY KEY, url TEXT NOT NULL"
		") WITHOUT ROWID;",
		0, 0, 0);

	/* Feature 4: Commit graph */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS commit_graph("
		"  oid BLOB PRIMARY KEY, generation INT NOT NULL,"
		"  commit_time INT NOT NULL"
		") WITHOUT ROWID;"
		"CREATE INDEX IF NOT EXISTS idx_commit_graph_gen"
		"  ON commit_graph(generation);",
		0, 0, 0);

	/* Feature 6: Worktrees */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS worktrees("
		"  name TEXT PRIMARY KEY, path TEXT NOT NULL,"
		"  head_ref TEXT NOT NULL DEFAULT 'refs/heads/main'"
		") WITHOUT ROWID;",
		0, 0, 0);

	/* Feature 7: Alternates */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS alternates("
		"  path TEXT PRIMARY KEY"
		") WITHOUT ROWID;",
		0, 0, 0);

	/* Migrate existing databases: add kept and promisor columns */
	sqlite3_exec(db,
		"ALTER TABLE objects ADD COLUMN kept INTEGER NOT NULL DEFAULT 0;"
		"ALTER TABLE objects ADD COLUMN promisor INTEGER NOT NULL DEFAULT 0;",
		0, 0, 0);

	sqlite3_prepare_v3(db, "SELECT type, size, data, base FROM objects WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_read, 0);
	sqlite3_prepare_v3(db, "INSERT OR IGNORE INTO objects(oid, type, size, data, base, path, kept, promisor) VALUES(?,?,?,?,?,?,?,?)",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_write, 0);
	sqlite3_prepare_v3(db, "SELECT 1 FROM objects WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_exists, 0);
	sqlite3_prepare_v3(db, "SELECT oid, type, size FROM objects",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_list, 0);
	sqlite3_prepare_v3(db, "SELECT oid, size, data, base FROM objects WHERE type = ? AND base IS NULL ORDER BY ABS(size - ?) LIMIT 1",  -1, SQLITE_PREPARE_PERSISTENT, &st_find_base, 0);
	sqlite3_prepare_v3(db, "SELECT oid, size, data, base FROM objects WHERE type = ? AND base IS NULL AND path = ? ORDER BY ABS(size - ?) LIMIT 1",  -1, SQLITE_PREPARE_PERSISTENT, &st_find_base_named, 0);
	sqlite3_prepare_v3(db, "SELECT oid, symref FROM refs WHERE refname = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_ref_read, 0);
	sqlite3_prepare_v3(db, "INSERT OR REPLACE INTO refs(refname, oid, symref) VALUES(?,?,?)",  -1, SQLITE_PREPARE_PERSISTENT, &st_ref_write, 0);
	sqlite3_prepare_v3(db, "DELETE FROM refs WHERE refname = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_ref_delete, 0);
	sqlite3_prepare_v3(db, "SELECT refname, oid, symref FROM refs ORDER BY refname",  -1, SQLITE_PREPARE_PERSISTENT, &st_ref_list, 0);
	sqlite3_prepare_v3(db, "SELECT old_oid, new_oid, committer, timestamp, tz, msg FROM reflog WHERE refname = ? ORDER BY idx ASC",  -1, SQLITE_PREPARE_PERSISTENT, &st_reflog_read, 0);
	sqlite3_prepare_v3(db, "SELECT old_oid, new_oid, committer, timestamp, tz, msg FROM reflog WHERE refname = ? ORDER BY idx DESC",  -1, SQLITE_PREPARE_PERSISTENT, &st_reflog_read_rev, 0);
	sqlite3_prepare_v3(db, "SELECT DISTINCT refname FROM reflog ORDER BY refname",  -1, SQLITE_PREPARE_PERSISTENT, &st_reflog_list, 0);
	sqlite3_prepare_v3(db, "INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg) VALUES(?, COALESCE((SELECT MAX(idx)+1 FROM reflog WHERE refname = ?), 0), ?, ?, ?, ?, ?, ?)",  -1, SQLITE_PREPARE_PERSISTENT, &st_reflog_append, 0);
	sqlite3_prepare_v3(db, "SELECT 1 FROM reflog WHERE refname = ? LIMIT 1",  -1, SQLITE_PREPARE_PERSISTENT, &st_reflog_exists, 0);
	sqlite3_prepare_v3(db, "DELETE FROM reflog WHERE refname = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_reflog_delete, 0);
	sqlite3_prepare_v3(db, "SELECT size, data FROM lfs WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_lfs_read, 0);
	sqlite3_prepare_v3(db, "INSERT OR IGNORE INTO lfs(oid, size, data) VALUES(?,?,?)",  -1, SQLITE_PREPARE_PERSISTENT, &st_lfs_write, 0);
	sqlite3_prepare_v3(db, "SELECT 1 FROM lfs WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_lfs_exists, 0);

	sqlite3_prepare_v3(db, "UPDATE objects SET kept = 1 WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_mark_kept, 0);
	sqlite3_prepare_v3(db, "UPDATE objects SET kept = 0 WHERE kept = 1",  -1, SQLITE_PREPARE_PERSISTENT, &st_clear_kept, 0);
	sqlite3_prepare_v3(db, "SELECT 1 FROM objects WHERE oid = ? AND kept = 1",  -1, SQLITE_PREPARE_PERSISTENT, &st_have_kept, 0);
	sqlite3_prepare_v3(db, "UPDATE objects SET promisor = 1 WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_mark_promisor, 0);
	sqlite3_prepare_v3(db, "SELECT oid, type, size FROM objects WHERE promisor = 1",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_list_promisor, 0);
	sqlite3_prepare_v3(db, "SELECT oid, type, size FROM objects WHERE kept = 0",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_list_skip_kept, 0);
	sqlite3_prepare_v3(db, "SELECT oid FROM objects",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_oids, 0);

	/* Load alternates from persisted table */
	load_alternates();

	return 0;
}

void storage_close(void) {
	sqlite3_finalize(st_obj_read); sqlite3_finalize(st_obj_write);
	sqlite3_finalize(st_obj_exists); sqlite3_finalize(st_obj_list);
	sqlite3_finalize(st_find_base); sqlite3_finalize(st_find_base_named);
	sqlite3_finalize(st_ref_read); sqlite3_finalize(st_ref_write);
	sqlite3_finalize(st_ref_delete); sqlite3_finalize(st_ref_list);
	sqlite3_finalize(st_reflog_read); sqlite3_finalize(st_reflog_read_rev);
	sqlite3_finalize(st_reflog_append); sqlite3_finalize(st_reflog_list);
	sqlite3_finalize(st_reflog_exists); sqlite3_finalize(st_reflog_delete);
	sqlite3_finalize(st_lfs_read); sqlite3_finalize(st_lfs_write);
	sqlite3_finalize(st_lfs_exists);
	sqlite3_finalize(st_mark_kept); sqlite3_finalize(st_clear_kept);
	sqlite3_finalize(st_have_kept); sqlite3_finalize(st_mark_promisor);
	sqlite3_finalize(st_obj_list_promisor); sqlite3_finalize(st_obj_list_skip_kept);
	sqlite3_finalize(st_obj_oids);
	batch_kept = 0; batch_promisor = 0;
	/* Close alternate connections */
	while (alt_list) {
		struct alternate *a = alt_list;
		alt_list = a->next;
		if (a->read_stmt) sqlite3_finalize(a->read_stmt);
		if (a->db) sqlite3_close(a->db);
		free(a->path);
		free(a);
	}
	if (sdb_owned) sqlite3_close(sdb);
	sdb = NULL;
}

sqlite3 *storage_db(void) { return sdb; }

void storage_refresh(void) {
	if (sdb)
		sqlite3_wal_checkpoint_v2(sdb, NULL,
					  SQLITE_CHECKPOINT_PASSIVE,
					  NULL, NULL);
}

void storage_mark_kept(const git_oid *oid) {
	sqlite3_bind_blob(st_mark_kept, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_step(st_mark_kept);
	sqlite3_reset(st_mark_kept);
}

void storage_mark_kept_recent(void) { batch_kept = 1; }
void storage_end_kept_batch(void) { batch_kept = 0; }

void storage_mark_promisor_recent(void) { batch_promisor = 1; }
void storage_end_promisor_batch(void) { batch_promisor = 0; }

void storage_clear_kept(void) {
	sqlite3_step(st_clear_kept);
	sqlite3_reset(st_clear_kept);
	batch_kept = 0;
}

int storage_have_kept(const git_oid *oid) {
	int found;
	sqlite3_bind_blob(st_have_kept, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	found = (sqlite3_step(st_have_kept) == SQLITE_ROW);
	sqlite3_reset(st_have_kept);
	return found;
}

void storage_mark_promisor(const git_oid *oid) {
	sqlite3_bind_blob(st_mark_promisor, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_step(st_mark_promisor);
	sqlite3_reset(st_mark_promisor);
}

int storage_obj_oids(git_odb_foreach_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(st_obj_oids, "SELECT oid FROM objects");
	while (sqlite3_step(st) == SQLITE_ROW) {
		git_oid oid;
		memcpy(oid.id, sqlite3_column_blob(st, 0), GIT_OID_SHA1_SIZE);
		if (cb(&oid, data)) { stmt_release(st_obj_oids, st); return -1; }
	}
	stmt_release(st_obj_oids, st);
	return 0;
}

int storage_obj_list_filtered(int promisor_only, int skip_kept,
			      storage_obj_cb cb, void *data) {
	sqlite3_stmt *st = promisor_only ? st_obj_list_promisor :
			   skip_kept ? st_obj_list_skip_kept :
			   st_obj_list;
	while (sqlite3_step(st) == SQLITE_ROW) {
		git_oid oid;
		memcpy(oid.id, sqlite3_column_blob(st, 0), GIT_OID_SHA1_SIZE);
		git_object_t type = (git_object_t)sqlite3_column_int(st, 1);
		size_t size = (size_t)sqlite3_column_int64(st, 2);
		if (cb(&oid, type, size, data)) {
			sqlite3_reset(st);
			return -1;
		}
	}
	sqlite3_reset(st);
	return 0;
}

int storage_convert_oid(const git_oid *src, const char *algo,
		       git_oid *dest) {
	sqlite3_stmt *st;
	int found = 0;
	sqlite3_prepare_v2(sdb,
		"SELECT dest FROM oid_map WHERE src = ? AND algo = ?",
		-1, &st, 0);
	sqlite3_bind_blob(st, 1, src->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, algo, -1, SQLITE_STATIC);
	if (sqlite3_step(st) == SQLITE_ROW) {
		memcpy(dest->id, sqlite3_column_blob(st, 0),
		       GIT_OID_SHA1_SIZE);
		found = 1;
	}
	sqlite3_finalize(st);
	return found ? 0 : -1;
}

void storage_store_oid_map(const git_oid *src, const git_oid *dest,
			   const char *algo) {
	sqlite3_stmt *st;
	sqlite3_prepare_v2(sdb,
		"INSERT OR REPLACE INTO oid_map(src, dest, algo) VALUES(?,?,?)",
		-1, &st, 0);
	sqlite3_bind_blob(st, 1, src->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_blob(st, 2, dest->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_text(st, 3, algo, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);
}

/*
 * Verify connectivity of kept objects using the storage-backed
 * libgit2 repo (git0_backend.c). Walk each kept commit via
 * git_revwalk; if any referenced object is missing, the walk
 * or object lookup fails.
 */
int storage_check_connectivity(void) {
	git_repository *repo = git0_storage_repo();
	git_revwalk *walk = NULL;
	sqlite3_stmt *st;
	int ret = 0;

	if (!repo)
		return -1;

	if (git_revwalk_new(&walk, repo) < 0)
		return -1;

	/* Push all kept commits */
	sqlite3_prepare_v2(sdb,
		"SELECT oid FROM objects WHERE kept = 1 AND type = 1",
		-1, &st, 0);
	while (sqlite3_step(st) == SQLITE_ROW) {
		git_oid oid;
		memcpy(oid.id, sqlite3_column_blob(st, 0), GIT_OID_SHA1_SIZE);
		git_revwalk_push(walk, &oid);
	}
	sqlite3_finalize(st);

	/* Walk the graph; each commit's tree and parents are resolved.
	 * If any referenced object is missing, the lookup returns an error. */
	{
		git_oid oid;
		while (git_revwalk_next(&oid, walk) == 0) {
			git_commit *commit = NULL;
			git_tree *tree = NULL;

			if (git_commit_lookup(&commit, repo, &oid) < 0) {
				ret = -1; break;
			}
			if (git_commit_tree(&tree, commit) < 0) {
				git_commit_free(commit);
				ret = -1; break;
			}
			git_tree_free(tree);
			git_commit_free(commit);
		}
	}

	git_revwalk_free(walk);
	return ret;
}

/* ---- GC: mark-sweep unreachable objects ---- */

static int mark_reachable_oid(const git_oid *oid, sqlite3_stmt *st_insert) {
	sqlite3_bind_blob(st_insert, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_step(st_insert);
	sqlite3_reset(st_insert);
	return 0;
}

static int is_marked(const git_oid *oid, sqlite3_stmt *st_check) {
	sqlite3_bind_blob(st_check, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	int found = (sqlite3_step(st_check) == SQLITE_ROW);
	sqlite3_reset(st_check);
	return found;
}

static void mark_tree_reachable(git_repository *repo, const git_oid *tree_oid,
				sqlite3_stmt *st_insert, sqlite3_stmt *st_check) {
	if (is_marked(tree_oid, st_check))
		return;
	mark_reachable_oid(tree_oid, st_insert);

	git_tree *tree = NULL;
	if (git_tree_lookup(&tree, repo, tree_oid) < 0)
		return;

	size_t count = git_tree_entrycount(tree);
	for (size_t i = 0; i < count; i++) {
		const git_tree_entry *entry = git_tree_entry_byindex(tree, i);
		const git_oid *entry_oid = git_tree_entry_id(entry);
		git_object_t entry_type = git_tree_entry_type(entry);
		if (entry_type == GIT_OBJECT_TREE) {
			mark_tree_reachable(repo, entry_oid, st_insert, st_check);
		} else {
			mark_reachable_oid(entry_oid, st_insert);
		}
	}
	git_tree_free(tree);
}

struct gc_ref_ctx {
	git_repository *repo;
	git_revwalk *walk;
	sqlite3_stmt *st_insert;
	sqlite3_stmt *st_check;
};

static int gc_ref_cb(const char *refname, const git_oid *oid,
		     const char *symref, void *data) {
	(void)refname;
	struct gc_ref_ctx *ctx = data;
	git_oid resolved;

	/* Resolve symrefs to their target OID */
	if (symref && *symref) {
		char sym2[4096];
		if (storage_ref_read(symref, &resolved, sym2, sizeof(sym2)) < 0)
			return 0;
		oid = &resolved;
	}
	if (!oid) return 0;

	/* Check if it's a tag; if so, peel to the target */
	git_object_t type;
	size_t size;
	unsigned char *obj_data;
	if (storage_read_object(oid, &type, &size, &obj_data) < 0)
		return 0;
	free(obj_data);

	if (type == GIT_OBJECT_TAG) {
		mark_reachable_oid(oid, ctx->st_insert);
		git_tag *tag = NULL;
		if (git_tag_lookup(&tag, ctx->repo, oid) == 0) {
			const git_oid *target_oid = git_tag_target_id(tag);
			git_object_t target_type = git_tag_target_type(tag);
			if (target_type == GIT_OBJECT_COMMIT) {
				git_revwalk_push(ctx->walk, target_oid);
			} else if (target_type == GIT_OBJECT_TREE) {
				mark_tree_reachable(ctx->repo, target_oid,
						    ctx->st_insert, ctx->st_check);
			} else {
				mark_reachable_oid(target_oid, ctx->st_insert);
			}
			git_tag_free(tag);
		}
	} else if (type == GIT_OBJECT_COMMIT) {
		git_revwalk_push(ctx->walk, oid);
	} else if (type == GIT_OBJECT_TREE) {
		mark_tree_reachable(ctx->repo, oid, ctx->st_insert, ctx->st_check);
	} else {
		mark_reachable_oid(oid, ctx->st_insert);
	}

	return 0;
}

int storage_gc(void) {
	git_repository *repo = git0_storage_repo();
	if (!repo) return -1;

	/* Phase 1: Mark reachable objects */
	sqlite3_exec(sdb, "CREATE TEMP TABLE reachable(oid BLOB PRIMARY KEY) WITHOUT ROWID", 0, 0, 0);

	sqlite3_stmt *st_insert = NULL, *st_check = NULL;
	sqlite3_prepare_v2(sdb, "INSERT OR IGNORE INTO temp.reachable(oid) VALUES(?)", -1, &st_insert, 0);
	sqlite3_prepare_v2(sdb, "SELECT 1 FROM temp.reachable WHERE oid = ?", -1, &st_check, 0);

	git_revwalk *walk = NULL;
	if (git_revwalk_new(&walk, repo) < 0) {
		sqlite3_finalize(st_insert);
		sqlite3_finalize(st_check);
		sqlite3_exec(sdb, "DROP TABLE IF EXISTS temp.reachable", 0, 0, 0);
		return -1;
	}

	struct gc_ref_ctx ctx = { repo, walk, st_insert, st_check };
	storage_ref_list(NULL, gc_ref_cb, &ctx);

	/* Walk all reachable commits, marking each commit and its tree */
	git_oid oid;
	while (git_revwalk_next(&oid, walk) == 0) {
		mark_reachable_oid(&oid, st_insert);
		git_commit *commit = NULL;
		if (git_commit_lookup(&commit, repo, &oid) == 0) {
			const git_oid *tree_oid = git_commit_tree_id(commit);
			mark_tree_reachable(repo, tree_oid, st_insert, st_check);
			git_commit_free(commit);
		}
	}
	git_revwalk_free(walk);
	sqlite3_finalize(st_check);
	sqlite3_finalize(st_insert);

	/* Phase 2: Sweep unreachable objects (preserve kept and promisor) */
	sqlite3_stmt *st_sweep = NULL;
	sqlite3_prepare_v2(sdb,
		"DELETE FROM objects WHERE oid NOT IN (SELECT oid FROM temp.reachable)"
		" AND kept = 0 AND promisor = 0",
		-1, &st_sweep, 0);
	sqlite3_step(st_sweep);
	int deleted = sqlite3_changes(sdb);
	sqlite3_finalize(st_sweep);

	sqlite3_exec(sdb, "DROP TABLE IF EXISTS temp.reachable", 0, 0, 0);

	/* Phase 3: Repack surviving objects */
	storage_repack();

	return deleted;
}

/* ---- Repack: improve delta compression ---- */

#define REPACK_WINDOW 10
#define MAX_CHAIN_DEPTH 50

static int delta_chain_depth(const git_oid *oid) {
	int depth = 0;
	git_oid cur;
	memcpy(&cur, oid, sizeof(git_oid));

	while (depth <= MAX_DELTA_DEPTH) {
		sqlite3_stmt *st = NULL;
		sqlite3_prepare_v2(sdb, "SELECT base FROM objects WHERE oid = ?", -1, &st, 0);
		sqlite3_bind_blob(st, 1, cur.id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
		if (sqlite3_step(st) != SQLITE_ROW) {
			sqlite3_finalize(st);
			break;
		}
		const void *base_blob = sqlite3_column_blob(st, 0);
		if (!base_blob) {
			sqlite3_finalize(st);
			break; /* not a delta, depth is 0 from here */
		}
		memcpy(cur.id, base_blob, GIT_OID_SHA1_SIZE);
		sqlite3_finalize(st);
		depth++;
	}
	return depth;
}

int storage_repack(void) {
	sqlite3_stmt *st_scan = NULL, *st_update = NULL;
	int repacked = 0;

	sqlite3_prepare_v2(sdb,
		"SELECT oid, type, size, data, base, path FROM objects"
		" ORDER BY type, path, size",
		-1, &st_scan, 0);
	sqlite3_prepare_v2(sdb,
		"UPDATE objects SET data = ?, base = ? WHERE oid = ?",
		-1, &st_update, 0);

	/* Sliding window of recent objects of the same type */
	struct window_entry {
		git_oid oid;
		git_object_t type;
		unsigned char *full_data;
		size_t full_size;
		int chain_depth; /* 0 = not a delta */
	};
	struct window_entry window[REPACK_WINDOW];
	memset(window, 0, sizeof(window));
	int win_count = 0;
	int win_pos = 0; /* next slot to fill (circular) */
	git_object_t prev_type = GIT_OBJECT_INVALID;

	while (sqlite3_step(st_scan) == SQLITE_ROW) {
		git_oid oid;
		memcpy(oid.id, sqlite3_column_blob(st_scan, 0), GIT_OID_SHA1_SIZE);
		git_object_t type = (git_object_t)sqlite3_column_int(st_scan, 1);
		size_t orig_size = (size_t)sqlite3_column_int64(st_scan, 2);
		const void *comp = sqlite3_column_blob(st_scan, 3);
		int comp_len = sqlite3_column_bytes(st_scan, 3);
		const void *base_blob = sqlite3_column_blob(st_scan, 4);

		/* When type changes, flush the window */
		if (type != prev_type) {
			for (int i = 0; i < win_count; i++)
				free(window[i].full_data);
			memset(window, 0, sizeof(window));
			win_count = 0;
			win_pos = 0;
			prev_type = type;
		}

		/* Resolve current object to full content */
		unsigned char *full_data = NULL;
		size_t full_size = 0;
		if (!base_blob) {
			/* Not a delta: decompress directly */
			full_data = zdecompress(comp, comp_len, orig_size);
			full_size = orig_size;
		} else {
			/* Is a delta: resolve through storage_read_object */
			git_object_t rd_type;
			if (storage_read_object(&oid, &rd_type, &full_size, &full_data) < 0) {
				/* Skip if unreadable */
				goto next_entry;
			}
		}
		if (!full_data) goto next_entry;

		/* Current compressed size (what we're trying to beat) */
		size_t cur_stored_size = (size_t)comp_len;

		/*
		 * Try delta against each window entry.
		 * Storage format: deltas are stored as raw fossil delta bytes
		 * (not zlib-compressed), while non-delta objects are zlib-compressed.
		 * Compare raw delta size against current stored blob size.
		 */
		int best_delta_len = 0;
		char *best_delta = NULL;
		int best_win_idx = -1;

		for (int i = 0; i < win_count; i++) {
			struct window_entry *we = &window[i];
			if (!we->full_data) continue;

			/* Check chain depth: if base is itself a delta, its depth
			 * plus one must not exceed MAX_CHAIN_DEPTH */
			if (we->chain_depth + 1 > MAX_CHAIN_DEPTH) continue;

			/* Size filter: skip if target < base/32 */
			if (full_size < we->full_size / 32) continue;

			char *dbuf = malloc(full_size + 60);
			if (!dbuf) continue;
			int dlen = delta_create((const char *)we->full_data, we->full_size,
						(const char *)full_data, full_size, dbuf);
			if (dlen <= 0) { free(dbuf); continue; }

			/* Raw delta must be smaller than current stored data blob
			 * AND smaller than any previously found best delta */
			if ((size_t)dlen < cur_stored_size &&
			    (best_delta == NULL || dlen < best_delta_len)) {
				free(best_delta);
				best_delta = dbuf;
				best_delta_len = dlen;
				best_win_idx = i;
			} else {
				free(dbuf);
			}
		}

		/* If we found a better delta, update the row */
		if (best_delta && best_win_idx >= 0) {
			sqlite3_bind_blob(st_update, 1, best_delta, best_delta_len, SQLITE_STATIC);
			sqlite3_bind_blob(st_update, 2, window[best_win_idx].oid.id,
					  GIT_OID_SHA1_SIZE, SQLITE_STATIC);
			sqlite3_bind_blob(st_update, 3, oid.id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
			sqlite3_step(st_update);
			sqlite3_reset(st_update);
			repacked++;
		}
		free(best_delta);

		/* Add this object to the window */
		{
			int slot = win_pos % REPACK_WINDOW;
			free(window[slot].full_data);
			memcpy(&window[slot].oid, &oid, sizeof(git_oid));
			window[slot].type = type;
			window[slot].full_data = full_data;
			window[slot].full_size = full_size;
			/* Compute chain depth for this object as stored */
			if (best_win_idx >= 0)
				window[slot].chain_depth = window[best_win_idx].chain_depth + 1;
			else if (base_blob)
				window[slot].chain_depth = delta_chain_depth(&oid);
			else
				window[slot].chain_depth = 0;
			full_data = NULL; /* ownership transferred to window */
			win_pos++;
			if (win_count < REPACK_WINDOW) win_count++;
		}

next_entry:
		free(full_data);
	}

	/* Free window entries */
	for (int i = 0; i < win_count; i++)
		free(window[i].full_data);

	sqlite3_finalize(st_scan);
	sqlite3_finalize(st_update);
	return repacked;
}

void storage_destroy(void) {
	sqlite3_exec(sdb, "DROP TABLE IF EXISTS objects;"
		     "DROP TABLE IF EXISTS refs;"
		     "DROP TABLE IF EXISTS reflog;"
		     "DROP TABLE IF EXISTS lfs;", 0, 0, 0);
}

void storage_begin(void) { sqlite3_exec(sdb, "BEGIN", 0, 0, 0); }
void storage_commit(void) { sqlite3_exec(sdb, "COMMIT", 0, 0, 0); }

void storage_savepoint(const char *name) {
	char *sql = sqlite3_mprintf("SAVEPOINT \"%w\"", name);
	if (sql) { sqlite3_exec(sdb, sql, 0, 0, 0); sqlite3_free(sql); }
}

void storage_release(const char *name) {
	char *sql = sqlite3_mprintf("RELEASE \"%w\"", name);
	if (sql) { sqlite3_exec(sdb, sql, 0, 0, 0); sqlite3_free(sql); }
}

void storage_rollback_to(const char *name) {
	char *sql = sqlite3_mprintf("ROLLBACK TO \"%w\"; RELEASE \"%w\"", name, name);
	if (sql) { sqlite3_exec(sdb, sql, 0, 0, 0); sqlite3_free(sql); }
}

/* ---- Object read (resolves delta chains) ---- */

static int read_object_depth(const git_oid *oid, git_object_t *out_type,
			     size_t *out_size, unsigned char **out_data, int depth) {
	if (depth > MAX_DELTA_DEPTH) return -1;

	sqlite3_stmt *st = stmt_acquire(st_obj_read, "SELECT type, size, data, base FROM objects WHERE oid = ?");
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) {
		stmt_release(st_obj_read, st);

		/* Feature 7: try alternates */
		for (struct alternate *a = alt_list; a; a = a->next) {
			if (read_from_alternate(a, oid, out_type, out_size, out_data, depth) == 0)
				return 0;
		}

		/* Feature 3: try promised objects (on-demand fetch) */
		if (depth == 0 && storage_is_promised(oid)) {
			if (storage_fetch_promised(oid) == 0)
				return read_object_depth(oid, out_type, out_size, out_data, depth);
		}

		return -1;
	}

	*out_type = (git_object_t)sqlite3_column_int(st, 0);
	*out_size = (size_t)sqlite3_column_int64(st, 1);
	const void *comp = sqlite3_column_blob(st, 2);
	int comp_len = sqlite3_column_bytes(st, 2);
	const void *base_blob = sqlite3_column_blob(st, 3);

	if (!base_blob) {
		*out_data = zdecompress(comp, comp_len, *out_size);
		stmt_release(st_obj_read, st);
		return *out_data ? 0 : -1;
	}

	/* Delta: copy data before finalize/recursive call (buffer invalidation) */
	git_oid base_oid;
	memcpy(base_oid.id, base_blob, GIT_OID_SHA1_SIZE);
	int delta_len = comp_len;
	char *delta = malloc(delta_len ? delta_len : 1);
	if (!delta) { stmt_release(st_obj_read, st); return -1; }
	memcpy(delta, comp, delta_len);
	stmt_release(st_obj_read, st);

	git_object_t base_type;
	size_t base_size;
	unsigned char *base_data;
	if (read_object_depth(&base_oid, &base_type, &base_size, &base_data, depth + 1) < 0) {
		free(delta);
		return -1;
	}

	int target_size = delta_output_size(delta, delta_len);
	if (target_size < 0) { free(base_data); free(delta); return -1; }

	/* delta_apply writes a NUL terminator past the output data */
	*out_data = malloc(target_size + 1);
	if (!*out_data) { free(base_data); free(delta); return -1; }
	if (delta_apply((const char *)base_data, base_size,
			delta, delta_len, (char *)*out_data) < 0) {
		free(base_data); free(delta); free(*out_data);
		return -1;
	}
	*out_size = target_size;
	free(base_data); free(delta);
	return 0;
}

int storage_read_object(const git_oid *oid, git_object_t *out_type,
			size_t *out_size, unsigned char **out_data) {
	return read_object_depth(oid, out_type, out_size, out_data, 0);
}

/* ---- Object write (zlib + delta) ---- */

void storage_write_object_named(const git_oid *oid, git_object_t type,
				const void *data, size_t size,
				const char *path) {
	sqlite3_stmt *st = stmt_acquire(st_obj_exists, "SELECT 1 FROM objects WHERE oid = ?");
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	int exists = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(st_obj_exists, st);
	if (exists) return;

	git_oid base_oid;
	unsigned char *base_data = NULL;
	unsigned long base_len = 0;
	int use_delta = 0;
	char *delta_buf = NULL;
	int delta_len = 0;

	if (size > 64 && find_delta_base(type, size, path, &base_oid, &base_data, &base_len) == 0) {
		delta_buf = malloc(size + 60);
		if (delta_buf) {
			delta_len = delta_create((const char *)base_data, base_len,
						 (const char *)data, size, delta_buf);
			if (delta_len > 0 && (size_t)delta_len < size * 9 / 10)
					use_delta = 1;
				else { free(delta_buf); delta_buf = NULL; }
		}
		free(base_data);
	}

	st = stmt_acquire(st_obj_write, "INSERT OR IGNORE INTO objects(oid, type, size, data, base, path, kept, promisor) VALUES(?,?,?,?,?,?,?,?)");
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_int(st, 2, (int)type);
	sqlite3_bind_int64(st, 3, (sqlite3_int64)size);
	if (path && *path)
		sqlite3_bind_text(st, 6, path, -1, SQLITE_STATIC);
	else
		sqlite3_bind_null(st, 6);
	sqlite3_bind_int(st, 7, batch_kept);
	sqlite3_bind_int(st, 8, batch_promisor);

	if (use_delta) {
		sqlite3_bind_blob(st, 4, delta_buf, delta_len, SQLITE_STATIC);
		sqlite3_bind_blob(st, 5, base_oid.id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
		sqlite3_step(st);
		free(delta_buf);
	} else {
		unsigned long comp_len;
		unsigned char *comp = zcompress(data, size, &comp_len);
		if (!comp) { free(delta_buf); stmt_release(st_obj_write, st); return; }
		sqlite3_bind_blob(st, 4, comp, comp_len, SQLITE_STATIC);
		sqlite3_bind_null(st, 5);
		sqlite3_step(st);
		free(comp); free(delta_buf);
	}
	stmt_release(st_obj_write, st);
}

void storage_write_object(const git_oid *oid, git_object_t type,
			  const void *data, size_t size) {
	storage_write_object_named(oid, type, data, size, NULL);
}

int storage_object_exists(const git_oid *oid) {
	sqlite3_stmt *st = stmt_acquire(st_obj_exists, "SELECT 1 FROM objects WHERE oid = ?");
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	int found = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(st_obj_exists, st);
	if (found) return 1;

	/* Check alternates */
	for (struct alternate *a = alt_list; a; a = a->next) {
		sqlite3_reset(a->read_stmt);
		sqlite3_bind_blob(a->read_stmt, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
		if (sqlite3_step(a->read_stmt) == SQLITE_ROW) {
			sqlite3_reset(a->read_stmt);
			return 1;
		}
		sqlite3_reset(a->read_stmt);
	}

	return 0;
}

/* ---- Object listing ---- */

int storage_obj_list(storage_obj_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(st_obj_list, "SELECT oid, type, size FROM objects");
	while (sqlite3_step(st) == SQLITE_ROW) {
		git_oid oid;
		memcpy(oid.id, sqlite3_column_blob(st, 0), GIT_OID_SHA1_SIZE);
		git_object_t type = (git_object_t)sqlite3_column_int(st, 1);
		size_t size = (size_t)sqlite3_column_int64(st, 2);
		if (cb(&oid, type, size, data)) { stmt_release(st_obj_list, st); return 1; }
	}
	stmt_release(st_obj_list, st);
	return 0;
}

/* ---- Ref operations ---- */

int storage_ref_read(const char *refname, git_oid *oid, char *symref, size_t symref_len) {
	sqlite3_stmt *st = stmt_acquire(st_ref_read, "SELECT oid, symref FROM refs WHERE refname = ?");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) {
		stmt_release(st_ref_read, st);
		return -1;
	}
	const void *blob = sqlite3_column_blob(st, 0);
	const char *sym = (const char *)sqlite3_column_text(st, 1);
	if (blob && oid) memcpy(oid->id, blob, GIT_OID_SHA1_SIZE);
	if (sym && symref) snprintf(symref, symref_len, "%s", sym);
	else if (symref) symref[0] = '\0';
	stmt_release(st_ref_read, st);
	return 0;
}

void storage_ref_write(const char *refname, const git_oid *oid, const char *symref) {
	sqlite3_stmt *st = stmt_acquire(st_ref_write, "INSERT OR REPLACE INTO refs(refname, oid, symref) VALUES(?,?,?)");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	if (oid) sqlite3_bind_blob(st, 2, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	else sqlite3_bind_null(st, 2);
	if (symref) sqlite3_bind_text(st, 3, symref, -1, SQLITE_STATIC);
	else sqlite3_bind_null(st, 3);
	sqlite3_step(st);
	stmt_release(st_ref_write, st);
}

void storage_ref_delete(const char *refname) {
	sqlite3_stmt *st = stmt_acquire(st_ref_delete, "DELETE FROM refs WHERE refname = ?");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st);
	stmt_release(st_ref_delete, st);
}

int storage_ref_list(const char *prefix, storage_ref_cb cb, void *data) {
	if (prefix && *prefix) {
		char *pattern = sqlite3_mprintf("%s*", prefix);
		if (!pattern) return -1;
		sqlite3_stmt *st = NULL;
		int rc = sqlite3_prepare_v3(sdb,
			"SELECT refname, oid, symref FROM refs WHERE refname GLOB ? ORDER BY refname",
			-1, 0, &st, 0);
		if (rc != SQLITE_OK || !st) { sqlite3_free(pattern); return -1; }
		sqlite3_bind_text(st, 1, pattern, -1, sqlite3_free);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			git_oid oid = {{0}};
			const void *blob = sqlite3_column_blob(st, 1);
			if (blob) memcpy(oid.id, blob, GIT_OID_SHA1_SIZE);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (cb(name, blob ? &oid : NULL, sym, data)) break;
		}
		sqlite3_finalize(st);
	} else {
		sqlite3_stmt *st = stmt_acquire(st_ref_list, "SELECT refname, oid, symref FROM refs ORDER BY refname");
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			git_oid oid = {{0}};
			const void *blob = sqlite3_column_blob(st, 1);
			if (blob) memcpy(oid.id, blob, GIT_OID_SHA1_SIZE);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (cb(name, blob ? &oid : NULL, sym, data)) break;
		}
		stmt_release(st_ref_list, st);
	}
	return 0;
}

/* ---- Reflog ---- */

int storage_reflog_exists(const char *refname) {
	sqlite3_stmt *st = stmt_acquire(st_reflog_exists, "SELECT 1 FROM reflog WHERE refname = ? LIMIT 1");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	int found = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(st_reflog_exists, st);
	return found;
}

void storage_reflog_delete(const char *refname) {
	sqlite3_stmt *st = stmt_acquire(st_reflog_delete, "DELETE FROM reflog WHERE refname = ?");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st);
	stmt_release(st_reflog_delete, st);
}

int storage_reflog_read(const char *refname, storage_reflog_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(st_reflog_read, "SELECT old_oid, new_oid, committer, timestamp, tz, msg FROM reflog WHERE refname = ? ORDER BY idx ASC");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st) == SQLITE_ROW) {
		git_oid old_oid, new_oid;
		memcpy(old_oid.id, sqlite3_column_blob(st, 0), GIT_OID_SHA1_SIZE);
		memcpy(new_oid.id, sqlite3_column_blob(st, 1), GIT_OID_SHA1_SIZE);
		const char *committer = (const char *)sqlite3_column_text(st, 2);
		long long ts = sqlite3_column_int64(st, 3);
		int tz = sqlite3_column_int(st, 4);
		const char *msg = (const char *)sqlite3_column_text(st, 5);
		if (cb(&old_oid, &new_oid, committer, ts, tz, msg, data)) break;
	}
	stmt_release(st_reflog_read, st);
	return 0;
}

int storage_reflog_read_reverse(const char *refname, storage_reflog_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(st_reflog_read_rev, "SELECT old_oid, new_oid, committer, timestamp, tz, msg FROM reflog WHERE refname = ? ORDER BY idx DESC");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st) == SQLITE_ROW) {
		git_oid old_oid, new_oid;
		memcpy(old_oid.id, sqlite3_column_blob(st, 0), GIT_OID_SHA1_SIZE);
		memcpy(new_oid.id, sqlite3_column_blob(st, 1), GIT_OID_SHA1_SIZE);
		const char *committer = (const char *)sqlite3_column_text(st, 2);
		long long ts = sqlite3_column_int64(st, 3);
		int tz = sqlite3_column_int(st, 4);
		const char *msg = (const char *)sqlite3_column_text(st, 5);
		if (cb(&old_oid, &new_oid, committer, ts, tz, msg, data)) break;
	}
	stmt_release(st_reflog_read_rev, st);
	return 0;
}

int storage_reflog_list(storage_reflog_name_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(st_reflog_list, "SELECT DISTINCT refname FROM reflog ORDER BY refname");
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *refname = (const char *)sqlite3_column_text(st, 0);
		if (cb(refname, data)) break;
	}
	stmt_release(st_reflog_list, st);
	return 0;
}

void storage_reflog_append(const char *refname, const git_oid *old_oid,
			   const git_oid *new_oid, const char *committer,
			   long long timestamp, int tz, const char *msg) {
	sqlite3_stmt *st = stmt_acquire(st_reflog_append, "INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg) VALUES(?, COALESCE((SELECT MAX(idx)+1 FROM reflog WHERE refname = ?), 0), ?, ?, ?, ?, ?, ?)");
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, refname, -1, SQLITE_STATIC);
	sqlite3_bind_blob(st, 3, old_oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_blob(st, 4, new_oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_text(st, 5, committer, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 6, timestamp);
	sqlite3_bind_int(st, 7, tz);
	sqlite3_bind_text(st, 8, msg ? msg : "", -1, SQLITE_STATIC);
	sqlite3_step(st);
	stmt_release(st_reflog_append, st);
}

/* ---- Git delta apply ---- */

/*
 * Apply a git-format delta (as found in packfiles) to a base buffer.
 * This is distinct from the Fossil delta format used for storage.
 *
 * Git delta format:
 *   - Source size as varint
 *   - Target size as varint
 *   - Instructions until end of delta:
 *     - 0x00 is reserved (invalid)
 *     - High bit clear (1..127): insert N literal bytes that follow
 *     - High bit set: copy from base. Bits 0-3 select offset bytes (LE),
 *       bits 4-6 select size bytes (LE). Size=0 means 0x10000.
 *
 * Returns 0 on success, -1 on error. Caller must free *out.
 */
static int git_delta_apply(const unsigned char *base, size_t base_len,
			   const unsigned char *delta, size_t delta_len,
			   unsigned char **out, size_t *out_len)
{
	const unsigned char *dp = delta;
	const unsigned char *dend = delta + delta_len;

	/* Read source size varint */
	size_t src_size = 0;
	unsigned shift = 0;
	while (dp < dend) {
		unsigned char c = *dp++;
		src_size |= (size_t)(c & 0x7f) << shift;
		shift += 7;
		if (!(c & 0x80)) break;
	}
	if (src_size != base_len) return -1;

	/* Read target size varint */
	size_t tgt_size = 0;
	shift = 0;
	while (dp < dend) {
		unsigned char c = *dp++;
		tgt_size |= (size_t)(c & 0x7f) << shift;
		shift += 7;
		if (!(c & 0x80)) break;
	}

	unsigned char *buf = malloc(tgt_size);
	if (!buf) return -1;
	size_t pos = 0;

	while (dp < dend) {
		unsigned char cmd = *dp++;
		if (cmd & 0x80) {
			/* Copy from base */
			size_t cp_off = 0, cp_size = 0;
			if (cmd & 0x01) { if (dp >= dend) goto fail; cp_off  = *dp++; }
			if (cmd & 0x02) { if (dp >= dend) goto fail; cp_off |= (size_t)*dp++ << 8; }
			if (cmd & 0x04) { if (dp >= dend) goto fail; cp_off |= (size_t)*dp++ << 16; }
			if (cmd & 0x08) { if (dp >= dend) goto fail; cp_off |= (size_t)*dp++ << 24; }
			if (cmd & 0x10) { if (dp >= dend) goto fail; cp_size  = *dp++; }
			if (cmd & 0x20) { if (dp >= dend) goto fail; cp_size |= (size_t)*dp++ << 8; }
			if (cmd & 0x40) { if (dp >= dend) goto fail; cp_size |= (size_t)*dp++ << 16; }
			if (cp_size == 0) cp_size = 0x10000;
			if (cp_off + cp_size > base_len) goto fail;
			if (pos + cp_size > tgt_size) goto fail;
			memcpy(buf + pos, base + cp_off, cp_size);
			pos += cp_size;
		} else if (cmd) {
			/* Insert literal bytes */
			size_t n = cmd;
			if (dp + n > dend) goto fail;
			if (pos + n > tgt_size) goto fail;
			memcpy(buf + pos, dp, n);
			dp += n;
			pos += n;
		} else {
			/* cmd == 0 is reserved */
			goto fail;
		}
	}

	if (pos != tgt_size) goto fail;

	*out = buf;
	*out_len = tgt_size;
	return 0;

fail:
	free(buf);
	return -1;
}

/* ---- Packfile ingestion ---- */

/*
 * Inflate zlib-compressed data from a FILE stream.
 * Returns the decompressed buffer (caller frees).
 * *bytes_consumed is set to the number of compressed bytes read from in.
 */
static unsigned char *inflate_from_stream(FILE *in, size_t expected_size,
					  size_t *bytes_consumed)
{
	z_stream zs;
	memset(&zs, 0, sizeof(zs));
	if (inflateInit(&zs) != Z_OK) return NULL;

	unsigned char *out = malloc(expected_size ? expected_size : 1);
	if (!out) { inflateEnd(&zs); return NULL; }

	zs.next_out = out;
	zs.avail_out = (uInt)expected_size;

	unsigned char inbuf[8192];
	size_t total_in = 0;
	int ret = Z_OK;

	while (ret != Z_STREAM_END) {
		if (zs.avail_in == 0) {
			size_t n = fread(inbuf, 1, sizeof(inbuf), in);
			if (n == 0) { free(out); inflateEnd(&zs); return NULL; }
			zs.next_in = inbuf;
			zs.avail_in = (uInt)n;
		}
		ret = inflate(&zs, Z_NO_FLUSH);
		if (ret != Z_OK && ret != Z_STREAM_END) {
			free(out);
			inflateEnd(&zs);
			return NULL;
		}
	}

	/*
	 * zlib may have consumed more bytes from inbuf than it needed.
	 * The leftover (avail_in) bytes must be pushed back so subsequent
	 * reads from the FILE stream see them. Use ungetc in reverse order.
	 */
	if (zs.avail_in > 0) {
		/* Push back unconsumed bytes in reverse order */
		for (int i = (int)zs.avail_in - 1; i >= 0; i--)
			ungetc(zs.next_in[i], in);
	}

	total_in = zs.total_in;
	inflateEnd(&zs);

	if (bytes_consumed) *bytes_consumed = total_in;
	return out;
}

/*
 * Read exactly n bytes from the pack stream via fread.
 * Returns 0 on success, -1 on short read.
 */
static int pack_read_exact(FILE *in, void *buf, size_t n, size_t *offset)
{
	size_t got = fread(buf, 1, n, in);
	if (got != n) return -1;
	if (offset) *offset += n;
	return 0;
}

/* Packfile object types */
#define PACK_OBJ_COMMIT    1
#define PACK_OBJ_TREE      2
#define PACK_OBJ_BLOB      3
#define PACK_OBJ_TAG       4
#define PACK_OBJ_OFS_DELTA 6
#define PACK_OBJ_REF_DELTA 7

static git_object_t pack_type_to_git(int ptype) {
	switch (ptype) {
	case PACK_OBJ_COMMIT: return GIT_OBJECT_COMMIT;
	case PACK_OBJ_TREE:   return GIT_OBJECT_TREE;
	case PACK_OBJ_BLOB:   return GIT_OBJECT_BLOB;
	case PACK_OBJ_TAG:    return GIT_OBJECT_TAG;
	default:              return GIT_OBJECT_INVALID;
	}
}

int storage_write_packfile(FILE *in)
{
	/* 1. Read 12-byte header */
	unsigned char hdr[12];
	size_t stream_offset = 0;
	if (pack_read_exact(in, hdr, 12, NULL) < 0) return -1;

	/* Verify PACK magic */
	if (hdr[0] != 'P' || hdr[1] != 'A' || hdr[2] != 'C' || hdr[3] != 'K')
		return -1;

	/* Version (network byte order) */
	uint32_t version = ((uint32_t)hdr[4] << 24) | ((uint32_t)hdr[5] << 16) |
			   ((uint32_t)hdr[6] << 8)  | (uint32_t)hdr[7];
	if (version != 2 && version != 3) return -1;

	/* Object count (network byte order) */
	uint32_t obj_count = ((uint32_t)hdr[8] << 24) | ((uint32_t)hdr[9] << 16) |
			     ((uint32_t)hdr[10] << 8) | (uint32_t)hdr[11];

	/* 2. Allocate offset-to-OID mapping */
	typedef struct { size_t offset; git_oid oid; } ofs_entry;
	size_t ofs_cap = obj_count ? obj_count : 1;
	ofs_entry *ofs_map = calloc(ofs_cap, sizeof(ofs_entry));
	if (!ofs_map) return -1;

	int written = 0;

	/* 3. Process each object */
	for (uint32_t i = 0; i < obj_count; i++) {
		size_t obj_offset = stream_offset;

		/* Read type+size varint */
		int c = fgetc(in);
		if (c == EOF) goto err;
		stream_offset++;

		int ptype = (c >> 4) & 7;
		size_t size = c & 0x0f;
		unsigned shift = 4;
		while (c & 0x80) {
			c = fgetc(in);
			if (c == EOF) goto err;
			stream_offset++;
			size |= (size_t)(c & 0x7f) << shift;
			shift += 7;
		}

		if (ptype == PACK_OBJ_COMMIT || ptype == PACK_OBJ_TREE ||
		    ptype == PACK_OBJ_BLOB   || ptype == PACK_OBJ_TAG) {
			/* Non-delta object: inflate, hash, store */
			size_t consumed = 0;
			unsigned char *data = inflate_from_stream(in, size, &consumed);
			if (!data) goto err;
			stream_offset += consumed;

			git_object_t gtype = pack_type_to_git(ptype);
			git_oid oid;
			git_odb_hash(&oid, data, size, gtype);
			storage_write_object(&oid, gtype, data, size);
			free(data);

			ofs_map[i].offset = obj_offset;
			memcpy(&ofs_map[i].oid, &oid, sizeof(git_oid));
			written++;

		} else if (ptype == PACK_OBJ_OFS_DELTA) {
			/* OFS_DELTA: read negative offset, then delta */
			c = fgetc(in);
			if (c == EOF) goto err;
			stream_offset++;
			size_t base_offset = c & 127;
			while (c & 128) {
				base_offset += 1;
				c = fgetc(in);
				if (c == EOF) goto err;
				stream_offset++;
				base_offset = (base_offset << 7) + (c & 127);
			}
			size_t abs_base_offset = obj_offset - base_offset;

			/* Find base OID in offset map */
			git_oid base_oid;
			int found = 0;
			for (uint32_t j = 0; j < i; j++) {
				if (ofs_map[j].offset == abs_base_offset) {
					memcpy(&base_oid, &ofs_map[j].oid, sizeof(git_oid));
					found = 1;
					break;
				}
			}
			if (!found) goto err;

			/* Inflate delta */
			size_t consumed = 0;
			unsigned char *delta = inflate_from_stream(in, size, &consumed);
			if (!delta) goto err;
			stream_offset += consumed;

			/* Read base object */
			git_object_t base_type;
			size_t base_size;
			unsigned char *base_data;
			if (storage_read_object(&base_oid, &base_type, &base_size, &base_data) < 0) {
				free(delta);
				goto err;
			}

			/* Apply git delta */
			unsigned char *result;
			size_t result_len;
			if (git_delta_apply(base_data, base_size, delta, size,
					    &result, &result_len) < 0) {
				free(base_data);
				free(delta);
				goto err;
			}

			/* Hash and store */
			git_oid oid;
			git_odb_hash(&oid, result, result_len, base_type);
			storage_write_object(&oid, base_type, result, result_len);

			ofs_map[i].offset = obj_offset;
			memcpy(&ofs_map[i].oid, &oid, sizeof(git_oid));
			written++;

			free(base_data);
			free(delta);
			free(result);

		} else if (ptype == PACK_OBJ_REF_DELTA) {
			/* REF_DELTA: read 20-byte base OID, then delta */
			git_oid base_oid;
			if (pack_read_exact(in, base_oid.id, GIT_OID_SHA1_SIZE, &stream_offset) < 0)
				goto err;

			/* Inflate delta */
			size_t consumed = 0;
			unsigned char *delta = inflate_from_stream(in, size, &consumed);
			if (!delta) goto err;
			stream_offset += consumed;

			/* Read base object */
			git_object_t base_type;
			size_t base_size;
			unsigned char *base_data;
			if (storage_read_object(&base_oid, &base_type, &base_size, &base_data) < 0) {
				free(delta);
				goto err;
			}

			/* Apply git delta */
			unsigned char *result;
			size_t result_len;
			if (git_delta_apply(base_data, base_size, delta, size,
					    &result, &result_len) < 0) {
				free(base_data);
				free(delta);
				goto err;
			}

			/* Hash and store */
			git_oid oid;
			git_odb_hash(&oid, result, result_len, base_type);
			storage_write_object(&oid, base_type, result, result_len);

			ofs_map[i].offset = obj_offset;
			memcpy(&ofs_map[i].oid, &oid, sizeof(git_oid));
			written++;

			free(base_data);
			free(delta);
			free(result);

		} else {
			/* Unknown type */
			goto err;
		}
	}

	/* 4. Read trailing 20-byte pack checksum (skip verification) */
	{
		unsigned char checksum[20];
		/* Best-effort read; do not fail if stream ends early */
		fread(checksum, 1, 20, in);
	}

	free(ofs_map);
	return written;

err:
	free(ofs_map);
	return -1;
}

/* ---- Feature 3: Partial Clone ---- */

void storage_promise_object(const git_oid *oid, const char *remote) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"INSERT OR REPLACE INTO promised(oid, remote) VALUES(?,?)",
		-1, &st, 0);
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, remote, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);
}

int storage_is_promised(const git_oid *oid) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"SELECT 1 FROM promised WHERE oid = ?", -1, &st, 0);
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	int found = (sqlite3_step(st) == SQLITE_ROW);
	sqlite3_finalize(st);
	return found;
}

int storage_fetch_promised(const git_oid *oid) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"SELECT p.remote, r.url FROM promised p"
		" JOIN promisor_remotes r ON p.remote = r.name"
		" WHERE p.oid = ?",
		-1, &st, 0);
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) {
		sqlite3_finalize(st);
		return -1;
	}
	const char *url = (const char *)sqlite3_column_text(st, 1);
	if (!url) { sqlite3_finalize(st); return -1; }
	char *url_copy = strdup(url);
	sqlite3_finalize(st);

	/* Open the remote's sqlite.db */
	sqlite3 *remote_db = NULL;
	int rc = sqlite3_open_v2(url_copy, &remote_db, SQLITE_OPEN_READONLY, NULL);
	free(url_copy);
	if (rc != SQLITE_OK) {
		if (remote_db) sqlite3_close(remote_db);
		return -1;
	}

	/* Read the object from the remote db */
	sqlite3_stmt *rd = NULL;
	sqlite3_prepare_v2(remote_db,
		"SELECT type, size, data FROM objects WHERE oid = ? AND base IS NULL",
		-1, &rd, 0);
	sqlite3_bind_blob(rd, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	if (sqlite3_step(rd) != SQLITE_ROW) {
		sqlite3_finalize(rd);
		sqlite3_close(remote_db);
		return -1;
	}

	git_object_t type = (git_object_t)sqlite3_column_int(rd, 0);
	size_t size = (size_t)sqlite3_column_int64(rd, 1);
	const void *comp = sqlite3_column_blob(rd, 2);
	int comp_len = sqlite3_column_bytes(rd, 2);
	unsigned char *data = zdecompress(comp, comp_len, size);
	sqlite3_finalize(rd);
	sqlite3_close(remote_db);
	if (!data) return -1;

	/* Write locally and remove from promised */
	storage_write_object(oid, type, data, size);
	free(data);

	st = NULL;
	sqlite3_prepare_v2(sdb, "DELETE FROM promised WHERE oid = ?", -1, &st, 0);
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);

	return 0;
}

void storage_add_promisor_remote(const char *name, const char *url) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"INSERT OR REPLACE INTO promisor_remotes(name, url) VALUES(?,?)",
		-1, &st, 0);
	sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, url, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);
}

void storage_remove_promisor_remote(const char *name) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"DELETE FROM promisor_remotes WHERE name = ?", -1, &st, 0);
	sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);
}

/* ---- Feature 4: Commit Graph ---- */

/* Parse parent OIDs from raw commit data. Returns count, fills parents array. */
static int parse_commit_parents(const unsigned char *data, size_t size,
				git_oid *parents, int max_parents) {
	int count = 0;
	const char *p = (const char *)data;
	const char *end = p + size;

	while (p < end) {
		const char *nl = memchr(p, '\n', end - p);
		if (!nl) break;
		if (nl == p) break; /* empty line = end of header */
		if (nl - p > 47 && !strncmp(p, "parent ", 7)) {
			if (count < max_parents) {
				git_oid_fromstrn(&parents[count], p + 7, 40);
			}
			count++;
		}
		p = nl + 1;
	}
	return count;
}

/* Parse committer timestamp from raw commit data */
static long long parse_commit_time(const unsigned char *data, size_t size) {
	const char *p = (const char *)data;
	const char *end = p + size;

	while (p < end) {
		const char *nl = memchr(p, '\n', end - p);
		if (!nl) break;
		if (nl == p) break;
		if ((size_t)(nl - p) > 10 && !strncmp(p, "committer ", 10)) {
			/* Find the timestamp: last '>' then space then digits */
			const char *gt = NULL;
			for (const char *s = nl - 1; s > p; s--) {
				if (*s == '>') { gt = s; break; }
			}
			if (gt && gt + 2 < nl) {
				return strtoll(gt + 2, NULL, 10);
			}
		}
		p = nl + 1;
	}
	return 0;
}

int storage_build_commit_graph(void) {
	/* Collect all commits */
	sqlite3_stmt *st_scan = NULL;
	sqlite3_prepare_v2(sdb,
		"SELECT oid FROM objects WHERE type = 1", -1, &st_scan, 0);

	/* Use a temp table for BFS */
	sqlite3_exec(sdb,
		"CREATE TEMP TABLE IF NOT EXISTS cg_work("
		"  oid BLOB PRIMARY KEY, generation INT NOT NULL DEFAULT 0,"
		"  commit_time INT NOT NULL DEFAULT 0"
		") WITHOUT ROWID;"
		"DELETE FROM temp.cg_work;",
		0, 0, 0);

	/* Also store parent edges */
	sqlite3_exec(sdb,
		"CREATE TEMP TABLE IF NOT EXISTS cg_parents("
		"  child BLOB NOT NULL, parent BLOB NOT NULL"
		");"
		"DELETE FROM temp.cg_parents;",
		0, 0, 0);

	sqlite3_stmt *st_ins_work = NULL, *st_ins_parent = NULL;
	sqlite3_prepare_v2(sdb,
		"INSERT OR IGNORE INTO temp.cg_work(oid, commit_time) VALUES(?,?)",
		-1, &st_ins_work, 0);
	sqlite3_prepare_v2(sdb,
		"INSERT INTO temp.cg_parents(child, parent) VALUES(?,?)",
		-1, &st_ins_parent, 0);

	while (sqlite3_step(st_scan) == SQLITE_ROW) {
		git_oid oid;
		memcpy(oid.id, sqlite3_column_blob(st_scan, 0), GIT_OID_SHA1_SIZE);

		git_object_t type; size_t size; unsigned char *data;
		if (storage_read_object(&oid, &type, &size, &data) < 0) continue;
		if (type != GIT_OBJECT_COMMIT) { free(data); continue; }

		long long ctime = parse_commit_time(data, size);
		git_oid parents[64];
		int nparents = parse_commit_parents(data, size, parents, 64);
		free(data);

		sqlite3_bind_blob(st_ins_work, 1, oid.id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
		sqlite3_bind_int64(st_ins_work, 2, ctime);
		sqlite3_step(st_ins_work);
		sqlite3_reset(st_ins_work);

		for (int i = 0; i < nparents; i++) {
			sqlite3_bind_blob(st_ins_parent, 1, oid.id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
			sqlite3_bind_blob(st_ins_parent, 2, parents[i].id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
			sqlite3_step(st_ins_parent);
			sqlite3_reset(st_ins_parent);
		}
	}
	sqlite3_finalize(st_scan);
	sqlite3_finalize(st_ins_work);
	sqlite3_finalize(st_ins_parent);

	/* BFS: initialize roots (commits with no parents in cg_parents) to generation 1 */
	sqlite3_exec(sdb,
		"UPDATE temp.cg_work SET generation = 1"
		" WHERE oid NOT IN (SELECT DISTINCT child FROM temp.cg_parents);",
		0, 0, 0);

	/* Iterative relaxation until stable */
	int changed = 1;
	while (changed) {
		changed = 0;
		sqlite3_stmt *st_relax = NULL;
		sqlite3_prepare_v2(sdb,
			"UPDATE temp.cg_work SET generation = ("
			"  SELECT MAX(pw.generation) + 1"
			"  FROM temp.cg_parents cp"
			"  JOIN temp.cg_work pw ON cp.parent = pw.oid"
			"  WHERE cp.child = cg_work.oid"
			")"
			" WHERE oid IN (SELECT DISTINCT child FROM temp.cg_parents)"
			" AND generation < ("
			"  SELECT MAX(pw.generation) + 1"
			"  FROM temp.cg_parents cp"
			"  JOIN temp.cg_work pw ON cp.parent = pw.oid"
			"  WHERE cp.child = cg_work.oid"
			")",
			-1, &st_relax, 0);
		sqlite3_step(st_relax);
		changed = sqlite3_changes(sdb);
		sqlite3_finalize(st_relax);
	}

	/* Write results to commit_graph */
	sqlite3_exec(sdb,
		"INSERT OR REPLACE INTO commit_graph(oid, generation, commit_time)"
		" SELECT oid, generation, commit_time FROM temp.cg_work;",
		0, 0, 0);

	int count = 0;
	sqlite3_stmt *st_count = NULL;
	sqlite3_prepare_v2(sdb, "SELECT COUNT(*) FROM commit_graph", -1, &st_count, 0);
	if (sqlite3_step(st_count) == SQLITE_ROW)
		count = sqlite3_column_int(st_count, 0);
	sqlite3_finalize(st_count);

	/* Cleanup temp tables */
	sqlite3_exec(sdb,
		"DROP TABLE IF EXISTS temp.cg_parents;"
		"DROP TABLE IF EXISTS temp.cg_work;",
		0, 0, 0);

	return count;
}

int storage_commit_generation(const git_oid *oid) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"SELECT generation FROM commit_graph WHERE oid = ?", -1, &st, 0);
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	int gen = -1;
	if (sqlite3_step(st) == SQLITE_ROW)
		gen = sqlite3_column_int(st, 0);
	sqlite3_finalize(st);
	return gen;
}

/* ---- Feature 6: Worktrees ---- */

void storage_worktree_add(const char *name, const char *path, const char *branch) {
	/* Create the worktree entry */
	char head_ref[4096];
	snprintf(head_ref, sizeof(head_ref), "refs/heads/%s", branch);

	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"INSERT OR REPLACE INTO worktrees(name, path, head_ref) VALUES(?,?,?)",
		-1, &st, 0);
	sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 3, head_ref, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);

	/* Create symref refs/worktrees/<name>/HEAD pointing to the branch */
	char wt_head[4096];
	snprintf(wt_head, sizeof(wt_head), "refs/worktrees/%s/HEAD", name);
	storage_ref_write(wt_head, NULL, head_ref);
}

void storage_worktree_remove(const char *name) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"DELETE FROM worktrees WHERE name = ?", -1, &st, 0);
	sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);

	/* Remove the HEAD symref */
	char wt_head[4096];
	snprintf(wt_head, sizeof(wt_head), "refs/worktrees/%s/HEAD", name);
	storage_ref_delete(wt_head);
}

int storage_worktree_list(storage_worktree_cb cb, void *data) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"SELECT name, path, head_ref FROM worktrees ORDER BY name",
		-1, &st, 0);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *name = (const char *)sqlite3_column_text(st, 0);
		const char *path = (const char *)sqlite3_column_text(st, 1);
		const char *head_ref = (const char *)sqlite3_column_text(st, 2);
		if (cb(name, path, head_ref, data)) break;
	}
	sqlite3_finalize(st);
	return 0;
}

/* ---- Feature 7: Alternates ---- */

/* Read from an alternate, resolving delta chains within that alternate's db */
static int read_from_alternate(struct alternate *alt, const git_oid *oid,
			       git_object_t *out_type, size_t *out_size,
			       unsigned char **out_data, int depth) {
	if (depth > MAX_DELTA_DEPTH) return -1;

	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(alt->db,
		"SELECT type, size, data, base FROM objects WHERE oid = ?",
		-1, &st, 0);
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) {
		sqlite3_finalize(st);
		return -1;
	}

	*out_type = (git_object_t)sqlite3_column_int(st, 0);
	*out_size = (size_t)sqlite3_column_int64(st, 1);
	const void *comp = sqlite3_column_blob(st, 2);
	int comp_len = sqlite3_column_bytes(st, 2);
	const void *base_blob = sqlite3_column_blob(st, 3);

	if (!base_blob) {
		*out_data = zdecompress(comp, comp_len, *out_size);
		sqlite3_finalize(st);
		return *out_data ? 0 : -1;
	}

	/* Delta: copy data before finalize/recursive call */
	git_oid base_oid;
	memcpy(base_oid.id, base_blob, GIT_OID_SHA1_SIZE);
	int delta_len = comp_len;
	char *delta = malloc(delta_len ? delta_len : 1);
	if (!delta) { sqlite3_finalize(st); return -1; }
	memcpy(delta, comp, delta_len);
	sqlite3_finalize(st);

	/* Resolve base within the same alternate */
	git_object_t base_type;
	size_t base_size;
	unsigned char *base_data;
	if (read_from_alternate(alt, &base_oid, &base_type, &base_size, &base_data, depth + 1) < 0) {
		free(delta);
		return -1;
	}

	int target_size = delta_output_size(delta, delta_len);
	if (target_size < 0) { free(base_data); free(delta); return -1; }

	*out_data = malloc(target_size + 1);
	if (!*out_data) { free(base_data); free(delta); return -1; }
	if (delta_apply((const char *)base_data, base_size,
			delta, delta_len, (char *)*out_data) < 0) {
		free(base_data); free(delta); free(*out_data);
		return -1;
	}
	*out_size = target_size;
	free(base_data); free(delta);
	return 0;
}

static void alternate_open(const char *path) {
	/* Check if already in list */
	for (struct alternate *a = alt_list; a; a = a->next) {
		if (strcmp(a->path, path) == 0) return;
	}

	sqlite3 *db = NULL;
	if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
		if (db) sqlite3_close(db);
		return;
	}

	sqlite3_stmt *rd = NULL;
	sqlite3_prepare_v2(db,
		"SELECT type, size, data, base FROM objects WHERE oid = ?",
		-1, &rd, 0);
	if (!rd) { sqlite3_close(db); return; }

	struct alternate *a = calloc(1, sizeof(*a));
	a->db = db;
	a->read_stmt = rd;
	a->path = strdup(path);
	a->next = alt_list;
	alt_list = a;
}

void storage_alternate_add(const char *path) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"INSERT OR IGNORE INTO alternates(path) VALUES(?)", -1, &st, 0);
	sqlite3_bind_text(st, 1, path, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);

	alternate_open(path);
}

void storage_alternate_remove(const char *path) {
	/* Remove from linked list */
	struct alternate **pp = &alt_list;
	while (*pp) {
		if (strcmp((*pp)->path, path) == 0) {
			struct alternate *a = *pp;
			*pp = a->next;
			if (a->read_stmt) sqlite3_finalize(a->read_stmt);
			if (a->db) sqlite3_close(a->db);
			free(a->path);
			free(a);
			break;
		}
		pp = &(*pp)->next;
	}

	/* Remove from table */
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"DELETE FROM alternates WHERE path = ?", -1, &st, 0);
	sqlite3_bind_text(st, 1, path, -1, SQLITE_STATIC);
	sqlite3_step(st);
	sqlite3_finalize(st);
}

int storage_alternate_list(storage_alternate_cb cb, void *data) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"SELECT path FROM alternates ORDER BY path", -1, &st, 0);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *path = (const char *)sqlite3_column_text(st, 0);
		if (cb(path, data)) break;
	}
	sqlite3_finalize(st);
	return 0;
}

/* Load alternates from the table (called during storage_open / storage_init_db) */
static void load_alternates(void) {
	sqlite3_stmt *st = NULL;
	sqlite3_prepare_v2(sdb,
		"SELECT path FROM alternates", -1, &st, 0);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *path = (const char *)sqlite3_column_text(st, 0);
		alternate_open(path);
	}
	sqlite3_finalize(st);
}

/* ---- LFS ---- */

void storage_lfs_sha256(const void *data, size_t len,
			unsigned char out[LFS_OID_RAWSZ]) {
	SHA256_CTX ctx;
	sha256_init(&ctx);
	sha256_update(&ctx, data, len);
	sha256_final(&ctx, out);
}

void storage_lfs_oid_to_hex(const unsigned char oid[LFS_OID_RAWSZ],
			    char hex[LFS_OID_HEXSZ + 1]) {
	for (int i = 0; i < LFS_OID_RAWSZ; i++)
		sprintf(hex + 2*i, "%02x", oid[i]);
	hex[LFS_OID_HEXSZ] = '\0';
}

int storage_lfs_oid_from_hex(const char *hex,
			     unsigned char oid[LFS_OID_RAWSZ]) {
	for (int i = 0; i < LFS_OID_RAWSZ; i++) {
		unsigned int b;
		if (sscanf(hex + 2*i, "%02x", &b) != 1) return -1;
		oid[i] = (unsigned char)b;
	}
	return 0;
}

int storage_lfs_read(const unsigned char oid[LFS_OID_RAWSZ],
		     size_t *out_size, unsigned char **out_data) {
	sqlite3_stmt *st = stmt_acquire(st_lfs_read, "SELECT size, data FROM lfs WHERE oid = ?");
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) {
		stmt_release(st_lfs_read, st);
		return -1;
	}
	*out_size = (size_t)sqlite3_column_int64(st, 0);
	const void *comp = sqlite3_column_blob(st, 1);
	int comp_len = sqlite3_column_bytes(st, 1);
	*out_data = zdecompress(comp, comp_len, *out_size);
	stmt_release(st_lfs_read, st);
	return *out_data ? 0 : -1;
}

void storage_lfs_write(const unsigned char oid[LFS_OID_RAWSZ],
		       const void *data, size_t size) {
	sqlite3_stmt *st = stmt_acquire(st_lfs_exists, "SELECT 1 FROM lfs WHERE oid = ?");
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	int exists = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(st_lfs_exists, st);
	if (exists) return;

	unsigned long comp_len;
	unsigned char *comp = zcompress(data, size, &comp_len);
	if (!comp) return;

	st = stmt_acquire(st_lfs_write, "INSERT OR IGNORE INTO lfs(oid, size, data) VALUES(?,?,?)");
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_int64(st, 2, (sqlite3_int64)size);
	sqlite3_bind_blob(st, 3, comp, comp_len, SQLITE_STATIC);
	sqlite3_step(st);
	stmt_release(st_lfs_write, st);
	free(comp);
}

int storage_lfs_exists(const unsigned char oid[LFS_OID_RAWSZ]) {
	sqlite3_stmt *st = stmt_acquire(st_lfs_exists, "SELECT 1 FROM lfs WHERE oid = ?");
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	int found = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(st_lfs_exists, st);
	return found;
}
