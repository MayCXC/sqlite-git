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
		"  ON reflog(timestamp);",
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

/* Guard against circular delta chains in corrupt databases.
 * Git enforces no depth limit on read; this is our safety net.
 * 4095 matches pack-objects.h structural max (12-bit depth field). */
#define MAX_DELTA_DEPTH 4095

static int read_object_depth(const git_oid *oid, git_object_t *out_type,
			     size_t *out_size, unsigned char **out_data, int depth) {
	if (depth > MAX_DELTA_DEPTH) return -1;

	sqlite3_stmt *st = stmt_acquire(st_obj_read, "SELECT type, size, data, base FROM objects WHERE oid = ?");
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(st_obj_read, st); return -1; }

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
	return found;
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
