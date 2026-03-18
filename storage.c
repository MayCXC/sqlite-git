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
#include <sqlite3.h>
#include <git2.h>
#include "storage.h"
#include "vendor/delta.h"
#include "vendor/sha256.h"


static sqlite3 *sdb;
static int sdb_owned; /* 1 if we opened the db, 0 if borrowed */

static sqlite3_stmt *st_obj_read, *st_obj_write, *st_obj_exists, *st_obj_list;
static sqlite3_stmt *st_find_base;
static sqlite3_stmt *st_ref_read, *st_ref_write, *st_ref_delete, *st_ref_list;
static sqlite3_stmt *st_reflog_read, *st_reflog_read_rev, *st_reflog_append;
static sqlite3_stmt *st_reflog_exists, *st_reflog_delete, *st_reflog_list;
static sqlite3_stmt *st_lfs_read, *st_lfs_write, *st_lfs_exists;

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

static int find_delta_base(git_object_t type, git_oid *base_oid,
			   unsigned char **base_data, unsigned long *base_len) {
	sqlite3_stmt *st = stmt_acquire(st_find_base, "SELECT oid, size, data, base FROM objects WHERE type = ? AND base IS NULL LIMIT 1");
	sqlite3_bind_int(st, 1, (int)type);
	if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(st_find_base, st); return -1; }

	const void *oid_blob = sqlite3_column_blob(st, 0);
	if (!oid_blob) { stmt_release(st_find_base, st); return -1; }

	memcpy(base_oid->id, oid_blob, GIT_OID_SHA1_SIZE);
	size_t sz = (size_t)sqlite3_column_int64(st, 1);
	const void *comp = sqlite3_column_blob(st, 2);
	int comp_len = sqlite3_column_bytes(st, 2);
	if (!comp) { stmt_release(st_find_base, st); return -1; }

	if (sqlite3_column_blob(st, 3) != NULL) { stmt_release(st_find_base, st); return -1; }

	*base_data = zdecompress(comp, comp_len, sz);
	if (!*base_data) { stmt_release(st_find_base, st); return -1; }
	*base_len = sz;
	stmt_release(st_find_base, st);
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

int storage_open_db(sqlite3 *db) {
	if (sdb) return 0;
	sdb = db;
	/* Only create schema, skip prepared statements.
	 * Borrowed connections prepare/finalize per call to avoid
	 * keeping statements alive that prevent sqlite3_close(v1). */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS objects("
		"  oid BLOB PRIMARY KEY, type INTEGER NOT NULL,"
		"  size INTEGER NOT NULL, data BLOB NOT NULL, base BLOB"
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
		"  size INTEGER NOT NULL, data BLOB NOT NULL, base BLOB"
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
		") WITHOUT ROWID;",
		0, 0, 0);

	sqlite3_prepare_v3(db, "SELECT type, size, data, base FROM objects WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_read, 0);
	sqlite3_prepare_v3(db, "INSERT OR IGNORE INTO objects(oid, type, size, data, base) VALUES(?,?,?,?,?)",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_write, 0);
	sqlite3_prepare_v3(db, "SELECT 1 FROM objects WHERE oid = ?",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_exists, 0);
	sqlite3_prepare_v3(db, "SELECT oid, type, size FROM objects",  -1, SQLITE_PREPARE_PERSISTENT, &st_obj_list, 0);
	sqlite3_prepare_v3(db, "SELECT oid, size, data, base FROM objects WHERE type = ? AND base IS NULL LIMIT 1",  -1, SQLITE_PREPARE_PERSISTENT, &st_find_base, 0);
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

	return 0;
}

void storage_close(void) {
	sqlite3_finalize(st_obj_read); sqlite3_finalize(st_obj_write);
	sqlite3_finalize(st_obj_exists); sqlite3_finalize(st_obj_list);
	sqlite3_finalize(st_find_base);
	sqlite3_finalize(st_ref_read); sqlite3_finalize(st_ref_write);
	sqlite3_finalize(st_ref_delete); sqlite3_finalize(st_ref_list);
	sqlite3_finalize(st_reflog_read); sqlite3_finalize(st_reflog_read_rev);
	sqlite3_finalize(st_reflog_append); sqlite3_finalize(st_reflog_list);
	sqlite3_finalize(st_reflog_exists); sqlite3_finalize(st_reflog_delete);
	sqlite3_finalize(st_lfs_read); sqlite3_finalize(st_lfs_write);
	sqlite3_finalize(st_lfs_exists);
	if (sdb_owned) sqlite3_close(sdb);
	sdb = NULL;
}

sqlite3 *storage_db(void) { return sdb; }

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

void storage_write_object(const git_oid *oid, git_object_t type,
			  const void *data, size_t size) {
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

	if (size > 64 && find_delta_base(type, &base_oid, &base_data, &base_len) == 0) {
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

	st = stmt_acquire(st_obj_write, "INSERT OR IGNORE INTO objects(oid, type, size, data, base) VALUES(?,?,?,?,?)");
	sqlite3_bind_blob(st, 1, oid->id, GIT_OID_SHA1_SIZE, SQLITE_STATIC);
	sqlite3_bind_int(st, 2, (int)type);
	sqlite3_bind_int64(st, 3, (sqlite3_int64)size);

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
