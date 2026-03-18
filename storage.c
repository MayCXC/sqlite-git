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

#define RAW 20 /* GIT_OID_SHA1_SIZE */

static sqlite3 *sdb;

static sqlite3_stmt *st_obj_read, *st_obj_write, *st_obj_exists, *st_obj_list;
static sqlite3_stmt *st_find_base;
static sqlite3_stmt *st_ref_read, *st_ref_write, *st_ref_delete, *st_ref_list;
static sqlite3_stmt *st_reflog_read, *st_reflog_append, *st_reflog_exists, *st_reflog_delete;
static sqlite3_stmt *st_lfs_read, *st_lfs_write, *st_lfs_exists;

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
	sqlite3_reset(st_find_base);
	sqlite3_bind_int(st_find_base, 1, (int)type);
	if (sqlite3_step(st_find_base) != SQLITE_ROW)
		return -1;

	memcpy(base_oid->id, sqlite3_column_blob(st_find_base, 0), RAW);
	int sz = sqlite3_column_int(st_find_base, 1);
	const void *comp = sqlite3_column_blob(st_find_base, 2);
	int comp_len = sqlite3_column_bytes(st_find_base, 2);

	if (sqlite3_column_blob(st_find_base, 3) != NULL)
		return -1; /* only use full objects as bases */

	*base_data = zdecompress(comp, comp_len, sz);
	if (!*base_data) return -1;
	*base_len = sz;
	return 0;
}

/* ---- Database ---- */

int storage_open(const char *path_arg) {
	char path[4096], gitdir[4096];
	size_t len;

	snprintf(gitdir, sizeof(gitdir), "%s", path_arg);
	len = strlen(gitdir);
	if (len > 8 && !strcmp(gitdir + len - 8, "/objects"))
		gitdir[len - 8] = '\0';

	mkdir(gitdir, 0755);
	snprintf(path, sizeof(path), "%s/sqlite.db", gitdir);

	if (sqlite3_open(path, &sdb) != SQLITE_OK)
		return -1;

	sqlite3_exec(sdb,
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

	sqlite3_prepare_v2(sdb, "SELECT type, size, data, base FROM objects WHERE oid = ?", -1, &st_obj_read, 0);
	sqlite3_prepare_v2(sdb, "INSERT OR IGNORE INTO objects(oid, type, size, data, base) VALUES(?,?,?,?,?)", -1, &st_obj_write, 0);
	sqlite3_prepare_v2(sdb, "SELECT 1 FROM objects WHERE oid = ?", -1, &st_obj_exists, 0);
	sqlite3_prepare_v2(sdb, "SELECT oid, type, size FROM objects", -1, &st_obj_list, 0);
	sqlite3_prepare_v2(sdb, "SELECT oid, size, data, base FROM objects WHERE type = ? AND base IS NULL LIMIT 1", -1, &st_find_base, 0);
	sqlite3_prepare_v2(sdb, "SELECT oid, symref FROM refs WHERE refname = ?", -1, &st_ref_read, 0);
	sqlite3_prepare_v2(sdb, "INSERT OR REPLACE INTO refs(refname, oid, symref) VALUES(?,?,?)", -1, &st_ref_write, 0);
	sqlite3_prepare_v2(sdb, "DELETE FROM refs WHERE refname = ?", -1, &st_ref_delete, 0);
	sqlite3_prepare_v2(sdb, "SELECT refname, oid, symref FROM refs ORDER BY refname", -1, &st_ref_list, 0);
	sqlite3_prepare_v2(sdb, "SELECT old_oid, new_oid, committer, timestamp, tz, msg FROM reflog WHERE refname = ? ORDER BY idx ASC", -1, &st_reflog_read, 0);
	sqlite3_prepare_v2(sdb, "INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg) VALUES(?, COALESCE((SELECT MAX(idx)+1 FROM reflog WHERE refname = ?), 0), ?, ?, ?, ?, ?, ?)", -1, &st_reflog_append, 0);
	sqlite3_prepare_v2(sdb, "SELECT 1 FROM reflog WHERE refname = ? LIMIT 1", -1, &st_reflog_exists, 0);
	sqlite3_prepare_v2(sdb, "DELETE FROM reflog WHERE refname = ?", -1, &st_reflog_delete, 0);
	sqlite3_prepare_v2(sdb, "SELECT size, data FROM lfs WHERE oid = ?", -1, &st_lfs_read, 0);
	sqlite3_prepare_v2(sdb, "INSERT OR IGNORE INTO lfs(oid, size, data) VALUES(?,?,?)", -1, &st_lfs_write, 0);
	sqlite3_prepare_v2(sdb, "SELECT 1 FROM lfs WHERE oid = ?", -1, &st_lfs_exists, 0);

	return 0;
}

void storage_close(void) {
	sqlite3_finalize(st_obj_read); sqlite3_finalize(st_obj_write);
	sqlite3_finalize(st_obj_exists); sqlite3_finalize(st_obj_list);
	sqlite3_finalize(st_find_base);
	sqlite3_finalize(st_ref_read); sqlite3_finalize(st_ref_write);
	sqlite3_finalize(st_ref_delete); sqlite3_finalize(st_ref_list);
	sqlite3_finalize(st_reflog_read); sqlite3_finalize(st_reflog_append);
	sqlite3_finalize(st_reflog_exists); sqlite3_finalize(st_reflog_delete);
	sqlite3_finalize(st_lfs_read); sqlite3_finalize(st_lfs_write);
	sqlite3_finalize(st_lfs_exists);
	sqlite3_close(sdb);
}

sqlite3 *storage_db(void) { return sdb; }

/* ---- Object read (resolves delta chains) ---- */

int storage_read_object(const git_oid *oid, git_object_t *out_type,
			size_t *out_size, unsigned char **out_data) {
	sqlite3_reset(st_obj_read);
	sqlite3_bind_blob(st_obj_read, 1, oid->id, RAW, SQLITE_STATIC);
	if (sqlite3_step(st_obj_read) != SQLITE_ROW)
		return -1;

	*out_type = (git_object_t)sqlite3_column_int(st_obj_read, 0);
	*out_size = sqlite3_column_int(st_obj_read, 1);
	const void *comp = sqlite3_column_blob(st_obj_read, 2);
	int comp_len = sqlite3_column_bytes(st_obj_read, 2);
	const void *base_blob = sqlite3_column_blob(st_obj_read, 3);

	if (!base_blob) {
		*out_data = zdecompress(comp, comp_len, *out_size);
		return *out_data ? 0 : -1;
	}

	/* Delta: copy before recursive call (SQLite buffer invalidation) */
	git_oid base_oid;
	memcpy(base_oid.id, base_blob, RAW);
	int delta_len = comp_len;
	char *delta = malloc(delta_len);
	memcpy(delta, comp, delta_len);

	git_object_t base_type;
	size_t base_size;
	unsigned char *base_data;
	if (storage_read_object(&base_oid, &base_type, &base_size, &base_data) < 0) {
		free(delta);
		return -1;
	}

	int target_size = delta_output_size(delta, delta_len);
	if (target_size < 0) { free(base_data); free(delta); return -1; }

	*out_data = malloc(target_size);
	if (delta_apply((const char *)base_data, base_size,
			delta, delta_len, (char *)*out_data) < 0) {
		free(base_data); free(delta); free(*out_data);
		return -1;
	}
	*out_size = target_size;
	free(base_data); free(delta);
	return 0;
}

/* ---- Object write (zlib + delta) ---- */

void storage_write_object(const git_oid *oid, git_object_t type,
			  const void *data, size_t size) {
	sqlite3_reset(st_obj_exists);
	sqlite3_bind_blob(st_obj_exists, 1, oid->id, RAW, SQLITE_STATIC);
	if (sqlite3_step(st_obj_exists) == SQLITE_ROW)
		return;

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

	sqlite3_reset(st_obj_write);
	sqlite3_bind_blob(st_obj_write, 1, oid->id, RAW, SQLITE_STATIC);
	sqlite3_bind_int(st_obj_write, 2, (int)type);
	sqlite3_bind_int64(st_obj_write, 3, (sqlite3_int64)size);

	if (use_delta) {
		sqlite3_bind_blob(st_obj_write, 4, delta_buf, delta_len, SQLITE_STATIC);
		sqlite3_bind_blob(st_obj_write, 5, base_oid.id, RAW, SQLITE_STATIC);
		sqlite3_step(st_obj_write);
		free(delta_buf);
	} else {
		unsigned long comp_len;
		unsigned char *comp = zcompress(data, size, &comp_len);
		if (!comp) { free(delta_buf); return; }
		sqlite3_bind_blob(st_obj_write, 4, comp, comp_len, SQLITE_STATIC);
		sqlite3_bind_null(st_obj_write, 5);
		sqlite3_step(st_obj_write);
		free(comp); free(delta_buf);
	}
}

int storage_object_exists(const git_oid *oid) {
	sqlite3_reset(st_obj_exists);
	sqlite3_bind_blob(st_obj_exists, 1, oid->id, RAW, SQLITE_STATIC);
	return sqlite3_step(st_obj_exists) == SQLITE_ROW;
}

/* ---- Object listing ---- */

int storage_obj_list(storage_obj_cb cb, void *data) {
	sqlite3_reset(st_obj_list);
	while (sqlite3_step(st_obj_list) == SQLITE_ROW) {
		git_oid oid;
		memcpy(oid.id, sqlite3_column_blob(st_obj_list, 0), RAW);
		git_object_t type = (git_object_t)sqlite3_column_int(st_obj_list, 1);
		size_t size = sqlite3_column_int(st_obj_list, 2);
		if (cb(&oid, type, size, data)) return 1;
	}
	return 0;
}

/* ---- Ref operations ---- */

int storage_ref_read(const char *refname, git_oid *oid, char *symref, size_t symref_len) {
	sqlite3_reset(st_ref_read);
	sqlite3_bind_text(st_ref_read, 1, refname, -1, SQLITE_STATIC);
	if (sqlite3_step(st_ref_read) != SQLITE_ROW)
		return -1;
	const void *blob = sqlite3_column_blob(st_ref_read, 0);
	const char *sym = (const char *)sqlite3_column_text(st_ref_read, 1);
	if (blob && oid) memcpy(oid->id, blob, RAW);
	if (sym && symref) snprintf(symref, symref_len, "%s", sym);
	else if (symref) symref[0] = '\0';
	return 0;
}

void storage_ref_write(const char *refname, const git_oid *oid, const char *symref) {
	sqlite3_reset(st_ref_write);
	sqlite3_bind_text(st_ref_write, 1, refname, -1, SQLITE_STATIC);
	if (oid) sqlite3_bind_blob(st_ref_write, 2, oid->id, RAW, SQLITE_STATIC);
	else sqlite3_bind_null(st_ref_write, 2);
	if (symref) sqlite3_bind_text(st_ref_write, 3, symref, -1, SQLITE_STATIC);
	else sqlite3_bind_null(st_ref_write, 3);
	sqlite3_step(st_ref_write);
}

void storage_ref_delete(const char *refname) {
	sqlite3_reset(st_ref_delete);
	sqlite3_bind_text(st_ref_delete, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st_ref_delete);
}

int storage_ref_list(const char *prefix, storage_ref_cb cb, void *data) {
	if (prefix && *prefix) {
		char pattern[4096];
		snprintf(pattern, sizeof(pattern), "%s%%", prefix);
		sqlite3_stmt *st;
		sqlite3_prepare_v2(sdb,
			"SELECT refname, oid, symref FROM refs WHERE refname LIKE ? ORDER BY refname",
			-1, &st, 0);
		sqlite3_bind_text(st, 1, pattern, -1, SQLITE_STATIC);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			git_oid oid = {{0}};
			const void *blob = sqlite3_column_blob(st, 1);
			if (blob) memcpy(oid.id, blob, RAW);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (cb(name, blob ? &oid : NULL, sym, data)) break;
		}
		sqlite3_finalize(st);
	} else {
		sqlite3_reset(st_ref_list);
		while (sqlite3_step(st_ref_list) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st_ref_list, 0);
			git_oid oid = {{0}};
			const void *blob = sqlite3_column_blob(st_ref_list, 1);
			if (blob) memcpy(oid.id, blob, RAW);
			const char *sym = (const char *)sqlite3_column_text(st_ref_list, 2);
			if (cb(name, blob ? &oid : NULL, sym, data)) break;
		}
	}
	return 0;
}

/* ---- Reflog ---- */

int storage_reflog_exists(const char *refname) {
	sqlite3_reset(st_reflog_exists);
	sqlite3_bind_text(st_reflog_exists, 1, refname, -1, SQLITE_STATIC);
	return sqlite3_step(st_reflog_exists) == SQLITE_ROW;
}

void storage_reflog_delete(const char *refname) {
	sqlite3_reset(st_reflog_delete);
	sqlite3_bind_text(st_reflog_delete, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st_reflog_delete);
}

int storage_reflog_read(const char *refname, storage_reflog_cb cb, void *data) {
	sqlite3_reset(st_reflog_read);
	sqlite3_bind_text(st_reflog_read, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st_reflog_read) == SQLITE_ROW) {
		git_oid old_oid, new_oid;
		memcpy(old_oid.id, sqlite3_column_blob(st_reflog_read, 0), RAW);
		memcpy(new_oid.id, sqlite3_column_blob(st_reflog_read, 1), RAW);
		const char *committer = (const char *)sqlite3_column_text(st_reflog_read, 2);
		long long ts = sqlite3_column_int64(st_reflog_read, 3);
		int tz = sqlite3_column_int(st_reflog_read, 4);
		const char *msg = (const char *)sqlite3_column_text(st_reflog_read, 5);
		if (cb(&old_oid, &new_oid, committer, ts, tz, msg, data)) break;
	}
	return 0;
}

void storage_reflog_append(const char *refname, const git_oid *old_oid,
			   const git_oid *new_oid, const char *committer,
			   long long timestamp, int tz, const char *msg) {
	sqlite3_reset(st_reflog_append);
	sqlite3_bind_text(st_reflog_append, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st_reflog_append, 2, refname, -1, SQLITE_STATIC);
	sqlite3_bind_blob(st_reflog_append, 3, old_oid->id, RAW, SQLITE_STATIC);
	sqlite3_bind_blob(st_reflog_append, 4, new_oid->id, RAW, SQLITE_STATIC);
	sqlite3_bind_text(st_reflog_append, 5, committer, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st_reflog_append, 6, timestamp);
	sqlite3_bind_int(st_reflog_append, 7, tz);
	sqlite3_bind_text(st_reflog_append, 8, msg ? msg : "", -1, SQLITE_STATIC);
	sqlite3_step(st_reflog_append);
}

/* ---- LFS ---- */

void storage_lfs_sha256(const void *data, size_t len,
			unsigned char out[LFS_OID_RAWSZ]) {
	SHA256_CTX ctx;
	sha256_init(&ctx);
	sha256_update(&ctx, data, len);
	sha256_final(&ctx, out);
}

int storage_lfs_read(const unsigned char oid[LFS_OID_RAWSZ],
		     size_t *out_size, unsigned char **out_data) {
	sqlite3_reset(st_lfs_read);
	sqlite3_bind_blob(st_lfs_read, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	if (sqlite3_step(st_lfs_read) != SQLITE_ROW)
		return -1;
	*out_size = sqlite3_column_int(st_lfs_read, 0);
	const void *comp = sqlite3_column_blob(st_lfs_read, 1);
	int comp_len = sqlite3_column_bytes(st_lfs_read, 1);
	*out_data = zdecompress(comp, comp_len, *out_size);
	return *out_data ? 0 : -1;
}

void storage_lfs_write(const unsigned char oid[LFS_OID_RAWSZ],
		       const void *data, size_t size) {
	sqlite3_reset(st_lfs_exists);
	sqlite3_bind_blob(st_lfs_exists, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	if (sqlite3_step(st_lfs_exists) == SQLITE_ROW)
		return;

	unsigned long comp_len;
	unsigned char *comp = zcompress(data, size, &comp_len);
	if (!comp) return;

	sqlite3_reset(st_lfs_write);
	sqlite3_bind_blob(st_lfs_write, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_int64(st_lfs_write, 2, (sqlite3_int64)size);
	sqlite3_bind_blob(st_lfs_write, 3, comp, comp_len, SQLITE_STATIC);
	sqlite3_step(st_lfs_write);
	free(comp);
}

int storage_lfs_exists(const unsigned char oid[LFS_OID_RAWSZ]) {
	sqlite3_reset(st_lfs_exists);
	sqlite3_bind_blob(st_lfs_exists, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	return sqlite3_step(st_lfs_exists) == SQLITE_ROW;
}
