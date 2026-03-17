/*
 * Shared storage layer for sqlite-git.
 *
 * Provides the SQLite schema, compression, delta encoding, and
 * object/ref access used by both local and remote helpers.
 */
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <sqlite3.h>
#include "storage.h"
#include "vendor/delta.h"

static sqlite3 *db;

/* Prepared statements */
static sqlite3_stmt *st_obj_read, *st_obj_write, *st_obj_exists, *st_obj_list;
static sqlite3_stmt *st_find_base;
static sqlite3_stmt *st_ref_read, *st_ref_write, *st_ref_delete, *st_ref_list;
static sqlite3_stmt *st_reflog_read, *st_reflog_append, *st_reflog_exists, *st_reflog_delete;

/* ---- Hex/binary OID conversion ---- */

int hex2bin(const char *hex, unsigned char *bin, int binlen) {
	for (int i = 0; i < binlen; i++) {
		unsigned int byte;
		if (sscanf(hex + 2*i, "%02x", &byte) != 1) return -1;
		bin[i] = (unsigned char)byte;
	}
	return 0;
}

void bin2hex(const unsigned char *bin, int binlen, char *hex) {
	for (int i = 0; i < binlen; i++)
		sprintf(hex + 2*i, "%02x", bin[i]);
	hex[2*binlen] = '\0';
}

/* ---- Type conversion ---- */

const char *type_name(int t) {
	switch (t) {
		case 1: return "commit";
		case 2: return "tree";
		case 3: return "blob";
		case 4: return "tag";
		default: return "unknown";
	}
}

int type_from_name(const char *s) {
	if (!strcmp(s, "commit")) return 1;
	if (!strcmp(s, "tree")) return 2;
	if (!strcmp(s, "blob")) return 3;
	if (!strcmp(s, "tag")) return 4;
	return 0;
}

/* ---- Zlib helpers ---- */

static unsigned char *zlib_compress_buf(const void *src, unsigned long srclen,
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

static unsigned char *zlib_decompress_buf(const void *src, unsigned long srclen,
					  unsigned long orig_size) {
	unsigned char *dst = malloc(orig_size);
	unsigned long dstlen = orig_size;
	if (!dst) return NULL;
	if (uncompress(dst, &dstlen, src, srclen) != Z_OK) {
		free(dst);
		return NULL;
	}
	return dst;
}

/* ---- Delta helpers ---- */

static int find_delta_base(int type, unsigned char *base_oid,
			   unsigned char **base_data, unsigned long *base_len) {
	sqlite3_reset(st_find_base);
	sqlite3_bind_int(st_find_base, 1, type);
	if (sqlite3_step(st_find_base) != SQLITE_ROW)
		return -1;

	const void *oid_blob = sqlite3_column_blob(st_find_base, 0);
	memcpy(base_oid, oid_blob, OID_RAWSZ);

	int base_size = sqlite3_column_int(st_find_base, 1);
	const void *compressed = sqlite3_column_blob(st_find_base, 2);
	int compressed_len = sqlite3_column_bytes(st_find_base, 2);
	const void *delta_base = sqlite3_column_blob(st_find_base, 3);

	if (delta_base != NULL)
		return -1;

	*base_data = zlib_decompress_buf(compressed, compressed_len, base_size);
	if (!*base_data) return -1;
	*base_len = base_size;
	return 0;
}

/* ---- Database ---- */

int storage_open(const char *path_arg) {
	char path[4096];
	char gitdir[4096];
	size_t len;

	snprintf(gitdir, sizeof(gitdir), "%s", path_arg);
	len = strlen(gitdir);
	if (len > 8 && !strcmp(gitdir + len - 8, "/objects"))
		gitdir[len - 8] = '\0';

	snprintf(path, sizeof(path), "%s/sqlite.db", gitdir);

	/* Create directory if it doesn't exist */
	mkdir(gitdir, 0755);

	if (sqlite3_open(path, &db) != SQLITE_OK)
		return -1;

	sqlite3_exec(db,
		"PRAGMA busy_timeout = 5000;"
		"PRAGMA synchronous = NORMAL;"
		"PRAGMA journal_mode = WAL;"
		"PRAGMA page_size = 8192;"

		"CREATE TABLE IF NOT EXISTS objects("
		"  oid BLOB PRIMARY KEY,"
		"  type INTEGER NOT NULL,"
		"  size INTEGER NOT NULL,"
		"  data BLOB NOT NULL,"
		"  base BLOB"
		") WITHOUT ROWID;"

		"CREATE TABLE IF NOT EXISTS refs("
		"  refname TEXT PRIMARY KEY,"
		"  oid BLOB,"
		"  symref TEXT"
		") WITHOUT ROWID;"

		"CREATE TABLE IF NOT EXISTS reflog("
		"  refname TEXT NOT NULL,"
		"  idx INTEGER NOT NULL,"
		"  old_oid BLOB NOT NULL,"
		"  new_oid BLOB NOT NULL,"
		"  committer TEXT NOT NULL,"
		"  timestamp INTEGER NOT NULL,"
		"  tz INTEGER NOT NULL,"
		"  msg TEXT NOT NULL DEFAULT '',"
		"  PRIMARY KEY(refname, idx)"
		") WITHOUT ROWID;",
		0, 0, 0);

	sqlite3_prepare_v2(db,
		"SELECT type, size, data, base FROM objects WHERE oid = ?",
		-1, &st_obj_read, 0);
	sqlite3_prepare_v2(db,
		"INSERT OR IGNORE INTO objects(oid, type, size, data, base) VALUES(?, ?, ?, ?, ?)",
		-1, &st_obj_write, 0);
	sqlite3_prepare_v2(db,
		"SELECT 1 FROM objects WHERE oid = ?",
		-1, &st_obj_exists, 0);
	sqlite3_prepare_v2(db,
		"SELECT oid, type, size FROM objects",
		-1, &st_obj_list, 0);
	sqlite3_prepare_v2(db,
		"SELECT oid, size, data, base FROM objects WHERE type = ? AND base IS NULL LIMIT 1",
		-1, &st_find_base, 0);

	sqlite3_prepare_v2(db,
		"SELECT oid, symref FROM refs WHERE refname = ?",
		-1, &st_ref_read, 0);
	sqlite3_prepare_v2(db,
		"INSERT OR REPLACE INTO refs(refname, oid, symref) VALUES(?, ?, ?)",
		-1, &st_ref_write, 0);
	sqlite3_prepare_v2(db,
		"DELETE FROM refs WHERE refname = ?",
		-1, &st_ref_delete, 0);
	sqlite3_prepare_v2(db,
		"SELECT refname, oid, symref FROM refs ORDER BY refname",
		-1, &st_ref_list, 0);

	sqlite3_prepare_v2(db,
		"SELECT old_oid, new_oid, committer, timestamp, tz, msg "
		"FROM reflog WHERE refname = ? ORDER BY idx ASC",
		-1, &st_reflog_read, 0);
	sqlite3_prepare_v2(db,
		"INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg) "
		"VALUES(?, COALESCE((SELECT MAX(idx)+1 FROM reflog WHERE refname = ?), 0), ?, ?, ?, ?, ?, ?)",
		-1, &st_reflog_append, 0);
	sqlite3_prepare_v2(db,
		"SELECT 1 FROM reflog WHERE refname = ? LIMIT 1",
		-1, &st_reflog_exists, 0);
	sqlite3_prepare_v2(db,
		"DELETE FROM reflog WHERE refname = ?",
		-1, &st_reflog_delete, 0);

	return 0;
}

void storage_close(void) {
	sqlite3_finalize(st_obj_read);
	sqlite3_finalize(st_obj_write);
	sqlite3_finalize(st_obj_exists);
	sqlite3_finalize(st_obj_list);
	sqlite3_finalize(st_find_base);
	sqlite3_finalize(st_ref_read);
	sqlite3_finalize(st_ref_write);
	sqlite3_finalize(st_ref_delete);
	sqlite3_finalize(st_ref_list);
	sqlite3_finalize(st_reflog_read);
	sqlite3_finalize(st_reflog_append);
	sqlite3_finalize(st_reflog_exists);
	sqlite3_finalize(st_reflog_delete);
	sqlite3_close(db);
}

sqlite3 *storage_db(void) { return db; }

/* ---- Read object (resolves delta chains) ---- */

int storage_read_object(const unsigned char *oid_bin,
			int *out_type, unsigned long *out_size,
			unsigned char **out_data) {
	sqlite3_reset(st_obj_read);
	sqlite3_bind_blob(st_obj_read, 1, oid_bin, OID_RAWSZ, SQLITE_STATIC);
	if (sqlite3_step(st_obj_read) != SQLITE_ROW)
		return -1;

	*out_type = sqlite3_column_int(st_obj_read, 0);
	*out_size = sqlite3_column_int(st_obj_read, 1);
	const void *compressed = sqlite3_column_blob(st_obj_read, 2);
	int compressed_len = sqlite3_column_bytes(st_obj_read, 2);
	const void *base_blob = sqlite3_column_blob(st_obj_read, 3);

	if (base_blob == NULL) {
		*out_data = zlib_decompress_buf(compressed, compressed_len, *out_size);
		return *out_data ? 0 : -1;
	}

	/*
	 * Delta object: copy delta and base OID before recursive call
	 * because st_obj_read will be reset, invalidating SQLite's
	 * internal buffer pointers.
	 */
	unsigned char base_oid[OID_RAWSZ];
	memcpy(base_oid, base_blob, OID_RAWSZ);

	int delta_len = compressed_len;
	char *delta_raw = malloc(delta_len);
	memcpy(delta_raw, compressed, delta_len);

	int base_type;
	unsigned long base_size;
	unsigned char *base_data;
	if (storage_read_object(base_oid, &base_type, &base_size, &base_data) < 0) {
		free(delta_raw);
		return -1;
	}

	int target_size = delta_output_size(delta_raw, delta_len);
	if (target_size < 0) { free(base_data); free(delta_raw); return -1; }

	*out_data = malloc(target_size);
	if (delta_apply((const char *)base_data, base_size,
			delta_raw, delta_len,
			(char *)*out_data) < 0) {
		free(base_data);
		free(delta_raw);
		free(*out_data);
		return -1;
	}
	*out_size = target_size;
	free(base_data);
	free(delta_raw);
	return 0;
}

/* ---- Write object (zlib + delta compression) ---- */

void storage_write_object(const unsigned char *oid_bin, int type,
			  const unsigned char *data, unsigned long size) {
	sqlite3_reset(st_obj_exists);
	sqlite3_bind_blob(st_obj_exists, 1, oid_bin, OID_RAWSZ, SQLITE_STATIC);
	if (sqlite3_step(st_obj_exists) == SQLITE_ROW)
		return;

	unsigned char base_oid[OID_RAWSZ];
	unsigned char *base_data = NULL;
	unsigned long base_len = 0;
	int use_delta = 0;
	char *delta_buf = NULL;
	int delta_len = 0;

	if (size > 64 && find_delta_base(type, base_oid, &base_data, &base_len) == 0) {
		delta_buf = malloc(size + 60);
		if (delta_buf) {
			delta_len = delta_create(
				(const char *)base_data, base_len,
				(const char *)data, size,
				delta_buf);
			if (delta_len > 0 && (unsigned long)delta_len < size * 9 / 10)
				use_delta = 1;
			else {
				free(delta_buf);
				delta_buf = NULL;
			}
		}
		free(base_data);
	}

	sqlite3_reset(st_obj_write);
	sqlite3_bind_blob(st_obj_write, 1, oid_bin, OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_int(st_obj_write, 2, type);
	sqlite3_bind_int64(st_obj_write, 3, (sqlite3_int64)size);

	if (use_delta) {
		sqlite3_bind_blob(st_obj_write, 4, delta_buf, delta_len, SQLITE_STATIC);
		sqlite3_bind_blob(st_obj_write, 5, base_oid, OID_RAWSZ, SQLITE_STATIC);
	} else {
		unsigned long compressed_len;
		unsigned char *compressed = zlib_compress_buf(data, size, &compressed_len);
		if (!compressed) { free(delta_buf); return; }
		sqlite3_bind_blob(st_obj_write, 4, compressed, compressed_len, SQLITE_STATIC);
		sqlite3_bind_null(st_obj_write, 5);
		sqlite3_step(st_obj_write);
		free(compressed);
		free(delta_buf);
		return;
	}
	sqlite3_step(st_obj_write);
	free(delta_buf);
}

int storage_object_exists(const unsigned char *oid_bin) {
	sqlite3_reset(st_obj_exists);
	sqlite3_bind_blob(st_obj_exists, 1, oid_bin, OID_RAWSZ, SQLITE_STATIC);
	return sqlite3_step(st_obj_exists) == SQLITE_ROW;
}

/* Statement accessors */
sqlite3_stmt *storage_ref_read_stmt(void) { return st_ref_read; }
sqlite3_stmt *storage_ref_write_stmt(void) { return st_ref_write; }
sqlite3_stmt *storage_ref_delete_stmt(void) { return st_ref_delete; }
sqlite3_stmt *storage_ref_list_stmt(void) { return st_ref_list; }
sqlite3_stmt *storage_obj_list_stmt(void) { return st_obj_list; }
sqlite3_stmt *storage_reflog_read_stmt(void) { return st_reflog_read; }
sqlite3_stmt *storage_reflog_append_stmt(void) { return st_reflog_append; }
sqlite3_stmt *storage_reflog_exists_stmt(void) { return st_reflog_exists; }
sqlite3_stmt *storage_reflog_delete_stmt(void) { return st_reflog_delete; }
