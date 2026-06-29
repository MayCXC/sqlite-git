/*
 * Git-LFS content storage layer for sqlite-git.
 *
 * Built on the dependency-free core (storage.c) plus the vendored sha256, and
 * INDEPENDENT of the git object layer (storage_git.c): it includes only
 * storage_git_lfs.h -> storage.h, so it cannot reach a git object/ref symbol, and
 * the LFS-only consumers (the transfer agent, the standalone LFS extension) do
 * not link storage_git.o. A full sqlite-git repo composes this with storage_git
 * over one database (the lfs tables coexist with the objects/refs tables).
 *
 * LFS content is addressed by its own sha256 oid (git-lfs's content address, not
 * a git object id), stored raw (LFS media is already entropy-coded), chunked into
 * fixed-size frame rows under a manifest row.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT3
#else
  #include <sqlite3.h>
#endif
#include "storage_git_lfs.h"
#include "vendor/sha256.h"

enum stmt_id {
	STMT_LFS_MANIFEST_READ,
	STMT_LFS_MANIFEST_WRITE,
	STMT_LFS_CHUNK_READ,
	STMT_LFS_CHUNK_WRITE,
	STMT_LFS_EXISTS,
	STMT_COUNT
};

static const char *const stmt_sql[STMT_COUNT] = {
	[STMT_LFS_MANIFEST_READ] =
		"SELECT size, nchunks FROM lfs WHERE oid = ?",
	[STMT_LFS_MANIFEST_WRITE] =
		"INSERT OR IGNORE INTO lfs(oid, size, nchunks) VALUES(?,?,?)",
	[STMT_LFS_CHUNK_READ] =
		"SELECT data FROM lfs_chunk WHERE oid = ? ORDER BY seq",
	[STMT_LFS_CHUNK_WRITE] =
		"INSERT INTO lfs_chunk(oid, seq, data) VALUES(?,?,?)",
	[STMT_LFS_EXISTS] =
		"SELECT 1 FROM lfs WHERE oid = ?",
};

/* The prepared handles, parallel to stmt_sql[]. Zero-initialised: owned mode
 * caches each on first use and finalises them at close (the registered hook);
 * borrowed mode leaves it NULL and each call prepares + finalises its own. */
static sqlite3_stmt *stmt_prepared[STMT_COUNT];

static sqlite3_stmt *stmt_acquire(enum stmt_id id) {
	return storage_stmt(stmt_sql[id], &stmt_prepared[id]);
}

static void stmt_release(enum stmt_id id, sqlite3_stmt *used) {
	storage_stmt_done(used, &stmt_prepared[id]);
}

static void lfs_stmts_close(void) {
	for (int i = 0; i < STMT_COUNT; i++) {
		sqlite3_finalize(stmt_prepared[i]);
		stmt_prepared[i] = NULL;
	}
}

/* ---- Schema + init ---- */

/*
 * The LFS schema: a manifest row (oid -> size, nchunks) and the frame rows
 * (oid, seq -> data). lfs.data is absent; the chunked content lives in lfs_chunk,
 * a rowid table (large inline BLOB), keyed (oid, seq) by a UNIQUE index.
 */
static void create_lfs_schema(sqlite3 *db) {
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS lfs("
		"  oid BLOB PRIMARY KEY,"
		"  size INTEGER NOT NULL,"
		"  nchunks INTEGER NOT NULL"
		");", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS lfs_chunk("
		"  oid BLOB NOT NULL,"
		"  seq INTEGER NOT NULL,"
		"  data BLOB NOT NULL,"
		"  PRIMARY KEY(oid, seq)"
		");", 0, 0, 0);
}

int storage_lfs_init(void) {
	sqlite3 *db = storage_db();
	if (!db) return -1;
	create_lfs_schema(db);
	storage_at_close(lfs_stmts_close);
	return 0;
}

/* ---- LFS oid codec (sha256 content address) ---- */

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

/*
 * LFS content is chunked into fixed-size frame rows (lfs_chunk) under a manifest
 * row (lfs.oid -> size, nchunks). Frames are stored raw: LFS media is already
 * entropy-coded, so compression buys nothing, and chunking keeps each cell well
 * under SQLITE_MAX_LENGTH and bounds streaming RAM to one frame. Content is
 * addressed by its sha256 oid, so a write of an oid already present is a no-op.
 */
#define LFS_CHUNK_SIZE (4 * 1024 * 1024)

static int storage_lfs_exists(const unsigned char oid[LFS_OID_RAWSZ]) {
	sqlite3_stmt *st = stmt_acquire(STMT_LFS_EXISTS);
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	int exists = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(STMT_LFS_EXISTS, st);
	return exists;
}

static int storage_lfs_put_chunk(const unsigned char oid[LFS_OID_RAWSZ],
				 long long seq, const void *data, size_t len) {
	sqlite3_stmt *st = stmt_acquire(STMT_LFS_CHUNK_WRITE);
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_int64(st, 2, seq);
	int rc = sqlite3_bind_blob64(st, 3, data, len, SQLITE_STATIC);
	if (rc == SQLITE_OK) rc = sqlite3_step(st);
	stmt_release(STMT_LFS_CHUNK_WRITE, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

static int storage_lfs_put_manifest(const unsigned char oid[LFS_OID_RAWSZ],
				    size_t size, long long nchunks) {
	sqlite3_stmt *st = stmt_acquire(STMT_LFS_MANIFEST_WRITE);
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_int64(st, 2, (sqlite3_int64)size);
	sqlite3_bind_int64(st, 3, nchunks);
	int rc = sqlite3_step(st);
	stmt_release(STMT_LFS_MANIFEST_WRITE, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* Stream a content stream into the store a frame at a time (RAM bounded to one
 * frame): the transfer adapter's path for arbitrarily large objects. */
int storage_lfs_write_stream(const unsigned char oid[LFS_OID_RAWSZ],
			     FILE *src, size_t size) {
	if (storage_lfs_exists(oid)) return 0;
	if (storage_savepoint("lfs_write")) return -1;

	unsigned char *buf = malloc(LFS_CHUNK_SIZE);
	if (!buf) { storage_rollback_to("lfs_write"); return -1; }

	size_t remaining = size;
	long long seq = 0;
	int failed = 0;
	while (remaining > 0) {
		size_t want = remaining < LFS_CHUNK_SIZE ? remaining : LFS_CHUNK_SIZE;
		if (fread(buf, 1, want, src) != want ||
		    storage_lfs_put_chunk(oid, seq, buf, want) != 0) {
			failed = 1;
			break;
		}
		remaining -= want;
		seq++;
	}
	free(buf);
	if (failed || storage_lfs_put_manifest(oid, size, seq) != 0) {
		storage_rollback_to("lfs_write");
		return -1;
	}
	return storage_release("lfs_write");
}

/* Stream a stored object out to a sink a frame at a time (RAM bounded to one
 * frame). Returns -1 if the oid is not stored. */
int storage_lfs_read_stream(const unsigned char oid[LFS_OID_RAWSZ], FILE *dst) {
	sqlite3_stmt *mst = stmt_acquire(STMT_LFS_MANIFEST_READ);
	sqlite3_bind_blob(mst, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	int found = (sqlite3_step(mst) == SQLITE_ROW);
	stmt_release(STMT_LFS_MANIFEST_READ, mst);
	if (!found) return -1;

	sqlite3_stmt *st = stmt_acquire(STMT_LFS_CHUNK_READ);
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	int ret = 0;
	while (sqlite3_step(st) == SQLITE_ROW) {
		const void *data = sqlite3_column_blob(st, 0);
		int len = sqlite3_column_bytes(st, 0);
		if (len && (!data || fwrite(data, 1, (size_t)len, dst) != (size_t)len)) {
			ret = -2;	/* found, but writing to the sink failed (vs -1 = not stored) */
			break;
		}
	}
	stmt_release(STMT_LFS_CHUNK_READ, st);
	return ret;
}

/* Whole-buffer variant for the extension's SQL functions, where a value is
 * inherently int-bounded: chunk the buffer directly, no streaming source. */
int storage_lfs_write(const unsigned char oid[LFS_OID_RAWSZ],
		      const void *data, size_t size) {
	if (storage_lfs_exists(oid)) return 0;
	if (storage_savepoint("lfs_write")) return -1;

	size_t off = 0;
	long long seq = 0;
	int failed = 0;
	while (off < size) {
		size_t want = (size - off) < LFS_CHUNK_SIZE ? (size - off) : LFS_CHUNK_SIZE;
		if (storage_lfs_put_chunk(oid, seq, (const unsigned char *)data + off, want) != 0) {
			failed = 1;
			break;
		}
		off += want;
		seq++;
	}
	if (failed || storage_lfs_put_manifest(oid, size, seq) != 0) {
		storage_rollback_to("lfs_write");
		return -1;
	}
	return storage_release("lfs_write");
}

/* Whole-buffer variant for the extension: assemble the frames into one buffer
 * the caller frees. */
int storage_lfs_read(const unsigned char oid[LFS_OID_RAWSZ],
		     size_t *out_size, unsigned char **out_data) {
	sqlite3_stmt *mst = stmt_acquire(STMT_LFS_MANIFEST_READ);
	sqlite3_bind_blob(mst, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	if (sqlite3_step(mst) != SQLITE_ROW) {
		stmt_release(STMT_LFS_MANIFEST_READ, mst);
		return -1;
	}
	size_t size = (size_t)sqlite3_column_int64(mst, 0);
	stmt_release(STMT_LFS_MANIFEST_READ, mst);

	unsigned char *buf = malloc(size ? size : 1);
	if (!buf) return -1;

	sqlite3_stmt *st = stmt_acquire(STMT_LFS_CHUNK_READ);
	sqlite3_bind_blob(st, 1, oid, LFS_OID_RAWSZ, SQLITE_STATIC);
	size_t off = 0;
	int failed = 0;
	while (sqlite3_step(st) == SQLITE_ROW) {
		const void *data = sqlite3_column_blob(st, 0);
		int len = sqlite3_column_bytes(st, 0);
		if (len < 0 || off + (size_t)len > size) { failed = 1; break; }
		if (len) memcpy(buf + off, data, (size_t)len);
		off += (size_t)len;
	}
	stmt_release(STMT_LFS_CHUNK_READ, st);
	if (failed || off != size) { free(buf); return -1; }
	*out_size = size;
	*out_data = buf;
	return 0;
}
