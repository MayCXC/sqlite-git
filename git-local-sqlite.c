/*
 * git-local-sqlite: local helper for SQLite-backed git storage.
 *
 * Speaks the local helper protocol (helper.h) for both ODB and ref
 * operations. Storage uses:
 *
 *   - Binary OIDs (BLOB(20)) for all keys
 *   - Zlib compression for all object data
 *   - Fossil delta compression for similar objects
 *   - WITHOUT ROWID tables for clustered primary keys
 *
 * Schema:
 *   objects(oid BLOB PRIMARY KEY, type INT, size INT, data BLOB, base BLOB)
 *   refs(refname TEXT PRIMARY KEY, oid BLOB, symref TEXT)
 *   reflog(refname TEXT, idx INT, old_oid BLOB, new_oid BLOB,
 *          committer TEXT, timestamp INT, tz INT, msg TEXT)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>
#include <sqlite3.h>
#include "vendor/delta.h"

static sqlite3 *db;

/* ---- Hex/binary OID conversion ---- */

static int hex2bin(const char *hex, unsigned char *bin, int binlen) {
	for (int i = 0; i < binlen; i++) {
		unsigned int byte;
		if (sscanf(hex + 2*i, "%02x", &byte) != 1) return -1;
		bin[i] = (unsigned char)byte;
	}
	return 0;
}

static void bin2hex(const unsigned char *bin, int binlen, char *hex) {
	for (int i = 0; i < binlen; i++)
		sprintf(hex + 2*i, "%02x", bin[i]);
	hex[2*binlen] = '\0';
}

#define OID_RAWSZ 20
#define OID_HEXSZ 40

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

/*
 * Try to find a base object of the same type for delta compression.
 * Returns the base OID (binary) or NULL if no suitable base found.
 * The base data and length are returned via out params.
 *
 * Heuristic: pick the most recent object of the same type.
 */
static sqlite3_stmt *st_find_base;

static int find_delta_base(int type, unsigned char *base_oid,
			   unsigned char **base_data, unsigned long *base_len) {
	sqlite3_reset(st_find_base);
	sqlite3_bind_int(st_find_base, 1, type);
	if (sqlite3_step(st_find_base) != SQLITE_ROW)
		return -1;

	const void *oid_blob = sqlite3_column_blob(st_find_base, 0);
	memcpy(base_oid, oid_blob, OID_RAWSZ);

	/* Read the base object (decompress it) */
	int base_size = sqlite3_column_int(st_find_base, 1);
	const void *compressed = sqlite3_column_blob(st_find_base, 2);
	int compressed_len = sqlite3_column_bytes(st_find_base, 2);
	const void *delta_base = sqlite3_column_blob(st_find_base, 3);

	/* Only use full objects (not deltas) as bases */
	if (delta_base != NULL)
		return -1;

	*base_data = zlib_decompress_buf(compressed, compressed_len, base_size);
	if (!*base_data) return -1;
	*base_len = base_size;
	return 0;
}

/* ---- Prepared statements ---- */

static sqlite3_stmt *st_obj_read, *st_obj_write, *st_obj_exists, *st_obj_list;
static sqlite3_stmt *st_ref_read, *st_ref_write, *st_ref_delete, *st_ref_list;
static sqlite3_stmt *st_reflog_read, *st_reflog_append, *st_reflog_exists, *st_reflog_delete;

static const char *type_name(int t) {
	switch (t) {
		case 1: return "commit";
		case 2: return "tree";
		case 3: return "blob";
		case 4: return "tag";
		default: return "unknown";
	}
}

static int type_from_name(const char *s) {
	if (!strcmp(s, "commit")) return 1;
	if (!strcmp(s, "tree")) return 2;
	if (!strcmp(s, "blob")) return 3;
	if (!strcmp(s, "tag")) return 4;
	return 0;
}

static int open_db(const char *path_arg) {
	char path[4096];
	char gitdir[4096];
	size_t len;

	snprintf(gitdir, sizeof(gitdir), "%s", path_arg);
	len = strlen(gitdir);
	if (len > 8 && !strcmp(gitdir + len - 8, "/objects"))
		gitdir[len - 8] = '\0';

	snprintf(path, sizeof(path), "%s/sqlite.db", gitdir);

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

	/* Object statements */
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
	/* Find a base for delta compression (most recent same-type full object) */
	sqlite3_prepare_v2(db,
		"SELECT oid, size, data, base FROM objects WHERE type = ? AND base IS NULL "
		"LIMIT 1",
		-1, &st_find_base, 0);

	/* Ref statements (binary OIDs) */
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

	/* Reflog statements (binary OIDs) */
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

/* ---- Read an object, resolving delta chains ---- */

static int read_object(const unsigned char *oid_bin,
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
		/* Full object: decompress zlib */
		*out_data = zlib_decompress_buf(compressed, compressed_len, *out_size);
		return *out_data ? 0 : -1;
	}

	/*
	 * Delta object: data is a raw fossil delta (not zlib compressed).
	 * Copy the delta and base OID before the recursive call, because
	 * read_object will reset st_obj_read and invalidate the pointers
	 * into SQLite's internal buffer.
	 */
	unsigned char base_oid[OID_RAWSZ];
	memcpy(base_oid, base_blob, OID_RAWSZ);

	int delta_len = compressed_len;
	char *delta_raw = malloc(delta_len);
	memcpy(delta_raw, compressed, delta_len);

	int base_type;
	unsigned long base_size;
	unsigned char *base_data;
	if (read_object(base_oid, &base_type, &base_size, &base_data) < 0) {
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

/* ---- Write an object with delta compression ---- */

static void write_object(const unsigned char *oid_bin, int type,
			 const unsigned char *data, unsigned long size) {
	/* Check if already exists */
	sqlite3_reset(st_obj_exists);
	sqlite3_bind_blob(st_obj_exists, 1, oid_bin, OID_RAWSZ, SQLITE_STATIC);
	if (sqlite3_step(st_obj_exists) == SQLITE_ROW)
		return; /* Already stored */

	/* Try delta compression against a same-type base */
	unsigned char base_oid[OID_RAWSZ];
	unsigned char *base_data = NULL;
	unsigned long base_len = 0;
	int use_delta = 0;
	char *delta_buf = NULL;
	int delta_len = 0;

	if (size > 64 && find_delta_base(type, base_oid, &base_data, &base_len) == 0) {
		/* Allocate worst-case delta buffer */
		delta_buf = malloc(size + 60);
		if (delta_buf) {
			delta_len = delta_create(
				(const char *)base_data, base_len,
				(const char *)data, size,
				delta_buf);
			/* Only use delta if it saves space */
			if (delta_len > 0 && (unsigned long)delta_len < size * 9 / 10)
				use_delta = 1;
			else {
				free(delta_buf);
				delta_buf = NULL;
			}
		}
		free(base_data);
	}

	/*
	 * Store: full objects are zlib compressed, deltas are stored raw.
	 * This avoids double-compression and simplifies the read path.
	 */
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

/* ---- ODB protocol commands ---- */

static void cmd_capabilities(void) {
	printf("get\ninfo\nput\nhave\nlist-objects\nodb-transaction\n"
	       "read\nlist\ntransaction\ncreate\nremove\n"
	       "reflog-read\nreflog-append\nreflog-exists\nreflog-delete\n\n");
	fflush(stdout);
}

static void cmd_info(const char *hex_oid) {
	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) { printf("missing\n"); fflush(stdout); return; }

	int type;
	unsigned long size;
	unsigned char *data;
	if (read_object(oid, &type, &size, &data) < 0) {
		printf("missing\n");
	} else {
		printf("%s %lu\n", type_name(type), size);
		free(data);
	}
	fflush(stdout);
}

static void cmd_get(const char *hex_oid) {
	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) { printf("missing\n"); fflush(stdout); return; }

	int type;
	unsigned long size;
	unsigned char *data;
	if (read_object(oid, &type, &size, &data) < 0) {
		printf("missing\n");
		fflush(stdout);
		return;
	}
	printf("%s %lu\n", type_name(type), size);
	fflush(stdout);
	if (size > 0)
		fwrite(data, 1, size, stdout);
	fflush(stdout);
	free(data);
}

static void cmd_put(const char *args) {
	char hex_oid[OID_HEXSZ + 1], type_str[32];
	unsigned long size;
	if (sscanf(args, "%40s %31s %lu", hex_oid, type_str, &size) != 3)
		return;

	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) return;

	int type = type_from_name(type_str);
	unsigned char *data = malloc(size ? size : 1);
	size_t got = 0;
	while (got < size) {
		size_t n = fread(data + got, 1, size - got, stdin);
		if (n == 0) break;
		got += n;
	}

	write_object(oid, type, data, size);
	free(data);

	printf("%s\n", hex_oid);
	fflush(stdout);
}

static void cmd_have(const char *hex_oid) {
	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) { printf("false\n"); fflush(stdout); return; }

	sqlite3_reset(st_obj_exists);
	sqlite3_bind_blob(st_obj_exists, 1, oid, OID_RAWSZ, SQLITE_STATIC);
	printf(sqlite3_step(st_obj_exists) == SQLITE_ROW ? "true\n" : "false\n");
	fflush(stdout);
}

static void cmd_list_objects(void) {
	char hex[OID_HEXSZ + 1];
	sqlite3_reset(st_obj_list);
	while (sqlite3_step(st_obj_list) == SQLITE_ROW) {
		const unsigned char *oid = sqlite3_column_blob(st_obj_list, 0);
		int type = sqlite3_column_int(st_obj_list, 1);
		int size = sqlite3_column_int(st_obj_list, 2);
		bin2hex(oid, OID_RAWSZ, hex);
		printf("%s %s %d\n", hex, type_name(type), size);
	}
	printf("\n");
	fflush(stdout);
}

static void cmd_odb_transaction_begin(void) {
	sqlite3_exec(db, "BEGIN", 0, 0, 0);
	printf("ok\n"); fflush(stdout);
}

static void cmd_odb_transaction_commit(void) {
	sqlite3_exec(db, "COMMIT", 0, 0, 0);
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref protocol commands ---- */

static void cmd_read(const char *refname) {
	char hex[OID_HEXSZ + 1];
	sqlite3_reset(st_ref_read);
	sqlite3_bind_text(st_ref_read, 1, refname, -1, SQLITE_STATIC);
	if (sqlite3_step(st_ref_read) != SQLITE_ROW) {
		printf("missing\n");
	} else {
		const void *oid_blob = sqlite3_column_blob(st_ref_read, 0);
		const char *symref = (const char *)sqlite3_column_text(st_ref_read, 1);
		if (symref && *symref)
			printf("symref %s\n", symref);
		else if (oid_blob) {
			bin2hex(oid_blob, OID_RAWSZ, hex);
			printf("%s\n", hex);
		} else {
			printf("missing\n");
		}
	}
	fflush(stdout);
}

static void cmd_list(const char *prefix) {
	char hex[OID_HEXSZ + 1];
	if (prefix && *prefix) {
		char pattern[4096];
		snprintf(pattern, sizeof(pattern), "%s%%", prefix);
		sqlite3_stmt *st;
		sqlite3_prepare_v2(db,
			"SELECT refname, oid, symref FROM refs WHERE refname LIKE ? ORDER BY refname",
			-1, &st, 0);
		sqlite3_bind_text(st, 1, pattern, -1, SQLITE_STATIC);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			const void *oid_blob = sqlite3_column_blob(st, 1);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (sym && *sym)
				printf("%s symref %s\n", name, sym);
			else if (oid_blob) {
				bin2hex(oid_blob, OID_RAWSZ, hex);
				printf("%s %s\n", name, hex);
			}
		}
		sqlite3_finalize(st);
	} else {
		sqlite3_reset(st_ref_list);
		while (sqlite3_step(st_ref_list) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st_ref_list, 0);
			const void *oid_blob = sqlite3_column_blob(st_ref_list, 1);
			const char *sym = (const char *)sqlite3_column_text(st_ref_list, 2);
			if (sym && *sym)
				printf("%s symref %s\n", name, sym);
			else if (oid_blob) {
				bin2hex(oid_blob, OID_RAWSZ, hex);
				printf("%s %s\n", name, hex);
			}
		}
	}
	printf("\n");
	fflush(stdout);
}

static void cmd_create(void) { printf("ok\n"); fflush(stdout); }

static void cmd_remove(void) {
	sqlite3_exec(db, "DROP TABLE IF EXISTS objects;"
		     "DROP TABLE IF EXISTS refs;"
		     "DROP TABLE IF EXISTS reflog;", 0, 0, 0);
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref transaction commands ---- */

static int in_ref_transaction;

static void cmd_transaction_begin(void) {
	if (!in_ref_transaction) {
		sqlite3_exec(db, "SAVEPOINT ref_txn", 0, 0, 0);
		in_ref_transaction = 1;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_update(const char *args) {
	char refname[4096], new_hex[OID_HEXSZ + 1], old_hex[OID_HEXSZ + 1];
	unsigned char new_oid[OID_RAWSZ];
	if (sscanf(args, "%4095s %40s %40s", refname, new_hex, old_hex) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	if (hex2bin(new_hex, new_oid, OID_RAWSZ) < 0) {
		printf("error bad oid\n"); fflush(stdout); return;
	}
	sqlite3_reset(st_ref_write);
	sqlite3_bind_text(st_ref_write, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_blob(st_ref_write, 2, new_oid, OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_null(st_ref_write, 3);
	sqlite3_step(st_ref_write);
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_create(const char *args) {
	cmd_transaction_update(args); /* Same logic */
}

static void cmd_transaction_delete(const char *args) {
	char refname[4096];
	if (sscanf(args, "%4095s", refname) < 1) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	sqlite3_reset(st_ref_delete);
	sqlite3_bind_text(st_ref_delete, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st_ref_delete);
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_create_symref(const char *args) {
	char refname[4096], target[4096];
	if (sscanf(args, "%4095s %4095s", refname, target) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	sqlite3_reset(st_ref_write);
	sqlite3_bind_text(st_ref_write, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_null(st_ref_write, 2);
	sqlite3_bind_text(st_ref_write, 3, target, -1, SQLITE_STATIC);
	sqlite3_step(st_ref_write);
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_prepare(void) { printf("ok\n"); fflush(stdout); }

static void cmd_transaction_finish(void) {
	if (in_ref_transaction) {
		sqlite3_exec(db, "RELEASE ref_txn", 0, 0, 0);
		in_ref_transaction = 0;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_abort(void) {
	if (in_ref_transaction) {
		sqlite3_exec(db, "ROLLBACK TO ref_txn; RELEASE ref_txn", 0, 0, 0);
		in_ref_transaction = 0;
	}
	printf("ok\n"); fflush(stdout);
}

/* ---- Reflog commands ---- */

static void cmd_reflog_read(const char *refname) {
	char old_hex[OID_HEXSZ + 1], new_hex[OID_HEXSZ + 1];
	sqlite3_reset(st_reflog_read);
	sqlite3_bind_text(st_reflog_read, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st_reflog_read) == SQLITE_ROW) {
		const unsigned char *old_oid = sqlite3_column_blob(st_reflog_read, 0);
		const unsigned char *new_oid = sqlite3_column_blob(st_reflog_read, 1);
		const char *committer = (const char *)sqlite3_column_text(st_reflog_read, 2);
		sqlite3_int64 timestamp = sqlite3_column_int64(st_reflog_read, 3);
		int tz = sqlite3_column_int(st_reflog_read, 4);
		const char *msg = (const char *)sqlite3_column_text(st_reflog_read, 5);
		bin2hex(old_oid, OID_RAWSZ, old_hex);
		bin2hex(new_oid, OID_RAWSZ, new_hex);
		printf("%s %s %s %lld %+05d\t%s\n",
		       old_hex, new_hex, committer,
		       (long long)timestamp, tz, msg ? msg : "");
	}
	printf("\n");
	fflush(stdout);
}

static void cmd_reflog_exists(const char *refname) {
	sqlite3_reset(st_reflog_exists);
	sqlite3_bind_text(st_reflog_exists, 1, refname, -1, SQLITE_STATIC);
	printf(sqlite3_step(st_reflog_exists) == SQLITE_ROW ? "true\n" : "false\n");
	fflush(stdout);
}

static void cmd_reflog_append(const char *args) {
	/*
	 * reflog-append <refname> <old-oid> <new-oid> <committer> <ts> <tz> <msg>
	 * The committer field may contain spaces (e.g. "Name <email>").
	 * Parse fixed fields first, then treat rest as committer+ts+tz+msg.
	 */
	char refname[4096], old_hex[OID_HEXSZ + 1], new_hex[OID_HEXSZ + 1];
	unsigned char old_oid[OID_RAWSZ], new_oid[OID_RAWSZ];

	/* Parse refname old new */
	const char *p = args;
	const char *sp = strchr(p, ' ');
	if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(refname, sizeof(refname), "%.*s", (int)(sp - p), p);
	p = sp + 1;

	sp = strchr(p, ' ');
	if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(old_hex, sizeof(old_hex), "%.*s", (int)(sp - p), p);
	p = sp + 1;

	sp = strchr(p, ' ');
	if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(new_hex, sizeof(new_hex), "%.*s", (int)(sp - p), p);
	p = sp + 1;

	if (hex2bin(old_hex, old_oid, OID_RAWSZ) < 0 ||
	    hex2bin(new_hex, new_oid, OID_RAWSZ) < 0) {
		printf("error bad oid\n"); fflush(stdout); return;
	}

	/* Rest is: committer timestamp tz msg
	 * Find '>' to end committer, then parse ts and tz */
	const char *email_end = strchr(p, '>');
	if (!email_end) { printf("error bad committer\n"); fflush(stdout); return; }
	email_end++;

	char committer[512];
	snprintf(committer, sizeof(committer), "%.*s", (int)(email_end - p), p);

	long long timestamp = 0;
	int tz = 0;
	char msg[4096] = "";
	if (*email_end == ' ') {
		char *end;
		timestamp = strtoll(email_end + 1, &end, 10);
		if (*end == ' ') tz = strtol(end + 1, &end, 10);
		if (*end == '\t') snprintf(msg, sizeof(msg), "%s", end + 1);
	}

	sqlite3_reset(st_reflog_append);
	sqlite3_bind_text(st_reflog_append, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st_reflog_append, 2, refname, -1, SQLITE_STATIC);
	sqlite3_bind_blob(st_reflog_append, 3, old_oid, OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_blob(st_reflog_append, 4, new_oid, OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_text(st_reflog_append, 5, committer, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st_reflog_append, 6, timestamp);
	sqlite3_bind_int(st_reflog_append, 7, tz);
	sqlite3_bind_text(st_reflog_append, 8, msg, -1, SQLITE_STATIC);
	sqlite3_step(st_reflog_append);

	printf("ok\n"); fflush(stdout);
}

static void cmd_reflog_delete(const char *refname) {
	sqlite3_reset(st_reflog_delete);
	sqlite3_bind_text(st_reflog_delete, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st_reflog_delete);
	printf("ok\n"); fflush(stdout);
}

/* ---- Main loop ---- */

int local_main(int argc, char **argv) {
	if (argc < 2) {
		fprintf(stderr, "usage: git-local-sqlite <gitdir>\n");
		return 1;
	}

	if (open_db(argv[1]) < 0) {
		fprintf(stderr, "error: cannot open database at %s\n", argv[1]);
		return 1;
	}

	char line[8192];
	while (fgets(line, sizeof(line), stdin)) {
		size_t len = strlen(line);
		if (len > 0 && line[len - 1] == '\n') line[--len] = 0;

		if (!strcmp(line, "capabilities")) cmd_capabilities();
		else if (!strncmp(line, "info ", 5)) cmd_info(line + 5);
		else if (!strncmp(line, "get ", 4)) cmd_get(line + 4);
		else if (!strncmp(line, "put ", 4)) cmd_put(line + 4);
		else if (!strncmp(line, "have ", 5)) cmd_have(line + 5);
		else if (!strcmp(line, "list-objects")) cmd_list_objects();
		else if (!strcmp(line, "odb-transaction-begin")) cmd_odb_transaction_begin();
		else if (!strcmp(line, "odb-transaction-commit")) cmd_odb_transaction_commit();
		else if (!strncmp(line, "read ", 5)) cmd_read(line + 5);
		else if (!strncmp(line, "list ", 5)) cmd_list(line + 5);
		else if (!strcmp(line, "list")) cmd_list(NULL);
		else if (!strcmp(line, "create")) cmd_create();
		else if (!strcmp(line, "remove")) cmd_remove();
		else if (!strcmp(line, "transaction-begin")) cmd_transaction_begin();
		else if (!strncmp(line, "transaction-update ", 19)) cmd_transaction_update(line + 19);
		else if (!strncmp(line, "transaction-create-symref ", 25)) cmd_transaction_create_symref(line + 25);
		else if (!strncmp(line, "transaction-create ", 19)) cmd_transaction_create(line + 19);
		else if (!strncmp(line, "transaction-delete ", 19)) cmd_transaction_delete(line + 19);
		else if (!strcmp(line, "transaction-prepare")) cmd_transaction_prepare();
		else if (!strcmp(line, "transaction-finish")) cmd_transaction_finish();
		else if (!strcmp(line, "transaction-abort")) cmd_transaction_abort();
		else if (!strncmp(line, "reflog-read ", 12)) cmd_reflog_read(line + 12);
		else if (!strncmp(line, "reflog-append ", 14)) cmd_reflog_append(line + 14);
		else if (!strncmp(line, "reflog-exists ", 14)) cmd_reflog_exists(line + 14);
		else if (!strncmp(line, "reflog-delete ", 14)) cmd_reflog_delete(line + 14);
	}

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
	return 0;
}
