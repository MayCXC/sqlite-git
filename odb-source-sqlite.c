/*
 * SQLite-backed object database source for git.
 * Implements odb_source vtable with objects stored in SQLite tables.
 */

#include "git-compat-util.h"
#include "hash.h"
#include "hex.h"
#include "object.h"
#include "object-file.h"
#include "odb.h"
#include "odb/source.h"
#include "odb/source-sqlite.h"
#include "repository.h"
#include "strvec.h"

#include <sqlite3.h>

/* struct defined in source-sqlite.h */

static int ensure_db(struct odb_source_sqlite *src)
{
	if (src->db)
		return 0;
	if (sqlite3_open(src->base.path, &src->db) != SQLITE_OK)
		return -1;
	sqlite3_exec(src->db,
		"PRAGMA busy_timeout = 3000;"
		"PRAGMA synchronous = NORMAL;"
		"PRAGMA journal_mode = WAL;"
		"CREATE TABLE IF NOT EXISTS objects_loose("
		"  oid TEXT PRIMARY KEY,"
		"  type INTEGER NOT NULL,"
		"  size INTEGER NOT NULL,"
		"  data BLOB NOT NULL"
		");",
		0, 0, 0);
	sqlite3_prepare_v2(src->db,
		"SELECT type, size, data FROM objects_loose WHERE oid = ?",
		-1, &src->st_read_data, 0);
	sqlite3_prepare_v2(src->db,
		"INSERT OR IGNORE INTO objects_loose(oid, type, size, data) VALUES(?, ?, ?, ?)",
		-1, &src->st_write, 0);
	sqlite3_prepare_v2(src->db,
		"SELECT 1 FROM objects_loose WHERE oid = ?",
		-1, &src->st_exists, 0);
	sqlite3_prepare_v2(src->db,
		"SELECT oid, type, size FROM objects_loose",
		-1, &src->st_iterate, 0);
	return 0;
}

static void close_stmts(struct odb_source_sqlite *src)
{
	if (src->st_read_data) { sqlite3_finalize(src->st_read_data); src->st_read_data = NULL; }
	if (src->st_write) { sqlite3_finalize(src->st_write); src->st_write = NULL; }
	if (src->st_exists) { sqlite3_finalize(src->st_exists); src->st_exists = NULL; }
	if (src->st_iterate) { sqlite3_finalize(src->st_iterate); src->st_iterate = NULL; }
	if (src->db) { sqlite3_close(src->db); src->db = NULL; }
}

static void sqlite_source_free(struct odb_source *source)
{
	struct odb_source_sqlite *src = (struct odb_source_sqlite *)source;
	close_stmts(src);
	odb_source_release(&src->base);
	free(src);
}

static void sqlite_source_close(struct odb_source *source)
{
	close_stmts((struct odb_source_sqlite *)source);
}

static void sqlite_source_reprepare(struct odb_source *source UNUSED)
{
}

static int sqlite_source_read_object_info(struct odb_source *source,
					  const struct object_id *oid,
					  struct object_info *oi,
					  enum object_info_flags flags UNUSED)
{
	struct odb_source_sqlite *src = (struct odb_source_sqlite *)source;
	if (ensure_db(src) < 0)
		return -1;

	char hex[GIT_MAX_HEXSZ + 1];
	oid_to_hex_r(hex, oid);

	sqlite3_reset(src->st_read_data);
	sqlite3_bind_text(src->st_read_data, 1, hex, -1, SQLITE_STATIC);

	if (sqlite3_step(src->st_read_data) != SQLITE_ROW)
		return -1;

	if (oi->typep)
		*(oi->typep) = sqlite3_column_int(src->st_read_data, 0);
	if (oi->sizep)
		*(oi->sizep) = (unsigned long)sqlite3_column_int64(src->st_read_data, 1);
	if (oi->contentp) {
		int len = sqlite3_column_bytes(src->st_read_data, 2);
		const void *data = sqlite3_column_blob(src->st_read_data, 2);
		*oi->contentp = xmalloc(len);
		memcpy(*oi->contentp, data, len);
	}

	return 0;
}

static int sqlite_source_read_object_stream(struct odb_read_stream **out UNUSED,
					    struct odb_source *source UNUSED,
					    const struct object_id *oid UNUSED)
{
	return -1;
}

static int sqlite_source_for_each_object(struct odb_source *source,
					 const struct object_info *request,
					 odb_for_each_object_cb cb,
					 void *cb_data,
					 unsigned flags UNUSED)
{
	struct odb_source_sqlite *src = (struct odb_source_sqlite *)source;
	if (ensure_db(src) < 0)
		return -1;

	sqlite3_reset(src->st_iterate);
	while (sqlite3_step(src->st_iterate) == SQLITE_ROW) {
		const char *hex = (const char *)sqlite3_column_text(src->st_iterate, 0);
		struct object_id oid;
		if (get_oid_hex_any(hex, &oid) < 0)
			continue;

		struct object_info oi = OBJECT_INFO_INIT;
		enum object_type type = sqlite3_column_int(src->st_iterate, 1);
		unsigned long size = (unsigned long)sqlite3_column_int64(src->st_iterate, 2);
		if (request && request->typep) oi.typep = &type;
		if (request && request->sizep) oi.sizep = &size;

		int ret = cb(&oid, &oi, cb_data);
		if (ret)
			return ret;
	}
	return 0;
}

static int sqlite_source_freshen_object(struct odb_source *source,
					const struct object_id *oid)
{
	struct odb_source_sqlite *src = (struct odb_source_sqlite *)source;
	if (ensure_db(src) < 0)
		return 0;
	char hex[GIT_MAX_HEXSZ + 1];
	oid_to_hex_r(hex, oid);
	sqlite3_reset(src->st_exists);
	sqlite3_bind_text(src->st_exists, 1, hex, -1, SQLITE_STATIC);
	return sqlite3_step(src->st_exists) == SQLITE_ROW ? 1 : 0;
}

static int sqlite_source_write_object(struct odb_source *source,
				      const void *buf, unsigned long len,
				      enum object_type type,
				      struct object_id *oid,
				      struct object_id *compat_oid,
				      unsigned flags UNUSED)
{
	struct odb_source_sqlite *src = (struct odb_source_sqlite *)source;
	if (ensure_db(src) < 0)
		return -1;

	hash_object_file(src->base.odb->repo->hash_algo, buf, len, type, oid);

	char hex[GIT_MAX_HEXSZ + 1];
	oid_to_hex_r(hex, oid);

	sqlite3_reset(src->st_write);
	sqlite3_bind_text(src->st_write, 1, hex, -1, SQLITE_STATIC);
	sqlite3_bind_int(src->st_write, 2, type);
	sqlite3_bind_int64(src->st_write, 3, (sqlite3_int64)len);
	sqlite3_bind_blob(src->st_write, 4, buf, (int)len, SQLITE_STATIC);

	if (sqlite3_step(src->st_write) != SQLITE_DONE)
		return -1;
	if (compat_oid)
		oidcpy(compat_oid, oid);
	return 0;
}

static int sqlite_source_write_object_stream(struct odb_source *source UNUSED,
					     struct odb_write_stream *stream UNUSED,
					     size_t len UNUSED,
					     struct object_id *oid UNUSED)
{
	return -1;
}

static int sqlite_source_begin_transaction(struct odb_source *source UNUSED,
					   struct odb_transaction **out UNUSED)
{
	return -1;
}

static int sqlite_source_read_alternates(struct odb_source *source UNUSED,
					 struct strvec *out UNUSED)
{
	return 0;
}

static int sqlite_source_write_alternate(struct odb_source *source UNUSED,
					 const char *alternate UNUSED)
{
	return -1;
}

struct odb_source_sqlite *odb_source_sqlite_new(struct object_database *odb,
						const char *path,
						bool local)
{
	struct odb_source_sqlite *src;
	CALLOC_ARRAY(src, 1);
	odb_source_init(&src->base, odb, ODB_SOURCE_SQLITE, path, local);
	src->base.free = sqlite_source_free;
	src->base.close = sqlite_source_close;
	src->base.reprepare = sqlite_source_reprepare;
	src->base.read_object_info = sqlite_source_read_object_info;
	src->base.read_object_stream = sqlite_source_read_object_stream;
	src->base.for_each_object = sqlite_source_for_each_object;
	src->base.freshen_object = sqlite_source_freshen_object;
	src->base.write_object = sqlite_source_write_object;
	src->base.write_object_stream = sqlite_source_write_object_stream;
	src->base.begin_transaction = sqlite_source_begin_transaction;
	src->base.read_alternates = sqlite_source_read_alternates;
	src->base.write_alternate = sqlite_source_write_alternate;
	return src;
}
