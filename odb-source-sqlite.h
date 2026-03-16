#ifndef ODB_SOURCE_SQLITE_H
#define ODB_SOURCE_SQLITE_H

#include "odb/source.h"

struct sqlite3;
struct sqlite3_stmt;

/*
 * The SQLite object database source stores objects in a SQLite database.
 * Objects are stored in an objects_loose table keyed by hex OID.
 */
struct odb_source_sqlite {
	struct odb_source base;
	struct sqlite3 *db;
	struct sqlite3_stmt *st_read_data;
	struct sqlite3_stmt *st_write;
	struct sqlite3_stmt *st_exists;
	struct sqlite3_stmt *st_iterate;
};

/* Allocate and initialize a new SQLite object source. */
struct odb_source_sqlite *odb_source_sqlite_new(struct object_database *odb,
						const char *path,
						bool local);

#endif
