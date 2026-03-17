#ifndef STORAGE_H
#define STORAGE_H

#include <sqlite3.h>

#define OID_RAWSZ 20
#define OID_HEXSZ 40

/* Hex/binary OID conversion */
int hex2bin(const char *hex, unsigned char *bin, int binlen);
void bin2hex(const unsigned char *bin, int binlen, char *hex);

/* Type name conversion */
const char *type_name(int t);
int type_from_name(const char *s);

/*
 * Open (or create) the sqlite.db in the given directory.
 * If path ends with /objects, strips it to find the gitdir root.
 * Creates objects, refs, and reflog tables if they don't exist.
 */
int storage_open(const char *path_arg);

/* Close the database. */
void storage_close(void);

/* Get the database handle (for direct queries). */
sqlite3 *storage_db(void);

/*
 * Read an object, resolving delta chains.
 * Caller must free *out_data.
 */
int storage_read_object(const unsigned char *oid_bin,
			int *out_type, unsigned long *out_size,
			unsigned char **out_data);

/*
 * Write an object with zlib + delta compression.
 * The object is deduplicated (no-op if already stored).
 */
void storage_write_object(const unsigned char *oid_bin, int type,
			  const unsigned char *data, unsigned long size);

/* Check if an object exists. */
int storage_object_exists(const unsigned char *oid_bin);

/* Prepared statement accessors for ref/reflog operations. */
sqlite3_stmt *storage_ref_read_stmt(void);
sqlite3_stmt *storage_ref_write_stmt(void);
sqlite3_stmt *storage_ref_delete_stmt(void);
sqlite3_stmt *storage_ref_list_stmt(void);
sqlite3_stmt *storage_obj_list_stmt(void);
sqlite3_stmt *storage_reflog_read_stmt(void);
sqlite3_stmt *storage_reflog_append_stmt(void);
sqlite3_stmt *storage_reflog_exists_stmt(void);
sqlite3_stmt *storage_reflog_delete_stmt(void);

#endif
