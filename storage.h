#ifndef STORAGE_H
#define STORAGE_H

#include <sqlite3.h>
#include <git2.h>

/*
 * Open (or create) the sqlite.db in the given directory.
 * If path ends with /objects, strips it to find the gitdir root.
 */
int storage_open(const char *path_arg);

/*
 * Use an existing database connection for storage.
 * If persistent is true, prepared statements are cached for the
 * lifetime of the connection (caller must call storage_close or
 * use sqlite3_close_v2). If false, statements are prepared and
 * finalized per call (safe with sqlite3_close v1).
 */
int storage_open_db(sqlite3 *db, int persistent);
void storage_close(void);
void storage_destroy(void);
sqlite3 *storage_db(void);

/* Transaction control */
void storage_begin(void);
void storage_commit(void);
void storage_savepoint(const char *name);
void storage_release(const char *name);
void storage_rollback_to(const char *name);

/*
 * Object operations. OIDs are git_oid (binary 20 bytes).
 * Read resolves delta chains. Write handles compression + delta.
 * Caller must free *out_data from read.
 */
int storage_read_object(const git_oid *oid,
			git_object_t *out_type, size_t *out_size,
			unsigned char **out_data);

void storage_write_object(const git_oid *oid, git_object_t type,
			  const void *data, size_t size);

int storage_object_exists(const git_oid *oid);

/*
 * Ref operations. OIDs stored as BLOB(20) internally.
 */
int storage_ref_read(const char *refname, git_oid *oid, char *symref, size_t symref_len);
void storage_ref_write(const char *refname, const git_oid *oid, const char *symref);
void storage_ref_delete(const char *refname);

/* Callback for ref iteration. Return non-zero to stop. */
typedef int (*storage_ref_cb)(const char *refname, const git_oid *oid,
			      const char *symref, void *data);
int storage_ref_list(const char *prefix, storage_ref_cb cb, void *data);

/* Object listing callback. */
typedef int (*storage_obj_cb)(const git_oid *oid, git_object_t type,
			      size_t size, void *data);
int storage_obj_list(storage_obj_cb cb, void *data);

/* Reflog operations */
int storage_reflog_exists(const char *refname);
void storage_reflog_delete(const char *refname);

/* Reflog read callback */
typedef int (*storage_reflog_cb)(const git_oid *old_oid, const git_oid *new_oid,
				 const char *committer, long long timestamp,
				 int tz, const char *msg, void *data);
int storage_reflog_read(const char *refname, storage_reflog_cb cb, void *data);

int storage_reflog_read_reverse(const char *refname, storage_reflog_cb cb, void *data);

/* Reflog name listing callback. */
typedef int (*storage_reflog_name_cb)(const char *refname, void *data);
int storage_reflog_list(storage_reflog_name_cb cb, void *data);

void storage_reflog_append(const char *refname, const git_oid *old_oid,
			   const git_oid *new_oid, const char *committer,
			   long long timestamp, int tz, const char *msg);

/*
 * LFS content storage. OID is SHA-256 (32 bytes) per git-lfs spec.
 * Data is zlib compressed.
 */
#define LFS_OID_RAWSZ 32
#define LFS_OID_HEXSZ 64

void storage_lfs_sha256(const void *data, size_t len,
			unsigned char out[LFS_OID_RAWSZ]);
void storage_lfs_oid_to_hex(const unsigned char oid[LFS_OID_RAWSZ],
			    char hex[LFS_OID_HEXSZ + 1]);
int storage_lfs_oid_from_hex(const char *hex,
			     unsigned char oid[LFS_OID_RAWSZ]);
int storage_lfs_read(const unsigned char oid[LFS_OID_RAWSZ],
		     size_t *out_size, unsigned char **out_data);
void storage_lfs_write(const unsigned char oid[LFS_OID_RAWSZ],
		       const void *data, size_t size);
int storage_lfs_exists(const unsigned char oid[LFS_OID_RAWSZ]);

#endif
