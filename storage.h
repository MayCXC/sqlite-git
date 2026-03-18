#ifndef STORAGE_H
#define STORAGE_H

#include <sqlite3.h>
#include <git2.h>

/*
 * Open (or create) the sqlite.db in the given directory.
 * If path ends with /objects, strips it to find the gitdir root.
 */
int storage_open(const char *path_arg);
void storage_close(void);
sqlite3 *storage_db(void);

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

void storage_reflog_append(const char *refname, const git_oid *old_oid,
			   const git_oid *new_oid, const char *committer,
			   long long timestamp, int tz, const char *msg);

#endif
