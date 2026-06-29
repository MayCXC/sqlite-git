#ifndef STORAGE_GIT_LFS_H
#define STORAGE_GIT_LFS_H

/*
 * Git-LFS content storage layer, built on the dependency-free core (storage.h:
 * connection, transactions, the generic prepared-statement mechanism) plus the
 * vendored sha256. It is INDEPENDENT of the git object layer: it does NOT include
 * storage_git.h and its consumers do not link storage_git.o, so an LFS-only build
 * (the transfer agent, the standalone LFS extension) carries no git object store
 * and no libgit2. A full sqlite-git repo composes this with storage_git over one
 * database (the lfs tables coexist with the objects/refs tables).
 *
 * Function names keep the storage_lfs_* prefix unchanged; the file name marks the
 * git-lfs layer.
 */

#include <stdio.h>
#include "storage.h"

/*
 * LFS content storage. OID is SHA-256 (32 bytes) per git-lfs spec; this is
 * git-lfs's own content address, not a git object id, so it stays raw bytes.
 * Data is stored raw (LFS media is already entropy-coded; compression buys
 * nothing), chunked into frame rows.
 */
#define LFS_OID_RAWSZ 32
#define LFS_OID_HEXSZ 64

/*
 * Create the LFS schema (lfs + lfs_chunk) on the open storage_db() connection,
 * idempotently. Call once after storage_open()/storage_open_db(). A standalone
 * LFS database calls only this; a full repo calls both this and storage_git_init.
 */
int storage_lfs_init(void);

void storage_lfs_sha256(const void *data, size_t len,
			unsigned char out[LFS_OID_RAWSZ]);
void storage_lfs_oid_to_hex(const unsigned char oid[LFS_OID_RAWSZ],
			    char hex[LFS_OID_HEXSZ + 1]);
int storage_lfs_oid_from_hex(const char *hex,
			     unsigned char oid[LFS_OID_RAWSZ]);
int storage_lfs_read(const unsigned char oid[LFS_OID_RAWSZ],
		     size_t *out_size, unsigned char **out_data);
int storage_lfs_write(const unsigned char oid[LFS_OID_RAWSZ],
		       const void *data, size_t size);
/* Streamed variants: move content a frame at a time between a stdio stream and
 * the store, bounding RAM to one frame regardless of object size (the transfer
 * adapter's path). read_stream returns -1 if the oid is not stored. */
int storage_lfs_write_stream(const unsigned char oid[LFS_OID_RAWSZ],
			     FILE *src, size_t size);
int storage_lfs_read_stream(const unsigned char oid[LFS_OID_RAWSZ], FILE *dst);

#endif /* STORAGE_GIT_LFS_H */
