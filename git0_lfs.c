/*
** git0_lfs: large file storage in SQLite.
**
** Stores large file content via the shared storage layer.
** LFS pointer format (fully compatible with git-lfs):
**   version https://git-lfs.github.com/spec/v1
**   oid sha256:<64-char-hex>
**   size <bytes>
**
** Functions:
**   git0_lfs_store(data)        -> stores content, returns LFS pointer text
**   git0_lfs_fetch(pointer)     -> returns actual content from pointer text
**   git0_lfs_smudge(oid_hex)    -> returns content by sha256 hex oid
**   git0_lfs_pointer(data)      -> generates pointer text without storing
**
** Uses SHA-256 for content OIDs per the git-lfs specification.
*/

#include "git0.h"
#include "storage.h"

#ifndef SQLITE_CORE
  SQLITE_EXTENSION_INIT3
#endif

#include <stdio.h>
#include <string.h>



/* ---- git0_lfs_pointer(data) ---- */

static void fn_lfs_pointer(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const void *data = sqlite3_value_blob(argv[0]);
  int len = sqlite3_value_bytes(argv[0]);

  unsigned char oid[LFS_OID_RAWSZ];
  storage_lfs_sha256(data, len, oid);

  char hex[LFS_OID_HEXSZ + 1];
  storage_lfs_oid_to_hex(oid, hex);

  char pointer[512];
  int plen = snprintf(pointer, sizeof(pointer),
    "version https://git-lfs.github.com/spec/v1\n"
    "oid sha256:%s\n"
    "size %d\n",
    hex, len);

  sqlite3_result_text(ctx, pointer, plen, SQLITE_TRANSIENT);
}

/* ---- git0_lfs_store(data) ---- */

static void fn_lfs_store(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const void *data = sqlite3_value_blob(argv[0]);
  int len = sqlite3_value_bytes(argv[0]);

  unsigned char oid[LFS_OID_RAWSZ];
  storage_lfs_sha256(data, len, oid);
  storage_lfs_write(oid, data, len);

  char hex[LFS_OID_HEXSZ + 1];
  storage_lfs_oid_to_hex(oid, hex);

  char pointer[512];
  int plen = snprintf(pointer, sizeof(pointer),
    "version https://git-lfs.github.com/spec/v1\n"
    "oid sha256:%s\n"
    "size %d\n",
    hex, len);

  sqlite3_result_text(ctx, pointer, plen, SQLITE_TRANSIENT);
}

/* ---- git0_lfs_fetch(pointer_text) ---- */

static void fn_lfs_fetch(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *pointer = (const char *)sqlite3_value_text(argv[0]);
  if (!pointer) { sqlite3_result_null(ctx); return; }

  const char *oid_line = strstr(pointer, "oid sha256:");
  if (!oid_line) { sqlite3_result_null(ctx); return; }

  unsigned char oid[LFS_OID_RAWSZ];
  if (storage_lfs_oid_from_hex(oid_line + 11, oid) != 0) { sqlite3_result_null(ctx); return; }

  size_t size;
  unsigned char *data;
  if (storage_lfs_read(oid, &size, &data) == 0) {
    sqlite3_result_blob(ctx, data, size, free);
  } else {
    sqlite3_result_null(ctx);
  }
}

/* ---- git0_lfs_smudge(oid_hex) ---- */

static void fn_lfs_smudge(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
  (void)argc;
  const char *hex = (const char *)sqlite3_value_text(argv[0]);
  if (!hex) { sqlite3_result_null(ctx); return; }

  unsigned char oid[LFS_OID_RAWSZ];
  if (storage_lfs_oid_from_hex(hex, oid) != 0) { sqlite3_result_null(ctx); return; }

  size_t size;
  unsigned char *data;
  if (storage_lfs_read(oid, &size, &data) == 0) {
    sqlite3_result_blob(ctx, data, size, free);
  } else {
    sqlite3_result_null(ctx);
  }
}

/* ---- Registration ---- */

int git0_register_lfs(sqlite3 *db) {
  sqlite3_create_function(db, "git0_lfs_pointer", 1, SQLITE_UTF8, 0, fn_lfs_pointer, 0, 0);
  sqlite3_create_function(db, "git0_lfs_store", 1, SQLITE_UTF8, 0, fn_lfs_store, 0, 0);
  sqlite3_create_function(db, "git0_lfs_fetch", 1, SQLITE_UTF8, 0, fn_lfs_fetch, 0, 0);
  sqlite3_create_function(db, "git0_lfs_smudge", 1, SQLITE_UTF8, 0, fn_lfs_smudge, 0, 0);
  return SQLITE_OK;
}
