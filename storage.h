#ifndef STORAGE_H
#define STORAGE_H

#include <stdio.h>
#include <stddef.h>

#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
#else
  #include <sqlite3.h>
#endif

/*
 * Dependency-free storage core: the sqlite connection, transactions, database
 * maintenance, and the generic prepared-statement mechanism shared by the
 * git (storage_git.h) and git-lfs (storage_git_lfs.h) layers built on top.
 *
 * This layer links NOTHING beyond sqlite: no zlib, no sha256, no libgit2, no git
 * source. It holds no domain schema of its own; each layer above creates its own
 * tables via its *_init() and prepares statements through storage_stmt() here.
 * The dependency rule is one-directional: the leaf layers may call into this
 * core, never the reverse, so this header declares no git/lfs symbol.
 */

/*
 * Open (or create) the sqlite.db in the given directory (owned mode: this layer
 * opened the connection and closes it). If path ends with /objects, strips it to
 * find the gitdir root. Applies the connection pragmas only; call a layer's
 * *_init() afterwards to create that layer's schema.
 */
int storage_open(const char *path_arg);

/*
 * Adopt an existing database connection (borrowed mode: the extension borrows the
 * loader's). Applies the pragmas; call a layer's *_init() afterwards for its
 * schema. In borrowed mode statements are prepared per call rather than cached,
 * so the connection may be closed with sqlite3_close (v1); the owned-mode
 * storage_open() caches them instead.
 */
int storage_open_db(sqlite3 *db);
void storage_close(void);
void storage_refresh(void);
sqlite3 *storage_db(void);

/* Transaction control. All return 0 on success, -1 on failure; storage_release
 * is the durability point (its COMMIT failing -> -1), so a caller that acks a
 * write to git must check the propagated return rather than assume success. The
 * extension's borrowed connection no-ops these (the enclosing SQLite statement is
 * the transaction). */
int storage_begin(void);
int storage_commit(void);
int storage_savepoint(const char *name);
int storage_release(const char *name);
int storage_rollback_to(const char *name);


/*
 * Generic prepared-statement mechanism for the storage_git / storage_git_lfs
 * layers. Each layer keeps its own SQL table and a parallel array of cache slots,
 * and wraps these: storage_stmt() returns a reset, ready-to-bind statement, and
 * storage_stmt_done() releases it. In owned mode (storage_open) the slot caches
 * the prepared handle across calls; in borrowed mode (storage_open_db) the slot
 * stays NULL and each call prepares and finalizes its own, so no statement
 * outlives a borrowed sqlite3_close(v1). storage_at_close() registers a finalizer
 * (idempotently) run at storage_close, so each layer drops its cache before the
 * connection closes.
 */
sqlite3_stmt *storage_stmt(const char *sql, sqlite3_stmt **slot);
void storage_stmt_done(sqlite3_stmt *st, sqlite3_stmt **slot);
void storage_at_close(void (*fn)(void));

#endif /* STORAGE_H */
