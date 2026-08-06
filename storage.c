/*
 * Dependency-free storage core for sqlite-git.
 *
 * Holds the sqlite connection, transaction control, database maintenance, and
 * the generic prepared-statement mechanism the git (storage_git.c) and git-lfs
 * (storage_git_lfs.c) layers build on. Links NOTHING beyond sqlite: no zlib, no
 * sha256, no libgit2, no git source, and carries no domain schema of its own.
 * The leaf layers create their tables via their *_init() and acquire statements
 * through storage_stmt() here; the dependency rule is one-directional (leaves
 * call the core, never the reverse).
 */
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT3
#else
  #include <sqlite3.h>
#endif
#include "storage.h"

static sqlite3 *sdb;
static int sdb_owned; /* 1 if we opened the db, 0 if borrowed */

static int savepoint_depth = 0;
/* Set when storage's own outermost savepoint opened the transaction (autocommit
 * BEGIN IMMEDIATE). A transaction opened by an enclosing SQLite statement (a
 * write through the extension vtables) is committed by SQLite, not storage, so
 * the outermost release commits only when this is set. */
static int sdb_began_txn;

/* ---- Generic prepared-statement mechanism ----
 *
 * A leaf layer keeps its own SQL table and a parallel array of cache slots, one
 * per statement, and calls storage_stmt(sql, &slot) to acquire and
 * storage_stmt_done(st, &slot) to release. In owned mode (the helper's
 * storage_open) the slot caches the prepared handle across calls; in borrowed
 * mode (the extension's storage_open_db) the slot stays NULL so each call
 * prepares and finalizes its own, never keeping a statement alive past a borrowed
 * sqlite3_close(v1). This reproduces the cached/per-call split the layers relied
 * on while keeping the connection state private to the core.
 */
sqlite3_stmt *storage_stmt(const char *sql, sqlite3_stmt **slot) {
	if (slot && *slot) {
		sqlite3_reset(*slot);
		return *slot;
	}
	sqlite3_stmt *st = NULL;
	int rc = sqlite3_prepare_v3(sdb, sql, -1, SQLITE_PREPARE_PERSISTENT, &st, 0);
	if (rc != SQLITE_OK)
		fprintf(stderr, "sqlite-git: prepare failed (%d): %s\n", rc, sqlite3_errmsg(sdb));
	/* Cache only in owned mode; borrowed mode leaves the slot NULL so the
	 * statement is finalized per call (it must not outlive sqlite3_close v1). */
	if (sdb_owned && slot)
		*slot = st;
	return st;
}

void storage_stmt_done(sqlite3_stmt *st, sqlite3_stmt **slot) {
	/*
	 * storage_stmt() returns NULL when a prepare fails (e.g. a missing table);
	 * never dereference that here. A cached statement (slot still holds it) is
	 * cleared and reset for reuse; a per-call one (borrowed mode) is finalized.
	 */
	if (!st)
		return;
	if (slot && *slot == st) {
		sqlite3_clear_bindings(st);
		sqlite3_reset(st);
	} else {
		sqlite3_finalize(st);
	}
}

/* ---- Close hooks ----
 *
 * Each leaf layer registers a finalizer at *_init() so its cached statements are
 * dropped before the connection closes. The core stays unaware of which layers
 * exist (it calls opaque function pointers), keeping the dependency one-way.
 */
#define STORAGE_MAX_CLOSE_HOOKS 4
static void (*close_hooks[STORAGE_MAX_CLOSE_HOOKS])(void);
static int n_close_hooks;

void storage_at_close(void (*fn)(void)) {
	for (int i = 0; i < n_close_hooks; i++)
		if (close_hooks[i] == fn)
			return;	/* idempotent: a reopen must not register twice */
	if (n_close_hooks < STORAGE_MAX_CLOSE_HOOKS)
		close_hooks[n_close_hooks++] = fn;
}

/* ---- Database ---- */

/*
 * Connection pragmas, shared by both faces. page_size and auto_vacuum MUST
 * precede journal_mode (and any CREATE): both are fixed when the store is created,
 * and a later change is silently ignored once the WAL is set up or any table
 * exists, so issuing them first is the only way they take effect on a fresh store.
 * The 8 KiB page gives shallower btrees over the blob payloads; auto_vacuum =
 * INCREMENTAL lets the optimize command reclaim churn-freed pages in place with
 * PRAGMA incremental_vacuum instead of a full VACUUM, which rebuilds the whole
 * store into a ~2x-size scratch copy and fails when the disk is tight, exactly
 * when reclaiming is needed.
 */
static void apply_pragmas(sqlite3 *db) {
	sqlite3_exec(db, "PRAGMA page_size = 8192;", 0, 0, 0);
	sqlite3_exec(db, "PRAGMA auto_vacuum = INCREMENTAL;", 0, 0, 0);
	sqlite3_exec(db, "PRAGMA busy_timeout = 5000;", 0, 0, 0);
	sqlite3_exec(db, "PRAGMA synchronous = NORMAL;", 0, 0, 0);
	sqlite3_exec(db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
}

int storage_open(const char *path_arg) {
	size_t len = strlen(path_arg);
	char *gitdir = sqlite3_mprintf("%s", path_arg);
	if (!gitdir) return -1;

	if (len > 8 && !strcmp(gitdir + len - 8, "/objects"))
		gitdir[len - 8] = '\0';

	mkdir(gitdir, 0755);
	char *path = sqlite3_mprintf("%s/sqlite.db", gitdir);
	sqlite3_free(gitdir);
	if (!path) return -1;

	int rc = sqlite3_open(path, &sdb);
	sqlite3_free(path);
	if (rc != SQLITE_OK)
		return -1;
	sdb_owned = 1;

	/*
	 * Core opens the connection and applies the pragmas only; each layer's
	 * *_init() creates its schema and (owned mode) caches its statements lazily
	 * on first use. No schema migrations: sqlite-git is effectively version zero,
	 * so a layer's CREATE TABLE IF NOT EXISTS always defines the current columns.
	 * A store made by an older build is migrated by pushing from it and pulling
	 * into a fresh store on the new build, never by ALTER-ing in place.
	 */
	apply_pragmas(sdb);
	return 0;
}

int storage_open_db(sqlite3 *db) {
	if (sdb) return 0;
	sdb = db;
	/*
	 * The extension borrows an already-open connection (owned by the loader, so
	 * sdb_owned stays 0) and prepares statements per call, so it does NOT cache.
	 * Pragmas are otherwise identical to the helper's: both faces share one store
	 * and create it the same way, so the extension connection gets the same
	 * busy_timeout and WAL mode. (page_size only takes effect on a fresh DB; on an
	 * existing helper store it is a harmless no-op.)
	 */
	apply_pragmas(db);
	return 0;
}

void storage_close(void) {
	/* Each layer's hook finalizes and NULLs its cached statements first, so none
	 * outlives the connection; then close it (owned mode only). */
	for (int i = 0; i < n_close_hooks; i++)
		close_hooks[i]();
	if (sdb_owned) sqlite3_close(sdb);
	sdb = NULL;
	savepoint_depth = 0;
	sdb_began_txn = 0;
}

sqlite3 *storage_db(void) { return sdb; }

void storage_refresh(void) {
	if (!sdb) return;

	/*
	 * Release the WAL read snapshot so the next operation sees writes from other
	 * processes (subprocess helpers). In WAL mode, the snapshot is pinned at
	 * transaction start, not connection open. Resetting all statements ends any
	 * implicit read transaction held by prepared statements.
	 *
	 * ODB transactions use savepoints (not bare BEGIN), so they do not pin the WAL
	 * snapshot at the connection level. PASSIVE checkpoint reclaims WAL space.
	 *
	 * Reset ALL statements on this connection, including any leaked statements not
	 * in a cache. sqlite3_next_stmt walks the connection's statement list.
	 */
	{
		sqlite3_stmt *st = NULL;
		while ((st = sqlite3_next_stmt(sdb, st)))
			sqlite3_reset(st);
	}
	sqlite3_wal_checkpoint_v2(sdb, NULL,
				  SQLITE_CHECKPOINT_PASSIVE,
				  NULL, NULL);
}

/* ---- Transactions ---- */

int storage_begin(void) { return storage_savepoint("odb_txn"); }
int storage_commit(void) { return storage_release("odb_txn"); }

/*
 * Transaction control applies to the helper, which owns its connection. The
 * extension runs every storage call from inside SQLite statement execution (a
 * vtab xUpdate or a scalar function), where the connection has a statement in
 * progress and SAVEPOINT/BEGIN/COMMIT are refused (SQLITE_BUSY, "statements in
 * progress"); there the enclosing statement is the transaction, it commits on
 * success and rolls back when a call returns an error to SQLite, so storage
 * adds no transaction of its own. Hence these three no-op on a borrowed (!owned)
 * connection.
 */
int storage_savepoint(const char *name) {
	if (!sdb_owned)
		return 0;
	/*
	 * The outermost savepoint opens the transaction with BEGIN IMMEDIATE to take
	 * the write lock up front, avoiding a deadlock between two processes that each
	 * hold a deferred read lock and then try to upgrade.
	 */
	if (savepoint_depth == 0 && sqlite3_get_autocommit(sdb)) {
		if (sqlite3_exec(sdb, "BEGIN IMMEDIATE", 0, 0, 0) != SQLITE_OK)
			return -1;
		sdb_began_txn = 1;
	}
	char *sql = sqlite3_mprintf("SAVEPOINT \"%w\"", name);
	int rc = sql ? sqlite3_exec(sdb, sql, 0, 0, 0) : SQLITE_NOMEM;
	sqlite3_free(sql);
	if (rc != SQLITE_OK) {
		if (sdb_began_txn) {
			sqlite3_exec(sdb, "ROLLBACK", 0, 0, 0);
			sdb_began_txn = 0;
		}
		return -1;
	}
	savepoint_depth++;
	return 0;
}

int storage_release(const char *name) {
	if (!sdb_owned)
		return 0;
	char *sql = sqlite3_mprintf("RELEASE \"%w\"", name);
	int rc = sql ? sqlite3_exec(sdb, sql, 0, 0, 0) : SQLITE_NOMEM;
	sqlite3_free(sql);
	/*
	 * The outermost release commits and is the durability point: a failed COMMIT
	 * (FULL / IOERR / BUSY) is rolled back and reported as failure rather than as
	 * a persisted write.
	 */
	if (--savepoint_depth == 0 && sdb_began_txn) {
		sdb_began_txn = 0;
		if (sqlite3_exec(sdb, "COMMIT", 0, 0, 0) != SQLITE_OK) {
			sqlite3_exec(sdb, "ROLLBACK", 0, 0, 0);
			return -1;
		}
	}
	return rc == SQLITE_OK ? 0 : -1;
}

int storage_rollback_to(const char *name) {
	if (!sdb_owned)
		return 0;
	char *sql = sqlite3_mprintf("ROLLBACK TO \"%w\"; RELEASE \"%w\"", name, name);
	int rc = sql ? sqlite3_exec(sdb, sql, 0, 0, 0) : SQLITE_NOMEM;
	sqlite3_free(sql);
	if (--savepoint_depth == 0 && sdb_began_txn) {
		sdb_began_txn = 0;
		sqlite3_exec(sdb, "ROLLBACK", 0, 0, 0);
	}
	return rc == SQLITE_OK ? 0 : -1;
}
