/*
 * Git object/ref storage layer for sqlite-git.
 *
 * Built on the dependency-free core (storage.c): adds zlib + our clean-room
 * git-format delta applier and holds everything git-specific (objects, refs,
 * reflogs, the reachability bitmap, commit-graph generation, pack membership,
 * prune, promisor, freshen, object format). Object ids cross this API as the
 * wire's own hex strings and types as their git names; rows key the oid as a raw
 * BLOB, with sqlite's built-in unhex()/hex() doing the conversion in SQL, so this
 * file interprets neither and links NO libgit2 and NO git source. The helper
 * passes git's wire strings straight through; the extension converts to/from
 * git_oid / git_object_t at its libgit2 boundary.
 */
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT3
#else
  #include <sqlite3.h>
#endif
#include "storage_git.h"

/* Guard against circular delta chains in corrupt databases.
 * Git enforces no depth limit on read; this is our safety net.
 * 4095 matches pack-objects.h structural max (12-bit depth field). */
#define MAX_DELTA_DEPTH 4095

enum stmt_id {
	STMT_OBJ_READ,
	STMT_OBJ_HEADER,
	STMT_OBJ_WRITE,
	STMT_OBJ_EXISTS,
	STMT_OBJ_LIST,
	STMT_REF_READ,
	STMT_REF_WRITE,
	STMT_REF_DELETE,
	STMT_REF_CLEAR,
	STMT_REF_LIST,
	STMT_REFLOG_READ,
	STMT_REFLOG_READ_REV,
	STMT_REFLOG_LIST,
	STMT_REFLOG_APPEND,
	STMT_REFLOG_EXISTS,
	STMT_REFLOG_CREATE,
	STMT_REFLOG_DELETE,
	STMT_REFLOG_CLEAR,
	STMT_REFLOG_MOVE,
	STMT_REFLOG_COPY,
	STMT_MARK_PROMISOR,
	STMT_OBJ_LIST_PROMISOR,
	STMT_OBJ_LIST_RANGE,
	STMT_COMMIT_GEN,
	STMT_COMMIT_GRAPH_PUT,
	STMT_COMMIT_GRAPH_CLEAR,
	STMT_REF_LIST_GLOB,
	STMT_PRUNE_OBJECT,
	STMT_PACK_BITMAP_PUT,
	STMT_PACK_BITMAP_GET,
	STMT_PACK_BITMAP_REMOVE,
	STMT_PACK_CONTENT_SWEEP,
	STMT_OBJ_REPLACE,
	STMT_OPTIMIZE_CHURN,
	STMT_OPTIMIZE_DONE,
	STMT_FRESHEN,
	STMT_OBJECT_FORMAT_GET,
	STMT_OBJECT_FORMAT_SET,
	STMT_INTEGRITY_CHECK,
	STMT_VACUUM,
	STMT_COUNT
};

/* The SQL for every statement, indexed by stmt_id. Immutable compile-time data:
 * const, shared, .rodata. The mutable prepared handles live in the parallel
 * stmt_prepared[] below; the enum keeps both arrays in lockstep. */
static const char *const stmt_sql[STMT_COUNT] = {
	[STMT_OBJ_READ] =
		/* iif() preserves NULL for a resolved (non-delta) object: SQLite's
		 * hex(NULL) yields '' (empty string), not NULL, which would make the
		 * base-oid NULL test below treat every resolved object as a delta. */
		"SELECT o.type, o.size, coalesce(p.content, o.data),"
		" iif(o.base IS NULL, NULL, lower(hex(o.base)))"
		" FROM objects o LEFT JOIN pack_content p ON p.pack_pos = o.pack_pos"
		" WHERE o.oid = unhex(?)",
	[STMT_OBJ_HEADER] =
		/* The header of an object: its type, the stored size and the base of a
		 * delta, and the stored bytes of a delta alone, whose start holds the
		 * size of the object it makes (see storage_read_header). The bytes of a
		 * whole object are not read. */
		"SELECT o.type, o.size, iif(o.base IS NULL, NULL, lower(hex(o.base))),"
		" iif(o.base IS NULL, NULL, coalesce(p.content, o.data))"
		" FROM objects o LEFT JOIN pack_content p ON p.pack_pos = o.pack_pos"
		" WHERE o.oid = unhex(?)",
	[STMT_OBJ_WRITE] =
		"INSERT OR IGNORE INTO objects(oid, type, size, data, base, promisor) VALUES(unhex(?),?,?,?,unhex(?),?)",
	[STMT_OBJ_EXISTS] =
		"SELECT 1 FROM objects WHERE oid = unhex(?)",
	[STMT_OBJ_LIST] =
		"SELECT lower(hex(oid)), type, size FROM objects",
	[STMT_REF_READ] =
		"SELECT lower(hex(oid)), symref FROM refs WHERE refname = ?",
	[STMT_REF_WRITE] =
		"INSERT OR REPLACE INTO refs(refname, oid, symref) VALUES(?,unhex(?),?)",
	[STMT_REF_DELETE] =
		"DELETE FROM refs WHERE refname = ?",
	[STMT_REF_CLEAR] =
		"DELETE FROM refs",
	[STMT_REF_LIST] =
		"SELECT refname, lower(hex(oid)), symref FROM refs ORDER BY refname",
	[STMT_REFLOG_READ] =
		"SELECT lower(hex(old_oid)), lower(hex(new_oid)), committer, timestamp, tz, msg FROM reflog WHERE refname = ? AND idx >= 0 ORDER BY idx ASC",
	[STMT_REFLOG_READ_REV] =
		"SELECT lower(hex(old_oid)), lower(hex(new_oid)), committer, timestamp, tz, msg FROM reflog WHERE refname = ? AND idx >= 0 ORDER BY idx DESC",
	[STMT_REFLOG_LIST] =
		"SELECT DISTINCT refname FROM reflog ORDER BY refname",
	[STMT_REFLOG_APPEND] =
		"INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg)"
		" VALUES(?, COALESCE((SELECT MAX(idx)+1 FROM reflog WHERE refname = ?), 0), unhex(?), unhex(?), ?, ?, ?, ?)",
	[STMT_REFLOG_EXISTS] =
		"SELECT 1 FROM reflog WHERE refname = ? LIMIT 1",
	[STMT_REFLOG_CREATE] =
		"INSERT OR IGNORE INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg)"
		" VALUES(?, -1, X'', X'', '', 0, 0, '')",
	[STMT_REFLOG_DELETE] =
		"DELETE FROM reflog WHERE refname = ?",
	[STMT_REFLOG_CLEAR] =
		"DELETE FROM reflog",
	[STMT_REFLOG_MOVE] =
		"UPDATE reflog SET refname = ? WHERE refname = ?",
	[STMT_REFLOG_COPY] =
		"INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg)"
		" SELECT ?, idx, old_oid, new_oid, committer, timestamp, tz, msg"
		" FROM reflog WHERE refname = ?",
	[STMT_MARK_PROMISOR] =
		"UPDATE objects SET promisor = 1 WHERE oid = unhex(?)",
	[STMT_OBJ_LIST_PROMISOR] =
		"SELECT lower(hex(oid)), type, size FROM objects WHERE promisor = 1",
	/* Prefix-scoped enumeration for abbreviation/disambiguation: the caller's
	 * hex prefix is matched server-side so only the matching ids cross the IPC
	 * boundary, instead of streaming the whole table for git to filter. The
	 * prefix is converted to the raw-byte range [?1, ?2) so this seeks the oid
	 * primary-key index; a GLOB on lower(hex(oid)) would instead scan every row
	 * (the function call defeats the index). ?3 folds in the promisor filter
	 * (0 = all) so one statement serves the bare and promisor-only cases. */
	[STMT_OBJ_LIST_RANGE] =
		"SELECT lower(hex(oid)), type, size FROM objects"
		" WHERE oid >= ?1 AND oid < ?2 AND (?3 = 0 OR promisor = 1)",
	[STMT_COMMIT_GEN] =
		"SELECT generation FROM commit_graph WHERE oid = unhex(?)",
	[STMT_COMMIT_GRAPH_PUT] =
		/* Generation cache: git computes it at commit-graph-write time, the
		 * helper stores it for the extension's git0_generation(). Idempotent.
		 * Only the generation is stored; tree/parents/date live in the commit
		 * object, so the commit-graph reduces to this one derived scalar. */
		"INSERT OR REPLACE INTO commit_graph(oid, generation) "
		"VALUES(unhex(?), ?)",
	[STMT_COMMIT_GRAPH_CLEAR] =
		"DELETE FROM commit_graph",
	[STMT_REF_LIST_GLOB] =
		"SELECT refname, lower(hex(oid)), symref FROM refs WHERE refname GLOB ? ORDER BY refname",
	[STMT_PRUNE_OBJECT] =
		/* Never delete an object that is still the delta base of any object
		 * left in the store. git computes reachability over the logical
		 * object graph and is blind to our storage-level delta edges, so an
		 * unreachable object can be the base a surviving (reachable) object
		 * is stored against; deleting it would orphan that dependent. The
		 * caller loops prune to a fixpoint, so unreachable delta chains still
		 * drain fully (a base becomes deletable once its dependents are gone).
		 * The files backend never hits this: loose prune only unlinks loose
		 * objects, and packed delta bases are repack's domain, not prune's.
		 *
		 * Expire by last_used, not created_at: a write or freshen of an existing
		 * object bumps last_used, so a recently-touched object survives prune
		 * even if old and unreachable (files' loose-mtime grace, and the cruft
		 * grace period). created_at stays immutable for the gc-auto churn count.
		 *
		 * The objects of a pack git keeps with a .keep file never reach the
		 * store (git leaves the pack in its objects directory), so the grace
		 * and the delta-base guard are the files retention rule as one
		 * predicate. */
		"DELETE FROM objects WHERE oid = unhex(?) AND last_used <= ? "
		"AND NOT EXISTS (SELECT 1 FROM objects AS d WHERE d.base = objects.oid)",
	[STMT_PACK_BITMAP_PUT] =
		/* The one reachability bitmap of the store, replaced as a whole. */
		"INSERT OR REPLACE INTO pack_bitmap(id, pack_size, idx, rev, bitmap)"
		" VALUES(0, ?, ?, ?, ?)",
	[STMT_PACK_BITMAP_GET] =
		"SELECT pack_size, idx, rev, bitmap FROM pack_bitmap WHERE id = 0",
	[STMT_PACK_BITMAP_REMOVE] =
		"DELETE FROM pack_bitmap",
	[STMT_PACK_CONTENT_SWEEP] =
		/* Drop the clustered content no object points at: an object stored
		 * again or pruned leaves its pack_content row behind. */
		"DELETE FROM pack_content WHERE pack_pos NOT IN"
		" (SELECT pack_pos FROM objects WHERE pack_pos IS NOT NULL)",
	[STMT_OBJ_REPLACE] =
		/* Repack re-representation (put / put-raw with replace=1): overwrite
		 * an object's stored representation in place, switching it between full
		 * (base NULL, bound via unhex(NULL)) and delta (base set) as git's
		 * pack-objects re-deltifies at gc. A per-row UPSERT, not INSERT OR
		 * REPLACE: the DO UPDATE touches only type/size/data/base and clears
		 * pack_pos, so promisor and created_at survive (INSERT OR REPLACE would
		 * reset both, losing the promisor mark and the prune-grace age), and the
		 * read takes the new data, not clustered content in pack_content that
		 * holds the object as it was stored before. The fetch path stays
		 * keep-existing (STMT_OBJ_WRITE); only repack, which owns an object's
		 * representation the way files' repack owns its packs, may replace.
		 * Shares STMT_OBJ_WRITE's param layout so both writers bind
		 * identically. */
		"INSERT INTO objects(oid, type, size, data, base, promisor) VALUES(unhex(?),?,?,?,unhex(?),?)"
		" ON CONFLICT(oid) DO UPDATE SET type=excluded.type, size=excluded.size,"
		" data=excluded.data, base=excluded.base, pack_pos=NULL",
	[STMT_OPTIMIZE_CHURN] =
		/* gc-auto heuristic: how many objects were added since the last
		 * optimize. Counts objects whose created_at is newer than the stored
		 * 'last_optimize_at' watermark (0 if never optimized -> counts all),
		 * the relational analog of files' loose-objects-since-last-pack. Robust
		 * to prune (it counts new objects, not the total) and to repack (which
		 * preserves created_at), so it falls back to 0 right after an optimize. */
		"SELECT count(*) FROM objects WHERE created_at >"
		" COALESCE((SELECT value FROM meta WHERE key = 'last_optimize_at'), 0)",
	[STMT_OPTIMIZE_DONE] =
		/* Stamp the optimize watermark so the churn count resets to ~0. */
		"INSERT OR REPLACE INTO meta(key, value)"
		" VALUES('last_optimize_at', strftime('%s','now'))",
	[STMT_FRESHEN] =
		/* Freshen: bump an existing object's last_used to now so prune keeps it
		 * within the grace window (files utime's a loose object / its pack on a
		 * duplicate write or an explicit freshen). No-op if the oid is absent. */
		"UPDATE objects SET last_used = strftime('%s','now') WHERE oid = unhex(?)",
	[STMT_OBJECT_FORMAT_GET] =
		/* The repository's object format (hash): the 'objectformat' meta row,
		 * 0 or absent = sha1, 1 = sha256. Written at git0_init, read wherever the
		 * extension hashes objects (the helper never reads it; oids are opaque). */
		"SELECT value FROM meta WHERE key = 'objectformat'",
	[STMT_OBJECT_FORMAT_SET] =
		"INSERT OR REPLACE INTO meta(key, value) VALUES('objectformat', ?)",
	/* The verify command's whole-database integrity check. Cached in the array
	 * like every other statement a git operation (here, fsck) drives, not a
	 * one-off raw prepare. */
	[STMT_INTEGRITY_CHECK] =
		"PRAGMA integrity_check",
	/* incremental_vacuum (the optimize command): reclaims the pages object and ref
	 * churn freed to the freelist, in place, skipping the whole-store rebuild (and
	 * ~2x scratch) a full VACUUM does. Relies on auto_vacuum = INCREMENTAL, set in
	 * apply_pragmas when the store is created. Yields a columnless row per page
	 * freed, so its driver steps it to done; cached in the array like every other
	 * statement an operation drives, not exec'd. */
	[STMT_VACUUM] =
		"PRAGMA incremental_vacuum",
};

/* The prepared handles, parallel to stmt_sql[] and indexed by the same stmt_id.
 * A static array, so zero-initialised to all-NULL: owned mode (helper) caches
 * each on first use and finalises them at close (the registered close hook);
 * borrowed mode (extension) leaves it NULL and each call prepares + finalises
 * its own. The core storage_stmt() mechanism implements both. */
static sqlite3_stmt *stmt_prepared[STMT_COUNT];

static sqlite3_stmt *stmt_acquire(enum stmt_id id) {
	return storage_stmt(stmt_sql[id], &stmt_prepared[id]);
}

static void stmt_release(enum stmt_id id, sqlite3_stmt *used) {
	storage_stmt_done(used, &stmt_prepared[id]);
}

/* Registered with storage_at_close: finalize this layer's cached statements and
 * clear the slots so a subsequent reopen re-prepares cleanly. */
static void git_stmts_close(void) {
	for (int i = 0; i < STMT_COUNT; i++) {
		sqlite3_finalize(stmt_prepared[i]);
		stmt_prepared[i] = NULL;
	}
}

/* ---- Zlib ---- */

static unsigned char *zcompress(const void *src, unsigned long srclen,
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

/* Inflate src into a buffer of exactly orig bytes, or NULL when it inflates to
 * any other length, so that a short stream never passes off the rest of the
 * buffer as content. */
static unsigned char *zdecompress(const void *src, unsigned long srclen,
				  unsigned long orig) {
	unsigned char *dst = malloc(orig ? orig : 1);
	unsigned long dstlen = orig;
	if (!dst) return NULL;
	if (uncompress(dst, &dstlen, src, srclen) != Z_OK || dstlen != orig) {
		free(dst);
		return NULL;
	}
	return dst;
}

/* ---- Git-format delta applier (clean-room) ----
 *
 * Apply a git-format delta (the "deltified representation" used in packfiles)
 * to a base buffer. git core creates these deltas (via delta.h) and sends them
 * compressed with the put-raw verb; the helper stores them verbatim and resolves
 * them here on read (inflate, then apply against the base).
 *
 * Format reference: Documentation/technical/pack-format.txt (git.git),
 * "Deltified representation":
 *   - source size as a varint, then target size as a varint
 *   - instructions until end of delta: 0x00 is reserved; high bit clear
 *     (1..127) inserts that many literal bytes that follow; high bit set copies
 *     from the base, with bits 0-3 selecting offset bytes (LE) and bits 4-6
 *     selecting size bytes (LE), and a size of 0 meaning 0x10000.
 * Clean-room implementation of that documented wire format: our own structure
 * and bounds checks, validated against (not copied from) git's patch_delta()
 * (patch-delta.c, declared in delta.h) and libgit2's git_delta_apply()
 * (src/libgit2/delta.c). Neither is linkable here: this helper/extension does
 * not link git, and libgit2's applier is -fvisibility=hidden and absent from
 * the public API, so we implement the frozen pack-delta format directly.
 */

/*
 * Read one of the two sizes a delta starts with, a little-endian base-128
 * varint, as git's get_delta_hdr_size() (delta.h) does. The shift is capped: an
 * over-long varint in a corrupt delta would otherwise shift by the width of a
 * size_t or more, which is undefined; a valid one is at most 10 bytes (shift
 * 63). Returns -1 for a varint running past `end` or past that width.
 */
static int delta_hdr_size(const unsigned char **datap, const unsigned char *end,
			  size_t *size)
{
	const unsigned char *dp = *datap;
	unsigned shift = 0;
	size_t v = 0;
	unsigned char c;

	do {
		if (dp >= end || shift >= sizeof(size_t) * 8) return -1;
		c = *dp++;
		v |= (size_t)(c & 0x7f) << shift;
		shift += 7;
	} while (c & 0x80);
	*datap = dp;
	*size = v;
	return 0;
}

/*
 * The size of the object a compressed delta makes, from the start of the delta
 * alone, as git's get_size_from_delta() (packfile.c) finds it: inflate only as
 * much as the two sizes the delta starts with can take, and read the second.
 */
static int delta_target_size(const void *comp, size_t comp_len, size_t *size)
{
	unsigned char head[20];
	const unsigned char *dp = head;
	size_t base_size, got;
	z_stream zs;
	int st;

	memset(&zs, 0, sizeof(zs));
	if (inflateInit(&zs) != Z_OK) return -1;
	zs.next_in = (Bytef *)comp;
	zs.avail_in = comp_len > UINT_MAX ? UINT_MAX : (uInt)comp_len;
	zs.next_out = head;
	zs.avail_out = sizeof(head);
	st = inflate(&zs, Z_FINISH);
	got = sizeof(head) - zs.avail_out;
	inflateEnd(&zs);
	if (st != Z_STREAM_END && got != sizeof(head)) return -1;
	if (delta_hdr_size(&dp, head + got, &base_size) ||
	    delta_hdr_size(&dp, head + got, size))
		return -1;
	return 0;
}

/* Apply `delta` to `base` into *out, which the caller frees. Returns 0, or -1
 * for a delta that does not apply to the base. */
static int git_delta_apply(const unsigned char *base, size_t base_len,
			   const unsigned char *delta, size_t delta_len,
			   unsigned char **out, size_t *out_len)
{
	const unsigned char *dp = delta;
	const unsigned char *dend = delta + delta_len;
	size_t src_size, tgt_size;

	if (delta_hdr_size(&dp, dend, &src_size) || src_size != base_len ||
	    delta_hdr_size(&dp, dend, &tgt_size))
		return -1;

	unsigned char *buf = malloc(tgt_size);
	if (!buf) return -1;
	size_t pos = 0;

	while (dp < dend) {
		unsigned char cmd = *dp++;
		if (cmd & 0x80) {
			/* Copy from base */
			size_t cp_off = 0, cp_size = 0;
			if (cmd & 0x01) { if (dp >= dend) goto fail; cp_off  = *dp++; }
			if (cmd & 0x02) { if (dp >= dend) goto fail; cp_off |= (size_t)*dp++ << 8; }
			if (cmd & 0x04) { if (dp >= dend) goto fail; cp_off |= (size_t)*dp++ << 16; }
			if (cmd & 0x08) { if (dp >= dend) goto fail; cp_off |= (size_t)*dp++ << 24; }
			if (cmd & 0x10) { if (dp >= dend) goto fail; cp_size  = *dp++; }
			if (cmd & 0x20) { if (dp >= dend) goto fail; cp_size |= (size_t)*dp++ << 8; }
			if (cmd & 0x40) { if (dp >= dend) goto fail; cp_size |= (size_t)*dp++ << 16; }
			if (cp_size == 0) cp_size = 0x10000;
			if (cp_off + cp_size < cp_off) goto fail;  /* offset+size overflow */
			if (cp_off + cp_size > base_len) goto fail;
			if (pos + cp_size > tgt_size) goto fail;
			memcpy(buf + pos, base + cp_off, cp_size);
			pos += cp_size;
		} else if (cmd) {
			/* Insert literal bytes */
			size_t n = cmd;
			if (dp + n > dend) goto fail;
			if (pos + n > tgt_size) goto fail;
			memcpy(buf + pos, dp, n);
			dp += n;
			pos += n;
		} else {
			/* cmd == 0 is reserved */
			goto fail;
		}
	}

	if (pos != tgt_size) goto fail;

	*out = buf;
	*out_len = tgt_size;
	return 0;

fail:
	free(buf);
	return -1;
}

/* ---- Schema + init ---- */

/*
 * The git schema. One CREATE per exec for error isolation. The store carries no
 * version and has no migrations (see storage_open).
 *
 * Tables that carry a non-trivial inline BLOB (objects.data, pack_bitmap's three)
 * are ordinary rowid tables, NOT WITHOUT ROWID. A WITHOUT ROWID table stores
 * whole rows in its primary-key btree, so a large inline value makes that btree
 * fat and deep and duplicates the BLOB key through its interior nodes; a point
 * lookup by key then pages through content it does not need. A rowid table keeps
 * a compact key->rowid index beside a rowid-keyed content btree, which measures
 * several times faster and smaller for these shapes. The text key stays the
 * PRIMARY KEY (a UNIQUE index).
 *
 * The WITHOUT ROWID tables carry no large value and are reached by their full
 * key: refs and commit_graph are small key/value rows; reflog clusters by
 * (refname, idx) for ordered range reads; meta is a tiny key/value table.
 * pack_content, which exploits physical clustering, keys on INTEGER PRIMARY
 * KEY, which is the rowid.
 */
static void create_git_schema(sqlite3 *db) {
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS objects("
		"  oid BLOB PRIMARY KEY, type TEXT NOT NULL,"
		"  size INTEGER NOT NULL, data BLOB NOT NULL,"
		"  base BLOB,"
		"  pack_pos INTEGER,"
		"  promisor INTEGER NOT NULL DEFAULT 0,"
		"  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),"
		"  last_used INTEGER NOT NULL DEFAULT (strftime('%s','now'))"
		");", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS refs("
		"  refname TEXT PRIMARY KEY, oid BLOB, symref TEXT"
		") WITHOUT ROWID;", 0, 0, 0);
	/* A reflog is its entries in idx order, from 0. A row at idx -1 marks a
	 * reflog that exists without entries, as reftable's null->null record
	 * does; reads skip it, while existence, listing, deletion and renaming
	 * treat it as any other row. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS reflog("
		"  refname TEXT NOT NULL, idx INTEGER NOT NULL,"
		"  old_oid BLOB NOT NULL, new_oid BLOB NOT NULL,"
		"  committer TEXT NOT NULL, timestamp INTEGER NOT NULL,"
		"  tz INTEGER NOT NULL, msg TEXT NOT NULL DEFAULT '',"
		"  PRIMARY KEY(refname, idx)) WITHOUT ROWID;", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS commit_graph("
		"  oid BLOB PRIMARY KEY, generation INT NOT NULL"
		") WITHOUT ROWID;", 0, 0, 0);
	/* Object-store metadata (key/value). Holds 'last_optimize_at', the unix
	 * time of the last optimize: the gc-auto heuristic counts objects newer
	 * than it (the relational analog of files' loose-objects-since-last-pack);
	 * and 'objectformat', the repository hash algorithm. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS meta("
		"  key TEXT PRIMARY KEY, value INTEGER NOT NULL"
		") WITHOUT ROWID;", 0, 0, 0);
	/* Delta-base guard index for prune: STMT_PRUNE_OBJECT rejects deleting an
	 * object still serving as another object's delta base via NOT EXISTS over
	 * objects(base). Partial (only delta rows carry a base), so it stays tiny and
	 * turns that per-candidate guard from a full WITHOUT ROWID scan into a probe. */
	sqlite3_exec(db,
		"CREATE INDEX IF NOT EXISTS idx_objects_base"
		"  ON objects(base) WHERE base IS NOT NULL;", 0, 0, 0);
	/* The reachability bitmap git writes for a pack of the objects, at most one
	 * per store: git's .bitmap verbatim, with the .idx and .rev of that pack,
	 * which relate its bits to the objects (the .rev empty when git wrote
	 * none), and the size the pack had. git opens them as it opens the bitmap
	 * of a pack whose objects it reads from elsewhere. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS pack_bitmap("
		"  id INTEGER PRIMARY KEY CHECK(id = 0), pack_size INTEGER NOT NULL,"
		"  idx BLOB NOT NULL, rev BLOB NOT NULL, bitmap BLOB NOT NULL);", 0, 0, 0);
	/* A store may hold objects clustered by pack position: their bytes live
	 * here keyed by objects.pack_pos (INTEGER PRIMARY KEY == the rowid, so rows
	 * lie in pack_pos order), with objects.data empty for them, and the read
	 * coalesces this over objects.data. No write clusters an object: re-storing
	 * one writes objects.data and clears its pack_pos, and optimize drops the
	 * rows no object points at. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS pack_content("
		"  pack_pos INTEGER PRIMARY KEY, type TEXT NOT NULL, size INTEGER NOT NULL,"
		"  base BLOB, content BLOB NOT NULL);", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE INDEX IF NOT EXISTS idx_objects_pack_pos"
		"  ON objects(pack_pos);", 0, 0, 0);
}

int storage_git_init(void) {
	sqlite3 *db = storage_db();
	if (!db) return -1;
	create_git_schema(db);
	storage_at_close(git_stmts_close);
	return 0;
}

/* Caller brackets the transaction (cmd_promisor marks the whole partial-clone
 * set in one savepoint), like storage_put_commit_graph; a self-savepoint here
 * would make each mark its own BEGIN IMMEDIATE write txn. */
int storage_mark_promisor(const char *oid) {
	sqlite3_stmt *st = stmt_acquire(STMT_MARK_PROMISOR);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_MARK_PROMISOR, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* Storage holds oids as raw blobs, half the hex width. */
#define STORAGE_OID_RAWSZ (STORAGE_OID_HEXSZ / 2)

/*
 * Decode a hex oid prefix to its raw-byte floor. Vendored from git's
 * parse_oid_prefix() (object-name.c); since the dependency-free helper has no
 * struct object_id, it writes a plain byte buffer (so it zeroes it first rather
 * than relying on a pre-cleared oid) and drops the algo/normalized-hex outputs
 * git also fills. Each hex digit is a nibble, the even-indexed one shifted into
 * the high half of its byte, so a dangling odd nibble leaves the trailing byte's
 * low half zero. Returns the byte length, or -1 on a non-hex digit.
 */
static int parse_oid_prefix(const char *name, int len, unsigned char *out) {
	memset(out, 0, (len + 1) / 2);
	for (int i = 0; i < len; i++) {
		unsigned char c = name[i], val;
		if (c >= '0' && c <= '9')
			val = c - '0';
		else if (c >= 'a' && c <= 'f')
			val = c - 'a' + 10;
		else if (c >= 'A' && c <= 'F')
			val = c - 'A' + 10;
		else
			return -1;
		if (!(i & 1))
			val <<= 4;
		out[i >> 1] |= val;
	}
	return (len + 1) / 2;
}

/*
 * Convert a hex oid prefix to the raw-byte range [lo, hi) of the oids that begin
 * with it, so a prefix lookup seeks the oid primary-key index instead of
 * scanning the table to GLOB lower(hex(oid)) per row. Abbreviation prefixes
 * anchor at the leading bytes (git stores oids sorted, so only a leading prefix
 * is a contiguous range); a prefix may be an odd number of hex digits, whose
 * trailing nibble is the high half of its byte.
 *
 * The floor is parse_oid_prefix() above (git's decode). git stops there and
 * scan-compares each candidate nibble-aware; we range-seek the index instead, so
 * on top of the floor we derive the exclusive ceiling here: the floor plus one
 * at the last specified nibble's place, carried up. An all-f prefix has no
 * finite successor, so hi is set longer than any oid, it still sorts after
 * every stored oid, keeping the query a two-sided range. Returns 0 on success,
 * -1 on a non-hex or over-long prefix (the caller then lists without scoping).
 */
static int hexprefix_range(const char *hex,
			   unsigned char *lo, int *lo_len,
			   unsigned char *hi, int *hi_len) {
	int nib = (int)strlen(hex);
	if (nib == 0 || nib > 2 * STORAGE_OID_RAWSZ)
		return -1;
	int nbytes = parse_oid_prefix(hex, nib, lo);
	if (nbytes < 0)
		return -1;
	*lo_len = nbytes;
	memcpy(hi, lo, nbytes);
	int carry = (nib & 1) ? 0x10 : 0x01;
	for (int i = nbytes - 1; i >= 0 && carry; i--) {
		int s = hi[i] + carry;
		hi[i] = (unsigned char)(s & 0xff);
		carry = s >> 8;
	}
	if (carry) {
		memset(hi, 0xff, STORAGE_OID_RAWSZ + 1);
		*hi_len = STORAGE_OID_RAWSZ + 1;
	} else {
		*hi_len = nbytes;
	}
	return 0;
}

int storage_obj_list_filtered(int promisor_only, const char *prefix,
			      storage_obj_cb cb, void *data) {
	/* A hex prefix scopes the listing to the raw-byte oid range it covers
	 * (STMT_OBJ_LIST_RANGE seeks the PK index and folds in the promisor
	 * filter); otherwise pick the matching bulk statement so a full
	 * enumeration keeps its no-WHERE fast path. */
	unsigned char lo[STORAGE_OID_RAWSZ], hi[STORAGE_OID_RAWSZ + 1];
	int lo_len = 0, hi_len = 0;
	int use_range = prefix && *prefix &&
			hexprefix_range(prefix, lo, &lo_len, hi, &hi_len) == 0;
	enum stmt_id sid = use_range ? STMT_OBJ_LIST_RANGE :
			   (promisor_only ? STMT_OBJ_LIST_PROMISOR : STMT_OBJ_LIST);
	sqlite3_stmt *st = stmt_acquire(sid);
	if (use_range) {
		sqlite3_bind_blob(st, 1, lo, lo_len, SQLITE_STATIC);
		sqlite3_bind_blob(st, 2, hi, hi_len, SQLITE_STATIC);
		sqlite3_bind_int(st, 3, promisor_only ? 1 : 0);
	}
	int aborted = 0;
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *oid = (const char *)sqlite3_column_text(st, 0);
		const char *type = (const char *)sqlite3_column_text(st, 1);
		size_t size = (size_t)sqlite3_column_int64(st, 2);
		if (cb(oid, type, size, data)) { aborted = 1; break; }
	}
	stmt_release(sid, st);
	return aborted ? -1 : 0;
}

/* ---- Reachability bitmap of a pack of the objects ---- */

int storage_put_pack_bitmap(long long pack_size,
			    const void *idx, size_t idx_len,
			    const void *rev, size_t rev_len,
			    const void *bitmap, size_t bitmap_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_PACK_BITMAP_PUT);
	/* bind_blob64, not bind_blob: the lengths are size_t, and the 32-bit bind
	 * would truncate a large blob to a negative length rather than reject it.
	 * A zero-length blob binds as X'' (a NULL pointer would bind NULL). */
	int rc = sqlite3_bind_int64(st, 1, (sqlite3_int64)pack_size);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_blob64(st, 2, idx_len ? idx : "", idx_len, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_blob64(st, 3, rev_len ? rev : "", rev_len, SQLITE_STATIC);
	if (rc == SQLITE_OK)
		rc = sqlite3_bind_blob64(st, 4, bitmap_len ? bitmap : "", bitmap_len,
					 SQLITE_STATIC);
	if (rc == SQLITE_OK) rc = sqlite3_step(st);
	stmt_release(STMT_PACK_BITMAP_PUT, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* Copy one BLOB column out as a freshly malloc'd buffer (at least one byte). */
static void *column_blob_dup(sqlite3_stmt *st, int col, size_t *len) {
	const void *b = sqlite3_column_blob(st, col);
	int blen = sqlite3_column_bytes(st, col);
	void *copy = malloc(blen ? (size_t)blen : 1);

	if (copy && blen)
		memcpy(copy, b, blen);
	*len = (size_t)blen;
	return copy;
}

/*
 * Fetch the stored bitmap with the index and reverse index of its pack as
 * freshly malloc'd buffers the caller owns. Returns 1 when one is stored, 0
 * when none is, and -1 on error. The column blobs are copied out before the
 * statement is reset (sqlite invalidates them on reset).
 */
int storage_get_pack_bitmap(long long *pack_size,
			    void **idx, size_t *idx_len,
			    void **rev, size_t *rev_len,
			    void **bitmap, size_t *bitmap_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_PACK_BITMAP_GET);
	int ret = 0;

	*idx = *rev = *bitmap = NULL;
	if (sqlite3_step(st) == SQLITE_ROW) {
		*pack_size = sqlite3_column_int64(st, 0);
		*idx = column_blob_dup(st, 1, idx_len);
		*rev = column_blob_dup(st, 2, rev_len);
		*bitmap = column_blob_dup(st, 3, bitmap_len);
		ret = (*idx && *rev && *bitmap) ? 1 : -1;
	}
	stmt_release(STMT_PACK_BITMAP_GET, st);
	if (ret < 0) {
		free(*idx); free(*rev); free(*bitmap);
		*idx = *rev = *bitmap = NULL;
	}
	return ret;
}

int storage_remove_pack_bitmap(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_PACK_BITMAP_REMOVE);
	int rc = sqlite3_step(st);
	stmt_release(STMT_PACK_BITMAP_REMOVE, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* ---- Prune: delete (or, dry-run, report) one unreachable object git identified ---- */

int storage_prune_object(const char *oid, long long expire) {
	sqlite3_stmt *st = stmt_acquire(STMT_PRUNE_OBJECT);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 2, (sqlite3_int64)expire);
	sqlite3_step(st);
	stmt_release(STMT_PRUNE_OBJECT, st);
	/*
	 * sqlite3_changes() reports whether the DELETE removed the row. The grace and
	 * the delta-base guard in the statement spare a recently written or
	 * still-needed object; the caller's fixpoint retries a base once its
	 * dependents are gone. A dry run is the caller running this for real inside a
	 * savepoint it rolls back, so it observes exactly what a real prune would do.
	 * The clustered content of a deleted object, if it had any, goes with the
	 * next optimize's sweep.
	 */
	return sqlite3_changes(storage_db()) > 0;
}

/*
 * gc-auto heuristic support. storage_optimize_churn() reports how many objects
 * have been added since the last optimize (the files-parallel of loose objects
 * accumulated since the last pack); git compares it against gc.auto to decide
 * whether to run "git gc". storage_optimize_done() stamps the watermark so the
 * churn resets after an optimize. The helper reports the raw count; git owns the
 * threshold policy, exactly as the files backend reads gc.auto git-side.
 */
long long storage_optimize_churn(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_OPTIMIZE_CHURN);
	long long churn = 0;
	if (sqlite3_step(st) == SQLITE_ROW)
		churn = sqlite3_column_int64(st, 0);
	stmt_release(STMT_OPTIMIZE_CHURN, st);
	return churn;
}

int storage_optimize_done(void) {
	if (storage_savepoint("optimize_done")) return -1;
	sqlite3_stmt *st = stmt_acquire(STMT_OPTIMIZE_DONE);
	int rc = sqlite3_step(st);
	stmt_release(STMT_OPTIMIZE_DONE, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("optimize_done"); return -1; }
	return storage_release("optimize_done");
}

/* ---- Maintenance: incremental_vacuum (optimize) and integrity check (verify) ---- */

int storage_vacuum(void) {
	sqlite3_stmt *st;
	int rc;

	/* Drop the clustered content no object points at, then reclaim its pages. */
	st = stmt_acquire(STMT_PACK_CONTENT_SWEEP);
	if (!st)
		return -1;
	rc = sqlite3_step(st);
	stmt_release(STMT_PACK_CONTENT_SWEEP, st);
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "sqlite-git: sweeping clustered content failed (%d): %s\n",
			rc, sqlite3_errmsg(storage_db()));
		return -1;
	}

	st = stmt_acquire(STMT_VACUUM);
	if (!st)
		return -1;
	/*
	 * The pragma yields a columnless row for each page it returns to the file,
	 * so a store with anything on its freelist answers the first step with a
	 * row, not done; it is stepped until it reports done, and only another
	 * answer is a failure.
	 */
	while ((rc = sqlite3_step(st)) == SQLITE_ROW)
		;
	if (rc != SQLITE_DONE)
		fprintf(stderr, "sqlite-git: incremental_vacuum failed (%d): %s\n",
			rc, sqlite3_errmsg(storage_db()));
	stmt_release(STMT_VACUUM, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

int storage_integrity_check(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_INTEGRITY_CHECK);
	int ok = 0;

	if (!st)
		return -1;
	/*
	 * PRAGMA integrity_check yields the single row "ok" when the database is
	 * sound, otherwise one row per problem found; the first non-"ok" row is enough
	 * to know the store is corrupt.
	 */
	if (sqlite3_step(st) == SQLITE_ROW) {
		const char *res = (const char *)sqlite3_column_text(st, 0);
		ok = res && !strcmp(res, "ok");
	}
	stmt_release(STMT_INTEGRITY_CHECK, st);
	return ok ? 0 : -1;
}

/*
 * Repository object format (the hash algorithm), persisted like the optimize
 * watermark above: one 'objectformat' meta row, written by git0_init and read
 * wherever the extension hashes objects. 0 is sha1 (the default for an unset or
 * sha1-only repo), 1 is sha256. The store keeps it as an opaque int and never
 * interprets it; the extension maps it to libgit2's oid_type. The helper never
 * reads it: it passes oids through as opaque hex, hash-agnostic.
 */
int storage_object_format(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJECT_FORMAT_GET);
	int fmt = 0;
	if (sqlite3_step(st) == SQLITE_ROW)
		fmt = sqlite3_column_int(st, 0);
	stmt_release(STMT_OBJECT_FORMAT_GET, st);
	return fmt;
}

int storage_set_object_format(int fmt) {
	if (storage_savepoint("set_object_format")) return -1;
	sqlite3_stmt *st = stmt_acquire(STMT_OBJECT_FORMAT_SET);
	sqlite3_bind_int(st, 1, fmt);
	int rc = sqlite3_step(st);
	stmt_release(STMT_OBJECT_FORMAT_SET, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("set_object_format"); return -1; }
	return storage_release("set_object_format");
}

/*
 * Bump an object's last_used to now (the prune-grace timestamp). Returns 1 if a
 * row was updated (the object exists), 0 otherwise. No savepoint of its own: the
 * write path calls it inside its open savepoint; storage_freshen() wraps it for
 * the standalone "freshen" verb.
 */
static int freshen_oid(const char *oid) {
	sqlite3_stmt *st = stmt_acquire(STMT_FRESHEN);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	sqlite3_step(st);
	int changed = sqlite3_changes(storage_db()) > 0;
	stmt_release(STMT_FRESHEN, st);
	return changed;
}

int storage_freshen(const char *oid) {
	if (storage_savepoint("freshen")) return -1;
	int changed = freshen_oid(oid);
	if (storage_release("freshen")) return -1;
	return changed;
}

/*
 * The refs backend's remove_on_disk (the helper's cmd_remove). Mirrors
 * files_ref_store_remove_on_disk, which removes only refs/ and logs/: TRUNCATE
 * the ref store (DELETE the rows, keeping the tables + indexes so the store stays
 * usable) and NEVER touch objects/lfs, since a ref-store remove must not destroy
 * the object store the same database holds.
 */
int storage_remove_ref_store(void) {
	sqlite3_stmt *st;
	int ok = 1;

	if (storage_savepoint("remove_ref_store")) return -1;
	st = stmt_acquire(STMT_REF_CLEAR);
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_REF_CLEAR, st);
	st = stmt_acquire(STMT_REFLOG_CLEAR);
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_REFLOG_CLEAR, st);
	if (!ok) { storage_rollback_to("remove_ref_store"); return -1; }
	return storage_release("remove_ref_store");
}

/* ---- Object read (resolves delta chains) ---- */

/* Record how an object is stored: whole when base is NULL, or as a delta
 * against base that inflates to size bytes. */
static void set_delta(struct storage_delta *delta, const char *base, size_t size) {
	if (!delta) return;
	snprintf(delta->base, sizeof(delta->base), "%s", base ? base : "");
	delta->size = base ? size : 0;
}

static int read_object_depth(const char *oid, char *out_type, size_t out_type_sz,
			     size_t *out_size, unsigned char **out_data,
			     struct storage_delta *out_delta, int depth) {
	if (depth > MAX_DELTA_DEPTH) return -1;

	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_READ);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) {
		stmt_release(STMT_OBJ_READ, st);
		return -1;
	}

	const char *type_name = (const char *)sqlite3_column_text(st, 0);
	if (out_type && type_name) snprintf(out_type, out_type_sz, "%s", type_name);
	*out_size = (size_t)sqlite3_column_int64(st, 1);
	const void *comp = sqlite3_column_blob(st, 2);
	int comp_len = sqlite3_column_bytes(st, 2);
	const char *base_hex = (const char *)sqlite3_column_text(st, 3);  /* lower(hex(base)) or NULL */
	set_delta(out_delta, base_hex, *out_size);

	if (!base_hex) {
		*out_data = zdecompress(comp, comp_len, *out_size);
		stmt_release(STMT_OBJ_READ, st);
		return *out_data ? 0 : -1;
	}

	/*
	 * Delta: copy the base oid, then inflate the stored (compressed) git-format
	 * delta before finalize/recursion (the sqlite blob is invalidated by the
	 * recursive base read). The `size` column holds the delta stream's
	 * uncompressed length, the size in the header of a pack entry, which
	 * put-raw carries, so a plain known-size inflate suffices.
	 * The resolved object size comes from git_delta_apply (it reads the delta
	 * header), exactly as git derives it from a pack delta.
	 */
	char base_oid[STORAGE_OID_HEXSZ + 1];
	snprintf(base_oid, sizeof(base_oid), "%s", base_hex);
	size_t delta_len = *out_size;
	unsigned char *delta = zdecompress(comp, comp_len, delta_len);
	stmt_release(STMT_OBJ_READ, st);
	if (!delta) return -1;

	char base_type[32];
	size_t base_size;
	unsigned char *base_data;
	if (read_object_depth(base_oid, base_type, sizeof(base_type), &base_size, &base_data,
			      NULL, depth + 1) < 0) {
		free(delta);
		return -1;
	}

	/*
	 * Resolve the git-format delta against the base. git_delta_apply reads the
	 * target size from the delta header and allocates *out_data; it also checks
	 * the delta's source size matches base_size. out_type was already set from
	 * this row above (the delta row records the resolved object's type/size).
	 */
	if (git_delta_apply(base_data, base_size,
			    (const unsigned char *)delta, (size_t)delta_len,
			    out_data, out_size) < 0) {
		free(base_data); free(delta);
		return -1;
	}
	free(base_data); free(delta);
	return 0;
}

int storage_read_object(const char *oid, char *out_type, size_t out_type_sz,
			size_t *out_size, unsigned char **out_data,
			struct storage_delta *delta) {
	return read_object_depth(oid, out_type, out_type_sz, out_size, out_data, delta, 0);
}

/*
 * Type and size without the contents. The stored `size` is the object's size
 * for a whole object but the length of the delta for a delta (see
 * read_object_depth), and the start of the delta holds the size of the object
 * it makes, which is all of the delta read, as git reads the size of a delta
 * in a packfile. type is correct for both (a delta row records the resolved
 * object's type). Returns 0, -1 if absent. out_type is a buffer of out_type_sz.
 */
int storage_read_header(const char *oid, char *out_type, size_t out_type_sz,
			size_t *out_size, struct storage_delta *delta) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_HEADER);
	int ret = 0;

	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(STMT_OBJ_HEADER, st); return -1; }
	const char *type = (const char *)sqlite3_column_text(st, 0);
	size_t size = (size_t)sqlite3_column_int64(st, 1);
	const char *base = (const char *)sqlite3_column_text(st, 2);
	if (out_type && type) snprintf(out_type, out_type_sz, "%s", type);
	set_delta(delta, base, size);
	if (!base) {
		if (out_size) *out_size = size;
	} else if (out_size) {
		ret = delta_target_size(sqlite3_column_blob(st, 3),
					(size_t)sqlite3_column_bytes(st, 3), out_size);
	}
	stmt_release(STMT_OBJ_HEADER, st);
	return ret;
}

/* ---- Object write (zlib + delta) ---- */

int storage_write_object(const char *oid, const char *type,
			  const void *data, size_t size, int replace) {
	if (storage_savepoint("write_object")) return -1;

	/*
	 * Keep-existing on the normal path: a fetch/loose write never overwrites a
	 * stored object (content-addressed, so the bytes would be identical). Only
	 * repack passes replace, re-representing an object in place via the UPSERT
	 * (here, converting a delta back to a full object: base bound NULL).
	 */
	if (!replace) {
		sqlite3_stmt *ex = stmt_acquire(STMT_OBJ_EXISTS);
		sqlite3_bind_text(ex, 1, oid, -1, SQLITE_STATIC);
		int exists = (sqlite3_step(ex) == SQLITE_ROW);
		stmt_release(STMT_OBJ_EXISTS, ex);
		/* Keep-existing, but freshen: a duplicate write renews the prune-grace
		 * timestamp, exactly as files utime's an object it declines to rewrite. */
		if (exists) { freshen_oid(oid); return storage_release("write_object"); }
	}

	enum stmt_id sid = replace ? STMT_OBJ_REPLACE : STMT_OBJ_WRITE;
	sqlite3_stmt *st = stmt_acquire(sid);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, type, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 3, (sqlite3_int64)size);
	sqlite3_bind_int(st, 6, 0);  /* promisor: set out-of-band via storage_mark_promisor */

	/*
	 * Store the object whole, zlib-compressed (base column NULL). The helper does
	 * not create deltas: git core supplies them, already compressed, via the
	 * put-raw verb (data = compressed delta, base = base oid). This path
	 * (put / storage_write_object) is for the whole objects git hands over raw.
	 */
	unsigned long comp_len;
	unsigned char *comp = zcompress(data, size, &comp_len);
	if (!comp) { stmt_release(sid, st); storage_rollback_to("write_object"); return -1; }
	/* bind_blob64 + checked rc: comp_len is unsigned long, so the 32-bit bind
	 * would truncate a payload near INT_MAX into a negative length (UB / mis-sized
	 * row) instead of a clean SQLITE_TOOBIG. */
	int rc = sqlite3_bind_blob64(st, 4, comp, comp_len, SQLITE_STATIC);
	sqlite3_bind_null(st, 5);
	if (rc == SQLITE_OK) rc = sqlite3_step(st);
	free(comp);
	stmt_release(sid, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("write_object"); return -1; }
	return storage_release("write_object");
}

/*
 * Store an object whose bytes are ALREADY in git's storage representation (a
 * zlib-compressed content stream or git-format delta, as the entry of a pack
 * holds it), persisting them verbatim: git sends the entries of a packfile as
 * they are, so the helper never (de)compresses on this path, it is a dumb byte
 * store, exactly where the files backend saves to disk. `cdata`/`clen` are the
 * compressed bytes; `usize` is the size they inflate to (the `size` column,
 * which read uses to inflate). Keep-existing + replace semantics mirror
 * storage_write_object; only the compression step differs (there is none).
 */
int storage_put_raw(const char *oid, const char *type, const void *cdata,
		     size_t clen, size_t usize, const char *base, int replace) {
	if (storage_savepoint("put_raw")) return -1;

	if (!replace) {
		sqlite3_stmt *ex = stmt_acquire(STMT_OBJ_EXISTS);
		sqlite3_bind_text(ex, 1, oid, -1, SQLITE_STATIC);
		int exists = (sqlite3_step(ex) == SQLITE_ROW);
		stmt_release(STMT_OBJ_EXISTS, ex);
		if (exists) { freshen_oid(oid); return storage_release("put_raw"); }
	}

	/*
	 * One verbatim store for both shapes of git's native object form: base
	 * NULL -> a whole object (data = the compressed content), base set -> a
	 * delta (data = the compressed git-format delta, base = its base oid),
	 * exactly the two entry shapes a pack holds. The bytes are git's already,
	 * stored as-is.
	 */
	enum stmt_id sid = replace ? STMT_OBJ_REPLACE : STMT_OBJ_WRITE;
	sqlite3_stmt *st = stmt_acquire(sid);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, type, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 3, (sqlite3_int64)usize);
	sqlite3_bind_int(st, 6, 0);  /* promisor: set out-of-band via storage_mark_promisor */
	/* bind_blob64: clen is size_t; the 32-bit bind truncates a payload near
	 * INT_MAX to a negative length instead of a clean SQLITE_TOOBIG. */
	int rc = sqlite3_bind_blob64(st, 4, cdata, clen, SQLITE_STATIC);
	if (base)
		sqlite3_bind_text(st, 5, base, -1, SQLITE_STATIC);
	else
		sqlite3_bind_null(st, 5);
	if (rc == SQLITE_OK) rc = sqlite3_step(st);
	stmt_release(sid, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("put_raw"); return -1; }
	return storage_release("put_raw");
}

int storage_object_exists(const char *oid) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_EXISTS);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	int found = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(STMT_OBJ_EXISTS, st);
	return found ? 1 : 0;
}

/* ---- get-raw: an object as it is stored ---- */

int storage_read_raw(const char *oid, char *out_type, size_t out_type_sz,
		     size_t *out_size, struct storage_delta *delta,
		     unsigned char **out_data, size_t *out_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_READ);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(STMT_OBJ_READ, st); return -1; }

	const char *type = (const char *)sqlite3_column_text(st, 0);
	size_t size = (size_t)sqlite3_column_int64(st, 1);
	const void *data = sqlite3_column_blob(st, 2);
	size_t len = (size_t)sqlite3_column_bytes(st, 2);
	const char *base = (const char *)sqlite3_column_text(st, 3);
	unsigned char *copy = malloc(len ? len : 1);

	if (!copy) { stmt_release(STMT_OBJ_READ, st); return -1; }
	if (len) memcpy(copy, data, len);
	if (out_type && type) snprintf(out_type, out_type_sz, "%s", type);
	set_delta(delta, base, size);
	stmt_release(STMT_OBJ_READ, st);

	*out_size = size;
	*out_data = copy;
	*out_len = len;
	return 0;
}

/* ---- Object listing ---- */

int storage_obj_list(storage_obj_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_LIST);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *oid = (const char *)sqlite3_column_text(st, 0);
		const char *type = (const char *)sqlite3_column_text(st, 1);
		size_t size = (size_t)sqlite3_column_int64(st, 2);
		if (cb(oid, type, size, data)) { stmt_release(STMT_OBJ_LIST, st); return 1; }
	}
	stmt_release(STMT_OBJ_LIST, st);
	return 0;
}

/* ---- Ref operations ---- */

int storage_ref_read(const char *refname, char *oid, size_t oid_sz, char *symref, size_t symref_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_REF_READ);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	int step_rc = sqlite3_step(st);
	if (step_rc != SQLITE_ROW) {
		stmt_release(STMT_REF_READ, st);
		return -1;
	}
	const char *oid_hex = (const char *)sqlite3_column_text(st, 0);
	const char *sym = (const char *)sqlite3_column_text(st, 1);
	if (oid_hex && oid) snprintf(oid, oid_sz, "%s", oid_hex);
	else if (oid) oid[0] = '\0';
	if (sym && symref) snprintf(symref, symref_len, "%s", sym);
	else if (symref) symref[0] = '\0';
	stmt_release(STMT_REF_READ, st);
	return 0;
}

int storage_ref_write(const char *refname, const char *oid, const char *symref) {
	if (storage_savepoint("ref_write")) return -1;
	sqlite3_stmt *st = stmt_acquire(STMT_REF_WRITE);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	if (oid) sqlite3_bind_text(st, 2, oid, -1, SQLITE_STATIC);
	else sqlite3_bind_null(st, 2);
	if (symref) sqlite3_bind_text(st, 3, symref, -1, SQLITE_STATIC);
	else sqlite3_bind_null(st, 3);
	int rc = sqlite3_step(st);
	stmt_release(STMT_REF_WRITE, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("ref_write"); return -1; }
	return storage_release("ref_write");
}

int storage_ref_delete(const char *refname) {
	if (storage_savepoint("ref_delete")) return -1;
	sqlite3_stmt *st = stmt_acquire(STMT_REF_DELETE);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_REF_DELETE, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("ref_delete"); return -1; }
	return storage_release("ref_delete");
}

int storage_ref_list(const char *prefix, storage_ref_cb cb, void *data) {
	/* Propagate a callback abort (-1) instead of always returning 0: a caller
	 * that stops iteration early (the extension ref iterator can) must not see a
	 * truncated walk reported as a complete one. Mirrors storage_obj_list_filtered. */
	int aborted = 0;
	if (prefix && *prefix) {
		char *pattern = sqlite3_mprintf("%s*", prefix);
		if (!pattern) return -1;
		sqlite3_stmt *st = stmt_acquire(STMT_REF_LIST_GLOB);
		sqlite3_bind_text(st, 1, pattern, -1, sqlite3_free);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			const char *oid = (const char *)sqlite3_column_text(st, 1);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (cb(name, oid, sym, data)) { aborted = 1; break; }
		}
		stmt_release(STMT_REF_LIST_GLOB, st);
	} else {
		sqlite3_stmt *st = stmt_acquire(STMT_REF_LIST);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			const char *oid = (const char *)sqlite3_column_text(st, 1);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (cb(name, oid, sym, data)) { aborted = 1; break; }
		}
		stmt_release(STMT_REF_LIST, st);
	}
	return aborted ? -1 : 0;
}

/* ---- Reflog ---- */

int storage_reflog_exists(const char *refname) {
	sqlite3_stmt *st = stmt_acquire(STMT_REFLOG_EXISTS);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	int found = (sqlite3_step(st) == SQLITE_ROW);
	stmt_release(STMT_REFLOG_EXISTS, st);
	return found;
}

int storage_reflog_create(const char *refname) {
	if (storage_savepoint("reflog_create")) return -1;
	sqlite3_stmt *st = stmt_acquire(STMT_REFLOG_CREATE);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_REFLOG_CREATE, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("reflog_create"); return -1; }
	return storage_release("reflog_create");
}

int storage_reflog_delete(const char *refname) {
	if (storage_savepoint("reflog_delete")) return -1;
	sqlite3_stmt *st = stmt_acquire(STMT_REFLOG_DELETE);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_REFLOG_DELETE, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("reflog_delete"); return -1; }
	return storage_release("reflog_delete");
}

int storage_reflog_read(const char *refname, storage_reflog_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(STMT_REFLOG_READ);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *old_oid = (const char *)sqlite3_column_text(st, 0);
		const char *new_oid = (const char *)sqlite3_column_text(st, 1);
		const char *committer = (const char *)sqlite3_column_text(st, 2);
		long long ts = sqlite3_column_int64(st, 3);
		int tz = sqlite3_column_int(st, 4);
		const char *msg = (const char *)sqlite3_column_text(st, 5);
		if (cb(old_oid, new_oid, committer, ts, tz, msg, data)) break;
	}
	stmt_release(STMT_REFLOG_READ, st);
	return 0;
}

int storage_reflog_read_reverse(const char *refname, storage_reflog_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(STMT_REFLOG_READ_REV);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *old_oid = (const char *)sqlite3_column_text(st, 0);
		const char *new_oid = (const char *)sqlite3_column_text(st, 1);
		const char *committer = (const char *)sqlite3_column_text(st, 2);
		long long ts = sqlite3_column_int64(st, 3);
		int tz = sqlite3_column_int(st, 4);
		const char *msg = (const char *)sqlite3_column_text(st, 5);
		if (cb(old_oid, new_oid, committer, ts, tz, msg, data)) break;
	}
	stmt_release(STMT_REFLOG_READ_REV, st);
	return 0;
}

int storage_reflog_list(storage_reflog_name_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(STMT_REFLOG_LIST);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *refname = (const char *)sqlite3_column_text(st, 0);
		if (cb(refname, data)) break;
	}
	stmt_release(STMT_REFLOG_LIST, st);
	return 0;
}

int storage_reflog_append(const char *refname, const char *old_oid,
			   const char *new_oid, const char *committer,
			   long long timestamp, int tz, const char *msg) {
	if (storage_savepoint("reflog_append")) return -1;
	sqlite3_stmt *st = stmt_acquire(STMT_REFLOG_APPEND);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 3, old_oid, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 4, new_oid, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 5, committer, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 6, timestamp);
	sqlite3_bind_int(st, 7, tz);
	sqlite3_bind_text(st, 8, msg ? msg : "", -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_REFLOG_APPEND, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("reflog_append"); return -1; }
	return storage_release("reflog_append");
}

/*
 * Carry a ref's reflog across on rename or copy, inside the caller's open
 * savepoint so it commits atomically with the ref change. On rename
 * (delete_old) the rows are moved to the new name, leaving none behind; on
 * copy they are duplicated. The idx ordering is preserved either way, so a
 * subsequent reflog-append (the rename/creation entry) lands after the carried
 * history. Mirrors the files backend (which moves the reflog file) and
 * reftable's write_copy_table.
 */
int storage_reflog_copy(const char *oldref, const char *newref, int delete_old) {
	enum stmt_id id = delete_old ? STMT_REFLOG_MOVE : STMT_REFLOG_COPY;
	if (storage_savepoint("reflog_copy")) return -1;
	sqlite3_stmt *st = stmt_acquire(id);
	sqlite3_bind_text(st, 1, newref, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, oldref, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(id, st);
	if (rc != SQLITE_DONE) { storage_rollback_to("reflog_copy"); return -1; }
	return storage_release("reflog_copy");
}

/* ---- Commit graph ---- */

long long storage_commit_generation(const char *oid) {
	sqlite3_stmt *st = stmt_acquire(STMT_COMMIT_GEN);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	long long gen = -1;
	if (sqlite3_step(st) == SQLITE_ROW)
		gen = sqlite3_column_int64(st, 0);
	stmt_release(STMT_COMMIT_GEN, st);
	return gen;
}

/* ---- Commit-graph generation (write path; git computes at commit-graph time) ---- */

int storage_put_commit_graph(const char *oid, long long generation) {
	sqlite3_stmt *st = stmt_acquire(STMT_COMMIT_GRAPH_PUT);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 2, (sqlite3_int64)generation);
	int rc = sqlite3_step(st);
	stmt_release(STMT_COMMIT_GRAPH_PUT, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* Drop all stored generations; the write seam clears then repopulates atomically
 * so stale commits (no longer in the graph) never linger. */
int storage_clear_commit_graph(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_COMMIT_GRAPH_CLEAR);
	int rc = sqlite3_step(st);
	stmt_release(STMT_COMMIT_GRAPH_CLEAR, st);
	return rc == SQLITE_DONE ? 0 : -1;
}
