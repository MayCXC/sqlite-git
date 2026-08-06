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
	STMT_PACK_MEMBER_ADD,
	STMT_KEEP_ADD,
	STMT_KEEP_CLEAR,
	STMT_ABSORB_RELEASED,
	STMT_BITMAP_STORE,
	STMT_BITMAP_GET,
	STMT_BITMAP_CLEAR,
	STMT_COMMIT_BITMAP_STORE,
	STMT_COMMIT_BITMAP_GET,
	STMT_COMMIT_BITMAP_CLEAR,
	STMT_ORDER_ADD,
	STMT_ORDER_CLEAR,
	STMT_REV_ADD,
	STMT_REV_CLEAR,
	STMT_PACK_POS_CLEAR,
	STMT_PACK_POS_SET,
	STMT_POS_OF_OID,
	STMT_RESULT_POS_ADD,
	STMT_RESULT_POS_CLEAR,
	STMT_OIDS_AT_RESULT,
	STMT_BITMAP_COUNT,
	STMT_CONTENT_UNSPLIT,
	STMT_PACK_CONTENT_CLEAR,
	STMT_PACK_CONTENT_FILL,
	STMT_CONTENT_EMPTY,
	STMT_REUSE_STREAM,
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
		/* Header only: type, the stored size, and whether the row is a delta.
		 * Reads no content (no pack_content JOIN, no objects.data), so a whole
		 * object answers without inflating; a delta needs a full resolve for its
		 * logical size (see storage_read_header). */
		"SELECT type, size, base IS NOT NULL FROM objects WHERE oid = unhex(?)",
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
		"SELECT lower(hex(old_oid)), lower(hex(new_oid)), committer, timestamp, tz, msg FROM reflog WHERE refname = ? ORDER BY idx ASC",
	[STMT_REFLOG_READ_REV] =
		"SELECT lower(hex(old_oid)), lower(hex(new_oid)), committer, timestamp, tz, msg FROM reflog WHERE refname = ? ORDER BY idx DESC",
	[STMT_REFLOG_LIST] =
		"SELECT DISTINCT refname FROM reflog ORDER BY refname",
	[STMT_REFLOG_APPEND] =
		"INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg)"
		" VALUES(?, COALESCE((SELECT MAX(idx)+1 FROM reflog WHERE refname = ?), 0), unhex(?), unhex(?), ?, ?, ?, ?)",
	[STMT_REFLOG_EXISTS] =
		"SELECT 1 FROM reflog WHERE refname = ? LIMIT 1",
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
		 * Keep: an object in a pack whose .keep marker is still live is spared,
		 * the relational form of files keeping a .keep'd pack's objects. git owns
		 * the .keep lifecycle and sends the live pack ids into keep_set at prune
		 * time; the membership join holds the in-flight, not-yet-reachable objects
		 * past even --prune=now (the grace alone cannot, since last_used == now is
		 * not > now).
		 *
		 * Bitmap pin: an object carrying a pack_pos (a member of the stored
		 * reachability bitmap) is spared, so the bitmap never references a deleted
		 * object (it would make git try to send a gone object). pack_pos is
		 * rebuilt each optimize over the reachable set, where a now-unreachable
		 * member loses it and becomes prunable. The two NOT EXISTS terms, the
		 * pack_pos pin, and the grace are the files retention rule as one
		 * predicate. */
		"DELETE FROM objects WHERE oid = unhex(?) AND last_used <= ? "
		"AND NOT EXISTS (SELECT 1 FROM pack_objects po WHERE po.oid = objects.oid "
		"               AND po.pack_id IN (SELECT pack_id FROM keep_set)) "
		"AND objects.pack_pos IS NULL "
		"AND NOT EXISTS (SELECT 1 FROM objects AS d WHERE d.base = objects.oid)",
	[STMT_PACK_MEMBER_ADD] =
		/* Record that an object arrived in a received pack (M:N: OR IGNORE so a
		 * re-send under the same pack is a no-op, and an object can belong to
		 * several packs). pack_id is the pack's trailing hash. */
		"INSERT OR IGNORE INTO pack_objects(oid, pack_id) VALUES(unhex(?), unhex(?))",
	[STMT_KEEP_ADD] =
		"INSERT OR IGNORE INTO keep_set(pack_id) VALUES(unhex(?))",
	[STMT_KEEP_CLEAR] =
		"DELETE FROM keep_set",
	[STMT_ABSORB_RELEASED] =
		/* Drop membership for packs whose .keep is gone (not in the live set):
		 * their objects are no longer pinned by a keep and rejoin the general
		 * store, as repack absorbs a no-longer-kept pack into the main pack. */
		"DELETE FROM pack_objects WHERE pack_id NOT IN (SELECT pack_id FROM keep_set)",
	[STMT_BITMAP_STORE] =
		"INSERT OR REPLACE INTO git_bitmap(id, bitmap) VALUES(0, ?)",
	[STMT_BITMAP_GET] =
		"SELECT bitmap FROM git_bitmap WHERE id = 0",
	[STMT_BITMAP_CLEAR] =
		"DELETE FROM git_bitmap",
	[STMT_COMMIT_BITMAP_STORE] =
		/* One stored bitmap entry per bitmapped commit, keyed by commit oid;
		 * xor_base is the chain-base commit oid (NULL = full, non-xor entry),
		 * the relational analog of an object delta's base. */
		"INSERT OR REPLACE INTO commit_bitmap(commit_oid, xor_base, flags, ewah) "
		"VALUES(unhex(?), unhex(?), ?, ?)",
	[STMT_COMMIT_BITMAP_GET] =
		/* iif(): hex(NULL) is '' in SQLite, so map a NULL base back to NULL
		 * (a full entry) rather than an empty string. */
		"SELECT iif(xor_base IS NULL, NULL, lower(hex(xor_base))), flags, ewah "
		"FROM commit_bitmap WHERE commit_oid = unhex(?)",
	[STMT_COMMIT_BITMAP_CLEAR] =
		"DELETE FROM commit_bitmap",
	[STMT_ORDER_ADD] =
		"INSERT INTO bitmap_order(position, oid) VALUES(?, unhex(?))",
	[STMT_ORDER_CLEAR] =
		"DELETE FROM bitmap_order",
	[STMT_REV_ADD] =
		"INSERT INTO bitmap_rev(bit, idx) VALUES(?, ?)",
	[STMT_REV_CLEAR] =
		"DELETE FROM bitmap_rev",
	[STMT_PACK_POS_CLEAR] =
		"UPDATE objects SET pack_pos = NULL WHERE pack_pos IS NOT NULL",
	[STMT_PACK_POS_SET] =
		/* bit/pack order != index/oid-sorted order (the sha256 two-ordering bug),
		 * so JOIN both orderings: bitmap_order is index->oid, bitmap_rev is
		 * bit->index. UPDATE...FROM the precomputed (bit, oid) pairs, O(n log n)
		 * via the bitmap_order PK, not the O(n^2) of a correlated subquery with no
		 * index on bitmap_rev.idx. */
		"UPDATE objects SET pack_pos = v.bit FROM "
		"(SELECT r.bit AS bit, o.oid AS oid FROM bitmap_rev r "
		" JOIN bitmap_order o ON o.position = r.idx) v "
		"WHERE objects.oid = v.oid",
	[STMT_POS_OF_OID] =
		/* On-demand bit<->oid (2b, files-parity orderings): objects.pack_pos IS
		 * the bit position, so the source bitmap resolves a want/have to its bit
		 * with one indexed point query instead of fetching all N orderings. NULL
		 * if the object is not in the bitmap. */
		"SELECT pack_pos FROM objects WHERE oid = unhex(?)",
	[STMT_RESULT_POS_ADD] =
		/* Stage one bit the operation touches, for the batched bit->oid resolve
		 * below. OR IGNORE so a repeated bit is a no-op. */
		"INSERT OR IGNORE INTO result_pos(bit) VALUES(?)",
	[STMT_RESULT_POS_CLEAR] =
		"DELETE FROM result_pos",
	[STMT_OIDS_AT_RESULT] =
		/* Resolve the staged bits to oids in bit (pack_pos) order (2b): git hands
		 * an operation's bit set to the helper and gets back exactly those oids in
		 * one indexed join, no orderings fetched that the operation never touches,
		 * no page size, no file. */
		"SELECT lower(hex(o.oid)) FROM objects o JOIN result_pos r"
		" ON o.pack_pos = r.bit ORDER BY o.pack_pos",
	[STMT_BITMAP_COUNT] =
		/* The bitmap's object count N: bitmapped objects have pack_pos set (a
		 * dense [0,N) permutation), so git sizes its source bitmap by this. */
		"SELECT count(*) FROM objects WHERE pack_pos IS NOT NULL",
	[STMT_CONTENT_UNSPLIT] =
		/* Restore content still parked in pack_content (under the current pack_pos)
		 * back into objects.data, but only where objects.data was emptied. The gc
		 * re-deltify ingest normally rewrites objects.data first, making this a
		 * no-op; it guards the case it did not, so the recluster never clusters
		 * from an empty data (no content can be lost). */
		"UPDATE objects SET data = (SELECT content FROM pack_content"
		"   WHERE pack_content.pack_pos = objects.pack_pos)"
		" WHERE objects.pack_pos IS NOT NULL AND length(objects.data) = 0"
		"   AND EXISTS(SELECT 1 FROM pack_content WHERE pack_content.pack_pos = objects.pack_pos)",
	[STMT_PACK_CONTENT_CLEAR] =
		"DELETE FROM pack_content",
	[STMT_PACK_CONTENT_FILL] =
		/* Cluster the reachable objects into pack_content by the new pack_pos:
		 * rows are physically ordered by pack_pos (INTEGER PK == rowid), so a
		 * reachable bit-run is a contiguous range served verbatim, like a .pack
		 * region. The framing metadata (type/size/base) is co-located with the
		 * content (copied once, here) so the reuse serve is a single PK scan with
		 * no join, O(N) like the files region copy (#113). */
		"INSERT INTO pack_content(pack_pos, type, size, base, content)"
		" SELECT pack_pos, type, size, base, data FROM objects"
		" WHERE pack_pos IS NOT NULL",
	[STMT_CONTENT_EMPTY] =
		/* One home for packed content: it now lives in pack_content; empty the
		 * objects.data copy (the read coalesces pack_content over it). */
		"UPDATE objects SET data = X'' WHERE pack_pos IS NOT NULL",
	[STMT_REUSE_STREAM] =
		/* Stream the reusable pack_pos prefix [0, ?) as pack-entry records in
		 * pack_pos order: object type name, the pack-entry size field (the delta
		 * size for a delta, the object size otherwise, objects.size is already
		 * that, the put-raw idiom), the delta base oid or '-', then the verbatim
		 * compressed content. A SINGLE PK scan of pack_content (its framing
		 * metadata is co-located, no join), the O(N) bulk analog of the files
		 * backend copying a contiguous .pack region; git-core frames each exactly
		 * as it reuses one stored delta (REF_DELTA + base for a delta, plain
		 * header otherwise). Contiguity guarantees a delta's base is also in the
		 * prefix, so the REF_DELTA base is always being sent. */
		"SELECT type, size, iif(base IS NULL, '-', lower(hex(base))), content"
		" FROM pack_content WHERE pack_pos < ? ORDER BY pack_pos",
	[STMT_OBJ_REPLACE] =
		/* Repack re-representation (put / put-raw with replace=1): overwrite
		 * an object's stored representation in place, switching it between full
		 * (base NULL, bound via unhex(NULL)) and delta (base set) as git's
		 * pack-objects re-deltifies at gc. A per-row UPSERT, not INSERT OR
		 * REPLACE: the DO UPDATE touches only type/size/data/base, so promisor
		 * and created_at survive (INSERT OR REPLACE would reset both, losing the
		 * promisor mark and the prune-grace age). The fetch path stays keep-
		 * existing (STMT_OBJ_WRITE); only repack, which owns an object's
		 * representation the way files' repack owns its packs, may replace.
		 * Shares STMT_OBJ_WRITE's param layout so both writers bind identically. */
		"INSERT INTO objects(oid, type, size, data, base, promisor) VALUES(unhex(?),?,?,?,unhex(?),?)"
		" ON CONFLICT(oid) DO UPDATE SET type=excluded.type, size=excluded.size,"
		" data=excluded.data, base=excluded.base",
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
	 * apply_pragmas when the store is created. A no-result maintenance statement,
	 * cached in the array like every other statement an operation drives, not
	 * exec'd. */
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

static unsigned char *zdecompress(const void *src, unsigned long srclen,
				  unsigned long orig) {
	unsigned char *dst = malloc(orig);
	unsigned long dstlen = orig;
	if (!dst) return NULL;
	if (uncompress(dst, &dstlen, src, srclen) != Z_OK) {
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
 *
 * Returns 0 on success, -1 on error. Caller must free *out.
 */
static int git_delta_apply(const unsigned char *base, size_t base_len,
			   const unsigned char *delta, size_t delta_len,
			   unsigned char **out, size_t *out_len)
{
	const unsigned char *dp = delta;
	const unsigned char *dend = delta + delta_len;

	/* Read source size varint. Cap the shift: a corrupt/hostile delta with an
	 * over-long varint would otherwise shift by >= the width of size_t, which is
	 * undefined behavior. A valid size_t varint is at most 10 bytes (shift 63). */
	size_t src_size = 0;
	unsigned shift = 0;
	while (dp < dend) {
		unsigned char c = *dp++;
		if (shift >= sizeof(size_t) * 8) return -1;
		src_size |= (size_t)(c & 0x7f) << shift;
		shift += 7;
		if (!(c & 0x80)) break;
	}
	if (src_size != base_len) return -1;

	/* Read target size varint (same shift cap). */
	size_t tgt_size = 0;
	shift = 0;
	while (dp < dend) {
		unsigned char c = *dp++;
		if (shift >= sizeof(size_t) * 8) return -1;
		tgt_size |= (size_t)(c & 0x7f) << shift;
		shift += 7;
		if (!(c & 0x80)) break;
	}

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
 * Tables that carry a non-trivial inline BLOB (objects.data, commit_bitmap.ewah)
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
 * (refname, idx) for ordered range reads; pack_objects and keep_set are all-key
 * (no payload, so a rowid would store the key twice plus an unused rowid); meta
 * is a tiny key/value table. The pack-shaped tables that exploit physical
 * clustering key on INTEGER PRIMARY KEY, which is the rowid.
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
	/* Pack membership for the keep mechanism (see storage_git.h). Persistent: the
	 * fetch that ingests writes it, a later gc's prune (a different process)
	 * reads it. M:N so an object re-sent by another pack is kept by either. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS pack_objects("
		"  oid BLOB NOT NULL, pack_id BLOB NOT NULL,"
		"  PRIMARY KEY(oid, pack_id)) WITHOUT ROWID;", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE INDEX IF NOT EXISTS idx_pack_objects_pack"
		"  ON pack_objects(pack_id);", 0, 0, 0);
	/* Delta-base guard index for prune: STMT_PRUNE_OBJECT rejects deleting an
	 * object still serving as another object's delta base via NOT EXISTS over
	 * objects(base). Partial (only delta rows carry a base), so it stays tiny and
	 * turns that per-candidate guard from a full WITHOUT ROWID scan into a probe. */
	sqlite3_exec(db,
		"CREATE INDEX IF NOT EXISTS idx_objects_base"
		"  ON objects(base) WHERE base IS NOT NULL;", 0, 0, 0);
	/* Per-connection scratch holding the live-.keep pack ids git sends at prune
	 * time; the prune statement joins it. TEMP: it is rebuilt every prune and
	 * must not outlive the connection. Created before the prune statement (which
	 * references it) is ever prepared. */
	sqlite3_exec(db,
		"CREATE TEMP TABLE IF NOT EXISTS keep_set("
		"  pack_id BLOB PRIMARY KEY) WITHOUT ROWID;", 0, 0, 0);
	/* Per-connection scratch: git stages an operation's bit set here, then one
	 * join to objects on pack_pos resolves them to oids in bit order (2b). TEMP,
	 * rebuilt per resolve. */
	sqlite3_exec(db,
		"CREATE TEMP TABLE IF NOT EXISTS result_pos(bit INTEGER PRIMARY KEY);", 0, 0, 0);
	/* Reachability bitmap (see storage_git.h / pack-bitmap.c source backing). At
	 * most one per store: git_bitmap.bitmap is git's EWAH type-bitmap content
	 * verbatim, the one section a relational form cannot serve as a bitmap AND.
	 * git's .idx (position -> oid) and .rev (bit -> index) arrive at optimize only
	 * to derive each object's bit (objects.pack_pos); they are TEMP capture
	 * scratch, never persisted and never served (pack_pos resolves bit<->oid on
	 * demand). prune's bitmap pin is pack_pos IS NOT NULL: a member is spared so
	 * the stored bitmap never references a deleted object; pack_pos is rebuilt
	 * each optimize, where a now-unreachable member loses it and becomes prunable. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS git_bitmap("
		"  id INTEGER PRIMARY KEY CHECK(id = 0), bitmap BLOB NOT NULL);", 0, 0, 0);
	/* Pack-shaped content store (the "pack" analogue): the reachable objects'
	 * stored bytes keyed by pack_pos. INTEGER PRIMARY KEY == the rowid, so rows
	 * are physically clustered in pack_pos order, a reachable bit-run is a
	 * contiguous range, served verbatim (the .pack-region-copy parallel). gc moves
	 * objects.data here and NULLs objects.data for packed objects, so packed
	 * content lives in exactly one place; loose (post-gc) objects keep it in
	 * objects.data. Authoritative, not a cache. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS pack_content("
		"  pack_pos INTEGER PRIMARY KEY, type TEXT NOT NULL, size INTEGER NOT NULL,"
		"  base BLOB, content BLOB NOT NULL);", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE TEMP TABLE IF NOT EXISTS bitmap_order("
		"  position INTEGER PRIMARY KEY, oid BLOB NOT NULL);", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE INDEX IF NOT EXISTS idx_objects_pack_pos"
		"  ON objects(pack_pos);", 0, 0, 0);
	sqlite3_exec(db,
		"CREATE TEMP TABLE IF NOT EXISTS bitmap_rev("
		"  bit INTEGER PRIMARY KEY, idx INTEGER NOT NULL);", 0, 0, 0);
	/* Per-commit bitmap rows (commit_oid, xor_base edge, flags, ewah): the
	 * bitmap analog of object deltas, base-by-oid. */
	sqlite3_exec(db,
		"CREATE TABLE IF NOT EXISTS commit_bitmap("
		"  commit_oid BLOB PRIMARY KEY, xor_base BLOB, flags INTEGER NOT NULL,"
		"  ewah BLOB NOT NULL);", 0, 0, 0);
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

/* ---- Pack membership (the keep mechanism) ---- */

int storage_record_pack_membership(const char *oid, const char *pack_id) {
	sqlite3_stmt *st = stmt_acquire(STMT_PACK_MEMBER_ADD);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, pack_id, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_PACK_MEMBER_ADD, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

int storage_keep_clear(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_KEEP_CLEAR);
	int rc = sqlite3_step(st);
	stmt_release(STMT_KEEP_CLEAR, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

int storage_keep_add(const char *pack_id) {
	sqlite3_stmt *st = stmt_acquire(STMT_KEEP_ADD);
	sqlite3_bind_text(st, 1, pack_id, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_KEEP_ADD, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

int storage_absorb_released_packs(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_ABSORB_RELEASED);
	int rc = sqlite3_step(st);
	stmt_release(STMT_ABSORB_RELEASED, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* ---- Reachability bitmap (the open_bitmap source backing) ---- */

int storage_store_bitmap(const void *bitmap, size_t bitmap_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_BITMAP_STORE);
	/* bind_blob64, not bind_blob: bitmap_len is size_t, and the 32-bit bind would
	 * truncate a large EWAH blob to a negative length rather than reject it. */
	int rc = sqlite3_bind_blob64(st, 1, bitmap, bitmap_len, SQLITE_STATIC);
	if (rc == SQLITE_OK) rc = sqlite3_step(st);
	stmt_release(STMT_BITMAP_STORE, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

int storage_add_bitmap_order(long long position, const char *oid) {
	sqlite3_stmt *st = stmt_acquire(STMT_ORDER_ADD);
	sqlite3_bind_int64(st, 1, (sqlite3_int64)position);
	sqlite3_bind_text(st, 2, oid, -1, SQLITE_STATIC);
	int rc = sqlite3_step(st);
	stmt_release(STMT_ORDER_ADD, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

int storage_add_bitmap_rev(long long bit, long long idx) {
	sqlite3_stmt *st = stmt_acquire(STMT_REV_ADD);
	sqlite3_bind_int64(st, 1, (sqlite3_int64)bit);
	sqlite3_bind_int64(st, 2, (sqlite3_int64)idx);
	int rc = sqlite3_step(st);
	stmt_release(STMT_REV_ADD, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

int storage_clear_bitmap(void) {
	int ok = 1;
	sqlite3_stmt *st = stmt_acquire(STMT_BITMAP_CLEAR);
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_BITMAP_CLEAR, st);
	st = stmt_acquire(STMT_ORDER_CLEAR);
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_ORDER_CLEAR, st);
	st = stmt_acquire(STMT_REV_CLEAR);
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_REV_CLEAR, st);
	return ok ? 0 : -1;
}

/*
 * Fetch the EWAH bitmap blob as a freshly malloc'd buffer the caller owns;
 * returns 0 on success, -1 when none is stored. The column blob is copied out
 * before the statement is reset (sqlite invalidates it on reset).
 */
int storage_get_bitmap(void **bitmap, size_t *bitmap_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_BITMAP_GET);
	int ret = -1;

	if (sqlite3_step(st) == SQLITE_ROW) {
		const void *b = sqlite3_column_blob(st, 0);
		int blen = sqlite3_column_bytes(st, 0);

		*bitmap = malloc(blen ? (size_t)blen : 1);
		if (!*bitmap) { stmt_release(STMT_BITMAP_GET, st); return -1; }
		memcpy(*bitmap, b, blen);
		*bitmap_len = (size_t)blen;
		ret = 0;
	}
	stmt_release(STMT_BITMAP_GET, st);
	return ret;
}

/* ---- Per-commit bitmap rows (the bitmap analog of object deltas, base-by-oid) ---- */

int storage_store_commit_bitmap(const char *commit_oid, const char *xor_base,
				 int flags, const void *ewah, size_t ewah_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_COMMIT_BITMAP_STORE);
	sqlite3_bind_text(st, 1, commit_oid, -1, SQLITE_STATIC);
	if (xor_base && *xor_base)
		sqlite3_bind_text(st, 2, xor_base, -1, SQLITE_STATIC);
	else
		sqlite3_bind_null(st, 2);	/* unhex(NULL) = NULL: a full (non-xor) entry */
	sqlite3_bind_int(st, 3, flags);
	/* bind_blob64, not bind_blob: ewah_len is size_t, avoiding the 32-bit truncation. */
	int rc = sqlite3_bind_blob64(st, 4, ewah, ewah_len, SQLITE_STATIC);
	if (rc == SQLITE_OK) rc = sqlite3_step(st);
	stmt_release(STMT_COMMIT_BITMAP_STORE, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/*
 * Fetch one commit's stored bitmap by commit oid. xor_base_out (a buffer of at
 * least STORAGE_OID_HEXSZ+1) gets the chain-base commit's hex, or "" for a full
 * entry; ewah is a freshly malloc'd buffer the caller frees. Returns 0, or -1 if
 * no entry is stored for that commit.
 */
int storage_get_commit_bitmap(const char *commit_oid, char *xor_base_out,
			      int *flags, void **ewah, size_t *ewah_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_COMMIT_BITMAP_GET);
	int ret = -1;

	sqlite3_bind_text(st, 1, commit_oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) == SQLITE_ROW) {
		const char *xb = (const char *)sqlite3_column_text(st, 0);
		const void *e = sqlite3_column_blob(st, 2);
		int elen = sqlite3_column_bytes(st, 2);

		*flags = sqlite3_column_int(st, 1);
		if (xb) snprintf(xor_base_out, STORAGE_OID_HEXSZ + 1, "%s", xb);
		else xor_base_out[0] = 0;
		*ewah = malloc(elen ? (size_t)elen : 1);
		if (!*ewah) { stmt_release(STMT_COMMIT_BITMAP_GET, st); return -1; }
		memcpy(*ewah, e, elen);
		*ewah_len = (size_t)elen;
		ret = 0;
	}
	stmt_release(STMT_COMMIT_BITMAP_GET, st);
	return ret;
}

int storage_clear_commit_bitmaps(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_COMMIT_BITMAP_CLEAR);
	int rc = sqlite3_step(st);
	stmt_release(STMT_COMMIT_BITMAP_CLEAR, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/*
 * Rebuild the pack-shaped layout from the orderings store-bitmap just stored:
 * derive object.pack_pos (the bit/pack position), then cluster the reachable
 * objects' content into pack_content by that position, so a reachable bit-run is
 * a contiguous, verbatim-servable range (the files backend's .pack region).
 *
 * pack_pos and the oid-sorted index order DIFFER (the sha256 two-ordering bug),
 * so JOIN the two orderings rather than assume them equal: bitmap_order is
 * index->oid, bitmap_rev is bit->index, so an object's bit = bitmap_rev.bit where
 * bitmap_rev.idx = bitmap_order.position for its oid. Reset pack_pos first so an
 * object dropped from the new bitmap loses its stale position; objects not in the
 * bitmap (ingested since the last gc) keep pack_pos NULL and loose content.
 *
 * Content has ONE home: packed objects live in pack_content (objects.data
 * emptied), loose objects live in objects.data; the read coalesces. pack_pos
 * churns every gc, so the old pack_content is keyed by stale positions; we
 * re-cluster from objects.data, which the gc re-deltify ingest has just rewritten
 * for every reachable object. The unsplit step first salvages any object whose
 * data that ingest did not rewrite from the old pack_content (a no-op in the
 * normal case), so a wrong assumption about the ingest's replace mode can never
 * lose content. The whole sequence runs inside the caller's store_bitmap
 * savepoint, so pack_pos-set and pack_content-row stay in lockstep (the read's
 * coalesce depends on it).
 */
int storage_recluster_pack_content(void) {
	sqlite3_stmt *st;
	int ok = 1;

	st = stmt_acquire(STMT_CONTENT_UNSPLIT);	/* salvage un-rewritten data (old pack_pos) */
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_CONTENT_UNSPLIT, st);

	st = stmt_acquire(STMT_PACK_CONTENT_CLEAR);	/* drop the stale-keyed clustered content */
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_PACK_CONTENT_CLEAR, st);

	st = stmt_acquire(STMT_PACK_POS_CLEAR);	/* reset stale (dropped from new bitmap) */
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_PACK_POS_CLEAR, st);

	st = stmt_acquire(STMT_PACK_POS_SET);	/* bitmapped objects -> their bit position */
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_PACK_POS_SET, st);

	st = stmt_acquire(STMT_PACK_CONTENT_FILL);	/* cluster reachable content by new pack_pos */
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_PACK_CONTENT_FILL, st);

	st = stmt_acquire(STMT_CONTENT_EMPTY);	/* one home: empty the now-clustered objects.data */
	if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
	stmt_release(STMT_CONTENT_EMPTY, st);

	return ok ? 0 : -1;
}

/* ---- On-demand bitmap orderings (2b: files-parity, O(touched) not O(N)) ---- */

/* The bit position of an object in the source bitmap, or -1 if it is not in the
 * bitmap. objects.pack_pos IS the bit, so this is one indexed point query (used
 * to map a want/have oid to its bit), not a fetch of all N orderings. */
int storage_pack_pos_of_oid(const char *oid) {
	sqlite3_stmt *st = stmt_acquire(STMT_POS_OF_OID);
	int pos = -1;
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) == SQLITE_ROW &&
	    sqlite3_column_type(st, 0) != SQLITE_NULL)
		pos = sqlite3_column_int(st, 0);
	stmt_release(STMT_POS_OF_OID, st);
	return pos;
}

/* Stage one bit of an operation's set for the batched resolve below. */
int storage_result_pos_add(long long bit) {
	sqlite3_stmt *st = stmt_acquire(STMT_RESULT_POS_ADD);
	sqlite3_bind_int64(st, 1, (sqlite3_int64)bit);
	int rc = sqlite3_step(st);
	stmt_release(STMT_RESULT_POS_ADD, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* Drop all staged bits (call before staging a fresh set). */
int storage_result_pos_clear(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_RESULT_POS_CLEAR);
	int rc = sqlite3_step(st);
	stmt_release(STMT_RESULT_POS_CLEAR, st);
	return rc == SQLITE_DONE ? 0 : -1;
}

/* Resolve the staged bits to oids in bit (pack_pos) order: the batched bit->oid
 * for a bitmap operation, one join over exactly the bits it touches. */
int storage_oids_at_result(storage_obj_oid_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(STMT_OIDS_AT_RESULT);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *oid = (const char *)sqlite3_column_text(st, 0);
		if (cb(oid, data)) { stmt_release(STMT_OIDS_AT_RESULT, st); return -1; }
	}
	stmt_release(STMT_OIDS_AT_RESULT, st);
	return 0;
}

/* The bitmap's object count N (objects that have a bit position). */
int storage_bitmap_count(void) {
	sqlite3_stmt *st = stmt_acquire(STMT_BITMAP_COUNT);
	int n = (sqlite3_step(st) == SQLITE_ROW) ? sqlite3_column_int(st, 0) : 0;
	stmt_release(STMT_BITMAP_COUNT, st);
	return n;
}

/*
 * Stream the reusable pack_pos prefix [0, end_pos) as pack-entry records, in
 * pack_pos order, for source-backed pack reuse (the bulk analog of the files
 * backend copying a contiguous .pack region). cb is invoked once per object with
 * its type name, the pack-entry size field, the delta base oid ("-" if none) and
 * the verbatim compressed content; a non-zero return aborts. Returns 0 on
 * success, -1 if the callback aborted.
 */
int storage_reuse_stream(long long end_pos, storage_reuse_cb cb, void *data) {
	sqlite3_stmt *st = stmt_acquire(STMT_REUSE_STREAM);
	int ret = 0;
	sqlite3_bind_int64(st, 1, end_pos);
	while (sqlite3_step(st) == SQLITE_ROW) {
		const char *type = (const char *)sqlite3_column_text(st, 0);
		long long size = sqlite3_column_int64(st, 1);
		const char *base = (const char *)sqlite3_column_text(st, 2);
		const void *content = sqlite3_column_blob(st, 3);
		int clen = sqlite3_column_bytes(st, 3);
		if (cb(type, size, base, content, clen, data)) { ret = -1; break; }
	}
	stmt_release(STMT_REUSE_STREAM, st);
	return ret;
}

/* ---- Prune: delete (or, dry-run, report) one unreachable object git identified ---- */

int storage_prune_object(const char *oid, long long expire) {
	sqlite3_stmt *st = stmt_acquire(STMT_PRUNE_OBJECT);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 2, (sqlite3_int64)expire);
	sqlite3_step(st);
	stmt_release(STMT_PRUNE_OBJECT, st);
	/*
	 * sqlite3_changes() reports whether the DELETE removed the row. The keep
	 * (membership join), bitmap pin (pack_pos IS NULL), and delta-base guard in the
	 * statement spare a kept, packed, or still-needed object; the caller's fixpoint
	 * retries a base once its dependents are gone. A dry run is the caller running
	 * this for real inside a savepoint it rolls back, so it observes exactly what a
	 * real prune would do.
	 *
	 * No separate pack_content cleanup is needed: STMT_PRUNE_OBJECT only deletes a
	 * pack_pos-NULL (loose) row, so a deleted object never had clustered content. A
	 * packed object is pinned (never deleted here); its content is cleared wholesale
	 * by the next optimize's recluster (PACK_CONTENT_CLEAR), once it drops from the
	 * bitmap and loses its pack_pos. (A per-object pack_content delete here was dead
	 * code: deleted implies pack_pos NULL, so the packed branch never fired.)
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
	sqlite3_stmt *st = stmt_acquire(STMT_VACUUM);
	int rc;

	if (!st)
		return -1;
	rc = sqlite3_step(st);
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

static int read_object_depth(const char *oid, char *out_type, size_t out_type_sz,
			     size_t *out_size, unsigned char **out_data, int depth) {
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

	if (!base_hex) {
		*out_data = zdecompress(comp, comp_len, *out_size);
		stmt_release(STMT_OBJ_READ, st);
		return *out_data ? 0 : -1;
	}

	/*
	 * Delta: copy the base oid, then inflate the stored (compressed) git-format
	 * delta before finalize/recursion (the sqlite blob is invalidated by the
	 * recursive base read). The `size` column holds the delta stream's
	 * uncompressed length, git's pack entry header idiom, which index-pack
	 * hands us for free as obj->size, so a plain known-size inflate suffices.
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
	if (read_object_depth(base_oid, base_type, sizeof(base_type), &base_size, &base_data, depth + 1) < 0) {
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
			size_t *out_size, unsigned char **out_data) {
	return read_object_depth(oid, out_type, out_type_sz, out_size, out_data, 0);
}

/*
 * Cheap size+type without materializing (or inflating, or delta-resolving) the
 * content: read_header's reason for existing. The stored `size` is the object's
 * logical size for a whole object but the DELTA stream's length for a delta (see
 * read_object_depth), so a delta still needs a full resolve to report its logical
 * size; the common whole-object case answers from the header row alone. type is
 * correct for both (a delta row records the resolved object's type). Returns 0,
 * -1 if absent. out_type is a buffer of out_type_sz.
 */
int storage_read_header(const char *oid, char *out_type, size_t out_type_sz,
			size_t *out_size) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_HEADER);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(STMT_OBJ_HEADER, st); return -1; }
	const char *type = (const char *)sqlite3_column_text(st, 0);
	size_t size = (size_t)sqlite3_column_int64(st, 1);
	int is_delta = sqlite3_column_int(st, 2);
	if (out_type && type) snprintf(out_type, out_type_sz, "%s", type);
	stmt_release(STMT_OBJ_HEADER, st);

	if (!is_delta) {
		if (out_size) *out_size = size;
		return 0;
	}
	/* Delta: the logical size only falls out of applying it. Resolve and discard
	 * the bytes (type is already set from the header row). */
	{
		char t[32];
		unsigned char *data;
		if (storage_read_object(oid, t, sizeof(t), out_size, &data) < 0)
			return -1;
		free(data);
		return 0;
	}
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
 * zlib-compressed content stream, the same bytes the files backend writes into a
 * pack), persisting them verbatim. git owns compression now: it deflates before
 * `put`, or index-pack hands the incoming pack's compressed entry straight
 * through, so the helper never (de)compresses on the write path, it is a dumb
 * byte store, exactly where the files backend saves to disk. `cdata`/`clen` are
 * the compressed bytes; `usize` is the uncompressed size (the `size` column,
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

/* ---- get-delta / delta-info: serve a stored git-format delta verbatim ---- */

int storage_get_delta(const char *oid, char *base_oid, size_t base_oid_sz,
		      unsigned char **delta, size_t *delta_len, size_t *raw_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_READ);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(STMT_OBJ_READ, st); return 0; }

	const char *base_hex = (const char *)sqlite3_column_text(st, 3);
	if (!base_hex) { stmt_release(STMT_OBJ_READ, st); return 0; }  /* resolved, not a delta */
	snprintf(base_oid, base_oid_sz, "%s", base_hex);

	/*
	 * The size column is the delta's uncompressed length (the pack-entry-header
	 * idiom); the blob is the compressed delta, returned verbatim. The caller
	 * matches raw_len against its planned delta size and reuses the compressed
	 * bytes as-is.
	 */
	*raw_len = (size_t)sqlite3_column_int64(st, 1);
	const void *data = sqlite3_column_blob(st, 2);
	int dlen = sqlite3_column_bytes(st, 2);
	unsigned char *d = malloc(dlen ? dlen : 1);
	if (!d) { stmt_release(STMT_OBJ_READ, st); return 0; }
	memcpy(d, data, dlen);
	stmt_release(STMT_OBJ_READ, st);

	*delta = d;
	*delta_len = (size_t)dlen;
	return 1;
}

int storage_delta_info(const char *oid, char *base_oid, size_t base_oid_sz, size_t *delta_len) {
	sqlite3_stmt *st = stmt_acquire(STMT_OBJ_READ);
	sqlite3_bind_text(st, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) { stmt_release(STMT_OBJ_READ, st); return 0; }

	const char *base_hex = (const char *)sqlite3_column_text(st, 3);
	if (!base_hex) { stmt_release(STMT_OBJ_READ, st); return 0; }  /* resolved, not a delta */
	snprintf(base_oid, base_oid_sz, "%s", base_hex);
	/*
	 * The delta is stored compressed, so its uncompressed length is the size
	 * column (col1), not the blob's byte length. git core matches this against
	 * its planned delta size to decide reuse, then fetches the compressed bytes
	 * via get-delta.
	 */
	*delta_len = (size_t)sqlite3_column_int64(st, 1);
	stmt_release(STMT_OBJ_READ, st);
	return 1;
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
