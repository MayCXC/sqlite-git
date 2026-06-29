#ifndef STORAGE_GIT_H
#define STORAGE_GIT_H

/*
 * Git object/ref storage layer, built on the dependency-free core (storage.h:
 * connection, transactions, the generic prepared-statement mechanism). This
 * layer adds zlib + our clean-room git-format delta codec and holds everything
 * git-specific: objects, refs, reflogs, the reachability bitmap, the commit-graph
 * generation cache, pack membership, prune, promisor marks, freshen, and the
 * repository object format.
 *
 * Object ids cross this API as the wire's own hex strings (the oid as hex,
 * "95d0...", sha1 = 40 / sha256 = 64 chars) and types as their git names
 * ("commit"/"tree"/"blob"/"tag"). Rows key the oid as a raw BLOB, with sqlite's
 * built-in unhex()/hex() doing the hex<->blob conversion in SQL, so this layer
 * interprets neither and links NO libgit2 and NO git source. The helper passes
 * git's wire strings straight through; the extension converts to/from git_oid /
 * git_object_t at its libgit2 boundary (reusing git_oid_tostr /
 * git_object_type2string, never a reimplemented codec).
 *
 * Function names keep the bare storage_* prefix (this is the primary storage
 * consumer); the file name marks the layer. The LFS layer (storage_git_lfs.h)
 * does NOT include this header, so it cannot reach these symbols, and the LFS
 * extension does not link this object file: that, plus header placement, is what
 * enforces the layering, not the names.
 */

#include "storage.h"

#define STORAGE_OID_HEXSZ 64  /* sha256 hex; sha1 is 40 */

/*
 * Create the git schema (objects/refs/reflog/commit_graph/meta/pack tables +
 * the reachability-bitmap and pack-content tables + the per-connection TEMP
 * scratch) on the open storage_db() connection, idempotently (CREATE ... IF NOT
 * EXISTS). Call once after storage_open()/storage_open_db(). Statements are
 * prepared lazily on first use (cached in owned mode, per-call in borrowed mode)
 * via the core storage_stmt() mechanism, and finalized at storage_close().
 */
int storage_git_init(void);

int storage_remove_ref_store(void);

/*
 * Object operations. OID is hex, type is the type name; both stored verbatim.
 * Read resolves delta chains and copies the type name into out_type (a buffer
 * of out_type_sz). Write handles compression + delta. Caller frees *out_data.
 */
int storage_read_object(const char *oid,
			char *out_type, size_t out_type_sz,
			size_t *out_size, unsigned char **out_data);

/* Header (type + logical size) without materializing the content; see the
 * definition for the whole-object fast path vs delta fallback. */
int storage_read_header(const char *oid, char *out_type, size_t out_type_sz,
			size_t *out_size);

/*
 * replace selects keep-existing (0, the fetch/normal path: never overwrite a
 * stored object) versus repack re-representation (1: re-deltify in place via an
 * UPSERT preserving promisor + created_at). Full-object writes pass replace=1
 * only from gc/repack, where the object becomes (or stays) a full object.
 */
int storage_write_object(const char *oid, const char *type,
			  const void *data, size_t size, int replace);

/*
 * Store an object whose bytes are already git's compressed storage form,
 * persisting them verbatim (no compression). git deflates before `put` (or
 * index-pack passes the incoming pack's compressed entry through), so the helper
 * is a dumb byte store, exactly where the files backend saves to disk. `cdata`/
 * `clen` are the compressed bytes; `usize` is the uncompressed size. Used by the
 * helper's wire put; the in-process extension keeps storage_write_object (it
 * holds inflated objects from libgit2 and self-compresses, like a fresh loose
 * write). replace mirrors storage_write_object.
 */
int storage_put_raw(const char *oid, const char *type, const void *cdata,
		     size_t clen, size_t usize, const char *base, int replace);

/*
 * get-delta / delta-info: serve a stored git-format delta for reuse, verbatim.
 * A delta row holds the COMPRESSED git-format delta as its blob (stored by
 * storage_put_raw with a base), the base oid, and the delta's UNCOMPRESSED length
 * in the size column. git core created the delta (delta.h) and owns its
 * semantics; there is no separate delta-write entry point, because put-raw stores
 * both whole objects and deltas (a delta is simply base != NULL), the two shapes
 * a pack entry takes. The store never inflates a delta to serve it: it is handed
 * back compressed for verbatim reuse, the relational analog of git copying a
 * reusable delta straight out of one pack into another.
 *
 * Returns 1 for a delta row and: copies the base oid hex into base_oid (a buffer
 * of base_oid_sz); points *delta (caller frees) at the stored COMPRESSED delta
 * with *delta_len its compressed byte length; sets *raw_len to the uncompressed
 * delta length (the size column). Returns 0 if the object is stored resolved or
 * absent (caller falls back to get).
 */
int storage_get_delta(const char *oid, char *base_oid, size_t base_oid_sz,
		      unsigned char **delta, size_t *delta_len, size_t *raw_len);
/*
 * Like storage_get_delta but reports only the delta header (base oid hex + the
 * uncompressed delta length) without copying the bytes. Used to decide delta
 * reuse on a send pack (the "info" verb). Returns 1 for a delta row, 0 otherwise.
 */
int storage_delta_info(const char *oid, char *base_oid, size_t base_oid_sz,
		       size_t *delta_len);

int storage_object_exists(const char *oid);

/*
 * Ref operations. OIDs stored as hex TEXT internally. storage_ref_read copies
 * the oid hex into oid (a buffer of oid_sz) for a direct ref.
 */
int storage_ref_read(const char *refname, char *oid, size_t oid_sz,
		     char *symref, size_t symref_len);
int storage_ref_write(const char *refname, const char *oid, const char *symref);
int storage_ref_delete(const char *refname);

/* Callback for ref iteration. Return non-zero to stop. */
typedef int (*storage_ref_cb)(const char *refname, const char *oid,
			      const char *symref, void *data);
int storage_ref_list(const char *prefix, storage_ref_cb cb, void *data);

/* Object listing callback. */
typedef int (*storage_obj_cb)(const char *oid, const char *type,
			      size_t size, void *data);
int storage_obj_list(storage_obj_cb cb, void *data);

/* OID-only iteration callback (used by storage_oids_at_result). */
typedef int (*storage_obj_oid_cb)(const char *oid, void *data);

/* Enumerate objects, optionally promisor-only and/or scoped to a hex oid
 * prefix (NULL = no prefix). A prefix is matched server-side so only matching
 * ids cross the IPC boundary (abbreviation/disambiguation). */
int storage_obj_list_filtered(int promisor_only, const char *prefix,
			      storage_obj_cb cb, void *data);

/* Promisor object metadata (git marks objects it fetched from a promisor
 * remote; the filtered object list above reads the column back). The caller
 * brackets the transaction (cmd_promisor marks the whole set in one savepoint),
 * like storage_put_commit_graph. */
int storage_mark_promisor(const char *oid);

/*
 * Pack membership (the keep mechanism). An object that arrived in a received
 * pack is recorded here against that pack's id (its trailing hash, matching the
 * pack-<hash>.keep filename git's transport lifecycle manages). It is M:N: a
 * later fetch that re-sends an object records it under that pack too, so an
 * object kept by either fetch survives. This is the relational form of files
 * keeping a .keep'd pack's objects; git drives it (it owns the .keep files) and
 * the helper just stores the membership.
 */
int storage_record_pack_membership(const char *oid, const char *pack_id);

/*
 * Per-prune scratch holding the pack ids whose .keep marker is currently live,
 * which git sends at prune time. storage_keep_clear() empties it,
 * storage_keep_add() adds one, and storage_prune_object() spares any object in a
 * listed pack. storage_absorb_released_packs() drops membership for packs no
 * longer in the set (their .keep is gone), the relational image of repack
 * absorbing a no-longer-kept pack into the main store.
 */
int storage_keep_clear(void);
int storage_keep_add(const char *pack_id);
int storage_absorb_released_packs(void);

/*
 * Reachability bitmap storage (the source backing for git's open_bitmap). At
 * most one per store: storage_store_bitmap saves git's EWAH .bitmap blob,
 * storage_add_bitmap_order records the .idx (position -> oid in index order) and
 * storage_add_bitmap_rev the .rev (bit -> index position); together they
 * interpret the bits, the same two orderings a pack persists. git produces all
 * three at optimize. clear drops them (e.g. an optimize that produced none). get
 * returns the blob (freshly malloc'd, caller frees) or -1 when none is stored.
 * The order's oids are also pinned by prune so the stored bitmap never references
 * a deleted object (rebuilt each optimize, where unreachable members drop out).
 */
int storage_store_bitmap(const void *bitmap, size_t bitmap_len);

/*
 * Per-commit bitmap rows, the bitmap analog of object deltas: each bitmapped
 * commit is one row keyed by its oid, with xor_base the chain-base commit oid
 * (NULL/"" for a full, non-xor entry) and ewah the verbatim EWAH bytes. Stored
 * and served by commit key so a fetch recovers only the relevant commits.
 */
int storage_store_commit_bitmap(const char *commit_oid, const char *xor_base,
				 int flags, const void *ewah, size_t ewah_len);
int storage_get_commit_bitmap(const char *commit_oid, char *xor_base_out,
			      int *flags, void **ewah, size_t *ewah_len);
int storage_clear_commit_bitmaps(void);
/* Rebuild the pack-shaped layout (object.pack_pos + clustered pack_content) from
 * the stored orderings; call after store-bitmap has loaded bitmap_order +
 * bitmap_rev, inside its savepoint. */
int storage_recluster_pack_content(void);
int storage_add_bitmap_order(long long position, const char *oid);
int storage_add_bitmap_rev(long long bit, long long idx);
int storage_clear_bitmap(void);
int storage_get_bitmap(void **bitmap, size_t *bitmap_len);

/* On-demand bitmap orderings (2b): bit<->oid via objects.pack_pos, O(touched)
 * not O(N). storage_pack_pos_of_oid maps an oid to its bit (-1 if not in the
 * bitmap, for a want/have). result_pos_add/clear stage an operation's bit set and
 * storage_oids_at_result resolves them to oids in bit (pack_pos) order, the
 * batched bit->oid for a serve, exactly the bits the operation touches. */
int storage_pack_pos_of_oid(const char *oid);
int storage_result_pos_add(long long bit);
int storage_result_pos_clear(void);
int storage_oids_at_result(storage_obj_oid_cb cb, void *data);
int storage_bitmap_count(void);

/* Stream the reusable pack_pos prefix [0, end_pos) for source-backed pack reuse:
 * cb gets each object's type name, pack-entry size, delta base oid ("-" if none)
 * and verbatim compressed content, in pack_pos order. The bulk analog of files'
 * contiguous .pack-region copy (#113). Returns 0, or -1 if cb aborted. */
typedef int (*storage_reuse_cb)(const char *type, long long size,
				const char *base_hex, const void *content,
				int clen, void *data);
int storage_reuse_stream(long long end_pos, storage_reuse_cb cb, void *data);

/*
 * Prune one git-identified unreachable object stored at or before expire. An
 * object is spared when last_used > expire (the grace window), when it is in a
 * pack whose .keep is live (the keep_set membership join), or when it is still
 * the delta base of a retained object (the readability invariant git core's pack
 * reads rely on); these compose as the files retention rule. Returns 1 if the
 * DELETE removed the object, 0 otherwise (kept when still a base; the caller's
 * fixpoint retries it).
 */
int storage_prune_object(const char *oid, long long expire);

/*
 * gc-auto heuristic: storage_optimize_churn() returns the number of objects
 * added since the last optimize (git compares it to gc.auto to decide whether to
 * run gc); storage_optimize_done() stamps the optimize watermark so the churn
 * resets. The helper reports the count; git owns the threshold, as files does.
 */
long long storage_optimize_churn(void);
int storage_optimize_done(void);

/* Maintenance, driven by the optimize/verify operations: VACUUM reclaims free
 * pages left by object/ref churn; integrity_check (PRAGMA) reports whether the
 * store is sound. Both return 0 on success, -1 on failure. */
int storage_vacuum(void);
int storage_integrity_check(void);

/*
 * Repository object format (hash algorithm): 0 = sha1 (default), 1 = sha256.
 * Persisted as a meta row by git0_init and read wherever the extension hashes
 * objects. The store keeps it as an opaque int (the extension maps it to
 * libgit2's oid_type); the helper never reads it, treating oids as opaque hex.
 */
int storage_object_format(void);
int storage_set_object_format(int fmt);

/*
 * Freshen: bump an object's last_used to now so prune keeps it within the grace
 * window (the relational analog of files utime-ing a loose object). Returns 1 if
 * the object exists (was freshened), 0 otherwise. A duplicate write freshens
 * automatically; this is the explicit-freshen path (the "freshen" verb).
 */
int storage_freshen(const char *oid);

/* Reflog operations */
int storage_reflog_exists(const char *refname);
int storage_reflog_delete(const char *refname);

/* Reflog read callback (old/new oids are hex). */
typedef int (*storage_reflog_cb)(const char *old_oid, const char *new_oid,
				 const char *committer, long long timestamp,
				 int tz, const char *msg, void *data);
int storage_reflog_read(const char *refname, storage_reflog_cb cb, void *data);

int storage_reflog_read_reverse(const char *refname, storage_reflog_cb cb, void *data);

/* Reflog name listing callback. */
typedef int (*storage_reflog_name_cb)(const char *refname, void *data);
int storage_reflog_list(storage_reflog_name_cb cb, void *data);

int storage_reflog_append(const char *refname, const char *old_oid,
			   const char *new_oid, const char *committer,
			   long long timestamp, int tz, const char *msg);

/* Carry a ref's reflog to a new name on rename (delete_old) or copy. */
int storage_reflog_copy(const char *oldref, const char *newref, int delete_old);

/* Commit graph: generation number for a commit (read path; the graph is
 * populated git-side). */
long long storage_commit_generation(const char *oid);

/*
 * Commit-graph generation write path: git computes the generation number at
 * commit-graph-write time and stores it here (idempotent), so the extension's
 * git0_generation(oid) SQL function returns real numbers (libgit2 commit-graph
 * parity). git keeps its own commit-graph file for its walks; this is only the
 * SQL-queryable generation.
 */
int storage_put_commit_graph(const char *oid, long long generation);
/* Drop all stored generations (cleared then repopulated at the write seam). */
int storage_clear_commit_graph(void);

#endif /* STORAGE_GIT_H */
