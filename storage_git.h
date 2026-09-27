#ifndef STORAGE_GIT_H
#define STORAGE_GIT_H

/*
 * Git object/ref storage layer, built on the dependency-free core (storage.h:
 * connection, transactions, the generic prepared-statement mechanism). This
 * layer adds zlib + our clean-room git-format delta codec and holds everything
 * git-specific: objects, refs, reflogs, the reachability bitmap, the commit-graph
 * generation cache, prune, promisor marks, freshen, and the repository object
 * format.
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
 * Create the git schema (the objects, refs, reflog, commit_graph, meta,
 * pack_bitmap and pack_content tables) on the open storage_db() connection,
 * idempotently (CREATE ... IF NOT EXISTS). Call once after
 * storage_open()/storage_open_db(). Statements are prepared lazily on first use
 * (cached in owned mode, per-call in borrowed mode) via the core storage_stmt()
 * mechanism, and finalized at storage_close().
 */
int storage_git_init(void);

int storage_remove_ref_store(void);

/*
 * How an object is stored: as a git-format delta against `base`, the delta
 * inflating to `size` bytes, or whole, with `base` empty and `size` 0.
 */
struct storage_delta {
	char base[STORAGE_OID_HEXSZ + 1];
	size_t size;
};

/*
 * Object operations. OID is hex, type is the type name; both stored verbatim.
 * Read resolves delta chains and copies the type name into out_type (a buffer
 * of out_type_sz). Write handles compression + delta. Caller frees *out_data.
 * A non-NULL `delta` receives how the object is stored.
 */
int storage_read_object(const char *oid,
			char *out_type, size_t out_type_sz,
			size_t *out_size, unsigned char **out_data,
			struct storage_delta *delta);

/* Header (type + size) without materializing the content; see the definition
 * for how a delta gives the size of its object. A non-NULL `delta` receives how
 * the object is stored. */
int storage_read_header(const char *oid, char *out_type, size_t out_type_sz,
			size_t *out_size, struct storage_delta *delta);

/*
 * replace selects keep-existing (0, the fetch/normal path: never overwrite a
 * stored object) versus repack re-representation (1: re-deltify in place via an
 * UPSERT preserving promisor + created_at). Full-object writes pass replace=1
 * only from gc/repack, where the object becomes (or stays) a full object.
 */
int storage_write_object(const char *oid, const char *type,
			  const void *data, size_t size, int replace);

/*
 * Store an object whose bytes are already git's compressed storage form, as
 * the entry of a pack holds them, persisting them verbatim (no compression):
 * git sends the entries of a packfile as they are, so the helper is a dumb byte
 * store, exactly where the files backend saves to disk. `cdata`/`clen` are the
 * compressed bytes; `usize` is the size they inflate to. Used by the helper's
 * put-raw; the in-process extension keeps storage_write_object (it holds
 * inflated objects from libgit2 and self-compresses, like a fresh loose
 * write). replace mirrors storage_write_object.
 */
int storage_put_raw(const char *oid, const char *type, const void *cdata,
		     size_t clen, size_t usize, const char *base, int replace);

/*
 * Read an object as it is stored, the form storage_put_raw takes: the
 * zlib-compressed contents of a whole object, or of a git-format delta against
 * delta->base, handed back verbatim, as git copies the entry of an object out
 * of one packfile into another. *out_size is the size the bytes inflate to,
 * *out_data (caller frees) the bytes and *out_len their length; out_type gets
 * the type of the object. Returns 0, -1 if absent.
 */
int storage_read_raw(const char *oid, char *out_type, size_t out_type_sz,
		     size_t *out_size, struct storage_delta *delta,
		     unsigned char **out_data, size_t *out_len);

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
 * The reachability bitmap of the objects, at most one per store, as git writes
 * it for a pack of them: the .bitmap, with the .idx and .rev of that pack,
 * which relate its bits to the objects (rev_len 0 when git wrote no .rev), and
 * the size the pack had. put replaces any stored before. get hands back freshly
 * malloc'd copies the caller frees, and returns 1 when a bitmap is stored, 0
 * when none is, and -1 on error. put and remove return 0, or -1 on error.
 */
int storage_put_pack_bitmap(long long pack_size,
			    const void *idx, size_t idx_len,
			    const void *rev, size_t rev_len,
			    const void *bitmap, size_t bitmap_len);
int storage_get_pack_bitmap(long long *pack_size,
			    void **idx, size_t *idx_len,
			    void **rev, size_t *rev_len,
			    void **bitmap, size_t *bitmap_len);
int storage_remove_pack_bitmap(void);

/*
 * Prune one git-identified unreachable object stored at or before expire. An
 * object is spared when last_used > expire (the grace window) or when it is
 * still the delta base of a retained object (the readability invariant git
 * core's reads rely on); these compose as the files retention rule. Returns 1
 * if the DELETE removed the object, 0 otherwise (kept when still a base; the
 * caller's fixpoint retries it).
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

/* Maintenance, driven by the optimize/verify operations: incremental_vacuum
 * reclaims the free pages left by object/ref churn to the OS in place (needs
 * auto_vacuum = INCREMENTAL); integrity_check (PRAGMA) reports whether the store
 * is sound. Both return 0 on success, -1 on failure. */
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
/* Create an empty reflog for refname, unless it has one. */
int storage_reflog_create(const char *refname);
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
