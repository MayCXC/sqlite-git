/*
 * git-local-sqlite: local helper protocol handler.
 *
 * A pure pass-through over storage.h. git owns every hash and object type, so
 * the helper relays git's wire strings unchanged: the oid as hex (40 = sha1,
 * 64 = sha256) and the type as its name ("commit"/"tree"/"blob"/"tag"). It
 * never parses, formats, compares, or hashes an oid, links NO libgit2, and
 * carries no notion of which hash is in use; storage keys on the hex via
 * sqlite's unhex() (length -> algo), so one build serves sha1 and sha256
 * repositories alike: oids stay opaque strings, git alone owning hash
 * semantics. All storage goes through storage.h.
 */
#define _POSIX_C_SOURCE 200809L	/* getline() */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include "storage_git.h"

/*
 * Read one LF-terminated line from stdin into a growing, reusable buffer (no
 * fixed cap, so an arbitrarily long protocol line, e.g. a long reflog message,
 * is never split or truncated the way a fixed fgets() buffer would). Strips the
 * trailing LF; returns the line length, or -1 at EOF. Each caller passes its own
 * function-static buffer, so a command line and a handler's batch lines occupy
 * separate buffers and never clobber each other; the buffer stays reachable via
 * the static (reused across calls), so it is not leaked per call.
 */
static ssize_t read_line(char **buf, size_t *cap)
{
	ssize_t n = getline(buf, cap, stdin);
	if (n < 0)
		return -1;
	if (n > 0 && (*buf)[n - 1] == '\n')
		(*buf)[--n] = '\0';
	return n;
}

/*
 * Oid hex buffers are STORAGE_OID_HEXSZ + 1. The sscanf field widths below are
 * written as the literal %64s (a width must be a string-literal token, not a
 * macro expression), so pin the coupling: if the longest oid hex ever changes,
 * the formats must change with it.
 */
_Static_assert(STORAGE_OID_HEXSZ == 64, "sscanf %64s widths assume sha256 hex");

static void cmd_capabilities(void) {
	/*
	 * Advertise the protocol of gitlocal-helpers(7), for objects and refs
	 * alike. Before "optimize", git re-deltifies the store by running
	 * pack-objects and storing the result through "put-raw" with its replace
	 * argument set, which "replace" advertises (overwrite how an object is
	 * stored rather than keep it); "optimize" then reclaims the pages freed
	 * (incremental_vacuum). "get-raw" hands an object back as it is stored,
	 * which pack-objects writes into a pack as it is. "bitmap" stores the
	 * reachability bitmap git writes at gc ("put-bitmap"). "verify" runs a
	 * database integrity check for "git fsck". "graph" (store-commit-graph)
	 * carries the git-computed generation number per commit so
	 * git0_generation() reads it from the table (tree/parents/date stay in the
	 * commit objects, so the generation is the only datum stored).
	 */
	printf("get\ninfo\nput\nput-raw\nput-stream\nget-raw\nreplace\n"
	       "have\nfreshen\nlist-objects\nodb-transaction\n"
	       "read\nlist\ntransaction\ncreate\nremove\n"
	       "reflog\nreflog-read-reverse\n"
	       "refresh\npromisor\noptimize\noptimize-required\nverify\nprune\n"
	       "bitmap\ngraph\n\n");
	fflush(stdout);
}

/* ---- ODB commands ---- */

/*
 * The first line of a "get" or "info" reply: the resolved type and size, and
 * for an object stored as a git-format delta its base and the size of the
 * delta, which git shows as its delta base and uses to send the delta as it is
 * stored, fetching it with "get-raw" (gitlocal-helpers(7) "get").
 */
static void print_object_header(const char *type, size_t size,
				const struct storage_delta *delta) {
	if (delta->base[0])
		printf("%s %zu %s %zu\n", type, size, delta->base, delta->size);
	else
		printf("%s %zu\n", type, size);
}

static void cmd_info(const char *hex) {
	char type[32]; size_t size; struct storage_delta delta;
	if (storage_read_header(hex, type, sizeof(type), &size, &delta) < 0)
		printf("missing\n");
	else
		print_object_header(type, size, &delta);
	fflush(stdout);
}

/*
 * "get <oid>": reply with the header then the resolved object bytes. The helper
 * resolves locally (inflate + delta apply via storage_read_object), exactly as
 * the files backend resolves a pack object in-process and hands back a finished
 * object; "get-raw" serves the object as stored for pack-objects to reuse.
 */
static void cmd_get(const char *hex) {
	char type[32]; size_t size; unsigned char *data; struct storage_delta delta;
	if (storage_read_object(hex, type, sizeof(type), &size, &data, &delta) < 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	print_object_header(type, size, &delta);
	fflush(stdout);
	if (size > 0) fwrite(data, 1, size, stdout);
	fflush(stdout);
	free(data);
}

/*
 * "put <oid> <type> <size> [<replace>]" + payload: a locally-created object in
 * inflated form. The helper compresses it on store (storage_write_object),
 * exactly as the files backend deflates a loose object on write. Optional
 * trailing <replace> mirrors repack overwrite (0 or absent = keep-existing).
 */
static void cmd_put(const char *args) {
	char hex[STORAGE_OID_HEXSZ + 1], type_str[32];
	unsigned long size;
	int replace = 0;
	if (sscanf(args, "%64s %31s %lu %d", hex, type_str, &size, &replace) < 3) return;

	/*
	 * Always consume the announced payload so the protocol stays in sync,
	 * then ack or report failure. Never return mid-object: a failure (e.g. a
	 * malloc that could not size the payload) must not desync the stream and
	 * leave the caller hanging for an ack.
	 */
	unsigned char *data = malloc(size ? size : 1);
	size_t got = 0;
	/* Drain the announced payload even if malloc failed, so the stream stays in sync. */
	while (got < size) {
		unsigned char scratch[8192];
		unsigned char *dst = data ? data + got : scratch;
		size_t want = size - got;
		if (!data && want > sizeof(scratch))
			want = sizeof(scratch);
		size_t n = fread(dst, 1, want, stdin);
		if (n == 0) break;
		got += n;
	}

	if (data && storage_write_object(hex, type_str, data, got, replace) == 0) {
		printf("%s\n", hex);
	} else {
		/* A failed store (OOM draining, or a SQLite write/commit error) must
		 * report error, not echo the oid: git advances its state over an acked
		 * write, so a false ack here is silent data loss across the IPC boundary. */
		printf("error cannot store object %s\n", hex);
	}
	free(data);
	fflush(stdout);
}

/*
 * "put-raw <oid> <type> <usize> <base|0> <clen> [<replace>]" + clen bytes: git
 * hands an object as the entry of a packfile holds it and we persist it
 * verbatim, as the files backend keeps the compressed entries of a pack.
 * base "0" = a whole object (compressed content); a base oid = a compressed
 * git-format delta against it. usize is the size the bytes inflate to (the
 * pack entry-header size), clen the compressed length. The helper never
 * (de)compresses here: git sends the entries of the packs it moves into the
 * helper, those a fetch or push received and those pack-objects writes at gc.
 * Optional <replace> for gc's re-representation.
 */
static void cmd_put_raw(const char *args) {
	char hex[STORAGE_OID_HEXSZ + 1], type_str[32], base[STORAGE_OID_HEXSZ + 1];
	unsigned long usize, clen;
	int replace = 0;
	if (sscanf(args, "%64s %31s %lu %64s %lu %d",
		   hex, type_str, &usize, base, &clen, &replace) < 5)
		return;

	unsigned char *data = malloc(clen ? clen : 1);
	size_t got = 0;
	/* Drain the announced compressed payload even if malloc failed, so the stream stays in sync. */
	while (got < clen) {
		unsigned char scratch[8192];
		unsigned char *dst = data ? data + got : scratch;
		size_t want = clen - got;
		if (!data && want > sizeof(scratch))
			want = sizeof(scratch);
		size_t n = fread(dst, 1, want, stdin);
		if (n == 0) break;
		got += n;
	}

	int stored = 0;
	if (data) {
		/* base "0" is the whole-object sentinel (no real oid is "0"). */
		const char *base_arg = (base[0] == '0' && base[1] == '\0') ? NULL : base;
		stored = (storage_put_raw(hex, type_str, data, got, usize, base_arg, replace) == 0);
	}
	if (stored) {
		printf("%s\n", hex);
	} else {
		/* Failed verbatim store: report error, never echo the oid (a false ack
		 * lets git advance over an unpersisted object = silent data loss). */
		printf("error cannot store object %s\n", hex);
	}
	free(data);
	fflush(stdout);
}

/*
 * "get-raw <oid>": reply "<type> <size> <delta-base> <compressed-size>" and the
 * compressed bytes of the object as it is stored, as "put-raw" takes them: the
 * contents of a whole object, with a <delta-base> of "0", or a git-format delta
 * against <delta-base>, <size> being the size of what the bytes inflate to.
 * pack-objects writes the bytes into a pack as they are, as the files backend
 * copies the entry of an object out of one pack into another
 * (gitlocal-helpers(7) "get-raw").
 */
static void cmd_get_raw(const char *hex) {
	char type[32]; size_t size, len; unsigned char *data; struct storage_delta delta;
	if (storage_read_raw(hex, type, sizeof(type), &size, &delta, &data, &len) < 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	printf("%s %zu %s %zu\n", type, size, delta.base[0] ? delta.base : "0", len);
	if (len > 0) fwrite(data, 1, len, stdout);
	free(data);
	fflush(stdout);
}

static void cmd_have(const char *hex) {
	printf(storage_object_exists(hex) ? "true\n" : "false\n"); fflush(stdout);
}

/*
 * "freshen <oid>": bump the object's last_used so prune keeps it within the
 * grace window (git's odb freshen_object: protect a recently-referenced object
 * from gc even when unreachable). Replies "true" if it existed (was freshened),
 * "false" otherwise, mirroring "have".
 */
static void cmd_freshen(const char *hex) {
	printf(storage_freshen(hex) ? "true\n" : "false\n"); fflush(stdout);
}

static int print_obj(const char *oid, const char *type, size_t size, void *data) {
	(void)data;
	printf("%s %s %zu\n", oid, type, size);
	return 0;
}

static void cmd_list_objects(const char *args) {
	/*
	 * Grammar: list-objects [--promisor-only] [<hexprefix>]. A flag begins
	 * with "--", and one not known is ignored; a bare token is a hex oid prefix
	 * that scopes the enumeration to matching ids server-side
	 * (abbreviation/disambiguation), rather than streaming the whole table for
	 * git to filter. git owns the input, so a non-hex prefix is treated as
	 * absent (full enumeration), never trusted into the range.
	 */
	int promisor_only = 0;
	char prefix[STORAGE_OID_HEXSZ + 1];
	prefix[0] = '\0';
	for (const char *p = args; p && *p; ) {
		while (*p == ' ') p++;
		if (!*p) break;
		const char *end = p;
		while (*end && *end != ' ') end++;
		size_t len = (size_t)(end - p);
		if (len >= 2 && p[0] == '-' && p[1] == '-') {
			if (len == strlen("--promisor-only") &&
			    !strncmp(p, "--promisor-only", len))
				promisor_only = 1;
		} else if (len && len <= STORAGE_OID_HEXSZ) {
			size_t i = 0;
			while (i < len && isxdigit((unsigned char)p[i]))
				i++;
			if (i == len) snprintf(prefix, sizeof(prefix), "%.*s", (int)len, p);
		}
		p = end;
	}
	storage_obj_list_filtered(promisor_only, prefix[0] ? prefix : NULL,
				  print_obj, NULL);
	printf("\n"); fflush(stdout);
}

/*
 * "promisor": read OID lines until a blank line, mark each as
 * a promisor object, then ack once. Matches helper_mark_promisor() in the git
 * core backend (which sends the OID list, a blank terminator, and reads "ok").
 */
static void cmd_promisor(void) {
	static char *line = NULL;
	static size_t cap = 0;
	int failed = 0;
	/*
	 * Mark the whole set in one transaction (mirrors cmd_store_commit_graph):
	 * storage_mark_promisor is unwrapped, so without this each mark would be its
	 * own write txn (O(N) on the partial-clone path). Drain to the blank
	 * terminator even on failure so the stream stays in sync; report once.
	 */
	if (storage_savepoint("promisor")) {
		while (read_line(&line, &cap) >= 0) { if (!strlen(line)) break; }
		printf("error cannot mark promisor objects\n"); fflush(stdout); return;
	}
	while (read_line(&line, &cap) >= 0) {
		size_t len = strlen(line);
		if (!len) break;
		if (storage_mark_promisor(line) != 0) failed = 1;
	}
	if (failed) {
		storage_rollback_to("promisor");
		printf("error cannot mark promisor objects\n"); fflush(stdout); return;
	}
	if (storage_release("promisor")) {
		printf("error cannot mark promisor objects\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

/*
 * A3 maintenance: "optimize" reclaims, in place (incremental_vacuum), the
 * pages freed by object/ref churn. git sends it at gc after storing the
 * objects again as its delta search found best ("put-raw" with replace), so
 * this reclaims the space the re-representation frees, once the clustered
 * content no object points at is dropped. It then stamps the optimize
 * watermark so the gc-auto churn count (see "optimize-required") resets to ~0.
 */
static void cmd_optimize(void) {
	if (storage_vacuum() != 0) {
		printf("error optimize failed\n");
		fflush(stdout);
		return;
	}
	if (storage_optimize_done() != 0) {
		printf("error optimize failed\n");
		fflush(stdout);
		return;
	}
	printf("ok\n");
	fflush(stdout);
}

/*
 * "optimize-required" reports the gc-auto heuristic input: the number of objects
 * added since the last optimize (objects newer than the 'last_optimize_at'
 * watermark), the relational analog of files' loose-object count. git compares
 * it against gc.auto to decide whether "git gc --auto" should run; the helper
 * does not know the threshold, it just reports the count (git owns the policy).
 */
static void cmd_optimize_required(void) {
	printf("%lld\n", storage_optimize_churn());
	fflush(stdout);
}

/*
 * "store-commit-graph" + "<commit_hex> <generation>" lines,
 * blank-terminated: git computed these generations while writing its commit-graph;
 * we store them (clearing the prior set first, so stale commits drop out) and do
 * NOT write a commit-graph file. Generation is the only datum not derivable from
 * the commit objects (tree/parents/date are parsed from the commits on access),
 * so it is all we keep; git0_generation(oid) and the lazy commit-graph
 * reconstruction read it. Atomic clear-then-populate, as put-bitmap replaces.
 */
static void cmd_store_commit_graph(void) {
	static char *line = NULL;
	static size_t cap = 0;

	if (storage_savepoint("store_commit_graph")) {
		printf("error cannot store commit graph\n"); fflush(stdout); return;
	}
	int ok = (storage_clear_commit_graph() == 0);
	while (read_line(&line, &cap) >= 0) {
		char commit[STORAGE_OID_HEXSZ + 1];
		long long generation = 0;
		size_t len = strlen(line);
		if (!len) break;	/* blank terminator */
		/* 64 == STORAGE_OID_HEXSZ (sha256 hex); sha1 oids parse short of it. */
		if (sscanf(line, "%64s %lld", commit, &generation) == 2 &&
		    storage_put_commit_graph(commit, generation) != 0)
			ok = 0;
	}
	if (!ok) {
		storage_rollback_to("store_commit_graph");
		printf("error cannot store commit graph\n"); fflush(stdout); return;
	}
	if (storage_release("store_commit_graph")) {
		printf("error cannot store commit graph\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

/*
 * "commit-generation <commit_hex>" -> "<generation>" or "none": git faults a
 * commit's stored generation on demand during a walk (the lazy commit-graph
 * read), so generation cutoffs work without a commit-graph file. O(1) lookup.
 */
static void cmd_commit_generation(const char *args) {
	char commit[STORAGE_OID_HEXSZ + 1];
	long long gen;

	if (sscanf(args, "%64s", commit) != 1) { printf("none\n"); fflush(stdout); return; }
	gen = storage_commit_generation(commit);
	if (gen < 0) printf("none\n");
	else printf("%lld\n", gen);
	fflush(stdout);
}

/*
 * "prune [--dry-run] [<expire>]" is the helper's half of "git prune": git
 * computes reachability and sends the unreachable object ids to delete, one per
 * line, blank-terminated. We delete a candidate last written at or before
 * <expire> unless it is still a delta base (the predicate in
 * STMT_PRUNE_OBJECT). We report back the oids we pruned (one per line,
 * terminated by "ok") so git can show exactly what "git prune" removed; with
 * --dry-run we report what would be pruned without deleting. git owns
 * reachability (refs + reflogs + index); the helper just executes the
 * deletions, in one savepoint.
 *
 * We drain the list before writing any reply: git sends it in full before it
 * begins reading our response, so streaming mid-read could deadlock once both
 * pipe buffers fill.
 */
static void cmd_prune(const char *arg) {
	int dry_run = 0;
	long long expire = LLONG_MAX;
	static char *line = NULL;
	static size_t cap = 0;
	char (*cand)[STORAGE_OID_HEXSZ + 1] = NULL;
	char *pruned = NULL;
	size_t n = 0, alloc = 0, i;
	int oom = 0, progress;

	/* "prune [--dry-run] [<expire>]": without an expiry, whenever written. */
	if (!strncmp(arg, "--dry-run", 9) && (arg[9] == ' ' || !arg[9])) {
		dry_run = 1;
		arg += 9;
		while (*arg == ' ')
			arg++;
	}
	if (*arg)
		expire = strtoll(arg, NULL, 10);

	/* The candidate oids (blank-terminated), drained in full even when they
	 * cannot all be held, so that the protocol stays in sync. */
	while (read_line(&line, &cap) >= 0) {
		size_t len = strlen(line);
		if (!len) break;
		/* A prune candidate is an oid; ignore any line too long to be one. A
		 * truncated copy could otherwise unhex to a shorter oid and alias a
		 * different object; git only ever sends 40/64-char oids here. */
		if (len > STORAGE_OID_HEXSZ || oom) continue;
		if (n == alloc) {
			size_t na = alloc ? alloc * 2 : 64;
			char (*grown)[STORAGE_OID_HEXSZ + 1] = realloc(cand, na * sizeof(*cand));
			if (!grown) { oom = 1; continue; }
			cand = grown; alloc = na;
		}
		snprintf(cand[n++], STORAGE_OID_HEXSZ + 1, "%.*s", (int)STORAGE_OID_HEXSZ, line);
	}
	if (!oom && !(pruned = calloc(n ? n : 1, 1)))
		oom = 1;
	if (oom) {
		printf("error out of memory\n");
		goto out;
	}

	/*
	 * Prune to a fixpoint inside one savepoint. Each pass deletes only
	 * candidates that are not the delta base of any object still in the store
	 * (the predicate in STMT_PRUNE_OBJECT), so a surviving object is never
	 * orphaned by losing its base. Deleting a leaf frees its base for the next
	 * pass, so an unreachable delta chain drains fully; an already-deleted
	 * candidate reports 0 rows. A dry run runs the real deletes inside the
	 * savepoint and rolls them back, reporting exactly what a real prune would
	 * remove. The oids are named only once the deletes are committed, as git
	 * takes each one named as removed.
	 */
	if (storage_savepoint("prune")) {
		printf("error cannot open a transaction to prune in\n");
		goto out;
	}
	do {
		progress = 0;
		for (i = 0; i < n; i++) {
			if (storage_prune_object(cand[i], expire)) {
				pruned[i] = 1;
				progress = 1;
			}
		}
	} while (progress);
	if (dry_run)
		storage_rollback_to("prune");
	else if (storage_release("prune")) {
		printf("error cannot commit the prune\n");
		goto out;
	}
	for (i = 0; i < n; i++)
		if (pruned[i])
			printf("%s\n", cand[i]);
	printf("ok\n");
out:
	fflush(stdout);
	free(pruned);
	free(cand);
}

/* ---- Reachability bitmap ---- */

/* Read exactly len bytes from stdin into a fresh buffer, draining the stream to
 * stay in sync even if the allocation fails (then returns NULL). */
static unsigned char *read_payload(size_t len) {
	unsigned char *buf = malloc(len ? len : 1);
	size_t got = 0;
	while (got < len) {
		unsigned char scratch[8192];
		unsigned char *dst = buf ? buf + got : scratch;
		size_t want = len - got;
		if (!buf && want > sizeof(scratch)) want = sizeof(scratch);
		size_t n = fread(dst, 1, want, stdin);
		if (!n) break;
		got += n;
	}
	if (buf && got < len) { free(buf); buf = NULL; }
	return buf;
}

/*
 * "put-bitmap <pack-size> <index-size> <reverse-index-size> <bitmap-size>": git
 * wrote a reachability bitmap for a pack of the objects at gc and keeps the
 * objects here, so it sends the .idx and .rev of that pack along with the
 * .bitmap, which relate its bits to the objects: the three follow the command,
 * in that order and of those sizes, the .rev possibly empty. They replace the
 * bitmap stored before, verbatim; git opens them as it opens the bitmap of a
 * pack (gitlocal-helpers(7) "put-bitmap").
 */
static void cmd_put_bitmap(const char *args) {
	long long pack_size;
	size_t idx_len, rev_len, bitmap_len;
	unsigned char *idx, *rev, *bitmap;
	int ok;

	/* Without the sizes, how much follows is unknown: nothing can be read in
	 * sync after this, so end the session as git does on a malformed reply. */
	if (sscanf(args, "%lld %zu %zu %zu", &pack_size, &idx_len, &rev_len,
		   &bitmap_len) != 4) {
		printf("error malformed put-bitmap\n"); fflush(stdout); exit(1);
	}
	idx = read_payload(idx_len);
	rev = read_payload(rev_len);
	bitmap = read_payload(bitmap_len);
	ok = idx && rev && bitmap &&
	     !storage_put_pack_bitmap(pack_size, idx, idx_len, rev, rev_len,
				      bitmap, bitmap_len);
	free(idx); free(rev); free(bitmap);
	printf(ok ? "ok\n" : "error cannot store the bitmap\n");
	fflush(stdout);
}

/*
 * "get-bitmap": reply "<pack-size> <index-size> <reverse-index-size>
 * <bitmap-size>" and the .idx, .rev and .bitmap "put-bitmap" stored, or
 * "missing" when none is stored.
 */
static void cmd_get_bitmap(void) {
	long long pack_size;
	void *idx, *rev, *bitmap;
	size_t idx_len, rev_len, bitmap_len;
	int ret = storage_get_pack_bitmap(&pack_size, &idx, &idx_len, &rev, &rev_len,
					  &bitmap, &bitmap_len);

	if (ret <= 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	printf("%lld %zu %zu %zu\n", pack_size, idx_len, rev_len, bitmap_len);
	fwrite(idx, 1, idx_len, stdout);
	fwrite(rev, 1, rev_len, stdout);
	fwrite(bitmap, 1, bitmap_len, stdout);
	free(idx); free(rev); free(bitmap);
	fflush(stdout);
}

/* "remove-bitmap": drop the stored bitmap, if any (a gc that wrote none). */
static void cmd_remove_bitmap(void) {
	printf(storage_remove_pack_bitmap() ? "error cannot remove the bitmap\n" : "ok\n");
	fflush(stdout);
}

/*
 * A3 verify: "verify" checks the integrity of the backing database (PRAGMA
 * integrity_check). Reached from "git fsck"; git separately content-checks the
 * objects it reads back from the helper, so this covers the storage layer that
 * the helper alone owns.
 */
static void cmd_verify(void) {
	if (storage_integrity_check() != 0) {
		printf("error backing database failed integrity check\n");
		fflush(stdout);
		return;
	}
	printf("ok\n");
	fflush(stdout);
}

static void cmd_odb_transaction_begin(void) {
	if (storage_begin() != 0) {
		printf("error cannot begin transaction\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_odb_transaction_commit(void) {
	/* The commit is the durability point: a failed COMMIT (FULL/IOERR/BUSY) must
	 * report error so git does not treat the batched objects as persisted. */
	if (storage_commit() != 0) {
		printf("error transaction commit failed\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref commands ---- */

static void cmd_read(const char *refname) {
	char oid[STORAGE_OID_HEXSZ + 1], symref[4096];
	int rc = storage_ref_read(refname, oid, sizeof(oid), symref, sizeof(symref));
	if (rc < 0)
		printf("missing\n");
	else if (symref[0])
		printf("symref %s\n", symref);
	else
		printf("%s\n", oid);
	fflush(stdout);
}

static int print_ref(const char *name, const char *oid, const char *sym, void *data) {
	(void)data;
	if (sym && *sym)
		printf("%s symref %s\n", name, sym);
	else if (oid && *oid)
		printf("%s %s\n", name, oid);
	return 0;
}

static void cmd_list(const char *prefix) {
	storage_ref_list(prefix, print_ref, NULL);
	printf("\n"); fflush(stdout);
}

static void cmd_create(void) { printf("ok\n"); fflush(stdout); }

static void cmd_remove(void) {
	if (storage_remove_ref_store() != 0) {
		printf("error cannot remove ref store\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref transactions ---- */

static int in_txn;
/* Set when any update inside the open ref transaction fails to persist. finish
 * refuses to commit a transaction so marked, rolling the whole set back instead
 * (helper-enforced atomicity: a partial set never lands even if the caller does
 * not abort on the error reply). */
static int txn_failed;

static void cmd_txn_begin(void) {
	if (!in_txn) {
		if (storage_savepoint("ref_txn") != 0) {
			printf("error cannot begin ref transaction\n"); fflush(stdout); return;
		}
		in_txn = 1;
		txn_failed = 0;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_update(const char *args) {
	char refname[4096], new_hex[STORAGE_OID_HEXSZ + 1],
	     old_hex[STORAGE_OID_HEXSZ + 1];
	int n;

	n = sscanf(args, "%4095s %64s %64s", refname, new_hex, old_hex);
	if (n < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}

	/*
	 * Enforce the old-value precondition the backend forwards (helper.h
	 * "Helper obligations"). The only race-free place to check is here,
	 * inside the helper's transaction (the backend never re-checks across
	 * the boundary). An all-zero old means the ref must not yet exist.
	 * Both current and expected values are lowercase hex (storage returns
	 * lower(hex(oid)); git sends lowercase), so a direct strcmp is exact.
	 */
	if (n >= 3) {
		char cur_sym[4096] = {0};
		char cur_oid[STORAGE_OID_HEXSZ + 1] = {0};
		int exists = storage_ref_read(refname, cur_oid, sizeof(cur_oid),
					      cur_sym, sizeof(cur_sym)) >= 0;
		int old_is_zero = strspn(old_hex, "0") == strlen(old_hex);

		if (old_is_zero) {
			if (exists) {
				printf("error reference '%s' already exists\n",
				       refname);
				fflush(stdout); return;
			}
		} else if (!exists || cur_sym[0] ||
			   strcmp(cur_oid, old_hex) != 0) {
			printf("error reference '%s' value mismatch\n", refname);
			fflush(stdout); return;
		}
	}

	/*
	 * Symref following is handled by the git core helper backend
	 * (refs_transaction_split_symref_update). Write the ref directly.
	 */
	if (storage_ref_write(refname, new_hex, NULL) != 0) {
		txn_failed = 1;
		printf("error cannot write reference '%s'\n", refname);
		fflush(stdout); return;
	}

	/*
	 * Do NOT write a reflog here. The landed protocol logs via a separate
	 * reflog-append command (helper_reflog_update in the git core backend);
	 * logging inline from the update's message double-logged every value ref
	 * (the explicit reflog-append plus this one). transaction-update only
	 * writes the ref value.
	 */

	printf("ok\n"); fflush(stdout);
}

/*
 * Force-create / upsert a value ref: set <refname> to <new> with no old-value
 * precondition (the protocol's create semantics, distinct from update's
 * compare-and-swap). A reflog message may follow <new> on the line; it is
 * ignored here, the backend logs through a separate reflog-append. (Directory/
 * file conflicts are rejected by the git-core backend before this is sent.)
 */
static void cmd_txn_create(const char *args) {
	char refname[4096], new_hex[STORAGE_OID_HEXSZ + 1];

	if (sscanf(args, "%4095s %64s", refname, new_hex) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	if (storage_ref_write(refname, new_hex, NULL) != 0) {
		txn_failed = 1;
		printf("error cannot write reference '%s'\n", refname);
		fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_delete(const char *args) {
	char refname[4096], old_hex[STORAGE_OID_HEXSZ + 1];
	int n = sscanf(args, "%4095s %64s", refname, old_hex);
	if (n < 1) { printf("error bad arguments\n"); fflush(stdout); return; }

	/*
	 * Honour the old-value precondition like cmd_txn_update(); an all-zero
	 * or absent old value is a force delete with no precondition.
	 */
	if (n >= 2 && strspn(old_hex, "0") != strlen(old_hex)) {
		char cur_sym[4096] = {0};
		char cur_oid[STORAGE_OID_HEXSZ + 1] = {0};
		if (storage_ref_read(refname, cur_oid, sizeof(cur_oid), cur_sym, sizeof(cur_sym)) < 0 ||
		    cur_sym[0] || strcmp(cur_oid, old_hex) != 0) {
			printf("error reference '%s' value mismatch\n", refname);
			fflush(stdout); return;
		}
	}

	if (storage_ref_delete(refname) != 0) {
		txn_failed = 1;
		printf("error cannot delete reference '%s'\n", refname);
		fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_create_symref(const char *args) {
	char refname[4096], target[4096];
	if (sscanf(args, "%4095s %4095s", refname, target) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	if (storage_ref_write(refname, NULL, target) != 0) {
		txn_failed = 1;
		printf("error cannot write symref '%s'\n", refname);
		fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_prepare(void) {
	/* A failed update marks the whole transaction unpreparable; reporting error
	 * here makes git core abort it (its prepare loop treats non-ok as fatal). */
	if (txn_failed) {
		printf("error ref transaction had a failed update\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_finish(void) {
	if (in_txn) {
		if (txn_failed) {
			/* Never commit a partially-failed transaction: discard it whole. */
			storage_rollback_to("ref_txn"); in_txn = 0;
			printf("error ref transaction had a failed update\n");
			fflush(stdout); return;
		}
		/* The release is the refs durability point: a failed COMMIT must report
		 * error so git does not treat the ref updates as landed. */
		int rc = storage_release("ref_txn");
		in_txn = 0;
		if (rc != 0) {
			printf("error ref transaction commit failed\n"); fflush(stdout); return;
		}
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_abort(void) {
	if (in_txn) { storage_rollback_to("ref_txn"); in_txn = 0; }
	txn_failed = 0;
	printf("ok\n"); fflush(stdout);
}

/* ---- Reflog commands ---- */

static int print_reflog(const char *old_oid, const char *new_oid,
			const char *committer, long long ts, int tz,
			const char *msg, void *data) {
	(void)data;
	printf("%s %s %s %lld %+05d\t%s\n", old_oid, new_oid, committer, ts, tz, msg ? msg : "");
	return 0;
}

static void cmd_reflog_read(const char *refname) {
	storage_reflog_read(refname, print_reflog, NULL);
	printf("\n"); fflush(stdout);
}

static void cmd_reflog_append(const char *args) {
	char refname[4096], old_hex[STORAGE_OID_HEXSZ + 1], new_hex[STORAGE_OID_HEXSZ + 1];
	const char *p = args, *sp;

	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(refname, sizeof(refname), "%.*s", (int)(sp - p), p); p = sp + 1;
	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(old_hex, sizeof(old_hex), "%.*s", (int)(sp - p), p); p = sp + 1;
	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(new_hex, sizeof(new_hex), "%.*s", (int)(sp - p), p); p = sp + 1;

	const char *email_end = strchr(p, '>');
	if (!email_end) { printf("error bad committer\n"); fflush(stdout); return; }
	email_end++;
	char committer[512];
	snprintf(committer, sizeof(committer), "%.*s", (int)(email_end - p), p);

	long long ts = 0; int tz = 0; char msg[4096] = "";
	if (*email_end == ' ') {
		char *end;
		ts = strtoll(email_end + 1, &end, 10);
		if (*end == ' ') tz = strtol(end + 1, &end, 10);
		if (*end == '\t') snprintf(msg, sizeof(msg), "%s", end + 1);
	}

	/* reflog-append is sent inside the ref transaction's commit, so a failure
	 * marks the transaction so finish rolls the whole thing back. */
	if (storage_reflog_append(refname, old_hex, new_hex, committer, ts, tz, msg) != 0) {
		if (in_txn) txn_failed = 1;
		printf("error cannot append reflog for '%s'\n", refname);
		fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_reflog_exists(const char *refname) {
	printf(storage_reflog_exists(refname) ? "true\n" : "false\n"); fflush(stdout);
}

static void cmd_reflog_create(const char *refname) {
	if (storage_reflog_create(refname) != 0) {
		if (in_txn) txn_failed = 1;
		printf("error cannot create reflog for '%s'\n", refname);
		fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_reflog_delete(const char *refname) {
	if (storage_reflog_delete(refname) != 0) {
		if (in_txn) txn_failed = 1;
		printf("error cannot delete reflog for '%s'\n", refname);
		fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

/* ---- New protocol commands ---- */

static int print_reflog_name(const char *refname, void *data) {
	(void)data;
	printf("%s\n", refname);
	return 0;
}

static void cmd_reflog_list(void) {
	storage_reflog_list(print_reflog_name, NULL);
	printf("\n"); fflush(stdout);
}

static void cmd_reflog_read_reverse(const char *refname) {
	storage_reflog_read_reverse(refname, print_reflog, NULL);
	printf("\n"); fflush(stdout);
}

/*
 * reflog-copy <old> <new> <delete_old>: carry <old>'s reflog onto <new> inside
 * the open ref transaction. delete_old=1 moves it (rename), 0 duplicates it
 * (copy). Refnames never contain whitespace, so %s parsing is safe.
 */
static void cmd_reflog_copy(const char *args) {
	char oldref[4096], newref[4096];
	int delete_old = 0;
	if (sscanf(args, "%4095s %4095s %d", oldref, newref, &delete_old) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	/* Carried inside the open ref transaction, so a failure marks it for
	 * rollback at finish. */
	if (storage_reflog_copy(oldref, newref, delete_old) != 0) {
		if (in_txn) txn_failed = 1;
		printf("error cannot copy reflog\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_put_stream(const char *args) {
	char type_str[32];
	unsigned long size;
	if (sscanf(args, "%31s %lu", type_str, &size) != 2) return;

	unsigned char *data = malloc(size ? size : 1);
	size_t got = 0;
	/* Drain the announced payload even on malloc failure, to stay in sync. */
	while (got < size) {
		unsigned char scratch[8192];
		unsigned char *dst = data ? data + got : scratch;
		size_t want = size - got;
		if (!data && want > sizeof(scratch))
			want = sizeof(scratch);
		size_t n = fread(dst, 1, want, stdin);
		if (n == 0) break;
		got += n;
	}

	/*
	 * The oid follows the payload as a trailer. git streams the content
	 * without knowing the oid up front, hashes it itself (git owns the hash
	 * and is runtime-dual), then sends the hex; the helper stays
	 * hash-agnostic and passes git's oid straight to storage, which keys it
	 * via unhex (length -> algo), exactly as the non-stream `put` does.
	 * Always consume the line so a malloc failure never desyncs the stream.
	 */
	char hex[STORAGE_OID_HEXSZ + 2]; /* hex + newline + NUL */
	if (!fgets(hex, sizeof(hex), stdin)) { free(data); return; }
	hex[strcspn(hex, "\n")] = '\0';

	/* Streamed (large) objects are always full and keep-existing: git never
	 * deltifies a blob over core.bigFileThreshold, so repack has no large-object
	 * representation to replace. */
	if (data && storage_write_object(hex, type_str, data, got, 0) == 0) {
		printf("%s\n", hex);
	} else {
		printf("error cannot store object\n");
	}
	free(data);
	fflush(stdout);
}

/* ---- Main loop ---- */

int main(int argc, char **argv) {
	if (argc < 2) { fprintf(stderr, "usage: git-local-sqlite <gitdir>\n"); return 1; }
	if (storage_open(argv[1]) < 0) { fprintf(stderr, "error: cannot open db\n"); return 1; }
	storage_git_init();	/* create the git schema on the opened connection */

	static char *line = NULL;
	static size_t cap = 0;
	while (read_line(&line, &cap) >= 0) {

		if (!strcmp(line, "capabilities")) cmd_capabilities();
		else if (!strncmp(line, "info ", 5)) cmd_info(line + 5);
		else if (!strncmp(line, "get ", 4)) cmd_get(line + 4);
		else if (!strncmp(line, "put ", 4)) cmd_put(line + 4);
		else if (!strncmp(line, "put-raw ", 8)) cmd_put_raw(line + 8);
		else if (!strncmp(line, "get-raw ", 8)) cmd_get_raw(line + 8);
		else if (!strncmp(line, "have ", 5)) cmd_have(line + 5);
		else if (!strncmp(line, "freshen ", 8)) cmd_freshen(line + 8);
		else if (!strncmp(line, "list-objects", 12))
			cmd_list_objects(line[12] == ' ' ? line + 13 : NULL);
		else if (!strcmp(line, "odb-transaction-begin")) cmd_odb_transaction_begin();
		else if (!strcmp(line, "odb-transaction-commit")) cmd_odb_transaction_commit();
		else if (!strncmp(line, "read ", 5)) cmd_read(line + 5);
		else if (!strncmp(line, "list ", 5)) cmd_list(line + 5);
		else if (!strcmp(line, "list")) cmd_list(NULL);
		else if (!strcmp(line, "create")) cmd_create();
		else if (!strcmp(line, "remove")) cmd_remove();
		else if (!strcmp(line, "transaction-begin")) cmd_txn_begin();
		else if (!strncmp(line, "transaction-update ", 19)) cmd_txn_update(line + 19);
		else if (!strncmp(line, "transaction-create-symref ", 25)) cmd_txn_create_symref(line + 25);
		else if (!strncmp(line, "transaction-create ", 19)) cmd_txn_create(line + 19);
		else if (!strncmp(line, "transaction-delete ", 19)) cmd_txn_delete(line + 19);
		else if (!strcmp(line, "transaction-prepare")) cmd_txn_prepare();
		else if (!strcmp(line, "transaction-finish")) cmd_txn_finish();
		else if (!strcmp(line, "transaction-abort")) cmd_txn_abort();
		else if (!strncmp(line, "put-stream ", 11)) cmd_put_stream(line + 11);
		else if (!strncmp(line, "reflog-read-reverse ", 20)) cmd_reflog_read_reverse(line + 20);
		else if (!strncmp(line, "reflog-read ", 12)) cmd_reflog_read(line + 12);
		else if (!strncmp(line, "reflog-append ", 14)) cmd_reflog_append(line + 14);
		else if (!strncmp(line, "reflog-exists ", 14)) cmd_reflog_exists(line + 14);
		else if (!strncmp(line, "reflog-create ", 14)) cmd_reflog_create(line + 14);
		else if (!strncmp(line, "reflog-delete ", 14)) cmd_reflog_delete(line + 14);
		else if (!strncmp(line, "reflog-copy ", 12)) cmd_reflog_copy(line + 12);
		else if (!strcmp(line, "reflog-list")) cmd_reflog_list();
		/*
		 * Read-visibility reset; fire-and-forget (no reply), the backend
		 * does not wait for one. See helper.h "refresh".
		 */
		else if (!strcmp(line, "refresh")) storage_refresh();
		else if (!strcmp(line, "promisor")) cmd_promisor();
		else if (!strcmp(line, "optimize")) cmd_optimize();
		else if (!strcmp(line, "optimize-required")) cmd_optimize_required();
		else if (!strcmp(line, "prune")) cmd_prune("");
		else if (!strncmp(line, "prune ", 6)) cmd_prune(line + 6);
		else if (!strcmp(line, "store-commit-graph")) cmd_store_commit_graph();
		else if (!strncmp(line, "commit-generation ", 18)) cmd_commit_generation(line + 18);
		else if (!strncmp(line, "put-bitmap ", 11)) cmd_put_bitmap(line + 11);
		else if (!strcmp(line, "get-bitmap")) cmd_get_bitmap();
		else if (!strcmp(line, "remove-bitmap")) cmd_remove_bitmap();
		else if (!strcmp(line, "verify")) cmd_verify();
		else if (!strcmp(line, "close")) break;
	}

	storage_close();
	return 0;
}
