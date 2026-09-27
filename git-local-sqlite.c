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
	 * Advertise the landed protocol (the commands the git core helper
	 * backend sends). "optimize" (A3) repacks: it re-deltifies the store by
	 * driving git's pack-objects and storing the result through "put"/"put-raw"
	 * with their replace argument set, then reclaims (incremental_vacuum). "replace" advertises that
	 * those verbs honor that trailing argument (overwrite an object's stored
	 * representation in place rather than keep-existing); only repack sets it.
	 * "verify" (A3) runs a database integrity check for "git fsck". "keep-pack"
	 * records which objects arrived in a kept pack so "prune" spares them while
	 * its .keep is live (the relational form of files keeping a .keep'd pack).
	 * "graph" (store-commit-graph) carries the git-computed generation number per
	 * commit so git0_generation() and the commit-graph fill-in read it from the
	 * table instead of a commit-graph file (tree/parents/date stay in the commit
	 * objects, so the generation is the only datum stored).
	 * The connectivity-check remains deferred to the rest of A3; partial-clone
	 * remotes, worktrees, alternates and hash conversion are handled git-side,
	 * and write-packfile is subsumed by odb-transaction.
	 */
	printf("get\ninfo\nput\nput-raw\nput-stream\nget-delta\nreplace\n"
	       "have\nfreshen\nlist-objects\nodb-transaction\n"
	       "read\nlist\ntransaction\ncreate\nremove\n"
	       "reflog\nreflog-read-reverse\n"
	       "refresh\npromisor\noptimize\noptimize-required\nverify\nprune\n"
	       "keep-pack\nbitmap\ncommit-bitmap\ngraph\nreuse-pack\n\n");
	fflush(stdout);
}

/* ---- ODB commands ---- */

static void cmd_info(const char *hex) {
	char type[32]; size_t size;
	if (storage_read_header(hex, type, sizeof(type), &size) < 0) {
		printf("missing\n");
	} else {
		/*
		 * Report the resolved type and size; if the object is stored as a
		 * git-format delta, also report its base oid and raw delta length so
		 * git core can reuse the stored delta on a send pack instead of
		 * recomputing it (the bytes are fetched at write time via "get-delta").
		 * A caller that does not care just parses the first two fields.
		 */
		char base_hex[STORAGE_OID_HEXSZ + 1]; size_t delta_len;
		if (storage_delta_info(hex, base_hex, sizeof(base_hex), &delta_len))
			printf("%s %zu %s %zu\n", type, size, base_hex, delta_len);
		else
			printf("%s %zu\n", type, size);
	}
	fflush(stdout);
}

/*
 * "get <oid>": reply "<type> <size>" then the resolved object bytes. The helper
 * resolves locally (inflate + delta apply via storage_read_object), exactly as
 * the files backend resolves a pack object in-process and hands back a finished
 * object; "get-delta" serves the unresolved delta for pack-objects reuse.
 */
static void cmd_get(const char *hex) {
	char type[32]; size_t size; unsigned char *data;
	if (storage_read_object(hex, type, sizeof(type), &size, &data) < 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	printf("%s %zu\n", type, size);
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
 * hands an object in its native pack-entry storage form and we persist it
 * verbatim, exactly as the files backend keeps a received pack's compressed
 * entries. base "0" = a whole object (compressed content); a base oid = a
 * compressed git-format delta against it. usize is the stored uncompressed
 * length (the pack entry-header size), clen the compressed length. The helper
 * never (de)compresses here, git already prepared the bytes (index-pack's
 * entry on receive, pack-objects' on repack), so this is the recompress-free
 * receive path. Optional <replace> for repack overwrite.
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
 * "get-delta <oid>": if the object is stored as a git-format delta, reply
 * "delta <base-oid> <raw-len> <clen>" + the clen COMPRESSED delta bytes, so git
 * core reuses the delta verbatim on a send pack (its cached-delta path writes the
 * compressed bytes straight into the wire pack, no inflate, no recompress) and a
 * push migrate copies it straight in. <raw-len> is the uncompressed delta length
 * git matches against its planned delta size; <clen> is how many compressed bytes
 * follow. Otherwise reply "plain" and let git fetch it with "get". Optional
 * capability; absent -> git uses "get".
 */
static void cmd_get_delta(const char *hex) {
	char base_hex[STORAGE_OID_HEXSZ + 1];
	unsigned char *delta;
	size_t delta_len, raw_len;

	if (!storage_get_delta(hex, base_hex, sizeof(base_hex), &delta, &delta_len, &raw_len)) {
		printf("plain\n");
		fflush(stdout);
		return;
	}
	printf("delta %s %zu %zu\n", base_hex, raw_len, delta_len);
	if (delta_len > 0) fwrite(delta, 1, delta_len, stdout);
	free(delta);
	fflush(stdout);
}

/*
 * "reuse-pack <end-pack-pos>": stream the reusable pack_pos prefix [0, end) for
 * git's source-backed pack reuse, the bulk analog of the files backend copying a
 * contiguous .pack region. Each object is one record "<type> <size> <base-oid|->
 * <clen>" followed by the clen verbatim COMPRESSED content bytes, in pack_pos
 * order; a blank line terminates. git frames each into the output pack (REF_DELTA
 * + base for a delta, plain header otherwise), reusing the stored compressed
 * bytes with no inflate or recompress, exactly as "get-delta" reuses one delta.
 */
static int reuse_emit(const char *type, long long size, const char *base,
		      const void *content, int clen, void *data) {
	(void)data;
	printf("%s %lld %s %d\n", type, size, base, clen);
	if (clen > 0) fwrite(content, 1, clen, stdout);
	return 0;
}

static void cmd_reuse_pack(const char *arg) {
	storage_reuse_stream(strtoll(arg, NULL, 10), reuse_emit, NULL);
	printf("\n");	/* blank line terminates the stream */
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
	 * Grammar: list-objects [--promisor-only] [--skip-kept] [<hexprefix>].
	 * A flag begins with "--"; a bare token is a hex oid prefix that scopes the
	 * enumeration to matching ids server-side (abbreviation/disambiguation),
	 * rather than streaming the whole table for git to filter. --skip-kept is
	 * accepted and ignored: kept is a pack-level notion with no per-object
	 * meaning here, but git may send it. git owns the input, so a non-hex prefix
	 * is treated as absent (full enumeration), never trusted into the GLOB.
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
 * pages freed by object/ref churn. Reached from "git gc"/"git repack" AFTER git
 * has re-deltified the store (pack-objects | index-pack --re-deltify), so this
 * reclaims the space the re-representation frees. It then stamps the optimize
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
 * "keep-pack <pack_id>" records that the objects of a just-ingested received
 * pack belong to it (one oid per line, blank-terminated). git sends it while the
 * ingest's odb-transaction is still open (just before commit), so the membership
 * lands in the same transaction as the objects: a concurrent prune can never see
 * the objects without their keep already in place. <pack_id> is the pack's
 * trailing hash, matching the pack-<hash>.keep file git's transport lifecycle
 * creates and later removes. While that .keep is live, "prune" spares these
 * objects (the relational form of files keeping a .keep'd pack's objects).
 */
static void cmd_keep_pack(const char *pack_id) {
	static char *line = NULL;
	static size_t cap = 0;
	int failed = 0;
	/* Drain to the blank terminator even after a failure so the stream stays in
	 * sync; report error once at the end. The membership lands in the ingest's
	 * open odb-transaction, so its abort/rollback discards a partial set. */
	while (read_line(&line, &cap) >= 0) {
		size_t len = strlen(line);
		if (!len) break;
		if (len > STORAGE_OID_HEXSZ) continue;	/* not an oid; ignore */
		if (storage_record_pack_membership(line, pack_id) != 0) failed = 1;
	}
	if (failed) { printf("error cannot record pack membership\n"); fflush(stdout); return; }
	printf("ok\n");
	fflush(stdout);
}

/*
 * "store-commit-graph" + "<commit_hex> <generation>" lines,
 * blank-terminated: git computed these generations while writing its commit-graph;
 * we store them (clearing the prior set first, so stale commits drop out) and do
 * NOT write a commit-graph file. Generation is the only datum not derivable from
 * the commit objects (tree/parents/date are parsed from the commits on access),
 * so it is all we keep; git0_generation(oid) and the lazy commit-graph
 * reconstruction read it. Atomic clear-then-populate, mirroring store-bitmap.
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
 * "prune <dry_run> <expire>" is the helper's half of "git prune": git computes
 * reachability and sends the unreachable object ids to delete. The exchange has
 * two blank-terminated sections: first the pack ids whose .keep marker is still
 * live (each as "keep <pack_id>"; git owns the .keep lifecycle), then the
 * candidate oids (one per line). We delete a candidate stored at or before
 * <expire> unless it is in a kept pack or is still a delta base (the predicate in
 * STMT_PRUNE_OBJECT). We report back the oids we pruned (one per line, terminated
 * by "ok") so git can show exactly what "git prune" removed; with <dry_run> set
 * we report what would be pruned without deleting. git owns reachability (refs +
 * reflogs + index); the helper just executes the deletions, in one savepoint.
 *
 * We drain both lists before writing any reply: git sends them in full before it
 * begins reading our response, so streaming mid-read could deadlock once both
 * pipe buffers fill.
 */
static void cmd_prune(const char *arg) {
	int dry_run = 0;
	long long expire = 0;
	static char *line = NULL;
	static size_t cap = 0;
	char (*cand)[STORAGE_OID_HEXSZ + 1] = NULL;
	size_t n = 0, alloc = 0, i;

	if (sscanf(arg, "%d %lld", &dry_run, &expire) != 2) {
		printf("ok\n");
		fflush(stdout);
		return;
	}

	/* First section: the live kept pack ids (blank-terminated). A keep that fails
	 * to record would let prune over-delete a kept object, so track failure and
	 * prune nothing in that case (reported below as an empty prune). */
	int ok = (storage_keep_clear() == 0);
	while (read_line(&line, &cap) >= 0) {
		size_t len = strlen(line);
		if (!len) break;
		if (!strncmp(line, "keep ", 5) && storage_keep_add(line + 5) != 0) ok = 0;
	}

	/* Second section: the candidate oids (blank-terminated). */
	while (read_line(&line, &cap) >= 0) {
		size_t len = strlen(line);
		if (!len) break;
		/* A prune candidate is an oid; ignore any line too long to be one. A
		 * truncated copy could otherwise unhex to a shorter oid and alias a
		 * different object; git only ever sends 40/64-char oids here. */
		if (len > STORAGE_OID_HEXSZ) continue;
		if (n == alloc) {
			size_t na = alloc ? alloc * 2 : 64;
			char (*grown)[STORAGE_OID_HEXSZ + 1] = realloc(cand, na * sizeof(*cand));
			if (grown) { cand = grown; alloc = na; }
		}
		if (n < alloc)
			snprintf(cand[n++], STORAGE_OID_HEXSZ + 1, "%.*s", (int)STORAGE_OID_HEXSZ, line);
		/* else out of memory: keep draining stdin so the protocol stays in
		 * sync, pruning only the candidates we managed to capture. */
	}

	/*
	 * Prune to a fixpoint inside one savepoint. Membership of released packs is
	 * absorbed first (they rejoin the general store, as repack merges a
	 * no-longer-kept pack). Each pass deletes only candidates that are not in a
	 * kept pack and not the delta base of any object still in the store (the
	 * predicate in STMT_PRUNE_OBJECT), so a surviving object is never orphaned by
	 * losing its base. Deleting a leaf frees its base for the next pass, so an
	 * unreachable delta chain drains fully; an already-deleted candidate reports
	 * 0 rows and is not echoed again. A dry run runs the real deletes (and the
	 * absorb) inside the savepoint and rolls them back, reporting exactly what a
	 * real prune would remove.
	 */
	if (storage_savepoint("prune")) {
		/* Cannot open the transaction: prune nothing rather than delete outside
		 * one. The reply has only an "ok" terminator (no error channel), and an
		 * empty prune is safe. */
		printf("ok\n"); fflush(stdout); free(cand); return;
	}
	if (ok && storage_absorb_released_packs() != 0)
		ok = 0;
	if (ok) {
		int progress;
		do {
			progress = 0;
			for (i = 0; i < n; i++) {
				if (storage_prune_object(cand[i], expire)) {
					printf("%s\n", cand[i]);
					progress = 1;
				}
			}
		} while (progress);
	}
	/* dry-run, or a keep/absorb failure: roll back and prune nothing (a failure
	 * streamed no oids, so the reply is just the ok terminator). */
	if (dry_run || !ok)
		storage_rollback_to("prune");
	else
		storage_release("prune");
	printf("ok\n");
	fflush(stdout);
	free(cand);
}

/* ---- Reachability bitmap (the open_bitmap source backing) ---- */

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
 * "store-bitmap <bitmap_len>": git computed a reachability bitmap at optimize.
 * The payload is <bitmap_len> bytes of EWAH .bitmap content, then the .idx (one
 * index-order oid hex per line, blank-terminated; line index = index position),
 * then the .rev (one integer per line, blank-terminated; line index = bit
 * position, value = index position). Replace any prior bitmap atomically.
 */
static void cmd_store_bitmap(const char *args) {
	size_t bitmap_len = 0;
	unsigned char *bitmap;
	static char *line = NULL;
	static size_t cap = 0;
	long long pos = 0, bit = 0;

	if (sscanf(args, "%zu", &bitmap_len) != 1) {
		printf("ok\n"); fflush(stdout); return;
	}
	bitmap = read_payload(bitmap_len);

	if (storage_savepoint("store_bitmap")) {
		free(bitmap); printf("error cannot store bitmap\n"); fflush(stdout); return;
	}
	int ok = (storage_clear_bitmap() == 0);
	if (bitmap && storage_store_bitmap(bitmap, bitmap_len) != 0)
		ok = 0;
	while (read_line(&line, &cap) >= 0) {	/* .idx: index-order oids */
		size_t len = strlen(line);
		if (!len) break;
		if (bitmap && len <= STORAGE_OID_HEXSZ &&
		    storage_add_bitmap_order(pos++, line) != 0)
			ok = 0;
	}
	while (read_line(&line, &cap) >= 0) {	/* .rev: bit -> index */
		size_t len = strlen(line);
		char *end;
		long long idx;
		if (len && line[len - 1] == '\n') line[--len] = 0;
		if (!len) break;
		idx = strtoll(line, &end, 10);
		if (bitmap && end != line && !*end &&
		    storage_add_bitmap_rev(bit++, idx) != 0)
			ok = 0;
	}
	/* Recluster only over a fully, correctly stored ordering. */
	if (bitmap && ok && storage_recluster_pack_content() != 0)
		ok = 0;
	free(bitmap);
	if (!ok) {
		storage_rollback_to("store_bitmap");
		printf("error cannot store bitmap\n"); fflush(stdout); return;
	}
	if (storage_release("store_bitmap")) {
		printf("error cannot store bitmap\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

/* "clear-bitmap": drop any stored bitmap (e.g. an optimize that produced none). */
static void cmd_clear_bitmap(void) {
	if (storage_savepoint("clear_bitmap")) {
		printf("error cannot clear bitmap\n"); fflush(stdout); return;
	}
	if (storage_clear_bitmap() != 0) {
		storage_rollback_to("clear_bitmap");
		printf("error cannot clear bitmap\n"); fflush(stdout); return;
	}
	if (storage_release("clear_bitmap")) {
		printf("error cannot clear bitmap\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static int print_order_oid(const char *oid, void *data) {
	(void)data;
	printf("%s\n", oid);
	return 0;
}

/*
 * "get-bitmap": reply "bitmap <type_bitmaps_len> <n_objects>" + the type-bitmap
 * bytes, or "missing" when none is stored. The bit<->oid orderings are NOT sent;
 * git resolves them on demand (pos-of-oid / result-oids) over objects.pack_pos.
 * n_objects sizes git's source bitmap (objects.pack_pos is a dense [0,N) perm).
 */
static void cmd_get_bitmap(void) {
	void *bitmap;
	size_t bitmap_len;

	if (storage_get_bitmap(&bitmap, &bitmap_len) < 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	printf("bitmap %zu %d\n", bitmap_len, storage_bitmap_count());
	if (bitmap_len) fwrite(bitmap, 1, bitmap_len, stdout);
	free(bitmap);
	fflush(stdout);
}

/*
 * Per-commit bitmap rows: the bitmap analog of object deltas (put-delta), stored
 * base-by-oid. At capture git decomposes its .bitmap into one EWAH entry per
 * bitmapped commit (reusing its own reader) and streams them all in one verb:
 *   "store-commit-bitmaps", then a record per commit
 *   "<commit_hex> <xor_base_hex|-> <flags> <ewah_len>\n" + <ewah_len> bytes,
 * blank-line terminated. The helper clears the existing rows and stores the set
 * in a single transaction (atomic, one write), mirroring store-commit-graph. At
 * serve git requests the relevant commits:
 *   "get-commit-bitmap <commit_hex>" -> "commit-bitmap <xor_base|-> <flags> <len>"
 * + bytes, or "missing". A failed store rolls the whole set back and reports
 * error rather than acking, and git rebuilds the bitmap at the next gc (it
 * verifies independently via rev-list --test-bitmap).
 */
static void cmd_store_commit_bitmaps(void) {
	static char *line = NULL;
	static size_t cap = 0;

	if (storage_savepoint("store_commit_bitmaps")) {
		printf("error cannot store commit bitmaps\n"); fflush(stdout); return;
	}
	int ok = (storage_clear_commit_bitmaps() == 0);
	while (read_line(&line, &cap) >= 0) {
		char commit[STORAGE_OID_HEXSZ + 1], xb[STORAGE_OID_HEXSZ + 1];
		int flags = 0;
		size_t ewah_len = 0;
		unsigned char *ewah;

		if (!strlen(line)) break;	/* blank terminator */
		if (sscanf(line, "%64s %64s %d %zu", commit, xb, &flags, &ewah_len) != 4) {
			ok = 0; break;	/* malformed header: payload length unknown, cannot drain */
		}
		ewah = read_payload(ewah_len);	/* always drain the announced bytes to stay in sync */
		if (!ewah || storage_store_commit_bitmap(commit, strcmp(xb, "-") ? xb : NULL,
							 flags, ewah, ewah_len) != 0)
			ok = 0;
		free(ewah);
	}
	if (!ok) {
		storage_rollback_to("store_commit_bitmaps");
		printf("error cannot store commit bitmaps\n"); fflush(stdout); return;
	}
	if (storage_release("store_commit_bitmaps")) {
		printf("error cannot store commit bitmaps\n"); fflush(stdout); return;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_get_commit_bitmap(const char *args) {
	char commit[STORAGE_OID_HEXSZ + 1], xb[STORAGE_OID_HEXSZ + 1];
	int flags = 0;
	void *ewah;
	size_t ewah_len;

	if (sscanf(args, "%64s", commit) != 1 ||
	    storage_get_commit_bitmap(commit, xb, &flags, &ewah, &ewah_len) < 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	printf("commit-bitmap %s %d %zu\n", *xb ? xb : "-", flags, ewah_len);
	if (ewah_len) fwrite(ewah, 1, ewah_len, stdout);
	free(ewah);
	fflush(stdout);
}

/*
 * On-demand bitmap orderings (2b: files-parity, O(touched) not O(N)). The source
 * bitmap resolves bit<->oid via objects.pack_pos on demand instead of fetching
 * all N orderings at open.
 *   "pos-of-oid <oid_hex>" -> "pos <bit>" or "none": one want/have's bit position.
 *   "result-oids" + an operation's bit positions (one per line, blank-terminated)
 *     -> their oids in bit (pack_pos) order, blank-terminated: the batched bit->oid
 *     for a serve, resolving exactly the bits the operation touches in one join.
 */
static void cmd_pos_of_oid(const char *args) {
	char oid[STORAGE_OID_HEXSZ + 1];
	int pos;

	if (sscanf(args, "%64s", oid) != 1) { printf("none\n"); fflush(stdout); return; }
	pos = storage_pack_pos_of_oid(oid);
	if (pos < 0) printf("none\n");
	else printf("pos %d\n", pos);
	fflush(stdout);
}

static void cmd_result_oids(void) {
	static char *line = NULL;
	static size_t cap = 0;

	int ok = (storage_result_pos_clear() == 0);
	while (read_line(&line, &cap) >= 0) {	/* the operation's bit set */
		size_t len = strlen(line);
		char *end;
		long long bit;
		if (len && line[len - 1] == '\n') line[--len] = 0;
		if (!len) break;
		bit = strtoll(line, &end, 10);
		if (end != line && !*end && storage_result_pos_add(bit) != 0)
			ok = 0;
	}
	/* Resolve only a fully-staged set: a partial result_pos would yield a
	 * truncated (or stale-padded) oid list, and this reply has no error channel,
	 * so an incomplete staging emits an empty result rather than a wrong one. */
	if (ok)
		storage_oids_at_result(print_order_oid, NULL);	/* oids in bit order */
	printf("\n");
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
		else if (!strncmp(line, "get-delta ", 10)) cmd_get_delta(line + 10);
		else if (!strncmp(line, "reuse-pack ", 11)) cmd_reuse_pack(line + 11);
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
		else if (!strncmp(line, "prune ", 6)) cmd_prune(line + 6);
		else if (!strncmp(line, "keep-pack ", 10)) cmd_keep_pack(line + 10);
		else if (!strcmp(line, "store-commit-graph")) cmd_store_commit_graph();
		else if (!strncmp(line, "commit-generation ", 18)) cmd_commit_generation(line + 18);
		else if (!strncmp(line, "store-bitmap ", 13)) cmd_store_bitmap(line + 13);
		else if (!strcmp(line, "get-bitmap")) cmd_get_bitmap();
		else if (!strcmp(line, "clear-bitmap")) cmd_clear_bitmap();
		else if (!strcmp(line, "store-commit-bitmaps")) cmd_store_commit_bitmaps();
		else if (!strncmp(line, "get-commit-bitmap ", 18)) cmd_get_commit_bitmap(line + 18);
		else if (!strncmp(line, "pos-of-oid ", 11)) cmd_pos_of_oid(line + 11);
		else if (!strcmp(line, "result-oids")) cmd_result_oids();
		else if (!strcmp(line, "verify")) cmd_verify();
		else if (!strcmp(line, "close")) break;
	}

	storage_close();
	return 0;
}
