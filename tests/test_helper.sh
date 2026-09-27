#!/bin/sh
#
# Test suite for git-local-sqlite and git-lfs-sqlite-transfer.
#
# Exercises the local helper protocol (the landed command set: objects, refs,
# transactions with CAS old-value preconditions, reflog, promisor, refresh) and
# the LFS transfer adapter. Each test creates a fresh database in a temp
# directory and cleans up after itself.
#
# Usage: ./tests/test_helper.sh
#   Requires: git-local-sqlite, git-lfs-sqlite-transfer built in the repo root.

set -e

PASS=0
FAIL=0
BINDIR="$(cd "$(dirname "$0")/.." && pwd)"
HELPER="$BINDIR/git-local-sqlite"
LFS_TRANSFER="$BINDIR/git-lfs-sqlite-transfer"
TMPDIR=$(mktemp -d)

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

ok() {
	PASS=$((PASS + 1))
	printf "ok %d - %s\n" "$PASS" "$1"
}

fail() {
	FAIL=$((FAIL + 1))
	printf "not ok - %s\n" "$1"
}

check() {
	if eval "$2"; then
		ok "$1"
	else
		fail "$1"
	fi
}

# Ensure binaries exist
test -x "$BINDIR/git-local-sqlite" || { echo "git-local-sqlite not built"; exit 1; }
test -x "$LFS_TRANSFER" || { echo "git-lfs-sqlite-transfer not built"; exit 1; }

# ---- Capabilities ----

CAP=$(printf 'capabilities\n' | "$HELPER" "$TMPDIR/cap-test" 2>/dev/null)

check "capabilities: lists get" \
	'echo "$CAP" | grep -q "^get$"'

check "capabilities: lists put-stream" \
	'echo "$CAP" | grep -q "^put-stream$"'

check "capabilities: lists reflog (the bundled reflog-store cap)" \
	'echo "$CAP" | grep -q "^reflog$"'

check "capabilities: lists reflog-read-reverse (separate: has a git-core fallback)" \
	'echo "$CAP" | grep -q "^reflog-read-reverse$"'

check "capabilities: lists promisor" \
	'echo "$CAP" | grep -q "^promisor$"'

check "capabilities: lists optimize" \
	'echo "$CAP" | grep -q "^optimize$"'

check "capabilities: lists verify" \
	'echo "$CAP" | grep -q "^verify$"'

check "capabilities: lists prune" \
	'echo "$CAP" | grep -q "^prune$"'

check "capabilities: lists bitmap" \
	'echo "$CAP" | grep -q "^bitmap$"'

check "capabilities: lists graph" \
	'echo "$CAP" | grep -q "^graph$"'

check "capabilities: lists put-raw" \
	'echo "$CAP" | grep -q "^put-raw$"'

check "capabilities: lists get-raw" \
	'echo "$CAP" | grep -q "^get-raw$"'

# The whole set, so that nothing git does not define is advertised.
CAP_ALL="bitmap create freshen get get-raw graph have info list list-objects
odb-transaction optimize optimize-required promisor prune put put-raw
put-stream read reflog reflog-read-reverse refresh remove replace transaction
verify"
check "capabilities: exactly the objects and refs protocol, plus graph" \
	'test "$(echo "$CAP" | grep . | sort | tr "\n" " ")" = "$(echo $CAP_ALL | tr " " "\n" | sort | tr "\n" " ")"'

# ---- Object operations ----

DB="$TMPDIR/obj-test"

# put + get round trip
OID=$(printf 'capabilities\nput %s blob 11\nhello world' \
	"95d09f2b10159347eece71399a7e2e907ea3df4f" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "put: returns correct oid" \
	'test "$OID" = "95d09f2b10159347eece71399a7e2e907ea3df4f"'

CONTENT=$(printf 'capabilities\nget %s\n' "$OID" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "get: returns content" \
	'test "$CONTENT" = "hello world"'

# info
INFO=$(printf 'capabilities\ninfo %s\n' "$OID" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "info: returns type and size" \
	'test "$INFO" = "blob 11"'

# have
HAVE=$(printf 'capabilities\nhave %s\n' "$OID" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "have: true for existing object" \
	'test "$HAVE" = "true"'

HAVE_NO=$(printf 'capabilities\nhave deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "have: false for missing object" \
	'test "$HAVE_NO" = "false"'

# A stored stream that inflates to less than its recorded size fails to read,
# rather than passing off the rest of a buffer as content.
if ! command -v sqlite3 >/dev/null 2>&1; then
	ok "short stored stream (skipped: needs sqlite3)"
else
	SDB="$TMPDIR/short-test"
	printf 'put %s blob 11\nhello world' "$OID" | "$HELPER" "$SDB" >/dev/null 2>&1
	sqlite3 "$SDB/sqlite.db" "UPDATE objects SET size = 12"
	check "get: a stream shorter than its recorded size is missing" \
		'test "$(printf "get %s\n" "$OID" | "$HELPER" "$SDB" 2>/dev/null)" = "missing"'
fi

# ---- put-raw / get-raw ----
# Store a git-format delta against the 11-byte base put above, in git's native
# pack form: the COMPRESSED delta bytes, stored verbatim (no recompress), base set.
# The delta is a copy-all: varint(11) varint(11) copy(offset=0,size=11) =
# \013 \013 \220 \013 (4 bytes uncompressed). The 12 bytes below are that delta
# zlib-compressed; hardcoding the compressed literal is portable because inflate
# is stable across zlib versions (only compression output varies). usize=4 is the
# uncompressed delta length, clen=12 the compressed byte count that follows.
DELTA_OID="1111111111111111111111111111111111111111"
DELTA_Z='\170\234\343\346\236\300\015\000\001\174\000\262'
PD=$(printf "capabilities\nput-raw %s blob 4 %s 12\n$DELTA_Z" \
	"$DELTA_OID" "$OID" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "put-raw: returns oid" \
	'test "$PD" = "1111111111111111111111111111111111111111"'

# "get" must inflate then resolve the stored delta against its base (exercises
# zdecompress + git_delta_apply).
DELTA_CONTENT=$(printf 'capabilities\nget %s\n' "$DELTA_OID" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "get: stored git-delta resolves to base content" \
	'test "$DELTA_CONTENT" = "hello world"'

# "get" and "info" report the resolved type and size of a stored delta, its
# base and the size of the delta. info reads the size off the start of the
# delta alone, as git reads the size of a delta in a packfile, so it answers
# for a delta whose base the store lacks.
GET_DELTA_HDR=$(printf 'get %s\n' "$DELTA_OID" | "$HELPER" "$DB" 2>/dev/null | head -1)
INFO_DELTA=$(printf 'capabilities\ninfo %s\n' "$DELTA_OID" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)
LONE_OID="2222222222222222222222222222222222222222"
LONE_BASE="3333333333333333333333333333333333333333"
printf "put-raw %s blob 4 %s 12\n$DELTA_Z" "$LONE_OID" "$LONE_BASE" \
	| "$HELPER" "$DB" >/dev/null 2>&1
INFO_LONE=$(printf 'info %s\n' "$LONE_OID" | "$HELPER" "$DB" 2>/dev/null)

check "get: a stored git-delta names its base and delta size" \
	'test "$GET_DELTA_HDR" = "blob 11 $OID 4"'
check "info: a stored git-delta names its base and delta size" \
	'test "$INFO_DELTA" = "blob 11 $OID 4"'
check "info: the size of a delta's object comes from the delta alone" \
	'test "$INFO_LONE" = "blob 11 $LONE_BASE 4"'

# "get-raw" hands a delta back as stored: its base, the size the bytes inflate
# to, and the 12 compressed bytes verbatim.
printf 'get-raw %s\n' "$DELTA_OID" | "$HELPER" "$DB" >"$TMPDIR/raw-delta" 2>/dev/null
printf "blob 4 %s 12\n$DELTA_Z" "$OID" >"$TMPDIR/raw-delta.expect"

check "get-raw: a delta comes back as put-raw stored it" \
	'cmp -s "$TMPDIR/raw-delta" "$TMPDIR/raw-delta.expect"'

# A whole object comes back compressed, with a base of 0; storing those bytes
# with put-raw elsewhere gives the object back.
printf 'get-raw %s\n' "$OID" | "$HELPER" "$DB" >"$TMPDIR/raw-whole" 2>/dev/null
RAW_HDR=$(head -1 "$TMPDIR/raw-whole")
RAW_LEN=${RAW_HDR##* }
{ printf 'put-raw %s blob 11 0 %s\n' "$OID" "$RAW_LEN"
  tail -c "$RAW_LEN" "$TMPDIR/raw-whole"; } | "$HELPER" "$TMPDIR/raw-copy" >/dev/null 2>&1
RAW_COPY=$(printf 'get %s\n' "$OID" | "$HELPER" "$TMPDIR/raw-copy" 2>/dev/null | tail -1)

check "get-raw: a whole object has base 0" \
	'test "${RAW_HDR% *}" = "blob 11 0"'
check "get-raw: the compressed bytes of a whole object store elsewhere intact" \
	'test "$(wc -c <"$TMPDIR/raw-whole")" -eq $((${#RAW_HDR} + 1 + RAW_LEN)) &&
	 test "$RAW_COPY" = "hello world"'
check "get-raw: missing for an unknown object" \
	'test "$(printf "get-raw deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n" | "$HELPER" "$DB" 2>/dev/null)" = "missing"'

# ---- objects clustered in pack_content ----
# A store may hold the bytes of objects in pack_content, keyed by their
# pack_pos, with objects.data empty. Reads find them there; re-storing an
# object moves it back into objects.data; optimize drops the pack_content rows
# no object points at.
if ! command -v sqlite3 >/dev/null 2>&1; then
	ok "clustered objects (skipped: needs sqlite3)"
else
	# The whole "hello world" at pack_pos 0 and the delta on it at 1.
	cluster_setup() {
		printf 'put %s blob 11\nhello world' "$OID" | "$HELPER" "$1" >/dev/null 2>&1
		printf "put-raw %s blob 4 %s 12\n$DELTA_Z" "$DELTA_OID" "$OID" \
			| "$HELPER" "$1" >/dev/null 2>&1
		sqlite3 "$1/sqlite.db" "
			INSERT INTO pack_content(pack_pos, type, size, base, content)
			  SELECT iif(base IS NULL, 0, 1), type, size, base, data FROM objects;
			UPDATE objects SET pack_pos = iif(base IS NULL, 0, 1), data = X'';"
	}
	# Store "hello world" whole under the oid $2, replacing how it is stored.
	put_whole() {
		{ printf 'put-raw %s blob 11 0 %s 1\n' "$2" "$RAW_LEN"
		  tail -c "$RAW_LEN" "$TMPDIR/raw-whole"; } | "$HELPER" "$1" >/dev/null 2>&1
	}
	CDB="$TMPDIR/cluster-test"
	cluster_setup "$CDB"
	cluster_get() { printf 'get %s\n' "$1" | "$HELPER" "$CDB" 2>/dev/null | tail -1; }

	check "clustered: a whole object reads from pack_content" \
		'test "$(cluster_get "$OID")" = "hello world"'
	check "clustered: a delta reads from pack_content" \
		'test "$(cluster_get "$DELTA_OID")" = "hello world"'
	printf 'get-raw %s\n' "$DELTA_OID" | "$HELPER" "$CDB" >"$TMPDIR/raw-cluster" 2>/dev/null
	check "clustered: get-raw hands back the clustered bytes" \
		'cmp -s "$TMPDIR/raw-cluster" "$TMPDIR/raw-delta.expect"'

	# Re-store the base; the delta still reads its own clustered bytes.
	put_whole "$CDB" "$OID"
	check "clustered: a replaced object leaves pack_content" \
		'test "$(sqlite3 "$CDB/sqlite.db" "SELECT pack_pos IS NULL AND length(data) > 0 FROM objects WHERE oid = unhex('\''$OID'\'')")" = 1'
	check "clustered: a delta on a replaced base still resolves" \
		'test "$(cluster_get "$DELTA_OID")" = "hello world"'

	printf 'optimize\n' | "$HELPER" "$CDB" >/dev/null 2>&1
	check "clustered: optimize drops only the rows no object points at" \
		'test "$(sqlite3 "$CDB/sqlite.db" "SELECT group_concat(pack_pos) FROM pack_content")" = 1'
	printf 'prune\n%s\n\noptimize\n' "$DELTA_OID" | "$HELPER" "$CDB" >/dev/null 2>&1
	check "clustered: the row of a pruned object goes at the next optimize" \
		'test "$(sqlite3 "$CDB/sqlite.db" "SELECT count(*) FROM pack_content")" = 0'

	# A delta re-stored whole reads as the whole object, not as its old delta.
	CDB="$TMPDIR/cluster-switch-test"
	cluster_setup "$CDB"
	put_whole "$CDB" "$DELTA_OID"
	check "clustered: a delta re-stored whole reads whole" \
		'test "$(printf "info %s\n" "$DELTA_OID" | "$HELPER" "$CDB" 2>/dev/null)" = "blob 11" &&
		 test "$(cluster_get "$DELTA_OID")" = "hello world"'
fi

# ---- prune ----
# Candidates go when last written at or before the expiry, bases once no
# object stored against them is left; the reply names what went, then "ok".
PRDB="$TMPDIR/prune-test"
prune_setup() {
	printf 'put %s blob 11\nhello world' "$OID" | "$HELPER" "$PRDB" >/dev/null 2>&1
	printf "put-raw %s blob 4 %s 12\n$DELTA_Z" "$DELTA_OID" "$OID" \
		| "$HELPER" "$PRDB" >/dev/null 2>&1
}
prune_setup
PR_BASE=$(printf 'prune\n%s\n\n' "$OID" | "$HELPER" "$PRDB" 2>/dev/null)
check "prune: keeps a base an object is still stored against" \
	'test "$PR_BASE" = "ok" &&
	 test "$(printf "have %s\n" "$OID" | "$HELPER" "$PRDB" 2>/dev/null)" = true'
PR_OLD=$(printf 'prune 0\n%s\n\n' "$DELTA_OID" | "$HELPER" "$PRDB" 2>/dev/null)
check "prune: keeps an object written after the expiry" \
	'test "$PR_OLD" = "ok"'
PR_DRY=$(printf 'prune --dry-run\n%s\n%s\n\n' "$OID" "$DELTA_OID" | "$HELPER" "$PRDB" 2>/dev/null)
check "prune --dry-run: names the chain it would remove" \
	'test "$(echo "$PR_DRY" | sort | tr "\n" " ")" = "$(printf "%s\n%s\nok\n" "$OID" "$DELTA_OID" | sort | tr "\n" " ")"'
check "prune --dry-run: removes nothing" \
	'test "$(printf "have %s\n" "$DELTA_OID" | "$HELPER" "$PRDB" 2>/dev/null)" = true'
PR_ALL=$(printf 'prune\n%s\n%s\n\n' "$OID" "$DELTA_OID" | "$HELPER" "$PRDB" 2>/dev/null)
check "prune: removes a delta and then its base" \
	'test "$PR_ALL" = "$PR_DRY" &&
	 test "$(printf "have %s\nhave %s\n" "$OID" "$DELTA_OID" | "$HELPER" "$PRDB" 2>/dev/null | tr "\n" " ")" = "false false "'

# ---- reachability bitmap ----
# put-bitmap stores the .idx, .rev and .bitmap git sends, verbatim, replacing
# any stored before; get-bitmap hands them back as they came.
BDB="$TMPDIR/bitmap-test"
check "get-bitmap: missing when none is stored" \
	'test "$(printf "get-bitmap\n" | "$HELPER" "$BDB" 2>/dev/null)" = "missing"'
PB=$(printf 'put-bitmap 1234 3 0 5\nidxbmbmb' | "$HELPER" "$BDB" 2>/dev/null)
check "put-bitmap: acks ok" \
	'test "$PB" = "ok"'
check "get-bitmap: returns the sizes and the bytes put-bitmap stored" \
	'test "$(printf "get-bitmap\n" | "$HELPER" "$BDB" 2>/dev/null)" = "$(printf "1234 3 0 5\nidxbmbmb")"'
# The payload is read to its end, so the next command follows in sync.
PB2=$(printf 'put-bitmap 99 2 3 1\nIIRRRBget-bitmap\n' | "$HELPER" "$BDB" 2>/dev/null)
check "put-bitmap: replaces the stored bitmap and keeps the stream in sync" \
	'test "$PB2" = "$(printf "ok\n99 2 3 1\nIIRRRB")"'
check "remove-bitmap: acks ok and leaves none" \
	'test "$(printf "remove-bitmap\nget-bitmap\n" | "$HELPER" "$BDB" 2>/dev/null)" = "$(printf "ok\nmissing")"'

# ---- commit-graph generation (store + serve the generation VALUE) ----
# git computes generation numbers while writing its commit-graph; the helper
# stores them (store-commit-graph, clearing the prior set) and serves them on
# demand (commit-generation), with NO commit-graph file. This drives that DB-serve
# path directly: t0620 covers only the testgit file-fallback (testgit has no graph
# capability), so the stored-generation VALUE is otherwise unasserted.
GDB="$TMPDIR/cg-test"
CGA=cccccccccccccccccccccccccccccccccccccccc
CGB=dddddddddddddddddddddddddddddddddddddddd
CGACK=$(printf 'store-commit-graph\n%s 42\n%s 7\n\n' "$CGA" "$CGB" | "$HELPER" "$GDB" 2>/dev/null | tail -1)
check "store-commit-graph: acks ok" 'test "$CGACK" = "ok"'
check "commit-generation: serves the stored generation value" \
	'test "$(printf "commit-generation %s\n" "$CGA" | "$HELPER" "$GDB" 2>/dev/null | tail -1)" = "42"'
check "commit-generation: serves each commit independently" \
	'test "$(printf "commit-generation %s\n" "$CGB" | "$HELPER" "$GDB" 2>/dev/null | tail -1)" = "7"'
check "commit-generation: none for an unknown commit" \
	'test "$(printf "commit-generation %s\n" "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" | "$HELPER" "$GDB" 2>/dev/null | tail -1)" = "none"'
# A re-store clears the prior set first (stale commits drop), as put-bitmap replaces.
printf 'store-commit-graph\n%s 99\n\n' "$CGA" | "$HELPER" "$GDB" >/dev/null 2>&1
check "store-commit-graph: re-store drops a commit no longer in the set" \
	'test "$(printf "commit-generation %s\n" "$CGB" | "$HELPER" "$GDB" 2>/dev/null | tail -1)" = "none"'
check "store-commit-graph: re-store updates a kept commit" \
	'test "$(printf "commit-generation %s\n" "$CGA" | "$HELPER" "$GDB" 2>/dev/null | tail -1)" = "99"'

# list-objects
LIST=$(printf 'capabilities\nlist-objects\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep "$OID")

check "list-objects: includes stored object" \
	'echo "$LIST" | grep -q "blob 11"'

# list-objects <prefix>: scope server-side to a hex oid prefix (the path git's
# abbreviation/disambiguation drives). A matching prefix yields the object; a
# non-matching one yields nothing.
# grep -c exits non-zero on zero matches (the MISS case); tolerate it under set -e.
LIST_HIT=$(printf 'capabilities\nlist-objects %s\n' "${OID%${OID#??????}}" \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "$OID" || true)
LIST_MISS=$(printf 'capabilities\nlist-objects ffffff\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "$OID" || true)

check "list-objects prefix: matching prefix yields the object" \
	'test "$LIST_HIT" -eq 1'
check "list-objects prefix: non-matching prefix yields nothing" \
	'test "$LIST_MISS" -eq 0'

# An odd-length prefix exercises the dangling-nibble range floor/ceiling
# (a distinct carry path in the hex->byte-range conversion).
LIST_ODD=$(printf 'capabilities\nlist-objects %s\n' "${OID%${OID#?????}}" \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "$OID" || true)
check "list-objects prefix: odd-length prefix matches" \
	'test "$LIST_ODD" -eq 1'

# put-stream: the caller (normally git) owns the hash and sends the oid as a
# trailer after the payload; the helper stores under it without hashing, then
# echoes it as the ack. 95d09f2b... is the canonical blob oid of "hello world".
STREAM_OID=95d09f2b10159347eece71399a7e2e907ea3df4f
STREAM_ECHO=$(printf 'capabilities\nput-stream blob 11\nhello world%s\n' "$STREAM_OID" \
	| "$HELPER" "$TMPDIR/stream-test" 2>/dev/null | tail -1)

check "put-stream: echoes the caller's oid" \
	'test "$STREAM_ECHO" = "$STREAM_OID"'

STREAM_GET=$(printf 'capabilities\nget %s\n' "$STREAM_OID" \
	| "$HELPER" "$TMPDIR/stream-test" 2>/dev/null | tail -1)

check "put-stream: content is readable" \
	'test "$STREAM_GET" = "hello world"'

# ---- Ref operations ----

DB="$TMPDIR/ref-test"
REF_OID="95d09f2b10159347eece71399a7e2e907ea3df4f"

# Store an object first so the ref target exists
printf 'capabilities\nput %s blob 11\nhello world' "$REF_OID" \
	| "$HELPER" "$DB" >/dev/null 2>&1

# Transaction: create ref
TXN=$(printf 'capabilities\ntransaction-begin\ntransaction-update refs/heads/main %s\ntransaction-prepare\ntransaction-finish\n' \
	"$REF_OID" | "$HELPER" "$DB" 2>/dev/null)

check "transaction: all steps return ok" \
	'echo "$TXN" | grep -c "^ok$" | grep -q 4'

# Read ref
READ=$(printf 'capabilities\nread refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "read: returns stored oid" \
	'test "$READ" = "$REF_OID"'

# Read missing ref
MISSING=$(printf 'capabilities\nread refs/heads/nonexistent\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "read: returns missing for unknown ref" \
	'test "$MISSING" = "missing"'

# Symref
printf 'capabilities\ntransaction-begin\ntransaction-create-symref HEAD refs/heads/main\ntransaction-prepare\ntransaction-finish\n' \
	| "$HELPER" "$DB" >/dev/null 2>&1

SYM=$(printf 'capabilities\nread HEAD\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "symref: HEAD points to refs/heads/main" \
	'test "$SYM" = "symref refs/heads/main"'

# List refs
LIST_REFS=$(printf 'capabilities\nlist\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "refs/\|HEAD")

check "list: returns stored refs" \
	'test "$LIST_REFS" -ge 2'

# List with prefix
LIST_PREFIX=$(printf 'capabilities\nlist refs/heads/\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "refs/heads/")

check "list prefix: filters by prefix" \
	'test "$LIST_PREFIX" -eq 1'

# Delete ref
printf 'capabilities\ntransaction-begin\ntransaction-delete refs/heads/main\ntransaction-prepare\ntransaction-finish\n' \
	| "$HELPER" "$DB" >/dev/null 2>&1

DELETED=$(printf 'capabilities\nread refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "delete: ref is gone after delete" \
	'test "$DELETED" = "missing"'

# ---- Reflog operations ----

DB="$TMPDIR/reflog-test"
ZERO="0000000000000000000000000000000000000000"
OID1="95d09f2b10159347eece71399a7e2e907ea3df4f"

# reflog-exists (false initially)
EXISTS=$(printf 'capabilities\nreflog-exists refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "reflog-exists: false initially" \
	'test "$EXISTS" = "false"'

# reflog-append
printf 'capabilities\nreflog-append refs/heads/main %s %s Test <test@example.com> 1234567890 +0000\tinitial\n' \
	"$ZERO" "$OID1" | "$HELPER" "$DB" >/dev/null 2>&1

EXISTS2=$(printf 'capabilities\nreflog-exists refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "reflog-exists: true after append" \
	'test "$EXISTS2" = "true"'

# reflog-read
REFLOG=$(printf 'capabilities\nreflog-read refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep "$OID1")

check "reflog-read: contains appended entry" \
	'echo "$REFLOG" | grep -q "initial"'

# Append a second entry
OID2="a8360ae6ad7e29a39e825abb0424e72cdab2bbce"
printf 'capabilities\nreflog-append refs/heads/main %s %s Test <test@example.com> 1234567891 +0000\tsecond\n' \
	"$OID1" "$OID2" | "$HELPER" "$DB" >/dev/null 2>&1

# reflog-read-reverse (newest first)
FIRST_REV=$(printf 'capabilities\nreflog-read-reverse refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep "second\|first" | head -1)

check "reflog-read-reverse: newest entry first" \
	'echo "$FIRST_REV" | grep -q "second"'

# reflog-list
printf 'capabilities\nreflog-append refs/heads/other %s %s Test <test@example.com> 1234567892 +0000\tother\n' \
	"$ZERO" "$OID1" | "$HELPER" "$DB" >/dev/null 2>&1

REFLOG_LIST=$(printf 'capabilities\nreflog-list\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "refs/heads/")

check "reflog-list: lists refs with reflogs" \
	'test "$REFLOG_LIST" -eq 2'

# reflog-delete
printf 'capabilities\nreflog-delete refs/heads/main\n' \
	| "$HELPER" "$DB" >/dev/null 2>&1

EXISTS3=$(printf 'capabilities\nreflog-exists refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "reflog-delete: removes reflog" \
	'test "$EXISTS3" = "false"'

# reflog-create: a reflog can exist without entries, and entries appended to
# it later read back alone.
CREATE=$(printf 'capabilities\nreflog-create refs/heads/empty\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)
EXISTS_EMPTY=$(printf 'capabilities\nreflog-exists refs/heads/empty\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)
READ_EMPTY=$(printf 'capabilities\nreflog-read refs/heads/empty\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "^$ZERO \|^$OID1 " || true)
LISTED_EMPTY=$(printf 'capabilities\nreflog-list\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "^refs/heads/empty$" || true)

check "reflog-create: replies ok" \
	'test "$CREATE" = "ok"'
check "reflog-create: the empty reflog exists" \
	'test "$EXISTS_EMPTY" = "true"'
check "reflog-create: the empty reflog reads no entries" \
	'test "$READ_EMPTY" -eq 0'
check "reflog-create: the empty reflog is listed" \
	'test "$LISTED_EMPTY" -eq 1'

printf 'capabilities\nreflog-create refs/heads/empty\nreflog-append refs/heads/empty %s %s Test <test@example.com> 1234567893 +0000\tfirst\n' \
	"$ZERO" "$OID1" | "$HELPER" "$DB" >/dev/null 2>&1
READ_ONE=$(printf 'capabilities\nreflog-read refs/heads/empty\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep -c "^$ZERO $OID1 " || true)

check "reflog-create: entries appended later read back alone" \
	'test "$READ_ONE" -eq 1'

# ---- ODB transactions ----

DB="$TMPDIR/txn-test"

# odb-transaction-begin/commit bracket object ingestion within ONE helper
# session (git holds the helper process open across a fetch); they are not
# independent per-invocation commands. Splitting them across two processes
# would roll the begin's savepoint back on exit and leave the commit with
# nothing to release. Drive them in a single session and confirm an object
# written inside the transaction is durable after the commit.
TXN=$(printf 'capabilities\nodb-transaction-begin\nput %s blob 11\nhello world\nodb-transaction-commit\n' \
	"95d09f2b10159347eece71399a7e2e907ea3df4f" \
	| "$HELPER" "$DB" 2>/dev/null)

check "odb-transaction: begin and commit both ack ok in one session" \
	'test "$(printf "%s\n" "$TXN" | grep -c "^ok$")" -eq 2'

TXN_HAVE=$(printf 'capabilities\nhave %s\n' \
	"95d09f2b10159347eece71399a7e2e907ea3df4f" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "odb-transaction: object written in the transaction persists after commit" \
	'test "$TXN_HAVE" = "true"'

# ---- Missing capabilities: create, remove, transaction-abort ----

DB="$TMPDIR/misc-test"

CREATE_OK=$(printf 'capabilities\ncreate\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "create: returns ok" \
	'test "$CREATE_OK" = "ok"'

# Store something so remove has work to do
printf 'capabilities\nput %s blob 5\nhello' \
	"aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d" \
	| "$HELPER" "$DB" >/dev/null 2>&1

REMOVE_OK=$(printf 'capabilities\nremove\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "remove: returns ok" \
	'test "$REMOVE_OK" = "ok"'

# transaction-abort: begin, update, abort, verify ref was not created
DB="$TMPDIR/abort-test"
REF_OID="95d09f2b10159347eece71399a7e2e907ea3df4f"

printf 'capabilities\ntransaction-begin\ntransaction-update refs/heads/aborted %s\ntransaction-abort\n' \
	"$REF_OID" | "$HELPER" "$DB" >/dev/null 2>&1

ABORTED=$(printf 'capabilities\nread refs/heads/aborted\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "transaction-abort: ref not created after abort" \
	'test "$ABORTED" = "missing"'

# ---- Contract: CAS old-value preconditions ----
# The helper must enforce the old-value the backend forwards (it is the only
# race-free place to check). A stale expected value must be rejected and leave
# the ref unchanged; the correct value must succeed.

DB="$TMPDIR/cas-test"
CAS_A=1111111111111111111111111111111111111111
CAS_B=2222222222222222222222222222222222222222

printf 'capabilities\ntransaction-begin\ntransaction-create refs/heads/main %s\ntransaction-prepare\ntransaction-finish\n' "$CAS_A" \
	| "$HELPER" "$DB" >/dev/null 2>&1

CAS_BAD=$(printf 'capabilities\ntransaction-begin\ntransaction-update refs/heads/main %s %s\ntransaction-abort\n' "$CAS_B" "$CAS_B" \
	| "$HELPER" "$DB" 2>/dev/null)

check "CAS: update with a stale old value is rejected" \
	'echo "$CAS_BAD" | grep -q "^error"'

CAS_VAL=$(printf 'capabilities\nread refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "CAS: ref unchanged after the rejected update" \
	'test "$CAS_VAL" = "$CAS_A"'

printf 'capabilities\ntransaction-begin\ntransaction-update refs/heads/main %s %s\ntransaction-prepare\ntransaction-finish\n' "$CAS_B" "$CAS_A" \
	| "$HELPER" "$DB" >/dev/null 2>&1
CAS_VAL2=$(printf 'capabilities\nread refs/heads/main\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "CAS: update with the correct old value succeeds" \
	'test "$CAS_VAL2" = "$CAS_B"'

# ---- Contract: a failed write is reported as error, never a false ack ----
# Silent-data-loss guard. When the backing store cannot persist a write, the
# helper must reply `error`, not echo the oid: git advances its state over an
# ack, so a false ack on a failed COMMIT/INSERT loses data across the IPC
# boundary. (The bug this guards: every mutator ignored the SQLite step/commit
# return code and the helper acked unconditionally.) Induce a real failure with
# a read-only store directory, so BEGIN IMMEDIATE cannot create its rollback
# journal. chmod cannot constrain root, so skip as root (the files backend's own
# permission tests use the same root guard).
if [ "$(id -u)" = 0 ] || ! command -v sqlite3 >/dev/null 2>&1; then
	ok "write failure reported as error (skipped: needs non-root + sqlite3)"
else
	RODB="$TMPDIR/ro-store"
	printf 'capabilities\n' | "$HELPER" "$RODB" >/dev/null 2>&1
	# Convert WAL -> rollback first: a WAL database cannot be opened where it
	# cannot create its -shm file, so the read-only reopen would fail at open
	# (before the put) rather than at the write we mean to exercise.
	sqlite3 "$RODB/sqlite.db" "PRAGMA journal_mode=DELETE;" >/dev/null 2>&1
	chmod 555 "$RODB"
	RO_RESP=$(printf 'capabilities\nput %s blob 4\ntest' \
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
		| "$HELPER" "$RODB" 2>/dev/null | tail -1)
	chmod 755 "$RODB"
	check "write failure is reported as error, not a false oid ack" \
		'test "$RO_RESP" != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" && echo "$RO_RESP" | grep -q "^error"'
fi

# ---- Contract: promisor OID-list command ----
# "promisor" reads OID lines until a blank line, marks each, and acks once.

DB="$TMPDIR/promisor-test"
# b6fc4c62... is the canonical blob oid of "hello"; sent as the put-stream trailer.
POID=b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0
printf 'capabilities\nput-stream blob 5\nhello%s\n' "$POID" \
	| "$HELPER" "$DB" >/dev/null 2>&1
PROM=$(printf 'capabilities\npromisor\n%s\n\n' "$POID" \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "promisor: OID-list command acks ok" \
	'test "$PROM" = "ok"'

PLIST=$(printf 'capabilities\nlist-objects --promisor-only\n' \
	| "$HELPER" "$DB" 2>/dev/null)

check "promisor: marked object appears in --promisor-only listing" \
	'echo "$PLIST" | grep -q "$POID"'

# ---- optimize (A3 maintenance: VACUUM) ----

OPT=$(printf 'capabilities\noptimize\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "optimize: command acks ok (VACUUM)" \
	'test "$OPT" = "ok"'

# ---- verify (A3: PRAGMA integrity_check) ----

VFY=$(printf 'capabilities\nverify\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "verify: command acks ok (integrity_check)" \
	'test "$VFY" = "ok"'

# ---- LFS transfer adapter ----

DB="$TMPDIR/lfs-test"

# Upload via LFS protocol
UPLOAD_RESP=$(printf '{"event":"init","operation":"upload","concurrent":true}\n{"event":"upload","oid":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0,"path":"/dev/null"}\n{"event":"terminate"}\n' \
	| "$LFS_TRANSFER" "$DB" 2>/dev/null)

check "lfs upload: completes without error" \
	'echo "$UPLOAD_RESP" | grep -q "\"event\":\"complete\""'

check "lfs upload: no error field" \
	'! echo "$UPLOAD_RESP" | grep -q "\"error\""'

# Download via LFS protocol
DOWNLOAD_RESP=$(printf '{"event":"init","operation":"download","concurrent":true}\n{"event":"download","oid":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0}\n{"event":"terminate"}\n' \
	| "$LFS_TRANSFER" "$DB" 2>/dev/null)

check "lfs download: completes with path" \
	'echo "$DOWNLOAD_RESP" | grep -q "\"path\""'

# Download missing oid
DOWNLOAD_MISSING=$(printf '{"event":"init","operation":"download","concurrent":true}\n{"event":"download","oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":0}\n{"event":"terminate"}\n' \
	| "$LFS_TRANSFER" "$DB" 2>/dev/null)

check "lfs download: 404 for missing oid" \
	'echo "$DOWNLOAD_MISSING" | grep -q "\"code\":404"'

# Bad oid validation
BAD_OID=$(printf '{"event":"init","operation":"download","concurrent":true}\n{"event":"download","oid":"notahex","size":0}\n{"event":"terminate"}\n' \
	| "$LFS_TRANSFER" "$DB" 2>/dev/null)

check "lfs download: 400 for bad oid" \
	'echo "$BAD_OID" | grep -q "\"code\":400"'

# Multi-frame round-trip: an object larger than one 4 MiB frame must split into
# several lfs_chunk rows on upload and reassemble byte-for-byte on download.
LFS_BIG="$TMPDIR/lfs-big.bin"
dd if=/dev/urandom of="$LFS_BIG" bs=1048576 count=5 2>/dev/null  # 5 MiB: spans two frames
LFS_BIG_OID=$(sha256sum "$LFS_BIG" | cut -d' ' -f1)
LFS_BIG_DB="$TMPDIR/lfs-big-db"
printf '{"event":"init","operation":"upload","concurrent":true}\n{"event":"upload","oid":"%s","size":5242880,"path":"%s"}\n{"event":"terminate"}\n' "$LFS_BIG_OID" "$LFS_BIG" \
	| "$LFS_TRANSFER" "$LFS_BIG_DB" >/dev/null 2>&1
LFS_BIG_DL=$(printf '{"event":"init","operation":"download","concurrent":true}\n{"event":"download","oid":"%s","size":5242880}\n{"event":"terminate"}\n' "$LFS_BIG_OID" \
	| "$LFS_TRANSFER" "$LFS_BIG_DB" 2>/dev/null)
LFS_BIG_PATH=$(echo "$LFS_BIG_DL" | grep -o '"path":"[^"]*"' | head -1 | cut -d'"' -f4)

check "lfs multi-frame: a >4 MiB object round-trips across chunks intact" \
	'test -n "$LFS_BIG_PATH" && test "$(sha256sum "$LFS_BIG_PATH" | cut -d" " -f1)" = "$LFS_BIG_OID"'

# ---- Summary ----

TOTAL=$((PASS + FAIL))
echo ""
echo "# $PASS/$TOTAL tests passed"
if [ "$FAIL" -gt 0 ]; then
	echo "# $FAIL FAILED"
	exit 1
fi
