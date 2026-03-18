#!/bin/sh
#
# Test suite for git-local-sqlite and git-lfs-sqlite-transfer.
#
# Exercises the local helper protocol (18 commands), LFS transfer
# adapter, and the git-remote-sqlite transport. Each test creates
# a fresh database in a temp directory and cleans up after itself.
#
# Usage: ./tests/test_helper.sh
#   Requires: git-sqlite, git-lfs-sqlite-transfer built in the repo root.

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
test -x "$BINDIR/git-sqlite" || { echo "git-sqlite not built"; exit 1; }
test -x "$LFS_TRANSFER" || { echo "git-lfs-sqlite-transfer not built"; exit 1; }

# ---- Capabilities ----

CAP=$(printf 'capabilities\n' | "$HELPER" "$TMPDIR/cap-test" 2>/dev/null)

check "capabilities: lists get" \
	'echo "$CAP" | grep -q "^get$"'

check "capabilities: lists put-stream" \
	'echo "$CAP" | grep -q "^put-stream$"'

check "capabilities: lists reflog-list" \
	'echo "$CAP" | grep -q "^reflog-list$"'

check "capabilities: lists reflog-read-reverse" \
	'echo "$CAP" | grep -q "^reflog-read-reverse$"'

check "capabilities: 18 total" \
	'test "$(echo "$CAP" | grep -c .)" -eq 18'

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

# list-objects
LIST=$(printf 'capabilities\nlist-objects\n' \
	| "$HELPER" "$DB" 2>/dev/null | grep "$OID")

check "list-objects: includes stored object" \
	'echo "$LIST" | grep -q "blob 11"'

# put-stream
STREAM_OID=$(printf 'capabilities\nput-stream blob 11\nhello world' \
	| "$HELPER" "$TMPDIR/stream-test" 2>/dev/null | tail -1)

check "put-stream: returns correct oid" \
	'test "$STREAM_OID" = "95d09f2b10159347eece71399a7e2e907ea3df4f"'

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

# ---- ODB transactions ----

DB="$TMPDIR/txn-test"

TXN_BEGIN=$(printf 'capabilities\nodb-transaction-begin\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "odb-transaction-begin: returns ok" \
	'test "$TXN_BEGIN" = "ok"'

TXN_COMMIT=$(printf 'capabilities\nodb-transaction-commit\n' \
	| "$HELPER" "$DB" 2>/dev/null | tail -1)

check "odb-transaction-commit: returns ok" \
	'test "$TXN_COMMIT" = "ok"'

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

# ---- argv[0] dispatch ----

check "git-sqlite: rejects unknown argv[0]" \
	'"$BINDIR/git-sqlite" 2>&1 | grep -q "invoke as"'

# ---- Summary ----

TOTAL=$((PASS + FAIL))
echo ""
echo "# $PASS/$TOTAL tests passed"
if [ "$FAIL" -gt 0 ]; then
	echo "# $FAIL FAILED"
	exit 1
fi
