#!/bin/sh
#
# Writable virtual tables (git0_objects / git0_refs), run against each build. They
# route every read and write through the storage_git object/ref API (the same
# store the scalars and the libgit2 backend use), so INSERT/SELECT/UPDATE/DELETE
# over SQL operate on the real object/ref store, not a divergent copy. Objects are
# content-addressed (the oid is computed from type + data); refs are keyed on the
# refname, with DELETE/UPDATE resolving the SQLite rowid back to the refname.
#
# Usage: GIT0=<build-dir>/git0 sh tests/test_vtab.sh

set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXT="${GIT0:-$ROOT/build/stock/git0}"
test -f "$EXT.so" || test -f "$EXT.dylib" || {
	echo "$EXT extension not built"; exit 1; }
SQLITE3="${SQLITE3:-sqlite3}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

PASS=0
FAIL=0
eq() {
	if [ "$2" = "$3" ]; then
		PASS=$((PASS + 1)); echo "ok - $1"
	else
		FAIL=$((FAIL + 1)); echo "NOT ok - $1 (got '$2', want '$3')"
	fi
}

# Fresh db per assertion; the vtab xConnect binds storage and creates the git
# schema, so no git0_init is needed. The asserted value is the final statement's
# output (CREATE/INSERT/DELETE print nothing).
session() { rm -f "$TMP/db"; $SQLITE3 "$TMP/db" -cmd ".load $EXT" "$1" 2>&1 | tail -1; }

OBJ="CREATE VIRTUAL TABLE o USING git0_objects;
INSERT INTO o(type, data) VALUES('blob', 'hello vtab');"

# An INSERT is content-addressed: the oid equals git_hash of the same bytes (both
# default to sha1 here), the data inflates back, and the size is the blob length.
eq "object INSERT computes the content-addressed oid" \
	"$(session "$OBJ SELECT (SELECT oid FROM o WHERE size=10) = git_hash('hello vtab','blob');")" "1"
eq "object SELECT data round-trips (inflated + delta-resolved)" \
	"$(session "$OBJ SELECT CAST((SELECT data FROM o WHERE size=10) AS TEXT);")" "hello vtab"
eq "object SELECT size" \
	"$(session "$OBJ SELECT size FROM o WHERE type='blob';")" "10"
# DELETE is rejected: libgit2's odb has no object delete (removal is gc/prune).
eq "object DELETE is rejected (immutable; gc/prune's domain)" \
	"$(session "$OBJ DELETE FROM o WHERE size=10;" | grep -c 'gc/prune')" "1"
eq "object UPDATE is rejected (content-addressed, immutable)" \
	"$(session "$OBJ UPDATE o SET type='tree' WHERE size=10;" | grep -c 'INSERT a new object')" "1"

# Writes route through libgit2 git_reference_create, which validates the target,
# so the tests use real oids: git0_init's HEAD commit and a git0_add blob oid.
REF="SELECT git0_init();
CREATE VIRTUAL TABLE r USING git0_refs;
INSERT INTO r(name, target) VALUES('refs/heads/x', (SELECT git0_ref('HEAD')));"

eq "ref INSERT (direct) target round-trips (= HEAD oid)" \
	"$(session "$REF SELECT (SELECT target FROM r WHERE name='refs/heads/x') = git0_ref('HEAD');")" "1"
eq "ref type is derived from the refname" \
	"$(session "$REF SELECT type FROM r WHERE name='refs/heads/x';")" "branch"
eq "ref UPDATE changes the target (to a git0_add blob oid)" \
	"$(session "$REF UPDATE r SET target=(SELECT git0_add('f','x')) WHERE name='refs/heads/x'; SELECT (SELECT target FROM r WHERE name='refs/heads/x') = git0_add('f','x');")" "1"
eq "ref DELETE by name removes it (rowid -> refname)" \
	"$(session "$REF DELETE FROM r WHERE name='refs/heads/x'; SELECT count(*) FROM r WHERE name='refs/heads/x';")" "0"
eq "ref INSERT (symbolic) round-trips" \
	"$(session "SELECT git0_init(); CREATE VIRTUAL TABLE r USING git0_refs; INSERT INTO r(name, symref) VALUES('HEAD','refs/heads/x'); SELECT symref FROM r WHERE name='HEAD';")" \
	"refs/heads/x"

echo "# vtab: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
