#!/bin/sh
#
# Extension reflog-on-write, run against each build. A logged direct-ref update
# through the libgit2 refdb backend (storage_refdb_write, when libgit2 supplies a
# committer) persists a reflog entry, mirroring the files backend's logs/<ref>:
# git0_init logs the initial branch creation, a git_ref_create logs another. The
# reflog table is queried directly on the borrowed connection. The git_reflog
# refdb callbacks (reflog_read/write) cannot be implemented (libgit2 exposes no
# public git_reflog name accessor / constructor), so the reflog is owned by the
# ref-write path here and by the helper face.
#
# Usage: GIT0=<build-dir>/git0 sh tests/test_reflog.sh

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

# One session on a fresh db. The side-effecting ops run as their own top-level
# SELECT statements (git0_init writes through the backend; nesting it inside a
# CREATE ... AS SELECT would abort the enclosing write); the asserted value is the
# last statement's output, so take the final line.
session() { rm -f "$TMP/db"; $SQLITE3 "$TMP/db" -cmd ".load $EXT" "$1" 2>&1 | tail -1; }

# git0_init logs exactly one creation entry for the default branch: old oid all
# zero (a creation), new oid = the initial commit, a non-empty committer.
eq "init logs one master creation entry (zero old oid)" \
	"$(session "
		SELECT git0_init();
		SELECT count(*) FROM reflog
		 WHERE refname = 'refs/heads/master'
		   AND old_oid = zeroblob(length(old_oid));
	")" "1"

eq "init reflog new oid is the branch tip" \
	"$(session "
		SELECT git0_init();
		SELECT (lower(hex(new_oid)) = git0_ref('refs/heads/master'))
		 FROM reflog WHERE refname = 'refs/heads/master';
	")" "1"

eq "init reflog records a committer" \
	"$(session "
		SELECT git0_init();
		SELECT (length(committer) > 0)
		 FROM reflog WHERE refname = 'refs/heads/master';
	")" "1"

# A git_ref_create through the libgit2 refdb backend logs its own creation entry.
eq "git_ref_create through the backend logs a reflog entry" \
	"$(session "
		SELECT git0_init();
		SELECT git_ref_create(git0_repo(), 'refs/heads/probe', git0_ref('HEAD'));
		SELECT count(*) FROM reflog WHERE refname = 'refs/heads/probe';
	")" "1"

# git0_ref_create is a thin SQL wrapper over libgit2's git_reference_create (the
# refdb backend), so it logs a reflog entry exactly like git_ref_create above,
# not a storage bypass.
eq "scalar git0_ref_create logs a reflog (libgit2 git_reference_create)" \
	"$(session "
		SELECT git0_init();
		SELECT git0_ref_create('refs/heads/scalar', git0_ref('HEAD'));
		SELECT count(*) FROM reflog WHERE refname = 'refs/heads/scalar';
	")" "1"

echo "# reflog: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
