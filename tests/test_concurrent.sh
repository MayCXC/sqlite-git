#!/bin/sh
#
# Concurrent-cursor regression tests for the git0 SQLite extension.
#
# git0_repo_open() cached one repo handle in a process-global slot, so a TVF
# cursor scanning repo A had its handle freed the instant another cursor opened
# repo B (any cross-repo join or nested scan), and the first cursor then
# dereferenced freed memory. Each cursor now opens and frees its own handle.
#
# The count assertions below catch the corruption directly (the buggy code
# returned wrong counts). Run under ASan (make test-asan) to also catch the
# use-after-free as a memory error.
#
# Usage: ./tests/test_concurrent.sh   (requires the git0 extension built in the
# repo root). Set SQLITE3 to override the sqlite3 invocation, e.g. to preload
# the ASan runtime when the extension is sanitizer-instrumented.

set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXT="${GIT0:-$ROOT/git0}"
test -f "$EXT.so" || test -f "$EXT.dylib" || {
	echo "$EXT extension not built"; exit 1; }

SQLITE3="${SQLITE3:-sqlite3}"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
PASS=0
FAIL=0

mkrepo() {
	d="$1"; n="$2"
	mkdir -p "$d"
	git -C "$d" init -q
	git -C "$d" config user.email test@example.invalid
	git -C "$d" config user.name test
	i=0
	while [ "$i" -lt "$n" ]; do
		echo "line $i" >> "$d/file.txt"
		git -C "$d" add file.txt
		git -C "$d" commit -qm "commit $i"
		i=$((i + 1))
	done
}

check() { # description  sql  expected
	rm -f "$TMP/db"
	got=$($SQLITE3 "$TMP/db" -cmd ".load $EXT" "$2" 2>/dev/null)
	if [ "$got" = "$3" ]; then
		PASS=$((PASS + 1))
		printf 'ok %d - %s (= %s)\n' "$PASS" "$1" "$got"
	else
		FAIL=$((FAIL + 1))
		printf 'not ok - %s (got "%s", want "%s")\n' "$1" "$got" "$3"
	fi
}

mkrepo "$TMP/A" 3
mkrepo "$TMP/B" 4

# Two git_log cursors on different repos open at once: 3 commits x 4 commits.
check "git_log cross-repo join" \
	"SELECT count(*) FROM git_log('$TMP/A') a JOIN git_log('$TMP/B') b;" \
	12

# Correlated nested scan across repos: repo A re-opened for each of B's 4 rows.
check "git_log nested cross-repo scan" \
	"SELECT count(*) FROM git_log('$TMP/B') b WHERE (SELECT count(*) FROM git_log('$TMP/A') a) = 3;" \
	4

# A different pair of TVF cursor types, concurrently, on different repos. A
# fresh repo has exactly one ref (its default branch), so 1 x 4.
check "git_refs x git_log cross-repo join" \
	"SELECT count(*) FROM git_refs('$TMP/A') r JOIN git_log('$TMP/B') l;" \
	4

echo "# $PASS/$((PASS + FAIL)) concurrent-cursor tests passed"
[ "$FAIL" -eq 0 ]
