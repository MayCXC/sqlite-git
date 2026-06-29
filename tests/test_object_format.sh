#!/bin/sh
#
# Object-format support, run against each build. git0_init chooses the repo's
# hash like git init --object-format: sha1 always works (40 hex oids); sha256
# works only on a build linked against the experimental libgit2 (64 hex) and is
# cleanly rejected otherwise. Once chosen, every git0 function hashes under it.
#
# Usage: GIT0=<build-dir>/git0 sh tests/test_object_format.sh

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

# Each call runs on a fresh db so git0_init starts uninitialized.
on_fresh_db() {
	rm -f "$TMP/db"
	$SQLITE3 "$TMP/db" -cmd ".load $EXT" "$1" 2>&1
}

# sha1 is the default and is available in every build.
eq "default init hashes sha1 (40 hex)" \
	"$(on_fresh_db "SELECT length(git0_init())")" "40"
eq "explicit object-format sha1 (40 hex)" \
	"$(on_fresh_db "SELECT length(git0_init('sha1'))")" "40"

# sha256 is build-conditional: a sha256-capable build hashes every object to 64
# hex; a sha1-only build rejects the request rather than mislabel sha1 oids.
if [ "$(on_fresh_db "SELECT length(git0_init('sha256'))")" = "64" ]; then
	eq "sha256 init/add/mktree/mkcommit all hash to 64 hex" "$(on_fresh_db "
		SELECT length(git0_init('sha256'));
		SELECT length(git0_add('f', 'hi'));
		SELECT length(git0_mktree('100644 f ' || git0_add('f', 'hi')));
		SELECT length(git0_mkcommit(git0_mktree('100644 f ' || git0_add('f', 'hi')), NULL, 'm'));
	")" "64
64
64
64"
else
	case "$(on_fresh_db "SELECT git0_init('sha256')")" in
	*"experimental build"*)
		PASS=$((PASS + 1)); echo "ok - sha256 cleanly rejected on a sha1-only build" ;;
	*)
		FAIL=$((FAIL + 1)); echo "NOT ok - sha256 rejection message" ;;
	esac
fi

echo "# object-format: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
