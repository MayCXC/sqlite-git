#!/bin/sh
#
# Integration tests: a real git build driving git-local-sqlite as its object and
# ref storage helper (extensions.objectStorage=sqlite names the helper outright).
# These exercise the storage behaviors of the real helper that git's own tests
# run against the git-local-testgit stub only: delta preservation across a
# push, reachability bitmaps the helper stores and serves, kept packs, atomic
# pack ingest, gc deltification/prune/freshen, and pack-objects sending the
# deltas the helper stores.
#
# This is the sqlite-git side of the helper protocol: it depends on a git build
# (sqlite-git -> git-core, the correct direction), never the reverse. Point it at
# that build by setting GIT_BUILD in config.mak (the Makefile puts its
# bin-wrappers on PATH); against a git with no object-storage helper it skips.
#
# Run: sh tests/test_git_helper.sh   (or `make test`, which sets PATH/GIT_BUILD).

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PATH="$ROOT:$PATH"          # git spawns the build-tree git-local-sqlite by name
export PATH
GIT_TEST_DEFAULT_INITIAL_BRANCH_NAME=main   # git core resolves this name and drives the helper; main mirrors the t0620/t0621/t5801 family (git's breaking-changes default)
export GIT_TEST_DEFAULT_INITIAL_BRANCH_NAME
test -x "$ROOT/git-local-sqlite" || { echo "git-local-sqlite not built"; exit 1; }

TMP=$(mktemp -d)
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT
cd "$TMP"

# The helper repos are sha1 (git init's default), so the all-zero oid is 40 hex.
ZERO_OID=0000000000000000000000000000000000000000

# Skip cleanly when `git` here has no object-storage helper (e.g. system git):
# nothing to integrate against, not a failure.
if ! git init --object-storage=sqlite probe >/dev/null 2>&1; then
	echo "# git here has no object-storage helper (set GIT_BUILD); skipping"
	exit 0
fi
rm -rf probe

PASS=0
FAIL=0
# Run an && chain in an isolated subshell; pass iff it exits 0. On failure show
# the tail of its output (mirrors git's test_expect_success, in plain sh).
test_case() {
	if out=$(set -e; cd "$TMP" && eval "$2" 2>&1); then
		PASS=$((PASS + 1)); echo "ok - $1"
	else
		FAIL=$((FAIL + 1)); echo "NOT ok - $1"
		printf '%s\n' "$out" | tail -6 | sed 's/^/    /'
	fi
}

# git's test_commit, minimal: <name>.t holding <name>, committed and tagged.
test_commit() {
	d=.
	if [ "$1" = "-C" ]; then d="$2"; shift 2; fi
	echo "$1" >"$d/$1.t" &&
	git -C "$d" add "$1.t" &&
	git -C "$d" commit -q -m "$1" &&
	git -C "$d" tag "$1"
}

create_helper_repo() {
	git init "$1" &&
	git -C "$1" config core.repositoryFormatVersion 1 &&
	git -C "$1" config extensions.objectStorage sqlite
}

assert_no_loose() { test -z "$(find "$1/.git/objects" -type d -name '??')"; }
# git's test_cmp, minimal: the two files are byte-identical.
test_cmp() { cmp "$1" "$2"; }
no_packfile() { ! ls "$1"/.git/objects/pack/*.pack >/dev/null 2>&1; }
# A blob is stored as a delta when its delta base is a real (nonzero) oid.
has_delta_blob() {
	git -C "$1" cat-file --batch-all-objects \
		--batch-check="%(objecttype) %(deltabase)" |
		grep "^blob " | grep -qv "$ZERO_OID"
}

# --- delta preservation: a push staged as a pack migrates into the helper with
#     its delta intact (the helper stores deltas, unlike the whole-object stub).
test_case "push migrates the staged pack and preserves a delta" '
	git init pushsrc &&
	printf "%08000d\nAAAA\n" 0 >pushsrc/f.txt && git -C pushsrc add f.txt && git -C pushsrc commit -q -m one &&
	printf "%08000d\nBBBB\n" 0 >pushsrc/f.txt && git -C pushsrc add f.txt && git -C pushsrc commit -q -m two &&
	create_helper_repo pushdst &&
	git -C pushdst config transfer.unpackLimit 1 &&
	git -C pushsrc push "$PWD/pushdst" main:refs/heads/incoming &&
	git -C pushdst fsck &&
	assert_no_loose pushdst &&
	no_packfile pushdst &&
	has_delta_blob pushdst
'

# --- gc writes a reachability bitmap, as repack.writeBitmaps asks, which the
#     helper stores and serves back: git opens it from the helper, with no
#     .bitmap file on disk, and --test-bitmap verifies it reproduces a real
#     rev-list walk.
test_case "gc builds a reachability bitmap the helper stores and serves" '
	create_helper_repo bmstore &&
	git -C bmstore config repack.writeBitmaps true &&
	test_commit -C bmstore bm1 && test_commit -C bmstore bm2 && test_commit -C bmstore bm3 &&
	git -C bmstore gc &&
	! ls bmstore/.git/objects/pack/*.bitmap >/dev/null 2>&1 &&
	GIT_TRACE2_EVENT="$PWD/bm-trace" git -C bmstore rev-list --test-bitmap HEAD &&
	grep -q "\"key\":\"opened stored bitmap\"" bm-trace
'

# --- commits added after the bitmap gc are not in the stored bitmap; a bitmapped
#     walk must fill them in and match an unbitmapped walk.
test_case "reachability stays correct for commits added after the bitmap gc" '
	create_helper_repo stalebm &&
	git -C stalebm config repack.writeBitmaps true &&
	test_commit -C stalebm s1 && test_commit -C stalebm s2 &&
	git -C stalebm gc &&
	test_commit -C stalebm s3 && test_commit -C stalebm s4 &&
	git -C stalebm -c core.useBitmapIndex=false rev-list --all >expect.raw &&
	git -C stalebm rev-list --all --use-bitmap-index >actual.raw &&
	sort expect.raw >expect && sort actual.raw >actual &&
	test_cmp expect actual
'

# --- a truncated pack: unpack-objects ingests then fails the tail; the ingest
#     transaction must be discarded, leaving the helper store empty.
test_case "a failed pack ingest persists no objects in the helper store" '
	git init -q ingdonor &&
	test_commit -C ingdonor d1 && test_commit -C ingdonor d2 && test_commit -C ingdonor d3 &&
	git -C ingdonor repack -ad &&
	cp ingdonor/.git/objects/pack/pack-*.pack full.pack &&
	size=$(wc -c <full.pack) &&
	dd if=full.pack of=trunc.pack bs=$((size - 40)) count=1 2>/dev/null &&
	create_helper_repo ingrecv &&
	! git -C ingrecv unpack-objects <trunc.pack &&
	git -C ingrecv cat-file --batch-all-objects --batch-check="%(objectname)" >got &&
	test ! -s got &&
	git -C ingrecv fsck
'

# --- pack-objects, counting with the stored bitmap, sends the deltas the helper
#     stores: with no delta search (--window=0), every delta in the pack it
#     writes is one the helper stored, against the same base, and the pack is
#     byte-valid and complete.
test_case "pack-objects sends the deltas the helper stores" '
	create_helper_repo sendstore &&
	git -C sendstore config repack.writeBitmaps true &&
	i=1; while [ "$i" -le 40 ]; do seq 1 $((200 + i)) >sendstore/f$((i % 5)).txt && git -C sendstore add -A && git -C sendstore commit -q -m "c$i" || exit 1; i=$((i + 1)); done &&
	git -C sendstore gc &&
	has_delta_blob sendstore &&
	GIT_TRACE2_EVENT="$PWD/send-trace" git -C sendstore pack-objects --all --stdout --use-bitmap-index --window=0 </dev/null >send.pack &&
	grep -q "\"key\":\"opened stored bitmap\"" send-trace &&
	git index-pack send.pack &&
	test "$(git show-index <send.idx | wc -l)" -eq "$(git -C sendstore rev-list --all --objects | wc -l)" &&
	git verify-pack -v send.idx | awk "NF == 7 { print \$1, \$7 }" | sort >sent &&
	test -s sent &&
	git -C sendstore cat-file --batch-all-objects --batch-check="%(objectname) %(deltabase)" |
		grep -v " $ZERO_OID\$" | sort >stored &&
	cmp sent stored
'

# --- a pack kept by a .keep file stays in the objects directory, where prune
#     leaves its objects, while its .keep is live; once it is released, gc moves
#     it into the helper and prunes them.
test_case "prune spares a live kept pack and sweeps it once released" '
	create_helper_repo keepstore &&
	git -C keepstore config core.logAllRefUpdates false &&
	test_commit -C keepstore kbase && test_commit -C keepstore kdoomed &&
	doomed=$(git -C keepstore rev-parse HEAD:kdoomed.t) &&
	echo "$doomed" | git -C keepstore pack-objects --stdout >dpack &&
	git -C keepstore index-pack --stdin --keep=inflight <dpack &&
	keep=$(ls keepstore/.git/objects/pack/*.keep) && test -n "$keep" &&
	git -C keepstore reset --hard HEAD^ && git -C keepstore tag -d kdoomed &&
	git -C keepstore cat-file -e "$doomed" &&
	git -C keepstore gc --prune=now &&
	test -f "${keep%.keep}.pack" &&
	git -C keepstore cat-file -e "$doomed" &&
	rm "$keep" &&
	git -C keepstore gc --prune=now &&
	no_packfile keepstore &&
	! git -C keepstore cat-file -e "$doomed"
'

# --- a blob in two kept packs survives until BOTH .keep markers are released.
test_case "prune keeps an object until all its kept packs are released" '
	create_helper_repo mnstore &&
	git -C mnstore config core.logAllRefUpdates false &&
	test_commit -C mnstore mbase && test_commit -C mnstore mshared &&
	shared=$(git -C mnstore rev-parse HEAD:mshared.t) &&
	test_commit -C mnstore mx && x=$(git -C mnstore rev-parse HEAD:mx.t) &&
	test_commit -C mnstore my && y=$(git -C mnstore rev-parse HEAD:my.t) &&
	printf "%s\n%s\n" "$shared" "$x" | git -C mnstore pack-objects --stdout >packA &&
	printf "%s\n%s\n" "$shared" "$y" | git -C mnstore pack-objects --stdout >packB &&
	git -C mnstore index-pack --stdin --keep=A <packA &&
	git -C mnstore index-pack --stdin --keep=B <packB &&
	git -C mnstore reset --hard mbase && git -C mnstore tag -d mshared mx my &&
	git -C mnstore gc --prune=now && git -C mnstore cat-file -e "$shared" &&
	rm "$(ls mnstore/.git/objects/pack/*.keep | head -1)" &&
	git -C mnstore gc --prune=now && git -C mnstore cat-file -e "$shared" &&
	rm -f mnstore/.git/objects/pack/*.keep &&
	git -C mnstore gc --prune=now &&
	! git -C mnstore cat-file -e "$shared"
'

# --- prune must not orphan a reachable object by deleting a delta base it needs:
#     drop either branch, the survivor still resolves.
test_case "gc --prune keeps a delta base a reachable object still needs" '
	git init deltasrc &&
	( cd deltasrc && git checkout -b ba &&
	  printf "%08000d\nAAAA\n" 0 >f.txt && git add f.txt && git commit -q -m A &&
	  git checkout -q --orphan bb &&
	  printf "%08000d\nBBBB\n" 0 >f.txt && git add f.txt && git commit -q -m B ) &&
	for drop in ba bb; do
		if [ "$drop" = ba ]; then keep=bb; else keep=ba; fi &&
		create_helper_repo "deltastore-$drop" &&
		git -C "deltastore-$drop" config core.logAllRefUpdates false &&
		git -C "deltastore-$drop" -c transfer.unpackLimit=10000 fetch "file://$PWD/deltasrc" "refs/heads/ba:refs/heads/ba" "refs/heads/bb:refs/heads/bb" &&
		git -C "deltastore-$drop" update-ref -d "refs/heads/$drop" &&
		git -C "deltastore-$drop" gc --prune=now &&
		git -C "deltastore-$drop" fsck &&
		git -C "deltastore-$drop" cat-file blob "refs/heads/$keep:f.txt" >/dev/null || exit 1;
	done
'

# --- gc deltifies stored objects: two near-identical blobs end up with at least
#     one stored as a delta after gc (none before).
test_case "gc deltifies stored objects on the helper" '
	create_helper_repo deltifystore &&
	printf "%08000d\nfirst\n" 0 >deltifystore/f.txt && git -C deltifystore add f.txt && git -C deltifystore commit -q -m one &&
	printf "%08000d\nsecond\n" 0 >deltifystore/f.txt && git -C deltifystore add f.txt && git -C deltifystore commit -q -m two &&
	! has_delta_blob deltifystore &&
	git -C deltifystore gc &&
	git -C deltifystore fsck &&
	has_delta_blob deltifystore
'

# --- a second gc re-deltifies a store that already holds deltas: the first gc
#     stores deltas, the next ones store again objects held as deltas against
#     bases they store again too; three in a row prove the store stays
#     repackable and readable.
test_case "gc re-deltifies a store that already holds deltas" '
	create_helper_repo redeltastore &&
	i=1; while [ "$i" -le 40 ]; do seq 1 $((200 + i)) >redeltastore/f$((i % 5)).txt && git -C redeltastore add -A && git -C redeltastore commit -q -m "c$i" || exit 1; i=$((i + 1)); done &&
	git -C redeltastore gc &&
	has_delta_blob redeltastore &&
	git -C redeltastore gc &&
	git -C redeltastore gc &&
	git -C redeltastore fsck &&
	git -C redeltastore cat-file -p HEAD:f1.txt >readback &&
	test "$(wc -l <readback)" -eq 236
'

# --- gc re-represents a survivor and prunes a now-unreachable delta base, rather
#     than pinning the base forever via the delta edge.
test_case "gc unpins and prunes a delta base made unreachable" '
	git init unpinsrc &&
	( cd unpinsrc && git checkout -b ba &&
	  printf "%08000d\nAAAA\n" 0 >f.txt && git add f.txt && git commit -q -m A &&
	  git checkout -q --orphan bb &&
	  printf "%08000d\nBBBB\n" 0 >f.txt && git add f.txt && git commit -q -m B ) &&
	create_helper_repo unpinstore &&
	git -C unpinstore config core.logAllRefUpdates false &&
	git -C unpinstore -c transfer.unpackLimit=10000 fetch "file://$PWD/unpinsrc" "refs/heads/ba:refs/heads/ba" "refs/heads/bb:refs/heads/bb" &&
	blob_ba=$(git -C unpinstore rev-parse ba:f.txt) &&
	db_ba=$(printf "%s\n" "$blob_ba" | git -C unpinstore cat-file --batch-check="%(deltabase)") &&
	if [ "$db_ba" = "$ZERO_OID" ]; then base=$blob_ba; drop=ba; keep=bb; else base=$db_ba; drop=bb; keep=ba; fi &&
	git -C unpinstore update-ref -d "refs/heads/$drop" &&
	git -C unpinstore gc --prune=now && git -C unpinstore fsck &&
	git -C unpinstore cat-file blob "$keep:f.txt" >/dev/null &&
	! git -C unpinstore cat-file -e "$base"
'

# --- gc --auto consults the helper churn heuristic: below gc.auto it does not
#     optimize (no delta), and once enough objects accumulate it does.
test_case "gc --auto fires only once enough objects accumulate" '
	create_helper_repo autostore &&
	git -C autostore config gc.auto 64 && git -C autostore config gc.autoDetach false &&
	printf "%08000d\nfirst\n" 0 >autostore/f.txt && git -C autostore add f.txt && git -C autostore commit -q -m one &&
	printf "%08000d\nsecond\n" 0 >autostore/f.txt && git -C autostore add f.txt && git -C autostore commit -q -m two &&
	git -C autostore gc --auto &&
	! has_delta_blob autostore &&
	i=1; while [ "$i" -le 64 ]; do echo "$i" >"autostore/n$i" || exit 1; i=$((i + 1)); done &&
	git -C autostore add . && git -C autostore commit -q -m bulk &&
	git -C autostore gc --auto && git -C autostore fsck &&
	has_delta_blob autostore
'

# --- prune keys on last_used, not created_at: a freshened (re-touched) object
#     survives the grace window while an equally-old untouched one is dropped.
test_case "prune keeps a freshened object and drops a stale one" '
	create_helper_repo gracestore &&
	git -C gracestore config core.logAllRefUpdates false &&
	test_commit -C gracestore base &&
	keep=$(printf "keepme\n" | git -C gracestore hash-object -w --stdin) &&
	drop=$(printf "dropme\n" | git -C gracestore hash-object -w --stdin) &&
	sleep 4 &&
	printf "keepme\n" | git -C gracestore hash-object -w --stdin >/dev/null &&
	git -C gracestore gc --prune=3.seconds.ago &&
	git -C gracestore fsck &&
	git -C gracestore cat-file -e "$keep" &&
	! git -C gracestore cat-file -e "$drop"
'

# --- git odb migrate: a plain files repo's whole object store is copied into the
#     real sqlite helper, the config is flipped to it, and the source files store
#     is discarded. Refs stay on files, so only the object-storage keys are
#     written (refStorage stays unset) and every object now answers from the
#     helper, since nothing is left on disk to answer from.
test_case "odb migrate moves a files repo into the sqlite helper" '
	git init migloose &&
	test_commit -C migloose m1 && test_commit -C migloose m2 && test_commit -C migloose m3 &&
	head=$(git -C migloose rev-parse HEAD) &&
	git -C migloose rev-list --all --objects | sort >before.objs &&
	git -C migloose odb migrate --object-storage=sqlite &&
	test "$(git -C migloose config extensions.objectStorage)" = helper://sqlite &&
	test -z "$(git -C migloose config extensions.refStorage)" &&
	assert_no_loose migloose &&
	no_packfile migloose &&
	test -s migloose/.git/sqlite.db &&
	git -C migloose fsck &&
	test "$(git -C migloose rev-parse HEAD)" = "$head" &&
	git -C migloose rev-list --all --objects | sort >after.objs &&
	cmp before.objs after.objs
'

# --- git odb migrate enumerates a PACKED source: after a repack the files store
#     holds a single pack, so migrate must walk packed objects (not just loose)
#     through the source vtable and then discard the pack.
test_case "odb migrate enumerates and discards a packed source" '
	git init migpack &&
	test_commit -C migpack p1 && test_commit -C migpack p2 && test_commit -C migpack p3 &&
	git -C migpack repack -ad &&
	ls migpack/.git/objects/pack/*.pack >/dev/null &&
	count=$(git -C migpack rev-list --all --objects | wc -l) &&
	git -C migpack odb migrate --object-storage=sqlite &&
	assert_no_loose migpack &&
	no_packfile migpack &&
	git -C migpack fsck &&
	test "$(git -C migpack rev-list --all --objects | wc -l)" -eq "$count"
'

echo "# git+helper integration: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
