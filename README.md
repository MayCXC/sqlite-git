# sqlite-git

Git storage in SQLite. Four tools over one shared, dependency-light storage core:

- **git0** (`git0.so`, SQLite extension): query any git repo from SQL (`git_blob()`, `git_log()`, `git_tree()`, ...), or run a self-contained git repo entirely inside a SQLite database via a libgit2 odb/refdb backend.
- **git-local-sqlite** (local helper): use SQLite as git's own object, ref, and reflog backend. One `.db` file replaces `.git/objects` and `.git/refs`. libgit2-free.
- **gitlfs** (`gitlfs.so`, SQLite extension): a standalone git-lfs content store, usable alone or alongside the others. libgit2-free.
- **git-lfs-sqlite-transfer** (LFS transfer adapter): git-lfs custom-transfer agent backed by the same database. libgit2-free.

## Architecture

The storage layer is split into three dependency tiers, each its own translation unit, so a tool links only what it needs:

| File | Tier | Links | Holds |
|------|------|-------|-------|
| `storage.c` | core | sqlite only | connection, transactions, db maintenance, the prepared-statement mechanism |
| `storage_git.c` | git | + zlib + clean-room delta | objects, refs, reflogs, the reachability bitmap, the commit-graph generation cache, pack membership, prune, object format |
| `storage_git_lfs.c` | lfs | + sha256 | git-lfs content (independent of the git object layer) |

The link graph, not naming, enforces the layering: only `git0.so` links libgit2; the helper, the lfs extension, and the transfer agent are libgit2-free; the lfs units carry no git object store.

| Unit | core | git | lfs | sha256 | zlib | libgit2 |
|------|:--:|:--:|:--:|:--:|:--:|:--:|
| `git0.so` | x | x | | | x | x |
| `git-local-sqlite` | x | x | | | x | |
| `gitlfs.so` / `git-lfs-sqlite-transfer` | x | | x | x | | |

Object ids cross every storage API as the wire's own hex strings (sqlite's `unhex()`/`hex()` does the hex<->blob conversion in SQL), so the helper and storage layers are hash-agnostic: one build serves both sha1 (40 hex) and sha256 (64 hex) repositories.

## Build

Requires SQLite3 and zlib always, and libgit2 for the `git0`/`gitlfs` extensions:

```sh
# Debian/Ubuntu
apt install libgit2-dev libsqlite3-dev zlib1g-dev
# macOS
brew install libgit2 sqlite zlib
```

```sh
make           # git0.so (stock libgit2) + git-local-sqlite + git-lfs-sqlite-transfer + gitlfs.so
make experimental  # also build/experimental/git0.so against libgit2-experimental (sha256)
make install   # install to ~/.local/{lib,bin,include}
```

`git0.so` is built twice, to `build/stock` and `build/experimental`, both named `git0.so`, differing only in which libgit2 they link: stock (sha1) vs libgit2-experimental (sha1 + sha256). Hash support follows from the headers; there is no define of ours. `gitlfs.so`, `git-local-sqlite`, and `git-lfs-sqlite-transfer` are libgit2-free and built once.

## Local helper (git-local-sqlite)

Stores git objects, refs, and reflogs in a single SQLite database. Git talks to the helper over a line-based protocol on stdin/stdout, with the fine granularity (random access by oid/refname, per-ref transactions) of the in-tree filesystem backends.

### Setup

```sh
git init --ref-format=sqlite --object-storage=sqlite myrepo
cd myrepo
```

This sets two extensions in `.git/config`, one per subsystem: each storage backend names the helper that serves it outright, the way a transport is named. The object and ref helpers run as separate processes over one shared database, so reconfiguring one never touches the other:

```ini
[extensions]
    refstorage = sqlite
    objectstorage = sqlite
```

All git operations then go through SQLite (`<gitdir>/sqlite.db`):

```sh
echo hello | git hash-object -w --stdin   # writes to .git/sqlite.db
git update-ref refs/heads/main <oid>       # ref stored in SQLite
git cat-file blob <oid>                    # reads from SQLite
git for-each-ref                           # lists refs from SQLite
git gc                                     # repacks + bitmaps + commit-graph in SQLite
```

For LFS, also configure the transfer adapter:

```sh
git config lfs.customtransfer.sqlite.path git-lfs-sqlite-transfer
git config lfs.customtransfer.sqlite.args .git
git config lfs.standalonetransferagent sqlite
```

### Storage schema

The git object/ref store (`storage_git.c`):

```
objects(oid BLOB PRIMARY KEY, type TEXT, size INT, data BLOB, base BLOB,
        pack_pos INT, promisor INT, created_at INT, last_used INT)
refs(refname TEXT PRIMARY KEY, oid BLOB, symref TEXT)                    WITHOUT ROWID
reflog(refname TEXT, idx INT, old_oid BLOB, new_oid BLOB, committer TEXT,
       timestamp INT, tz INT, msg TEXT, PRIMARY KEY(refname, idx))       WITHOUT ROWID
commit_graph(oid BLOB PRIMARY KEY, generation INT)                       WITHOUT ROWID
meta(key TEXT PRIMARY KEY, value INT)                                    WITHOUT ROWID
pack_objects(oid BLOB, pack_id BLOB, PRIMARY KEY(oid, pack_id))          WITHOUT ROWID
git_bitmap(id INT PRIMARY KEY CHECK(id = 0), bitmap BLOB)
pack_content(pack_pos INT PRIMARY KEY, type TEXT, size INT, base BLOB, content BLOB)
commit_bitmap(commit_oid BLOB PRIMARY KEY, xor_base BLOB, flags INT, ewah BLOB)
```

The git-lfs content store (`storage_git_lfs.c`):

```
lfs(oid BLOB PRIMARY KEY, size INT, nchunks INT)
lfs_chunk(oid BLOB, seq INT, data BLOB, PRIMARY KEY(oid, seq))
```

Design notes:

- **Binary oids**: keys are raw oid blobs (20 bytes sha1 / 32 sha256), half the hex width, converted in SQL via `unhex()`/`hex()`.
- **rowid vs WITHOUT ROWID** is chosen per table by measurement: a table carrying a large inline BLOB (`objects.data`, `commit_bitmap.ewah`) is a rowid table (a fat WITHOUT-ROWID primary-key btree pages through content it does not need); small key/value and all-key tables (`refs`, `reflog`, `commit_graph`, `meta`, `pack_objects`) stay WITHOUT ROWID.
- **Compression**: full objects are zlib-compressed; LFS frames are stored raw (LFS media is already entropy-coded).
- **Deltas**: git owns delta creation. On a `put-raw` the helper stores git's already-compressed delta bytes verbatim (base in the `base` column) and resolves them on read with a clean-room git-format delta applier (`git_delta_apply`, modelled on `Documentation/technical/pack-format.txt`, validated against but not copied from git's `patch-delta.c` and libgit2's `delta.c`). There is no fossil delta.
- **Pack shape**: at `gc`, reachable objects are clustered into `pack_content` keyed by `pack_pos` (the bit position from git's reachability bitmap), so a contiguous bit-run is served verbatim, the relational analog of copying a `.pack` region. `git_bitmap` holds git's EWAH type-bitmap; `commit_bitmap` holds the per-commit (xor-chained) bitmaps; `commit_graph` holds only the generation numbers (everything else is re-derived from the commit objects). No `.bitmap`/`.rev`/`.midx`/commit-graph file is written.
- **Prune**: `prune` deletes git-identified unreachable objects past their grace window, sparing kept-pack members and any object still serving as a delta base.
- **Transactions**: in owned mode (the helper) every durable write is bracketed by a savepoint over `BEGIN IMMEDIATE`/`COMMIT`; in borrowed mode (an extension on a loaded connection) the enclosing SQLite statement is the transaction, so storage adds none.

### Protocol

The helper speaks the git local-helper protocol: a flat command namespace on stdin/stdout. The authoritative reference is `helper.h` in the git fork; the families are: object ops (`info`/`get`/`put`/`put-raw`/`get-delta`/`have`/`list-objects`/`put-stream`/`odb-transaction-*`), maintenance (`optimize`/`verify`/`prune`/`refresh`), reachability + pack-reuse + commit-graph (`store-bitmap`/`get-bitmap`/`clear-bitmap`/`pos-of-oid`/`result-oids`/`reuse-pack`/`store-commit-bitmaps`/`get-commit-bitmap`/`store-commit-graph`/`commit-generation`), refs (`read`/`list`/`transaction-*`/`create`/`remove`), and reflogs (`reflog-read`/`-read-reverse`/`-append`/`-exists`/`-delete`/`-list`/`-copy`). Each optional family is gated on a capability the helper advertises via `capabilities`.

## SQLite extension (git0)

Query any git repo from SQL, or run a self-contained repo with no `.git` directory:

```sql
.load build/stock/git0

-- Query an existing .git repo
SELECT git_blob('.', 'HEAD~1', 'README.md');
SELECT * FROM git_log('.', 'main') LIMIT 20;
SELECT status, path FROM git_diff('.', 'v1.0', 'v2.0');

-- Or build a self-contained repo inside SQLite (file-backed db)
SELECT git0_init();
SELECT git0_ref_create('refs/heads/main',
  git0_mkcommit(
    git0_mktree('100644 hello.txt ' || git0_add('hello.txt', 'hello world')),
    git0_ref('HEAD'), 'initial commit'));

-- Then drive all of libgit2 against it via git0_repo()
SELECT * FROM git_log(git0_repo());
SELECT git_merge_base(git0_repo(), 'HEAD', 'refs/heads/main');
```

`git0_repo()` returns a handle to a storage-backed libgit2 repository (a custom odb + refdb backend over the same tables), so every `git_*` function works with no filesystem `.git`. `git0_init` chooses the object format (`sha1` default; `sha256` on the experimental build). A logged ref update through the libgit2 backend records a reflog entry, like the files backend.

The extension exposes: the `git_*` **scalar** functions (`git_blob`, `git_type`, `git_size`, `git_hash`, `git_write`, `git_rev_parse`, `git_describe`, `git_commit_*`, `git_ref`/`git_ref_create`/`git_ref_delete`, `git_merge_base`, `git_config`/`git_config_set`); the `git0_*` **storage-native** functions (`git0_init`, `git0_add`, `git0_mktree`, `git0_mkcommit`, `git0_repo`, `git0_cat`, `git0_type`/`size`/`exists`/`blob`/`ref`/`ref_create`/`ref_delete`/`commit_*`, `git0_generation`, `git0_name_hash`); the **table-valued** functions (`git_log`, `git_tree`, `git_diff`, `git_refs`, `git_ancestors`, `git_status`, `git_blame`, `git_config_list`, `git_stash`, `git_tag`); and two **writable virtual tables** over the storage-backed store:

```sql
CREATE VIRTUAL TABLE objs USING git0_objects;   -- oid, type, size, data
CREATE VIRTUAL TABLE refs USING git0_refs;      -- name, type, target, symref

INSERT INTO objs(type, data) VALUES('blob', 'hi');  -- content-addressed; oid computed
SELECT oid, size FROM objs;
DELETE FROM objs WHERE oid = '<hex>';

INSERT INTO refs(name, target) VALUES('refs/heads/x', '<oid-hex>');
INSERT INTO refs(name, symref) VALUES('HEAD', 'refs/heads/x');
UPDATE refs SET target = '<oid-hex>' WHERE name = 'refs/heads/x';
DELETE FROM refs WHERE name = 'refs/heads/x';
```

Both vtabs route through the same `storage_git` API as the scalars and the libgit2 backend (one store, no divergent SQL). Objects are content-addressed and immutable (an object UPDATE is rejected); refs are keyed on the refname.

## gitlfs extension (gitlfs.so) and the LFS transfer adapter

`gitlfs.so` is a standalone git-lfs content store, loadable on its own or alongside `git0.so` over the same database:

```sql
.load build/gitlfs
SELECT git0_lfs_store('large content');     -- stores it, returns the LFS pointer text
SELECT git0_lfs_fetch('<pointer text>');    -- content from a pointer
SELECT git0_lfs_smudge('<sha256-hex>');     -- content by oid
SELECT git0_lfs_pointer('data');            -- pointer text without storing
```

`git-lfs-sqlite-transfer` is the matching git-lfs custom-transfer agent (libgit2-free), speaking the [git-lfs custom transfer protocol](https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md) and streaming content a frame at a time into `lfs`/`lfs_chunk`. Content is addressed by its sha256 oid per the git-lfs spec.

## Testing

```sh
make test       # all suites, against both git0 builds
make test-asan  # the same, under AddressSanitizer + UndefinedBehaviorSanitizer
```

- `tests/test_helper.sh` (helper, 80 tests): protocol commands, put-raw/get-delta, reuse-pack streaming, commit-graph generation, bitmaps, prune/keep, freshen, LFS transfer round-trips.
- `tests/test_basic.sql`, `test_concurrent.sh`, `test_object_format.sh`, `test_reflog.sh`, `test_vtab.sh`: the `git0.so` extension (scalars, TVFs, storage-native, object formats, reflog-on-write, the writable vtabs), run against both builds.
- `tests/test_lfs.sql`: the `gitlfs.so` extension.
- `tests/test_git_helper.sh` (integration, 14 tests): drives a real `git` (set `GIT_BUILD` in `config.mak`) against `git-local-sqlite` for the helper scenarios (delta-preserving push, gc bitmaps, pack reuse, M:N kept packs, delta-base prune, odb migrate).

## Git upstream patches

The local helper backend requires patches to git. Most of the ODB vtable work landed upstream via `ps/odb-sources` and `ps/object-counting`. What remains:

- **Series 1**: adds `write_packfile`, `for_each_unique_abbrev`, and `convert_object_id` to the ODB source vtable, and routes the `object-name.c` abbreviation/disambiguation paths through `for_each_unique_abbrev` instead of files-backend internals.
- **Series 2**: extracts shared symref/HEAD transaction splitting into `refs.c`, then adds `git-local-<name>` helper backends for both ODB and refs with worktree support, plus the reachability-bitmap / pack-reuse / commit-graph seams that let a helper serve them from its store with no on-disk file.

Both series live on our [git fork](https://github.com/MayCXC/git).

## Dependencies

- SQLite3 and zlib (all tools)
- [libgit2](https://libgit2.org/) 1.7+ (the `git0`/`gitlfs` extensions only; stock for sha1, experimental for sha256)

## License

BSD-3-Clause. The clean-room git-format delta applier is our own implementation of the public pack-delta format. SHA-256 in `vendor/sha256.c` is public domain (Brad Conte).
