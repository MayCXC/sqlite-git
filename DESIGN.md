# sqlite-git design

## Architecture

sqlite-git provides three interfaces to git objects stored in SQLite:

```
                     ┌─────────────────────┐
                     │   SQLite database    │
                     │  (objects, refs,     │
                     │   reflog, lfs)       │
                     └─────────┬───────────┘
                               │
                     ┌─────────┴───────────┐
                     │    storage layer     │
                     │    (storage.c)       │
                     └───┬─────┬───────┬───┘
                         │     │       │
              ┌──────────┘     │       └──────────┐
              │                │                  │
    ┌─────────┴────┐  ┌───────┴───────┐  ┌───────┴───────┐
    │ git-sqlite   │  │ git0 extension│  │ git0_backend  │
    │ (helper      │  │ (SQL scalar   │  │ (libgit2 ODB  │
    │  protocol)   │  │  functions)   │  │  + refdb)     │
    └──────────────┘  └───────────────┘  └───────┬───────┘
                                                 │
                                         ┌───────┴───────┐
                                         │  libgit2 APIs │
                                         │  (revwalk,    │
                                         │  diff, blame) │
                                         └───────────────┘
```

All three interfaces share the same storage layer. The storage layer
owns all SQL, compression, and delta encoding. No SQL appears outside
storage.c.

## Storage layer (storage.c)

Single source of truth for all database operations.

**Schema:**
```sql
objects(oid BLOB PRIMARY KEY, type INT, size INT, data BLOB, base BLOB) WITHOUT ROWID
refs(refname TEXT PRIMARY KEY, oid BLOB, symref TEXT) WITHOUT ROWID
reflog(refname TEXT, idx INT, ..., PRIMARY KEY(refname, idx)) WITHOUT ROWID
lfs(oid BLOB PRIMARY KEY, size INT, data BLOB) WITHOUT ROWID
```

**Indices:**
```sql
idx_objects_type_base ON objects(type) WHERE base IS NULL
```
Partial index for delta base selection: `find_delta_base` needs a full
(non-delta) object of the same type. Without this index, every object
write scans the full objects table.

**Binary OIDs:** Keys are 20-byte SHA-1 blobs (BLOB(20)), not 40-char
hex text. Halves index size and enables clustered access with WITHOUT
ROWID.

**Compression:** Full objects are zlib-compressed. When a same-type base
exists and the delta is smaller than 90% of the original, a fossil delta
is stored instead (raw, not compressed). Delta chains are resolved
recursively on read with a depth limit of 4095 (matching git's
pack-objects.h structural maximum).

**Statement lifecycle:** `stmt_acquire`/`stmt_release` functions handle
dual mode:
- **Owned mode** (helper binary via `storage_open`): persistent cached
  statements, reset on reuse. Best performance.
- **Borrowed mode** (extension via `storage_open_db(db, 0)`): prepare
  fresh per call, finalize after. Safe with `sqlite3_close` v1.
- **Persistent borrowed** (`storage_open_db(db, 1)`): cached statements
  on a borrowed connection. Caller must call `storage_close` or use
  `sqlite3_close_v2`.

## Helper binary (git-sqlite)

Single binary dispatched by `argv[0]`:
- `git-local-sqlite`: local helper protocol (18 commands)
- `git-remote-sqlite`: remote helper protocol (list/fetch/push)

**Local helper protocol** (line-based on stdin/stdout):

ODB commands: `info`, `get`, `put`, `put-stream`, `have`,
`list-objects`, `odb-transaction-begin`, `odb-transaction-commit`

Ref commands: `read`, `list`, `create`, `remove`,
`transaction-begin/update/create/delete/create-symref/prepare/finish/abort`

Reflog commands: `reflog-read`, `reflog-read-reverse`, `reflog-append`,
`reflog-exists`, `reflog-delete`, `reflog-list`

`put-stream` differs from `put`: the helper computes the OID (caller
sends `<type> <size>` + data, helper returns `<oid>`). Used for
streaming writes where the caller hashes incrementally.

`reflog-read-reverse` sends entries newest-first, enabling streaming
reverse iteration without collecting all entries in memory.

## SQLite extension (git0)

Two families of SQL functions:

**`git_*(repo, ...)`** functions operate on .git repos via libgit2:
```sql
SELECT * FROM git_log('.', 'main');
SELECT git_blob('.', 'HEAD', 'README.md');
```

**`git0_*(...)`** functions operate on the storage layer directly:
```sql
SELECT git0_init();
SELECT git0_blob(git0_add('file.txt', 'content'));
SELECT git0_commit_summary(git0_ref('HEAD'));
```

Both use libgit2 under the hood. The `git0_*` functions go through a
custom `git_odb_backend` and `git_refdb_backend` that wrap the storage
layer. `git0_repo()` returns a handle (`:storage:`) that routes the
`git_*` functions through these custom backends:

```sql
SELECT * FROM git_log(git0_repo());       -- revwalk on storage
SELECT git_merge_base(git0_repo(), a, b); -- LCA on storage
SELECT * FROM git_diff(git0_repo(), 'HEAD~1', 'HEAD');
```

This means revwalk, diff, blame, merge-base, describe, and rev-parse
all work against SQLite storage in `:memory:` with no filesystem access.
The libgit2 algorithms are used as-is through the custom backend
interface, not reimplemented.

**Only two functions bypass libgit2:**
- `git0_exists(oid)`: calls `storage_object_exists` directly
- `git0_cat(oid)`: calls `storage_read_object` directly (raw content)

## Git upstream patches

The local helper protocol requires patches to git-core on the
[`ps/local-helper-backends`](https://github.com/MayCXC/git/tree/ps/local-helper-backends)
branch (4 commits):

1. `odb_source_files_try()` for safe downcasting in heterogeneous source chains
2. Convert all 20+ callsites from `odb_source_files_downcast()` to `odb_source_files_try()`
3. `odb_storage_format` dispatch table (mirroring `ref_storage_format`)
4. Complete local helper backends: ODB + refs + reflogs, streaming reads/writes,
   reflog iterator, pipe draining on early abort, 13 tests

Configuration: `extensions.objectStorage = helper://sqlite` and
`extensions.refStorage = helper://sqlite` in `.git/config`.

A shared `helper_process` (one per repo, stored on `struct repository`)
handles both ODB and ref operations in a single external process,
matching how remote helpers handle both refs and objects.

## Dependencies

- **libgit2** (1.7+): git operations, custom ODB/refdb backend
- **SQLite3**: storage
- **zlib**: object compression
- **fossil-delta.c** (vendored, BSD-2): delta compression
- **sha256.c** (vendored, public domain): SHA-256 for LFS OIDs
