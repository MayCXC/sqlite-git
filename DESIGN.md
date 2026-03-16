# sqlite-git ODB helper design

## Context

Git v2.53 (March 2026) introduced `odb_source`, a vtable abstraction for
the object database. This mirrors the `ref_storage_be` abstraction that
enabled the reftable refs backend. The ODB vtable defines 11 callbacks
(read, write, iterate, stream, transaction, alternates) and a `type` enum
with `ODB_SOURCE_FILES` as the only implementation.

The vtable is complete for generic object operations. However, 37 callsites
in the codebase downcast `odb_source` to `odb_source_files` to access
files-specific internals (pack store, loose cache, MIDX). These represent
operations not yet abstracted into the vtable:

- Pack enumeration and cache management
- Multi-pack-index operations
- Loose object directory scanning cache
- Loose object compatibility map (SHA-1/SHA-256 transition)

## ODB helper protocol

Following the remote helper pattern (`git-remote-<name>`), the ODB helper
protocol delegates object storage to an external process discovered via
PATH as `git-odb-<name>`.

Enabled by: `GIT_ODB_HELPER=<name>`

The helper is spawned once, kept alive, and communicates via stdin/stdout:

    -> capabilities\n
    <- get\n
    <- info\n
    <- put\n
    <- have\n
    <- list\n
    <- \n

    -> info <oid>\n
    <- <type> <size>\n | missing\n

    -> get <oid>\n
    <- <type> <size>\n
    <- <size bytes of data>
    (or: <- missing\n)

    -> put <oid> <type> <size>\n
    -> <size bytes of data>
    <- <oid>\n

    -> have <oid>\n
    <- true\n | false\n

    -> list\n
    <- <oid> <type> <size>\n  (repeated)
    <- \n

## Architecture

The helper source is added as the first alternate after the files primary:

    odb->sources = files_source -> helper_source -> NULL

This preserves files as the primary (handles writes, temp files, packs,
transactions). The helper gets first chance at reads via the source chain
iteration. Writes go only to the files primary.

To sync objects from files to the helper, use `git-remote-sqlite push`.

## Downcast guards

The 37 callsites that downcast `odb_source` to `odb_source_files` are
guarded with NULL checks. The downcast returns NULL for non-files sources
instead of calling BUG(). Callers in iteration loops `continue` past
non-files sources. Callers in standalone functions return early.

This is the incremental fix. The proper upstream fix would add pack/loose
operations to the vtable (like refs did for reftable), eliminating all
downcasts. Our patch demonstrates why this is needed by providing a
second backend that exercises the abstraction.

## Alignment with refs backend pattern

The refs system provides the model:

| Aspect | refs | odb (current) | odb (target) |
|--------|------|---------------|--------------|
| Backend vtable | `ref_storage_be` | `odb_source` | `odb_source` |
| Format selection | `--ref-format=reftable` | hardcoded files | `--object-format=sqlite` |
| Config storage | `extensions.refStorage` | none | `extensions.objectStorage` |
| Downcasts | zero | 37 | zero (target) |
| Backends | files, reftable, packed | files | files, sqlite, helper |

## Files

- `odb-source-dispatch.c` - Patched `odb_source_new()` with helper dispatch
- `odb-source-helper.c/.h` - Helper source implementation (spawn, pipe, protocol)
- `odb-source-sqlite.c/.h` - Direct SQLite source (links libsqlite3 into git)
- `git-odb-sqlite.c` - The helper binary
- `patches/0001-odb-source-helper.patch` - Complete patch for git v2.53
