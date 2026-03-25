# sqlite-git

Git storage in SQLite. Three tools:

- **git0** (SQLite extension): Query any git repo from SQL. `git_blob()`, `git_log()`, `git_tree()`, etc.
- **git-local-sqlite** (local helper): Use SQLite as the storage backend for a git repo. Objects, refs, and reflogs in one `.db` file.
- **git-remote-sqlite** (remote helper): Push/fetch between git repos and SQLite databases.

All three are built from one source tree. `git-local-sqlite` and `git-remote-sqlite` are symlinks to the `git-sqlite` binary, which dispatches on `argv[0]`.

## Build

Requires libgit2 (1.7+), SQLite3, and zlib:

```sh
# Debian/Ubuntu
apt install libgit2-dev libsqlite3-dev zlib1g-dev

# macOS
brew install libgit2 sqlite zlib
```

```sh
make            # builds git0.so + git-sqlite (with symlinks)
make install    # install to ~/.local/{lib,bin,include}
```

## Local helper (git-local-sqlite)

Stores git objects, refs, and reflogs in a single SQLite database. Git communicates with the helper via a line-based protocol on stdin/stdout.

### Setup

```sh
git init myrepo && cd myrepo
git config extensions.repositoryformatversion 1
git config extensions.objectStorage helper://sqlite
git config extensions.refStorage helper://sqlite
```

With both extensions set, all git operations go through SQLite:

```sh
echo "hello" | git hash-object -w --stdin   # writes to .git/sqlite.db
git update-ref refs/heads/main <oid>         # ref stored in SQLite
git cat-file blob <oid>                      # reads from SQLite
git for-each-ref                             # lists refs from SQLite
```

### Storage

Everything lives in `<gitdir>/sqlite.db`:

```
objects(oid BLOB PRIMARY KEY, type INT, size INT, data BLOB, base BLOB,
        kept INT DEFAULT 0, promisor INT DEFAULT 0, reachable INT DEFAULT 0,
        created_at INT DEFAULT (strftime('%s','now')))
refs(refname TEXT PRIMARY KEY, oid BLOB, symref TEXT)
reflog(refname TEXT, idx INT, old_oid BLOB, new_oid BLOB, committer TEXT,
       timestamp INT, tz INT, msg TEXT, PRIMARY KEY(refname, idx))
lfs(oid BLOB PRIMARY KEY, size INT, data BLOB)
commit_graph(oid BLOB PRIMARY KEY, generation INT, commit_time INT)
oid_map(src BLOB, dest BLOB, algo TEXT, PRIMARY KEY(src, algo))
promised(oid BLOB PRIMARY KEY, remote TEXT)
promisor_remotes(name TEXT PRIMARY KEY, url TEXT)
worktrees(name TEXT PRIMARY KEY, path TEXT, head_ref TEXT)
alternates(path TEXT PRIMARY KEY)
```

All tables use `WITHOUT ROWID` for clustered primary key access.

**Binary OIDs**: Keys are 20-byte SHA-1 blobs, not 40-char hex text. Halves index size.

**Zlib compression**: Full objects are zlib-compressed before storage.

**Fossil delta compression**: When a new object is similar to an existing same-type object, a [fossil delta](https://fossil-scm.org/home/doc/trunk/www/delta_format.wiki) is stored instead. Delta objects reference their base via the `base` column and are resolved recursively on read. The delta algorithm (BSD-2 licensed from fossil-scm.org) uses a 16-byte rolling hash with sliding window matching.

**GC and repack**: `gc` marks reachable objects via libgit2 revwalk, sweeps unreachable ones (preserving kept, promisor, and recently-created objects), then repacks surviving objects with sliding-window delta compression. Grace period defaults to 2 weeks, configurable via `gc.pruneexpire` (seconds). `repack` sorts by `git0_name_hash(path)` (same as git-core's `pack_name_hash`) for optimal delta locality, with cycle detection to prevent delta chain loops.

**Commit graph**: Precomputed generation numbers for all commits, stored in `commit_graph`. Built via libgit2 topological revwalk (parents before children). Used for fast reachability queries.

**Partial clone**: Objects can be marked as promised (known to exist on a promisor remote but not yet fetched). `fetch-object` transparently resolves delta chains from the remote database.

**Worktrees**: Named worktrees with separate HEAD refs, stored in the `worktrees` table.

**Alternates**: Linked alternate databases for shared object storage. Objects not found locally are resolved from alternates, including delta chain resolution across database boundaries.

**Transactions**: Every public write function wraps itself in a SQLite savepoint. Callers manage transactions externally; savepoints nest transparently. Error paths rollback, success paths release.

**Statement lifecycle**: All statements use `sqlite3_prepare_v3` with `SQLITE_PREPARE_PERSISTENT`. In owned mode (helper binary), statements are prepared once at open and cached in globals. In borrowed mode (extension loaded via `sqlite3` CLI), statements are prepared and finalized per call to avoid unfinalized statements at `sqlite3_close` (v1).

### Protocol

The helper speaks the git local helper protocol. Commands use a flat namespace matching how remote helpers use `list`/`fetch`/`push`:

| Command | Response |
|---------|----------|
| **Objects** | |
| `info <oid>` | `<type> <size>` or `missing` |
| `get <oid>` | `<type> <size>` + raw data, or `missing` |
| `put <oid> <type> <size>` + data | `<oid>` |
| `put-stream <type> <size>` + data | `<oid>` (helper computes OID) |
| `have <oid>` | `true` or `false` |
| `list-objects` | `<oid> <type> <size>` per line, blank end |
| `write-packfile` + packfile on stdin | `<count>` objects written |
| **Refs** | |
| `read <refname>` | `<oid>`, `symref <target>`, or `missing` |
| `list [<prefix>]` | `<refname> <oid\|symref target>` per line, blank end |
| `transaction-begin/update/create/delete/create-symref` | `ok` |
| `transaction-prepare/finish/abort` | `ok` or `error <msg>` |
| **Reflog** | |
| `reflog-read <refname>` | native reflog format lines, blank end |
| `reflog-read-reverse <refname>` | entries newest-first, blank end |
| `reflog-append <refname> ...` | `ok` |
| `reflog-exists <refname>` | `true` or `false` |
| `reflog-delete <refname>` | `ok` |
| `reflog-list` | refnames with reflogs, one per line, blank end |
| **Transactions** | |
| `odb-transaction-begin` | `ok` |
| `odb-transaction-commit` | `ok` |
| `create` | `ok` |
| `remove` | `ok` |
| **GC and repack** | |
| `gc` | `<deleted>` objects removed |
| `repack` | `<repacked>` objects re-deltified |
| `mark-kept <oid>` | `ok` |
| `mark-kept-recent` | `ok` |
| `clear-kept` | `ok` |
| `have-kept <oid>` | `true` or `false` |
| `mark-promisor <oid>` | `ok` |
| `mark-promisor-recent` | `ok` |
| `connectivity-check` | `ok` or `error <msg>` |
| **Partial clone** | |
| `promise <oid> <remote>` | `ok` |
| `promisor-remote-add <name> <url>` | `ok` |
| `promisor-remote-remove <name>` | `ok` |
| `fetch-object <oid>` | `ok` or `error` |
| **Commit graph** | |
| `build-commit-graph` | `<count>` entries |
| **Worktrees** | |
| `worktree-add <name> <path> <branch>` | `ok` |
| `worktree-remove <name>` | `ok` |
| `worktree-list` | `<name> <path> <head_ref>` per line, blank end |
| **Alternates** | |
| `alternate-add <path>` | `ok` |
| `alternate-remove <path>` | `ok` |
| `alternate-list` | `<path>` per line, blank end |
| **Misc** | |
| `capabilities` | list of supported commands, blank line terminates |
| `convert-oid <hex> <algo>` | converted oid or `missing` |
| `refresh` | clears kept marks, reloads state |
| `close` | exits |

## Remote helper (git-remote-sqlite)

Push and fetch between a git repo and a SQLite database:

```sh
git remote add backup sqlite:///path/to/backup.db
git push backup main
git fetch backup
```

The remote helper walks the object graph (commits, trees, blobs) and transfers objects between the local git repo and the SQLite database.

## SQLite extension (git0)

Query any git repo from SQL, or work with a self-contained SQLite repo:

```sql
.load ./git0

-- Query an existing .git repo
SELECT git_blob('.', 'HEAD~1', 'README.md');
SELECT * FROM git_log('.', 'main') LIMIT 20;
SELECT status, path FROM git_diff('.', 'v1.0', 'v2.0');

-- Or create a self-contained repo in SQLite (no .git needed)
SELECT git0_init();
SELECT git0_add('hello.txt', 'hello world');
SELECT git0_ref_create('refs/heads/main',
  git0_mkcommit(
    git0_mktree('100644 hello.txt ' || git0_add('hello.txt', 'hello world')),
    git0_ref('HEAD'), 'initial commit'));

-- Then use all libgit2 features via git0_repo()
SELECT * FROM git_log(git0_repo());
SELECT * FROM git_tree(git0_repo(), 'refs/heads/main');
SELECT git_merge_base(git0_repo(), 'HEAD', 'refs/heads/main');
```

`git0_repo()` returns a handle to the storage-backed libgit2 repository. All `git_*` functions (log, tree, diff, refs, ancestors, merge-base, blame, describe) work against it in `:memory:` with no filesystem access.

### Scalar functions

| Function | Args | Returns |
|----------|------|---------|
| `git_version()` | | version string |
| `git_blob` | `(repo, oid)` or `(repo, rev, path)` | blob content |
| `git_type` | `(repo, oid)` | `'blob'`\|`'tree'`\|`'commit'`\|`'tag'` |
| `git_size` | `(repo, oid)` | object size in bytes |
| `git_exists` | `(repo, oid)` | 1 or 0 |
| `git_hash` | `(content, type)` | oid without writing |
| `git_write` | `(repo, content, type)` | oid (writes to odb) |
| `git_rev_parse` | `(repo, spec)` | resolved oid |
| `git_describe` | `(repo)` or `(repo, rev)` | descriptive string |
| `git_commit_message` | `(repo, rev)` | full message |
| `git_commit_summary` | `(repo, rev)` | first line |
| `git_commit_tree` | `(repo, rev)` | tree oid |
| `git_commit_author` | `(repo, rev)` | `'name <email> time tz'` |
| `git_commit_parent` | `(repo, rev, n)` | nth parent oid |
| `git_commit_parents` | `(repo, rev)` | parent count |
| `git_ref` | `(repo, name)` | resolved oid |
| `git_ref_create` | `(repo, name, oid)` or `(repo, name, oid, force)` | void |
| `git_ref_delete` | `(repo, name)` | void |
| `git_merge_base` | `(repo, oid1, oid2)` | common ancestor oid |
| `git_config` | `(repo, key)` | config value |
| `git_config_set` | `(repo, key, value)` | void |

### Storage-native functions

These operate directly on the SQLite storage layer (no `.git` repo needed). Use after `git0_init()`.

| Function | Args | Returns |
|----------|------|---------|
| `git0_init()` | | initial commit oid |
| `git0_add` | `(path, data)` | blob oid |
| `git0_mktree` | `(entries)` | tree oid |
| `git0_mkcommit` | `(tree, parent, msg, author?)` | commit oid |
| `git0_repo()` | | `:storage:` handle for git_* functions |
| `git0_type` | `(oid)` | object type |
| `git0_size` | `(oid)` | object size |
| `git0_exists` | `(oid)` | 1 or 0 |
| `git0_cat` | `(oid)` | raw object content |
| `git0_blob` | `(oid)` or `(rev, path)` | blob content |
| `git0_ref` | `(name)` | resolved oid |
| `git0_ref_create` | `(name, oid)` | void |
| `git0_ref_delete` | `(name)` | void |
| `git0_commit_tree` | `(rev)` | tree oid |
| `git0_commit_message` | `(rev)` | full message |
| `git0_commit_summary` | `(rev)` | first line |
| `git0_commit_author` | `(rev)` | author line |
| `git0_commit_parent` | `(rev, n)` | nth parent oid |
| `git0_commit_parents` | `(rev)` | parent count |
| `git0_name_hash` | `(path)` | sortable uint32 hash (same as git-core `pack_name_hash`) |
| `git0_lfs_store` | `(data)` | LFS pointer text |
| `git0_lfs_fetch` | `(pointer)` | content |
| `git0_lfs_pointer` | `(data)` | pointer text (no store) |
| `git0_lfs_smudge` | `(oid_hex)` | content |

### Table-valued functions

| Function | Args | Columns |
|----------|------|---------|
| `git_log` | `(repo, rev?, path?)` | oid, message, author_name, author_email, author_when, committer_name, committer_email, committer_when, parents |
| `git_tree` | `(repo, rev, path?)` | name, mode, type, oid, size |
| `git_diff` | `(repo, old_rev, new_rev)` | status, path, old_oid, new_oid, old_mode, new_mode |
| `git_refs` | `(repo, pattern?)` | name, type, oid |
| `git_ancestors` | `(repo, rev?)` | oid |
| `git_status` | `(repo)` | path, status |
| `git_blame` | `(repo, path, rev?)` | line_start, line_count, oid, orig_path, author_name, author_email, author_when |
| `git_config_list` | `(repo)` | key, value |
| `git_stash` | `(repo)` | index, message, oid |
| `git_tag` | `(repo, pattern?)` | name, oid, tagger_name, tagger_email, tagger_when, message, target_oid, target_type |

## LFS (git-lfs-sqlite-transfer)

Large file content stored in the same SQLite database, compressed with zlib. Uses SHA-256 OIDs per the git-lfs spec.

```sh
git config lfs.customtransfer.sqlite.path git-lfs-sqlite-transfer
git config lfs.customtransfer.sqlite.args .git
git config lfs.standalonetransferagent sqlite
```

The transfer adapter speaks the [git-lfs custom transfer protocol](https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md) on stdin/stdout. Content is stored in:

```
lfs(oid BLOB PRIMARY KEY, size INT, data BLOB) WITHOUT ROWID
```

## Testing

```sh
make test       # run both test suites
make test-asan  # run with AddressSanitizer + UndefinedBehaviorSanitizer
```

- `tests/test_helper.sh`: 37 tests covering all 18 protocol commands, LFS transfer adapter, and argv[0] dispatch.
- `tests/test_basic.sql`: SQL extension tests for scalar functions, table-valued functions, virtual tables, storage-native functions, self-contained repo operations, and LFS round-trips.

## Git upstream patches

The local helper backend requires patches to git that add `git-local-*` support (pluggable ODB and ref storage via external processes). These patches are on the [`ps/local-helper-backends`](https://github.com/MayCXC/git/tree/ps/local-helper-backends) branch of our git fork. Series 1 has been [submitted upstream](https://lore.kernel.org/git/pull.2068.git.1773674983.gitgitgadget@gmail.com).

## Dependencies

- [libgit2](https://libgit2.org/) (1.7+)
- SQLite3
- zlib

## License

BSD-3-Clause. The fossil delta algorithm in `vendor/fossil-delta.c` is BSD-2 (Copyright D. Richard Hipp). SHA-256 in `vendor/sha256.c` is public domain (Brad Conte).
