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
objects(oid BLOB PRIMARY KEY, type INT, size INT, data BLOB, base BLOB)
refs(refname TEXT PRIMARY KEY, oid BLOB, symref TEXT)
reflog(refname TEXT, idx INT, old_oid BLOB, new_oid BLOB, committer TEXT,
       timestamp INT, tz INT, msg TEXT, PRIMARY KEY(refname, idx))
```

All tables use `WITHOUT ROWID` for clustered primary key access.

**Binary OIDs**: Keys are 20-byte SHA-1 blobs, not 40-char hex text. Halves index size.

**Zlib compression**: Full objects are zlib-compressed before storage.

**Fossil delta compression**: When a new object is similar to an existing same-type object, a [fossil delta](https://fossil-scm.org/home/doc/trunk/www/delta_format.wiki) is stored instead. Delta objects reference their base via the `base` column and are resolved recursively on read. The delta algorithm (BSD-2 licensed from fossil-scm.org) uses a 16-byte rolling hash with sliding window matching.

### Protocol

The helper speaks the git local helper protocol. Commands use a flat namespace matching how remote helpers use `list`/`fetch`/`push`:

| Command | Response |
|---------|----------|
| `capabilities` | list of supported commands, blank line terminates |
| `info <oid>` | `<type> <size>` or `missing` |
| `get <oid>` | `<type> <size>` + raw data, or `missing` |
| `put <oid> <type> <size>` + data | `<oid>` |
| `have <oid>` | `true` or `false` |
| `list-objects` | `<oid> <type> <size>` per line, blank end |
| `odb-transaction-begin` | `ok` |
| `odb-transaction-commit` | `ok` |
| `read <refname>` | `<oid>`, `symref <target>`, or `missing` |
| `list [<prefix>]` | `<refname> <oid\|symref target>` per line, blank end |
| `transaction-begin/update/create/delete/create-symref` | `ok` |
| `transaction-prepare/finish/abort` | `ok` or `error <msg>` |
| `create` | `ok` |
| `remove` | `ok` |
| `reflog-read <refname>` | native reflog format lines, blank end |
| `reflog-append <refname> ...` | `ok` |
| `reflog-exists <refname>` | `true` or `false` |
| `reflog-delete <refname>` | `ok` |

## Remote helper (git-remote-sqlite)

Push and fetch between a git repo and a SQLite database:

```sh
git remote add backup sqlite:///path/to/backup.db
git push backup main
git fetch backup
```

The remote helper walks the object graph (commits, trees, blobs) and transfers objects between the local git repo and the SQLite database.

## SQLite extension (git0)

Query any git repo from SQL:

```sql
.load ./git0

-- Read a file at a specific commit
SELECT git_blob('.', 'HEAD~1', 'README.md');

-- Walk commit history
SELECT oid, author_name, substr(message, 1, 72)
FROM git_log('.', 'main') LIMIT 20;

-- List changed files
SELECT status, path FROM git_diff('.', 'v1.0', 'v2.0');

-- Browse a tree
SELECT name, type, oid FROM git_tree('.', 'HEAD', 'src');

-- Blame
SELECT line_start, line_count, author_name, substr(oid, 1, 8)
FROM git_blame('.', 'main.c');
```

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

## Dependencies

- [libgit2](https://libgit2.org/) (1.7+)
- SQLite3
- zlib

## License

MIT. The fossil delta algorithm in `vendor/fossil-delta.c` is BSD-2 (Copyright D. Richard Hipp).
