# sqlite-git

SQLite extension exposing git plumbing via libgit2. The fileio extension gives SQLite access to the filesystem with `readfile()`, `writefile()`, and `fsdir()`. git0 does the same for the git object database: `git_blob()` reads objects, `git_write()` writes them, and `git_tree()` lists entries.

## Build

Requires libgit2 (1.7+):

```sh
# Debian/Ubuntu
apt install libgit2-dev

# macOS
brew install libgit2

# From source
cmake .. -DBUILD_SHARED_LIBS=ON -DBUILD_TESTS=OFF
make && make install
```

Build the extension:

```sh
make            # shared library (git0.so / git0.dylib)
make static     # static archive (git0.a)
make install    # install to ~/.local/lib and ~/.local/include
make test       # run test suite
```

## Usage

```sql
.load ./git0

-- Read a file at a specific commit
SELECT git_blob('.', 'HEAD~1', 'README.md');

-- List changed files between commits
SELECT status, path FROM git_diff('.', 'v1.0', 'v2.0');

-- Walk commit history
SELECT oid, author_name, author_when, substr(message, 1, 72)
FROM git_log('.', 'main')
LIMIT 20;

-- Browse a directory tree at any revision
SELECT name, type, oid FROM git_tree('.', 'HEAD', 'src');

-- Find who changed each line
SELECT line_start, line_count, author_name, substr(oid, 1, 8)
FROM git_blame('.', 'main.c');
```

## Scalar Functions

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

## Table-Valued Functions

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

- [libgit2](https://libgit2.org/) (runtime + headers)
- SQLite3 headers (for building)

## License

MIT
