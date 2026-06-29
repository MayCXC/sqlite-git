.bail on

SELECT 'version: ' || git_version();

-- Scalar functions
SELECT 'rev_parse: ' || git_rev_parse('.', 'HEAD');
SELECT 'type: ' || git_type('.', 'HEAD');
SELECT 'size: ' || git_size('.', 'HEAD');
SELECT 'exists_yes: ' || git_exists('.', 'HEAD');
SELECT 'exists_no: ' || git_exists('.', 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef');
SELECT 'summary: ' || git_commit_summary('.', 'HEAD');
SELECT 'tree: ' || git_commit_tree('.', 'HEAD');
SELECT 'author: ' || git_commit_author('.', 'HEAD');
SELECT 'parents: ' || git_commit_parents('.', 'HEAD');
SELECT 'blob_len: ' || length(git_blob('.', 'HEAD', 'Makefile'));
SELECT 'hash: ' || git_hash('hello world', 'blob');
SELECT 'ref: ' || git_ref('.', 'master');
SELECT 'describe: ' || git_describe('.');
SELECT 'message_len: ' || length(git_commit_message('.', 'HEAD'));
SELECT 'parent0: ' || git_commit_parent('.', 'HEAD', 0);
SELECT 'blob_by_oid: ' || length(git_blob('.', git_rev_parse('.', 'HEAD:Makefile')));
SELECT 'merge_base: ' || git_merge_base('.', 'HEAD', 'HEAD');
SELECT 'hash_verify: ' || (git_hash('hello world', 'blob') = '95d09f2b10159347eece71399a7e2e907ea3df4f');
SELECT 'config_user: ' || COALESCE(git_config('.', 'user.name'), '(not set)');

-- Table-valued functions
SELECT 'log: ' || count(*) || ' commits' FROM git_log('.');
SELECT 'tree: ' || count(*) || ' entries' FROM git_tree('.', 'HEAD');
SELECT 'refs: ' || count(*) || ' refs' FROM git_refs('.');
SELECT 'ancestors: ' || count(*) || ' ancestors' FROM git_ancestors('.');
SELECT 'config: ' || count(*) || ' entries' FROM git_config_list('.');

-- Verify TVF columns return expected types
SELECT 'log_oid: ' || oid FROM git_log('.') LIMIT 1;
SELECT 'log_author: ' || author_name FROM git_log('.') LIMIT 1;
SELECT 'tree_name: ' || name FROM git_tree('.', 'HEAD') LIMIT 1;
SELECT 'tree_mode: ' || mode FROM git_tree('.', 'HEAD') LIMIT 1;
SELECT 'ref_name: ' || name FROM git_refs('.') LIMIT 1;

-- git_diff needs 2+ commits
SELECT 'diff: ' || count(*) || ' changes' FROM git_diff('.', 'HEAD~1', 'HEAD');
SELECT 'diff_path: ' || path FROM git_diff('.', 'HEAD~1', 'HEAD') LIMIT 1;
SELECT 'diff_status: ' || status FROM git_diff('.', 'HEAD~1', 'HEAD') LIMIT 1;

-- git_blame
SELECT 'blame: ' || count(*) || ' hunks' FROM git_blame('.', 'Makefile');

-- git_status
SELECT 'status: ' || count(*) || ' entries' FROM git_status('.');

-- Object and ref storage round-trips are exercised below through the public
-- storage-native interface (git0_add/git0_cat, git0_ref_create/git0_ref), which
-- writes and reads the same objects/refs tables.

-- Phase 3: self-contained repo (requires storage_open, skipped in :memory:)
-- These functions work when loaded into a file-backed database:
--   SELECT git0_init();
--   SELECT git0_add('test', 'hello world');
--   SELECT git0_mktree('100644 test.txt ' || git0_add('test.txt', 'data'));
--   SELECT git0_mkcommit(tree_oid, parent_oid, 'message');

-- LFS lives in its own extension now (gitlfs.so / tests/test_lfs.sql); git0.so
-- registers no git0_lfs_* functions.

-- Storage-native functions (use the same db connection)
SELECT 'storage_type: ' || git0_type(git0_add('test.txt', 'storage test'));
SELECT 'storage_size: ' || git0_size(git0_add('test.txt', 'storage test'));
SELECT 'storage_exists: ' || git0_exists(git0_add('test.txt', 'storage test'));
SELECT 'storage_cat: ' || length(git0_cat(git0_add('test.txt', 'storage test')));
SELECT 'storage_blob: ' || CAST(git0_blob(git0_add('test.txt', 'storage test')) AS TEXT);

-- Abbreviated-oid lookup resolves through the storage odb's read_prefix/exists_prefix
-- (git_revparse_single on a 12-hex prefix -> git_object_lookup_prefix -> backend).
SELECT 'storage_abbrev: ' || git_rev_parse(git0_repo(), substr(git0_add('abbrev.txt', 'abbrev probe'), 1, 12));

-- Ref operations
SELECT 'storage_ref: ' || git0_ref('HEAD');
SELECT 'storage_ref_master: ' || git0_ref('refs/heads/master');

-- Commit accessors
SELECT 'storage_tree: ' || git0_commit_tree(git0_ref('HEAD'));
SELECT 'storage_summary: ' || git0_commit_summary(git0_ref('HEAD'));
SELECT 'storage_author: ' || git0_commit_author(git0_ref('HEAD'));
SELECT 'storage_parents: ' || git0_commit_parents(git0_ref('HEAD'));

-- Commit with content, then read blob by path
SELECT git0_ref_create('refs/heads/master',
  git0_mkcommit(
    git0_mktree('100644 file.txt ' || git0_add('file.txt', 'path test')),
    git0_ref('HEAD'),
    'test commit'
  )
);
SELECT 'storage_blob_path: ' || CAST(git0_blob(git0_ref('refs/heads/master'), 'file.txt') AS TEXT);

-- Nested path: git0_mktree builds the intermediate dir/sub subtrees from the
-- slashed path, and git0_blob resolves the blob through them (covers subtree
-- creation, which the flat entries above do not).
SELECT git0_ref_create('refs/heads/nested',
  git0_mkcommit(
    git0_mktree('100644 dir/sub/file.txt ' || git0_add('dir/sub/file.txt', 'nested test')),
    git0_ref('HEAD'),
    'nested commit'
  )
);
SELECT 'storage_blob_nested: ' || CAST(git0_blob(git0_ref('refs/heads/nested'), 'dir/sub/file.txt') AS TEXT);

-- Commit parent
SELECT 'storage_parent0: ' || git0_commit_parent(git0_ref('refs/heads/master'), 0);

-- Refs list TVF
-- Refs are verified via git0_ref() above. The git0_refs vtable
-- (test_refs) was dropped after Phase 1/2 tests.

-- Ref delete
SELECT git0_ref_delete('refs/heads/master');
SELECT 'storage_ref_deleted: ' || COALESCE(git0_ref('refs/heads/master'), 'null');

SELECT 'All tests passed.';
