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

-- Phase 1/2: git0_objects and git0_refs virtual tables
CREATE VIRTUAL TABLE IF NOT EXISTS test_obj USING git0_objects;
CREATE VIRTUAL TABLE IF NOT EXISTS test_refs USING git0_refs;

INSERT INTO test_obj(type, data) VALUES('blob', 'hello world');
INSERT INTO test_obj(type, data) VALUES('blob', 'second blob');
INSERT INTO test_obj(type, data) VALUES('blob', 'hello world');  -- duplicate

SELECT 'obj_count: ' || count(*) FROM test_obj;
SELECT 'obj_lookup: ' || oid FROM test_obj WHERE oid = '95d09f2b10159347eece71399a7e2e907ea3df4f';

INSERT INTO test_refs(name, target) VALUES('refs/heads/main', '95d09f2b10159347eece71399a7e2e907ea3df4f');
INSERT INTO test_refs(name, target) VALUES('HEAD', '95d09f2b10159347eece71399a7e2e907ea3df4f');
SELECT 'ref_count: ' || count(*) FROM test_refs;
SELECT 'ref_type: ' || type FROM test_refs WHERE name = 'refs/heads/main';
SELECT 'head_type: ' || type FROM test_refs WHERE name = 'HEAD';

DROP TABLE test_obj;
DROP TABLE test_refs;

-- Phase 3: self-contained repo (requires storage_open, skipped in :memory:)
-- These functions work when loaded into a file-backed database:
--   SELECT git0_init();
--   SELECT git0_add('test', 'hello world');
--   SELECT git0_mktree('100644 test.txt ' || git0_add('test.txt', 'data'));
--   SELECT git0_mkcommit(tree_oid, parent_oid, 'message');

-- Phase 5: LFS pointer generation (no storage needed)
SELECT 'lfs_pointer: ' || length(git0_lfs_pointer('large content here')) || ' bytes';

SELECT 'All tests passed.';
