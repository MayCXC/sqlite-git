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

-- Table-valued functions
SELECT 'log: ' || count(*) || ' commits' FROM git_log('.');
SELECT 'tree: ' || count(*) || ' entries' FROM git_tree('.', 'HEAD');
SELECT 'refs: ' || count(*) || ' refs' FROM git_refs('.');
SELECT 'ancestors: ' || count(*) || ' ancestors' FROM git_ancestors('.');
SELECT 'config: ' || count(*) || ' entries' FROM git_config_list('.');

-- git_diff needs 2+ commits
SELECT 'diff: ' || count(*) || ' changes' FROM git_diff('.', 'HEAD~1', 'HEAD');

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

SELECT 'All tests passed.';
