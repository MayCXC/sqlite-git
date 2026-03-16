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

SELECT 'All tests passed.';
