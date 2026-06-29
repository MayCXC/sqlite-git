.bail on

-- The standalone git-lfs extension (gitlfs.so -> sqlite3_gitlfs_init) borrows the
-- loaded connection and creates its lfs schema on load, so these run against a
-- file-backed db with NO git object store present: LFS is independent of the git
-- object layer.

SELECT 'lfs_pointer: ' || length(git0_lfs_pointer('large content here')) || ' bytes';

-- store -> pointer -> fetch round-trip (fetch parses the oid out of the pointer).
SELECT 'lfs_roundtrip: ' ||
  (CAST(git0_lfs_fetch(git0_lfs_store('lfs round trip')) AS TEXT) = 'lfs round trip');

-- smudge by raw sha256 hex: pull the 64-hex oid out of the pointer text store
-- returns ('sha256:' is 7 chars, so the hex begins 7 past its position).
WITH p(ptr) AS (SELECT git0_lfs_store('smudge me'))
SELECT 'lfs_smudge: ' ||
  (CAST(git0_lfs_smudge(substr(ptr, instr(ptr, 'sha256:') + 7, 64)) AS TEXT) = 'smudge me')
  FROM p;

-- a missing oid fetches NULL, not an error.
SELECT 'lfs_missing: ' ||
  COALESCE(CAST(git0_lfs_smudge('00000000000000000000000000000000000000000000000000000000deadbeef') AS TEXT), 'null');

SELECT 'All LFS tests passed.';
