#ifndef OIDHEX_H
#define OIDHEX_H

/*
 * Hash-agnostic oid handling for the sqlite storage backend.
 *
 * The experimental libgit2 (built -DEXPERIMENTAL_SHA256=ON) installs a git2.h
 * that pulls in git2-experimental/experimental.h, which #defines
 * GIT_EXPERIMENTAL_SHA256 1; the stock install's git2.h does not. So a plain
 * #include <git2.h> self-selects: built against the experimental install the
 * sha256 code below activates, against the stock install it does not, chosen
 * purely by which libgit2 the -I/-L point at, with no extra define of our own.
 * git itself is always runtime-dual (the_hash_algo); we match it by deriving the
 * hash from the length of the hex oid the protocol hands us, so a build against
 * the experimental library serves both sha1 and sha256 repositories.
 */
#include <git2.h>

#include <string.h>

/*
 * Longest oid hex we accept on the wire (sha256 = 64; sha1 = 40). A plain
 * literal, usable directly as an sscanf field width (GIT_OID_MAX_HEXSIZE is a
 * macro expression, not a literal, so it cannot be stringized into a format).
 */
#define OID_HEX_MAX 64

/*
 * Parse a protocol hex oid into a git_oid, deriving the hash type from the hex
 * length (sha1 = 40, sha256 = 64). Returns 0 on success, non-zero if the length
 * is not a hash this build can represent (a sha1-only libgit2 has a 20-byte
 * git_oid that cannot hold a sha256 oid).
 *
 * The length->algo derivation models git.git's hash_algo_by_length() (git
 * hash.h). git and libgit2 both solve hash-agnostic oid handling internally, but
 * neither is reusable here: git's is typed on its own struct object_id (this
 * helper links libgit2, not git, so git's hash layer would collide with git_oid),
 * and libgit2's git_oid_size()/git_oid_type() are internal (src/, not the
 * installed public headers). libgit2's public API deliberately makes the caller
 * supply the type (git_oid_fromstr(out, str, type)) because it normally takes the
 * algo from repo context (git_repository_oid_type), not the hex; the helper is
 * stateless over the wire, so we derive it. Thin glue over the public API, not a
 * reimplementation.
 */
static inline int oid_from_hex(git_oid *out, const char *hex)
{
	size_t n = strlen(hex);
#ifdef GIT_EXPERIMENTAL_SHA256
	if (n == GIT_OID_SHA1_HEXSIZE)
		return git_oid_fromstr(out, hex, GIT_OID_SHA1);
	if (n == GIT_OID_SHA256_HEXSIZE)
		return git_oid_fromstr(out, hex, GIT_OID_SHA256);
	return -1;
#else
	if (n != GIT_OID_SHA1_HEXSIZE)
		return -1;
	return git_oid_fromstr(out, hex);
#endif
}

/*
 * Build-portable oid algorithm token. libgit2's git_oid_t (and GIT_OID_SHA1 /
 * GIT_OID_SHA256) exist only under the experimental SHA-256 build, so the
 * extension's callers and the hash helpers below speak this neutral token;
 * oid_hash maps it to the libgit2 type at the single experimental call site.
 */
typedef enum { OID_ALGO_SHA1 = 0, OID_ALGO_SHA256 = 1 } oid_algo;

/*
 * Hash `len` bytes of `data` as a git object of `type` into `out`, using the
 * repository's hash `algo`. The experimental libgit2 API takes the algo
 * explicitly; the stable API is sha1-only and omits it (the token is then
 * inert). The one place that conditional lives, so callers stay hash-agnostic.
 * Returns 0 on success (git_odb_hash's convention).
 */
static inline int oid_hash(git_oid *out, const void *data, size_t len,
			   git_object_t type, oid_algo algo)
{
#ifdef GIT_EXPERIMENTAL_SHA256
	return git_odb_hash(out, data, len, type,
			    algo == OID_ALGO_SHA256 ? GIT_OID_SHA256 : GIT_OID_SHA1);
#else
	(void)algo;
	return git_odb_hash(out, data, len, type);
#endif
}

#endif /* OIDHEX_H */
