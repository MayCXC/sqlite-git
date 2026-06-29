PREFIX ?= $(HOME)/.local
CFLAGS ?= -O2 -Wall -fPIC
LDFLAGS ?=
# The storage layer is split into three dependency tiers, each its own .c:
#   storage.c         (core): sqlite only; connection, txns, the stmt mechanism.
#   storage_git.c     (git):  + zlib + our clean-room delta; objects/refs/etc.
#   storage_git_lfs.c (lfs):  + sha256; git-lfs content, INDEPENDENT of the git
#                             object layer (it never links storage_git.c).
# A unit links exactly the tiers it uses, so the link graph (not naming) enforces
# the layering: the lfs extension and transfer agent carry no git object store and
# no libgit2, and only the git0 extension links libgit2.
#
# The git0 extension is built twice, to separate directories, both named git0.so
# so `.load <dir>/git0` resolves the same sqlite3_git_init regardless of
# directory. The two builds differ ONLY in which libgit2 they compile and link
# against: stock libgit2 (sha1) vs libgit2-experimental (sha1+sha256). Hash
# support follows from the headers, since the experimental install's git2.h
# defines GIT_EXPERIMENTAL_SHA256 and the stock one does not (see oidhex.h); there
# is no define or proxy of ours, just the -I/-L pointing at one libgit2 or the other.

# Local, machine-specific overrides (gitignored), the same mechanism git's own
# Makefile uses. Pin GIT_BUILD to run sqlite-git's tests against a specific git
# build, and LIBGIT2 / LIBGIT2_EXPERIMENTAL at your two libgit2 installs.
-include config.mak

ifeq ($(shell uname -s),Darwin)
  EXT = dylib
else
  EXT = so
endif

# The two libgit2 installs (override in config.mak).
LIBGIT2 ?= /usr/local
LIBGIT2_EXPERIMENTAL ?= /usr/local

# Storage tiers (see the header comment above).
STORAGE_CORE = storage.c
STORAGE_GIT = storage_git.c
STORAGE_GIT_LFS = storage_git_lfs.c vendor/sha256.c

# The git0 extension: core + git + libgit2 (NO lfs, NO sha256). git0_lfs.c is the
# separate gitlfs extension, not part of git0.so.
EXT_SRCS = git0.c git0_vtab.c git0_repo.c git0_scalar.c git0_backend.c \
	   git0_refs_vt.c git0_objects_vt.c \
	   $(STORAGE_CORE) $(STORAGE_GIT)
HDRS = $(wildcard *.h)

# "all" builds the stock (sha1) extension plus the libgit2-free binaries (the
# helper, the lfs transfer agent, and the standalone lfs extension). The dual
# git0 build needs libgit2-experimental, so it is its own target: make experimental
all: build/stock/git0.$(EXT) git-local-sqlite git-lfs-sqlite-transfer build/gitlfs.$(EXT)
experimental: build/experimental/git0.$(EXT)

build/stock/git0.$(EXT): $(EXT_SRCS) $(HDRS)
	@mkdir -p build/stock
	$(CC) -shared $(CFLAGS) -I$(LIBGIT2)/include -I. -o $@ $(EXT_SRCS) \
		$(LDFLAGS) -L$(LIBGIT2)/lib -Wl,-rpath,$(LIBGIT2)/lib -lgit2 -lz

build/experimental/git0.$(EXT): $(EXT_SRCS) $(HDRS)
	@mkdir -p build/experimental
	$(CC) -shared $(CFLAGS) -I$(LIBGIT2_EXPERIMENTAL)/include -I. -o $@ $(EXT_SRCS) \
		$(LDFLAGS) -L$(LIBGIT2_EXPERIMENTAL)/lib -Wl,-rpath,$(LIBGIT2_EXPERIMENTAL)/lib -lgit2-experimental -lz

# The local helper: core + git, libgit2-free (sqlite + zlib).
git-local-sqlite: git-local-sqlite.c $(STORAGE_CORE) $(STORAGE_GIT) $(HDRS)
	$(CC) $(CFLAGS) -DSQLITE_CORE -I. -o $@ $(filter %.c,$^) $(LDFLAGS) -lsqlite3 -lz

# The git-lfs transfer agent: core + lfs, libgit2-free and zlib-free (sqlite +
# vendored sha256; lfs content is stored raw).
git-lfs-sqlite-transfer: git-lfs-sqlite-transfer.c $(STORAGE_CORE) $(STORAGE_GIT_LFS) $(HDRS)
	$(CC) $(CFLAGS) -DSQLITE_CORE -I. -o $@ $(filter %.c,$^) $(LDFLAGS) -lsqlite3

# The standalone git-lfs extension: core + lfs, libgit2-free and zlib-free, built
# once (no sha1/sha256 flavor split, it carries its own sha256). `.load <dir>/gitlfs`
# resolves sqlite3_gitlfs_init.
build/gitlfs.$(EXT): git0_lfs.c $(STORAGE_CORE) $(STORAGE_GIT_LFS) $(HDRS)
	@mkdir -p build
	$(CC) -shared $(CFLAGS) -I. -o $@ git0_lfs.c $(STORAGE_CORE) $(STORAGE_GIT_LFS) $(LDFLAGS)

install: build/stock/git0.$(EXT) build/gitlfs.$(EXT) git-local-sqlite git-lfs-sqlite-transfer
	install -d $(PREFIX)/lib $(PREFIX)/include $(PREFIX)/bin
	install -m 755 build/stock/git0.$(EXT) $(PREFIX)/lib/git0.$(EXT)
	install -m 755 build/gitlfs.$(EXT) $(PREFIX)/lib/gitlfs.$(EXT)
	install -m 644 git0.h storage.h storage_git.h storage_git_lfs.h $(PREFIX)/include/
	install -m 755 git-local-sqlite git-lfs-sqlite-transfer $(PREFIX)/bin/

# When GIT_BUILD is set (via config.mak above, or the environment), sqlite-git's
# tests run against that build's git: prepend its bin-wrappers to PATH, which set
# GIT_EXEC_PATH and templates themselves. The tests stay framework-agnostic
# (plain `git`); unset, they use the system git.
ifdef GIT_BUILD
export PATH := $(GIT_BUILD)/bin-wrappers:$(PATH)
endif

# The helper and transfer agent are libgit2-free, tested once. The extension
# suite (sha1 behavior) is shared and run against BOTH builds: that is what
# catches a build-specific regression (e.g. a sha1-only buffer size). Tests that
# need sha256 behavior live in tests that load only the experimental build.
EXT_BUILDS = build/stock build/experimental

# Rebuild from clean so a prior "test-asan" (which builds the sanitized git0 into
# the same build/stock and build/experimental dirs) can never leave artifacts a
# stock test would load into a non-ASan sqlite3. clean is a $(MAKE) dispatch, not
# a prerequisite, so it is ordered before the build (a clean prerequisite races
# the build targets under parallel make). Symmetric with "test-asan".
test:
	$(MAKE) clean
	$(MAKE) all experimental
	sh tests/test_helper.sh
	for d in $(EXT_BUILDS); do \
		echo "# git0 extension suite: $$d"; \
		rm -f /tmp/git0-test.db; \
		sqlite3 /tmp/git0-test.db -cmd ".load $$d/git0" <tests/test_basic.sql || exit 1; \
		rm -f /tmp/git0-test.db; \
		GIT0=$$d/git0 sh tests/test_concurrent.sh || exit 1; \
		GIT0=$$d/git0 sh tests/test_object_format.sh || exit 1; \
		GIT0=$$d/git0 sh tests/test_reflog.sh || exit 1; \
		GIT0=$$d/git0 sh tests/test_vtab.sh || exit 1; \
	done
	echo "# git-lfs extension suite"; \
	rm -f /tmp/gitlfs-test.db; \
	sqlite3 /tmp/gitlfs-test.db -cmd ".load build/gitlfs" <tests/test_lfs.sql || exit 1; \
	rm -f /tmp/gitlfs-test.db
	sh tests/test_git_helper.sh

# Sanitizer build: ASan + UBSan, run against both builds.
SAN_CFLAGS = -O0 -g -Wall -fPIC -fsanitize=address,undefined -fno-omit-frame-pointer
SAN_LDFLAGS = -fsanitize=address,undefined

test-asan:
	$(MAKE) clean
	$(MAKE) all experimental 'CFLAGS=$(SAN_CFLAGS)' 'LDFLAGS=$(SAN_LDFLAGS)'
	ASAN_OPTIONS=detect_leaks=0 sh tests/test_helper.sh
	for d in $(EXT_BUILDS); do \
		echo "# git0 extension suite (asan): $$d"; \
		rm -f /tmp/git0-asan.db; \
		ASAN_OPTIONS=detect_leaks=0 LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) \
			sqlite3 /tmp/git0-asan.db -cmd ".load $$d/git0" <tests/test_basic.sql || exit 1; \
		rm -f /tmp/git0-asan.db; \
		ASAN_OPTIONS=detect_leaks=0 \
			SQLITE3="env LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) sqlite3" \
			GIT0=$$d/git0 sh tests/test_concurrent.sh || exit 1; \
		ASAN_OPTIONS=detect_leaks=0 \
			SQLITE3="env LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) sqlite3" \
			GIT0=$$d/git0 sh tests/test_object_format.sh || exit 1; \
		ASAN_OPTIONS=detect_leaks=0 \
			SQLITE3="env LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) sqlite3" \
			GIT0=$$d/git0 sh tests/test_reflog.sh || exit 1; \
		ASAN_OPTIONS=detect_leaks=0 \
			SQLITE3="env LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) sqlite3" \
			GIT0=$$d/git0 sh tests/test_vtab.sh || exit 1; \
	done
	echo "# git-lfs extension suite (asan)"; \
	rm -f /tmp/gitlfs-asan.db; \
	ASAN_OPTIONS=detect_leaks=0 LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) \
		sqlite3 /tmp/gitlfs-asan.db -cmd ".load build/gitlfs" <tests/test_lfs.sql || exit 1; \
	rm -f /tmp/gitlfs-asan.db
	ASAN_OPTIONS=detect_leaks=0 sh tests/test_git_helper.sh
	@echo "ASan + UBSan: all tests passed"

clean:
	rm -rf build
	rm -f git-local-sqlite git-lfs-sqlite-transfer *.o *.a

.PHONY: all experimental install test test-asan clean
