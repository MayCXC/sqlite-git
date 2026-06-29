PREFIX ?= $(HOME)/.local
CFLAGS ?= -O2 -Wall -fPIC
LDFLAGS ?=
# The storage layer is split into dependency tiers, each its own .c:
#   storage.c     (core): sqlite only, the connection, txns and stmt mechanism.
#   storage_git.c (git):  + zlib + our clean-room delta, objects/refs/etc.
# A unit links exactly the tiers it uses, so the link graph (not naming) enforces
# the layering; the helper carries no libgit2.

# Local machine-specific overrides (gitignored), the mechanism git's own Makefile
# uses. Pin GIT_BUILD to run sqlite-git's tests against a specific git build.
-include config.mak

STORAGE_CORE = storage.c
STORAGE_GIT = storage_git.c
HDRS = $(wildcard *.h)

all: git-local-sqlite

# The local helper: core + git, libgit2-free (sqlite + zlib).
git-local-sqlite: git-local-sqlite.c $(STORAGE_CORE) $(STORAGE_GIT) $(HDRS)
	$(CC) $(CFLAGS) -DSQLITE_CORE -I. -o $@ $(filter %.c,$^) $(LDFLAGS) -lsqlite3 -lz

# When GIT_BUILD is set, the tests run against that build's git through its
# bin-wrappers (they set GIT_EXEC_PATH and the templates).
ifdef GIT_BUILD
export PATH := $(GIT_BUILD)/bin-wrappers:$(PATH)
endif

test: git-local-sqlite
	sh tests/test_helper.sh
	sh tests/test_git_helper.sh

clean:
	rm -f git-local-sqlite *.o *.a

.PHONY: all test clean
