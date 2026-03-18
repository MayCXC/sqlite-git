PREFIX ?= $(HOME)/.local
CFLAGS ?= -O2 -Wall -fPIC
LDFLAGS ?= -lgit2

ifeq ($(shell uname -s),Darwin)
  EXT = dylib
else
  EXT = so
endif

ifneq ($(wildcard $(PREFIX)/include/git2.h),)
  CFLAGS += -I$(PREFIX)/include
  LDFLAGS += -L$(PREFIX)/lib -Wl,-rpath,$(PREFIX)/lib
endif

# Shared storage layer (used by extension and binaries)
STORAGE = storage.c vendor/fossil-delta.c vendor/sha256.c

# Extension sources
EXT_SRCS = git0.c git0_vtab.c git0_objects.c git0_refs_vt.c git0_repo.c git0_lfs.c git0_storage.c $(STORAGE)

# Targets
all: git0.$(EXT) git-sqlite git-lfs-sqlite-transfer

git0.$(EXT): $(EXT_SRCS) git0.h storage.h
	$(CC) -shared $(CFLAGS) -I. -o $@ $(EXT_SRCS) $(LDFLAGS) -lsqlite3 -lz

git-sqlite: git-sqlite.c git-local-sqlite.c git-remote-sqlite.c $(STORAGE)
	$(CC) $(CFLAGS) -I. -o $@ $^ $(LDFLAGS) -lsqlite3 -lz

git-local-sqlite: git-sqlite
	ln -sf $< $@

git-remote-sqlite: git-sqlite
	ln -sf $< $@

git-lfs-sqlite-transfer: git-lfs-sqlite-transfer.c $(STORAGE)
	$(CC) $(CFLAGS) -I. -o $@ $^ $(LDFLAGS) -lsqlite3 -lz

install: git0.$(EXT) git-sqlite git-lfs-sqlite-transfer
	install -d $(PREFIX)/lib $(PREFIX)/include $(PREFIX)/bin
	install -m 755 git0.$(EXT) $(PREFIX)/lib/
	install -m 644 git0.h storage.h $(PREFIX)/include/
	install -m 755 git-sqlite git-lfs-sqlite-transfer $(PREFIX)/bin/
	ln -sf git-sqlite $(PREFIX)/bin/git-local-sqlite
	ln -sf git-sqlite $(PREFIX)/bin/git-remote-sqlite

test: all git-local-sqlite
	bash tests/test_helper.sh
	@rm -f /tmp/git0-test.db
	sqlite3 /tmp/git0-test.db -cmd ".load ./git0" <tests/test_basic.sql
	@rm -f /tmp/git0-test.db

clean:
	rm -f git0.$(EXT) git-sqlite git-local-sqlite git-remote-sqlite git-lfs-sqlite-transfer *.o *.a

.PHONY: all install test clean
