PREFIX ?= $(HOME)/.local
CFLAGS ?= -O2 -Wall -fPIC
LDFLAGS ?= -lgit2

# Platform detection
ifeq ($(shell uname -s),Darwin)
  EXT = dylib
else
  EXT = so
endif

# Find libgit2 in PREFIX
ifneq ($(wildcard $(PREFIX)/include/git2.h),)
  CFLAGS += -I$(PREFIX)/include
  LDFLAGS += -L$(PREFIX)/lib -Wl,-rpath,$(PREFIX)/lib
endif

SRCS = git0.c git0_vtab.c git0_objects.c git0_refs_vt.c git0_repo.c git0_lfs.c
HDRS = git0.h

# Targets
all: git0.$(EXT) git-remote-sqlite git-helper-sqlite

git0.$(EXT): $(SRCS) $(HDRS)
	$(CC) -shared $(CFLAGS) -o $@ $(SRCS) $(LDFLAGS)

static: git0.a

git-remote-sqlite: git-remote-sqlite.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) -lsqlite3

git-helper-sqlite: git-helper-sqlite.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

git0.a: $(SRCS) $(HDRS)
	$(CC) -c $(CFLAGS) -DSQLITE_CORE -DGIT0_STATIC git0.c -o git0.o
	$(CC) -c $(CFLAGS) -DSQLITE_CORE -DGIT0_STATIC git0_vtab.c -o git0_vtab.o
	$(AR) rcs $@ git0.o git0_vtab.o
	@rm -f git0.o git0_vtab.o

install: git0.$(EXT) git-remote-sqlite $(HDRS)
	install -d $(PREFIX)/lib $(PREFIX)/include $(PREFIX)/bin
	install -m 755 git0.$(EXT) $(PREFIX)/lib/
	install -m 644 git0.h $(PREFIX)/include/
	install -m 755 git-remote-sqlite $(PREFIX)/bin/
	install -m 755 git-helper-sqlite $(PREFIX)/bin/

test: git0.$(EXT)
	@LD_LIBRARY_PATH=$(PREFIX)/lib sqlite3 -cmd ".load ./git0" < tests/test_basic.sql

clean:
	rm -f git0.$(EXT) git0.a git0.o git0_vtab.o git-remote-sqlite git-helper-sqlite

.PHONY: all static install test clean
