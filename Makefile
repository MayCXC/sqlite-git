PREFIX ?= $(HOME)/.local
CFLAGS ?= -O2 -Wall -fPIC
LDFLAGS ?= -lgit2

# Find libgit2 in common locations
ifneq ($(wildcard $(PREFIX)/include/git2.h),)
  CFLAGS += -I$(PREFIX)/include
  LDFLAGS += -L$(PREFIX)/lib -Wl,-rpath,$(PREFIX)/lib
endif

git0.so: git0.c
	$(CC) -shared $(CFLAGS) -o $@ $< $(LDFLAGS)

install: git0.so
	install -d $(PREFIX)/lib
	install -m 755 git0.so $(PREFIX)/lib/git0.so

test: git0.so
	@echo "SELECT git_rev_parse('.', 'HEAD');" | sqlite3 -cmd ".load ./git0"
	@echo "SELECT length(git_blob('.', 'HEAD', 'Makefile'));" | sqlite3 -cmd ".load ./git0"
	@echo "SELECT git_commit_summary('.', 'HEAD');" | sqlite3 -cmd ".load ./git0"
	@echo "SELECT git_type('.', 'HEAD');" | sqlite3 -cmd ".load ./git0"
	@echo "All tests passed."

clean:
	rm -f git0.so

.PHONY: install test clean
