PREFIX ?= $(HOME)/.local
CFLAGS ?= -O2 -Wall -fPIC
LDFLAGS ?= -lgit2

ifneq ($(wildcard $(PREFIX)/include/git2.h),)
  CFLAGS += -I$(PREFIX)/include
  LDFLAGS += -L$(PREFIX)/lib -Wl,-rpath,$(PREFIX)/lib
endif

SRCS = git0.c git0_vtab.c

git0.so: $(SRCS)
	$(CC) -shared $(CFLAGS) -o $@ $(SRCS) $(LDFLAGS)

install: git0.so
	install -d $(PREFIX)/lib
	install -m 755 git0.so $(PREFIX)/lib/git0.so

test: git0.so
	@sqlite3 -cmd ".load ./git0" "SELECT git_rev_parse('.', 'HEAD');"
	@sqlite3 -cmd ".load ./git0" "SELECT length(git_blob('.', 'HEAD', 'Makefile'));"
	@sqlite3 -cmd ".load ./git0" "SELECT git_commit_summary('.', 'HEAD');"
	@sqlite3 -cmd ".load ./git0" "SELECT * FROM git_log('.') LIMIT 3;"
	@sqlite3 -cmd ".load ./git0" "SELECT * FROM git_tree('.', 'HEAD');"
	@sqlite3 -cmd ".load ./git0" "SELECT * FROM git_refs('.');"
	@echo "All tests passed."

clean:
	rm -f git0.so

.PHONY: install test clean
