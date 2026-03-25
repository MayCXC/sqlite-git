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
EXT_SRCS = git0.c git0_vtab.c git0_objects.c git0_refs_vt.c git0_repo.c git0_lfs.c git0_storage.c git0_backend.c $(STORAGE)

# Targets
all: git0.$(EXT) git-local-sqlite git-lfs-sqlite-transfer

git0.$(EXT): $(EXT_SRCS) git0.h storage.h git0_internal.h
	$(CC) -shared $(CFLAGS) -I. -o $@ $(EXT_SRCS) $(LDFLAGS) -lz

git-local-sqlite: git-local-sqlite.c git0_backend.c $(STORAGE)
	$(CC) $(CFLAGS) -DSQLITE_CORE -I. -o $@ $^ $(LDFLAGS) -lsqlite3 -lz

git-lfs-sqlite-transfer: git-lfs-sqlite-transfer.c git0_backend.c $(STORAGE)
	$(CC) $(CFLAGS) -DSQLITE_CORE -I. -o $@ $^ $(LDFLAGS) -lsqlite3 -lz

install: git0.$(EXT) git-local-sqlite git-lfs-sqlite-transfer
	install -d $(PREFIX)/lib $(PREFIX)/include $(PREFIX)/bin
	install -m 755 git0.$(EXT) $(PREFIX)/lib/
	install -m 644 git0.h storage.h $(PREFIX)/include/
	install -m 755 git-local-sqlite git-lfs-sqlite-transfer $(PREFIX)/bin/

test: all
	bash tests/test_helper.sh
	@rm -f /tmp/git0-test.db
	sqlite3 /tmp/git0-test.db -cmd ".load ./git0" <tests/test_basic.sql
	@rm -f /tmp/git0-test.db

# Sanitizer build: ASan + UBSan for memory safety and undefined behavior checks
SAN_CFLAGS = -O0 -g -Wall -fPIC -fsanitize=address,undefined -fno-omit-frame-pointer
SAN_LDFLAGS = -fsanitize=address,undefined

test-asan: clean
	$(MAKE) 'CFLAGS=$(SAN_CFLAGS) -I$(PREFIX)/include' 'LDFLAGS=-lgit2 -L$(PREFIX)/lib -Wl,-rpath,$(PREFIX)/lib $(SAN_LDFLAGS)'
	ASAN_OPTIONS=detect_leaks=0 bash tests/test_helper.sh
	@rm -f /tmp/git0-asan.db
	ASAN_OPTIONS=detect_leaks=0 LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) \
		sqlite3 /tmp/git0-asan.db -cmd ".load ./git0" <tests/test_basic.sql
	@rm -f /tmp/git0-asan.db
	@echo "ASan + UBSan: all tests passed"

clean:
	rm -f git0.$(EXT) git-local-sqlite git-lfs-sqlite-transfer *.o *.a

.PHONY: all install test test-asan clean
