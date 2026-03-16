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

SRCS = git0.c git0_vtab.c git0_objects.c git0_refs_vt.c
HDRS = git0.h

# Targets
all: git0.$(EXT)

git0.$(EXT): $(SRCS) $(HDRS)
	$(CC) -shared $(CFLAGS) -o $@ $(SRCS) $(LDFLAGS)

static: git0.a

git0.a: $(SRCS) $(HDRS)
	$(CC) -c $(CFLAGS) -DSQLITE_CORE -DGIT0_STATIC git0.c -o git0.o
	$(CC) -c $(CFLAGS) -DSQLITE_CORE -DGIT0_STATIC git0_vtab.c -o git0_vtab.o
	$(AR) rcs $@ git0.o git0_vtab.o
	@rm -f git0.o git0_vtab.o

install: git0.$(EXT) $(HDRS)
	install -d $(PREFIX)/lib $(PREFIX)/include
	install -m 755 git0.$(EXT) $(PREFIX)/lib/
	install -m 644 git0.h $(PREFIX)/include/

test: git0.$(EXT)
	@LD_LIBRARY_PATH=$(PREFIX)/lib sqlite3 -cmd ".load ./git0" < tests/test_basic.sql

clean:
	rm -f git0.$(EXT) git0.a git0.o git0_vtab.o

.PHONY: all static install test clean
