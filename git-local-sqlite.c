/*
 * git-local-sqlite: unified local helper for SQLite-backed storage.
 *
 * Handles both ODB and ref operations in a single process, following
 * the protocol defined in git's helper.h. Invoked as:
 *
 *   git-local-sqlite <gitdir>
 *
 * Stores objects in objects_loose(oid, type, size, data) and refs in
 * refs(refname, target_oid, target_symref). Both tables live in a
 * single SQLite database at <gitdir>/sqlite.db.
 *
 * Also serves as git-remote-sqlite when invoked via argv[0] symlink.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sqlite3.h>

static sqlite3 *db;

/* Prepared statements */
static sqlite3_stmt *st_obj_read, *st_obj_write, *st_obj_exists, *st_obj_list;
static sqlite3_stmt *st_ref_read, *st_ref_write, *st_ref_delete, *st_ref_list;
static sqlite3_stmt *st_reflog_read, *st_reflog_append, *st_reflog_exists, *st_reflog_delete;

static const char *type_name(int t) {
	switch (t) {
		case 1: return "commit";
		case 2: return "tree";
		case 3: return "blob";
		case 4: return "tag";
		default: return "unknown";
	}
}

static int type_from_name(const char *s) {
	if (!strcmp(s, "commit")) return 1;
	if (!strcmp(s, "tree")) return 2;
	if (!strcmp(s, "blob")) return 3;
	if (!strcmp(s, "tag")) return 4;
	return 0;
}

static int open_db(const char *path_arg) {
	char path[4096];
	char gitdir[4096];
	size_t len;

	/*
	 * The ODB backend passes .git/objects as the path, while the
	 * ref backend passes .git. Normalize to the gitdir root by
	 * stripping a trailing /objects suffix.
	 */
	snprintf(gitdir, sizeof(gitdir), "%s", path_arg);
	len = strlen(gitdir);
	if (len > 8 && !strcmp(gitdir + len - 8, "/objects"))
		gitdir[len - 8] = '\0';

	snprintf(path, sizeof(path), "%s/sqlite.db", gitdir);

	if (sqlite3_open(path, &db) != SQLITE_OK)
		return -1;

	sqlite3_exec(db,
		"PRAGMA busy_timeout = 5000;"
		"PRAGMA synchronous = NORMAL;"
		"PRAGMA journal_mode = WAL;"

		"CREATE TABLE IF NOT EXISTS objects_loose("
		"  oid TEXT PRIMARY KEY,"
		"  type INTEGER NOT NULL,"
		"  size INTEGER NOT NULL,"
		"  data BLOB NOT NULL);"

		"CREATE TABLE IF NOT EXISTS refs("
		"  refname TEXT PRIMARY KEY,"
		"  target_oid TEXT,"
		"  target_symref TEXT);"

		"CREATE TABLE IF NOT EXISTS reflog("
		"  refname TEXT NOT NULL,"
		"  idx INTEGER NOT NULL,"
		"  old_oid TEXT NOT NULL,"
		"  new_oid TEXT NOT NULL,"
		"  committer TEXT NOT NULL,"
		"  timestamp INTEGER NOT NULL,"
		"  tz INTEGER NOT NULL,"
		"  msg TEXT NOT NULL DEFAULT '',"
		"  PRIMARY KEY(refname, idx));",
		0, 0, 0);

	/* Object statements */
	sqlite3_prepare_v2(db,
		"SELECT type, size, data FROM objects_loose WHERE oid = ?",
		-1, &st_obj_read, 0);
	sqlite3_prepare_v2(db,
		"INSERT OR IGNORE INTO objects_loose(oid, type, size, data) VALUES(?, ?, ?, ?)",
		-1, &st_obj_write, 0);
	sqlite3_prepare_v2(db,
		"SELECT 1 FROM objects_loose WHERE oid = ?",
		-1, &st_obj_exists, 0);
	sqlite3_prepare_v2(db,
		"SELECT oid, type, size FROM objects_loose",
		-1, &st_obj_list, 0);

	/* Ref statements */
	sqlite3_prepare_v2(db,
		"SELECT target_oid, target_symref FROM refs WHERE refname = ?",
		-1, &st_ref_read, 0);
	sqlite3_prepare_v2(db,
		"INSERT OR REPLACE INTO refs(refname, target_oid, target_symref) VALUES(?, ?, ?)",
		-1, &st_ref_write, 0);
	sqlite3_prepare_v2(db,
		"DELETE FROM refs WHERE refname = ?",
		-1, &st_ref_delete, 0);
	sqlite3_prepare_v2(db,
		"SELECT refname, target_oid, target_symref FROM refs ORDER BY refname",
		-1, &st_ref_list, 0);

	/* Reflog statements */
	sqlite3_prepare_v2(db,
		"SELECT old_oid, new_oid, committer, timestamp, tz, msg "
		"FROM reflog WHERE refname = ? ORDER BY idx ASC",
		-1, &st_reflog_read, 0);
	sqlite3_prepare_v2(db,
		"INSERT INTO reflog(refname, idx, old_oid, new_oid, committer, timestamp, tz, msg) "
		"VALUES(?, COALESCE((SELECT MAX(idx)+1 FROM reflog WHERE refname = ?), 0), ?, ?, ?, ?, ?, ?)",
		-1, &st_reflog_append, 0);
	sqlite3_prepare_v2(db,
		"SELECT 1 FROM reflog WHERE refname = ? LIMIT 1",
		-1, &st_reflog_exists, 0);
	sqlite3_prepare_v2(db,
		"DELETE FROM reflog WHERE refname = ?",
		-1, &st_reflog_delete, 0);

	return 0;
}

/* ---- ODB commands ---- */

static void cmd_capabilities(void) {
	printf("get\n");
	printf("info\n");
	printf("put\n");
	printf("have\n");
	printf("list-objects\n");
	printf("odb-transaction\n");
	printf("read\n");
	printf("list\n");
	printf("transaction\n");
	printf("create\n");
	printf("remove\n");
	printf("reflog-read\n");
	printf("reflog-exists\n");
	printf("reflog-delete\n");
	printf("\n");
	fflush(stdout);
}

static void cmd_info(const char *oid) {
	sqlite3_reset(st_obj_read);
	sqlite3_bind_text(st_obj_read, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st_obj_read) != SQLITE_ROW) {
		printf("missing\n");
	} else {
		int type = sqlite3_column_int(st_obj_read, 0);
		int size = sqlite3_column_int(st_obj_read, 1);
		printf("%s %d\n", type_name(type), size);
	}
	fflush(stdout);
}

static void cmd_get(const char *oid) {
	sqlite3_reset(st_obj_read);
	sqlite3_bind_text(st_obj_read, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st_obj_read) != SQLITE_ROW) {
		printf("missing\n");
		fflush(stdout);
		return;
	}
	int type = sqlite3_column_int(st_obj_read, 0);
	int size = sqlite3_column_int(st_obj_read, 1);
	const void *data = sqlite3_column_blob(st_obj_read, 2);

	printf("%s %d\n", type_name(type), size);
	fflush(stdout);
	if (size > 0)
		fwrite(data, 1, size, stdout);
	fflush(stdout);
}

static void cmd_put(const char *oid, const char *type_str, unsigned long size) {
	int type = type_from_name(type_str);
	unsigned char *data = malloc(size ? size : 1);
	size_t got = 0;

	while (got < size) {
		size_t n = fread(data + got, 1, size - got, stdin);
		if (n == 0) break;
		got += n;
	}

	sqlite3_reset(st_obj_write);
	sqlite3_bind_text(st_obj_write, 1, oid, -1, SQLITE_STATIC);
	sqlite3_bind_int(st_obj_write, 2, type);
	sqlite3_bind_int64(st_obj_write, 3, (sqlite3_int64)size);
	sqlite3_bind_blob(st_obj_write, 4, data, (int)size, SQLITE_STATIC);
	sqlite3_step(st_obj_write);

	free(data);
	printf("%s\n", oid);
	fflush(stdout);
}

static void cmd_have(const char *oid) {
	sqlite3_reset(st_obj_exists);
	sqlite3_bind_text(st_obj_exists, 1, oid, -1, SQLITE_STATIC);
	if (sqlite3_step(st_obj_exists) == SQLITE_ROW)
		printf("true\n");
	else
		printf("false\n");
	fflush(stdout);
}

static void cmd_list_objects(void) {
	sqlite3_reset(st_obj_list);
	while (sqlite3_step(st_obj_list) == SQLITE_ROW) {
		const char *oid = (const char *)sqlite3_column_text(st_obj_list, 0);
		int type = sqlite3_column_int(st_obj_list, 1);
		int size = sqlite3_column_int(st_obj_list, 2);
		printf("%s %s %d\n", oid, type_name(type), size);
	}
	printf("\n");
	fflush(stdout);
}

static void cmd_odb_transaction_begin(void) {
	sqlite3_exec(db, "BEGIN", 0, 0, 0);
	printf("ok\n");
	fflush(stdout);
}

static void cmd_odb_transaction_commit(void) {
	sqlite3_exec(db, "COMMIT", 0, 0, 0);
	printf("ok\n");
	fflush(stdout);
}

/* ---- Ref commands ---- */

static void cmd_read(const char *refname) {
	sqlite3_reset(st_ref_read);
	sqlite3_bind_text(st_ref_read, 1, refname, -1, SQLITE_STATIC);
	if (sqlite3_step(st_ref_read) != SQLITE_ROW) {
		printf("missing\n");
	} else {
		const char *oid = (const char *)sqlite3_column_text(st_ref_read, 0);
		const char *symref = (const char *)sqlite3_column_text(st_ref_read, 1);
		if (symref && *symref)
			printf("symref %s\n", symref);
		else
			printf("%s\n", oid);
	}
	fflush(stdout);
}

static void cmd_list(const char *prefix) {
	if (prefix && *prefix) {
		/* Filtered list using LIKE */
		char pattern[4096];
		snprintf(pattern, sizeof(pattern), "%s%%", prefix);
		sqlite3_stmt *st;
		sqlite3_prepare_v2(db,
			"SELECT refname, target_oid, target_symref FROM refs "
			"WHERE refname LIKE ? ORDER BY refname",
			-1, &st, 0);
		sqlite3_bind_text(st, 1, pattern, -1, SQLITE_STATIC);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			const char *oid = (const char *)sqlite3_column_text(st, 1);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (sym && *sym)
				printf("%s symref %s\n", name, sym);
			else
				printf("%s %s\n", name, oid);
		}
		sqlite3_finalize(st);
	} else {
		sqlite3_reset(st_ref_list);
		while (sqlite3_step(st_ref_list) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st_ref_list, 0);
			const char *oid = (const char *)sqlite3_column_text(st_ref_list, 1);
			const char *sym = (const char *)sqlite3_column_text(st_ref_list, 2);
			if (sym && *sym)
				printf("%s symref %s\n", name, sym);
			else
				printf("%s %s\n", name, oid);
		}
	}
	printf("\n");
	fflush(stdout);
}

static void cmd_create(void) {
	/* Tables already created in open_db. */
	printf("ok\n");
	fflush(stdout);
}

static void cmd_remove(void) {
	sqlite3_exec(db, "DROP TABLE IF EXISTS objects_loose;"
		     "DROP TABLE IF EXISTS refs;"
		     "DROP TABLE IF EXISTS reflog;", 0, 0, 0);
	printf("ok\n");
	fflush(stdout);
}

/* Transaction state for refs */
static int in_ref_transaction;

static void cmd_transaction_begin(void) {
	if (!in_ref_transaction) {
		sqlite3_exec(db, "SAVEPOINT ref_txn", 0, 0, 0);
		in_ref_transaction = 1;
	}
	printf("ok\n");
	fflush(stdout);
}

static void cmd_transaction_update(const char *args) {
	/* refname new_oid old_oid [msg] */
	char refname[4096], new_oid[65], old_oid[65];
	if (sscanf(args, "%4095s %64s %64s", refname, new_oid, old_oid) < 2) {
		printf("error bad arguments\n");
		fflush(stdout);
		return;
	}
	sqlite3_reset(st_ref_write);
	sqlite3_bind_text(st_ref_write, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st_ref_write, 2, new_oid, -1, SQLITE_STATIC);
	sqlite3_bind_null(st_ref_write, 3);
	sqlite3_step(st_ref_write);
	printf("ok\n");
	fflush(stdout);
}

static void cmd_transaction_create(const char *args) {
	/* refname new_oid [msg] */
	char refname[4096], new_oid[65];
	if (sscanf(args, "%4095s %64s", refname, new_oid) < 2) {
		printf("error bad arguments\n");
		fflush(stdout);
		return;
	}
	sqlite3_reset(st_ref_write);
	sqlite3_bind_text(st_ref_write, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st_ref_write, 2, new_oid, -1, SQLITE_STATIC);
	sqlite3_bind_null(st_ref_write, 3);
	sqlite3_step(st_ref_write);
	printf("ok\n");
	fflush(stdout);
}

static void cmd_transaction_delete(const char *args) {
	/* refname old_oid [msg] */
	char refname[4096];
	if (sscanf(args, "%4095s", refname) < 1) {
		printf("error bad arguments\n");
		fflush(stdout);
		return;
	}
	sqlite3_reset(st_ref_delete);
	sqlite3_bind_text(st_ref_delete, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st_ref_delete);
	printf("ok\n");
	fflush(stdout);
}

static void cmd_transaction_create_symref(const char *args) {
	/* refname target [msg] */
	char refname[4096], target[4096];
	if (sscanf(args, "%4095s %4095s", refname, target) < 2) {
		printf("error bad arguments\n");
		fflush(stdout);
		return;
	}
	sqlite3_reset(st_ref_write);
	sqlite3_bind_text(st_ref_write, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_null(st_ref_write, 2);
	sqlite3_bind_text(st_ref_write, 3, target, -1, SQLITE_STATIC);
	sqlite3_step(st_ref_write);
	printf("ok\n");
	fflush(stdout);
}

static void cmd_transaction_prepare(void) {
	printf("ok\n");
	fflush(stdout);
}

static void cmd_transaction_finish(void) {
	if (in_ref_transaction) {
		sqlite3_exec(db, "RELEASE ref_txn", 0, 0, 0);
		in_ref_transaction = 0;
	}
	printf("ok\n");
	fflush(stdout);
}

static void cmd_transaction_abort(void) {
	if (in_ref_transaction) {
		sqlite3_exec(db, "ROLLBACK TO ref_txn; RELEASE ref_txn", 0, 0, 0);
		in_ref_transaction = 0;
	}
	printf("ok\n");
	fflush(stdout);
}

/* ---- Reflog commands ---- */

static void cmd_reflog_read(const char *refname) {
	sqlite3_reset(st_reflog_read);
	sqlite3_bind_text(st_reflog_read, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st_reflog_read) == SQLITE_ROW) {
		const char *old_oid = (const char *)sqlite3_column_text(st_reflog_read, 0);
		const char *new_oid = (const char *)sqlite3_column_text(st_reflog_read, 1);
		const char *committer = (const char *)sqlite3_column_text(st_reflog_read, 2);
		sqlite3_int64 timestamp = sqlite3_column_int64(st_reflog_read, 3);
		int tz = sqlite3_column_int(st_reflog_read, 4);
		const char *msg = (const char *)sqlite3_column_text(st_reflog_read, 5);
		/* Native reflog format: old new committer timestamp tz\tmsg */
		printf("%s %s %s %lld %+05d\t%s\n",
		       old_oid, new_oid, committer,
		       (long long)timestamp,
		       tz, msg ? msg : "");
	}
	printf("\n");
	fflush(stdout);
}

static void cmd_reflog_exists(const char *refname) {
	sqlite3_reset(st_reflog_exists);
	sqlite3_bind_text(st_reflog_exists, 1, refname, -1, SQLITE_STATIC);
	if (sqlite3_step(st_reflog_exists) == SQLITE_ROW)
		printf("true\n");
	else
		printf("false\n");
	fflush(stdout);
}

static void cmd_reflog_delete(const char *refname) {
	sqlite3_reset(st_reflog_delete);
	sqlite3_bind_text(st_reflog_delete, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st_reflog_delete);
	printf("ok\n");
	fflush(stdout);
}

/* ---- Entry point ---- */

int local_main(int argc, char **argv) {
	if (argc < 2) {
		fprintf(stderr, "usage: git-local-sqlite <gitdir>\n");
		return 1;
	}

	if (open_db(argv[1]) < 0) {
		fprintf(stderr, "error: cannot open database at %s/sqlite.db\n", argv[1]);
		return 1;
	}

	char line[8192];
	while (fgets(line, sizeof(line), stdin)) {
		size_t len = strlen(line);
		if (len > 0 && line[len - 1] == '\n') line[--len] = 0;

		/* ODB commands */
		if (!strcmp(line, "capabilities")) {
			cmd_capabilities();
		} else if (!strncmp(line, "info ", 5)) {
			cmd_info(line + 5);
		} else if (!strncmp(line, "get ", 4)) {
			cmd_get(line + 4);
		} else if (!strncmp(line, "put ", 4)) {
			char oid[65], type_str[32];
			unsigned long size;
			if (sscanf(line + 4, "%64s %31s %lu", oid, type_str, &size) == 3)
				cmd_put(oid, type_str, size);
		} else if (!strncmp(line, "have ", 5)) {
			cmd_have(line + 5);
		} else if (!strcmp(line, "list-objects")) {
			cmd_list_objects();
		} else if (!strcmp(line, "odb-transaction-begin")) {
			cmd_odb_transaction_begin();
		} else if (!strcmp(line, "odb-transaction-commit")) {
			cmd_odb_transaction_commit();

		/* Ref commands */
		} else if (!strncmp(line, "read ", 5)) {
			cmd_read(line + 5);
		} else if (!strncmp(line, "list ", 5)) {
			cmd_list(line + 5);
		} else if (!strcmp(line, "list")) {
			cmd_list(NULL);
		} else if (!strcmp(line, "create")) {
			cmd_create();
		} else if (!strcmp(line, "remove")) {
			cmd_remove();
		} else if (!strcmp(line, "transaction-begin")) {
			cmd_transaction_begin();
		} else if (!strncmp(line, "transaction-update ", 19)) {
			cmd_transaction_update(line + 19);
		} else if (!strncmp(line, "transaction-create-symref ", 25)) {
			cmd_transaction_create_symref(line + 25);
		} else if (!strncmp(line, "transaction-create ", 19)) {
			cmd_transaction_create(line + 19);
		} else if (!strncmp(line, "transaction-delete ", 19)) {
			cmd_transaction_delete(line + 19);
		} else if (!strcmp(line, "transaction-prepare")) {
			cmd_transaction_prepare();
		} else if (!strcmp(line, "transaction-finish")) {
			cmd_transaction_finish();
		} else if (!strcmp(line, "transaction-abort")) {
			cmd_transaction_abort();

		/* Reflog commands */
		} else if (!strncmp(line, "reflog-read ", 12)) {
			cmd_reflog_read(line + 12);
		} else if (!strncmp(line, "reflog-exists ", 14)) {
			cmd_reflog_exists(line + 14);
		} else if (!strncmp(line, "reflog-delete ", 14)) {
			cmd_reflog_delete(line + 14);

		} else if (!len) {
			/* blank line, ignore */
		}
	}

	sqlite3_finalize(st_obj_read);
	sqlite3_finalize(st_obj_write);
	sqlite3_finalize(st_obj_exists);
	sqlite3_finalize(st_obj_list);
	sqlite3_finalize(st_ref_read);
	sqlite3_finalize(st_ref_write);
	sqlite3_finalize(st_ref_delete);
	sqlite3_finalize(st_ref_list);
	sqlite3_finalize(st_reflog_read);
	sqlite3_finalize(st_reflog_append);
	sqlite3_finalize(st_reflog_exists);
	sqlite3_finalize(st_reflog_delete);
	sqlite3_close(db);
	return 0;
}

