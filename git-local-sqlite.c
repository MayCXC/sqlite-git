/*
 * git-local-sqlite: local helper protocol handler.
 *
 * Translates the git local helper protocol (stdin/stdout) into
 * storage operations via the shared storage layer (storage.h).
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage.h"

/* ---- ODB commands ---- */

static void cmd_capabilities(void) {
	printf("get\ninfo\nput\nhave\nlist-objects\nodb-transaction\n"
	       "read\nlist\ntransaction\ncreate\nremove\n"
	       "reflog-read\nreflog-append\nreflog-exists\nreflog-delete\n\n");
	fflush(stdout);
}

static void cmd_info(const char *hex_oid) {
	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) { printf("missing\n"); fflush(stdout); return; }

	int type; unsigned long size; unsigned char *data;
	if (storage_read_object(oid, &type, &size, &data) < 0) {
		printf("missing\n");
	} else {
		printf("%s %lu\n", type_name(type), size);
		free(data);
	}
	fflush(stdout);
}

static void cmd_get(const char *hex_oid) {
	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) { printf("missing\n"); fflush(stdout); return; }

	int type; unsigned long size; unsigned char *data;
	if (storage_read_object(oid, &type, &size, &data) < 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	printf("%s %lu\n", type_name(type), size);
	fflush(stdout);
	if (size > 0) fwrite(data, 1, size, stdout);
	fflush(stdout);
	free(data);
}

static void cmd_put(const char *args) {
	char hex_oid[OID_HEXSZ + 1], type_str[32];
	unsigned long size;
	if (sscanf(args, "%40s %31s %lu", hex_oid, type_str, &size) != 3) return;

	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) return;

	unsigned char *data = malloc(size ? size : 1);
	size_t got = 0;
	while (got < size) {
		size_t n = fread(data + got, 1, size - got, stdin);
		if (n == 0) break;
		got += n;
	}

	storage_write_object(oid, type_from_name(type_str), data, size);
	free(data);
	printf("%s\n", hex_oid); fflush(stdout);
}

static void cmd_have(const char *hex_oid) {
	unsigned char oid[OID_RAWSZ];
	if (hex2bin(hex_oid, oid, OID_RAWSZ) < 0) { printf("false\n"); fflush(stdout); return; }
	printf(storage_object_exists(oid) ? "true\n" : "false\n"); fflush(stdout);
}

static void cmd_list_objects(void) {
	char hex[OID_HEXSZ + 1];
	sqlite3_stmt *st = storage_obj_list_stmt();
	sqlite3_reset(st);
	while (sqlite3_step(st) == SQLITE_ROW) {
		bin2hex(sqlite3_column_blob(st, 0), OID_RAWSZ, hex);
		printf("%s %s %d\n", hex, type_name(sqlite3_column_int(st, 1)),
		       sqlite3_column_int(st, 2));
	}
	printf("\n"); fflush(stdout);
}

static void cmd_odb_transaction_begin(void) {
	sqlite3_exec(storage_db(), "BEGIN", 0, 0, 0);
	printf("ok\n"); fflush(stdout);
}

static void cmd_odb_transaction_commit(void) {
	sqlite3_exec(storage_db(), "COMMIT", 0, 0, 0);
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref commands ---- */

static void cmd_read(const char *refname) {
	char hex[OID_HEXSZ + 1];
	sqlite3_stmt *st = storage_ref_read_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	if (sqlite3_step(st) != SQLITE_ROW) {
		printf("missing\n");
	} else {
		const void *oid_blob = sqlite3_column_blob(st, 0);
		const char *symref = (const char *)sqlite3_column_text(st, 1);
		if (symref && *symref) printf("symref %s\n", symref);
		else if (oid_blob) { bin2hex(oid_blob, OID_RAWSZ, hex); printf("%s\n", hex); }
		else printf("missing\n");
	}
	fflush(stdout);
}

static void cmd_list(const char *prefix) {
	char hex[OID_HEXSZ + 1];
	if (prefix && *prefix) {
		char pattern[4096];
		snprintf(pattern, sizeof(pattern), "%s%%", prefix);
		sqlite3_stmt *st;
		sqlite3_prepare_v2(storage_db(),
			"SELECT refname, oid, symref FROM refs WHERE refname LIKE ? ORDER BY refname",
			-1, &st, 0);
		sqlite3_bind_text(st, 1, pattern, -1, SQLITE_STATIC);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			const void *oid_blob = sqlite3_column_blob(st, 1);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (sym && *sym) printf("%s symref %s\n", name, sym);
			else if (oid_blob) { bin2hex(oid_blob, OID_RAWSZ, hex); printf("%s %s\n", name, hex); }
		}
		sqlite3_finalize(st);
	} else {
		sqlite3_stmt *st = storage_ref_list_stmt();
		sqlite3_reset(st);
		while (sqlite3_step(st) == SQLITE_ROW) {
			const char *name = (const char *)sqlite3_column_text(st, 0);
			const void *oid_blob = sqlite3_column_blob(st, 1);
			const char *sym = (const char *)sqlite3_column_text(st, 2);
			if (sym && *sym) printf("%s symref %s\n", name, sym);
			else if (oid_blob) { bin2hex(oid_blob, OID_RAWSZ, hex); printf("%s %s\n", name, hex); }
		}
	}
	printf("\n"); fflush(stdout);
}

static void cmd_create(void) { printf("ok\n"); fflush(stdout); }

static void cmd_remove(void) {
	sqlite3_exec(storage_db(),
		"DROP TABLE IF EXISTS objects;"
		"DROP TABLE IF EXISTS refs;"
		"DROP TABLE IF EXISTS reflog;", 0, 0, 0);
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref transactions ---- */

static int in_ref_transaction;

static void cmd_transaction_begin(void) {
	if (!in_ref_transaction) {
		sqlite3_exec(storage_db(), "SAVEPOINT ref_txn", 0, 0, 0);
		in_ref_transaction = 1;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_update(const char *args) {
	char refname[4096], new_hex[OID_HEXSZ + 1];
	unsigned char new_oid[OID_RAWSZ];
	if (sscanf(args, "%4095s %40s", refname, new_hex) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	if (hex2bin(new_hex, new_oid, OID_RAWSZ) < 0) {
		printf("error bad oid\n"); fflush(stdout); return;
	}
	sqlite3_stmt *st = storage_ref_write_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_blob(st, 2, new_oid, OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_null(st, 3);
	sqlite3_step(st);
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_create(const char *args) { cmd_transaction_update(args); }

static void cmd_transaction_delete(const char *args) {
	char refname[4096];
	if (sscanf(args, "%4095s", refname) < 1) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	sqlite3_stmt *st = storage_ref_delete_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st);
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_create_symref(const char *args) {
	char refname[4096], target[4096];
	if (sscanf(args, "%4095s %4095s", refname, target) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	sqlite3_stmt *st = storage_ref_write_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_null(st, 2);
	sqlite3_bind_text(st, 3, target, -1, SQLITE_STATIC);
	sqlite3_step(st);
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_prepare(void) { printf("ok\n"); fflush(stdout); }

static void cmd_transaction_finish(void) {
	if (in_ref_transaction) {
		sqlite3_exec(storage_db(), "RELEASE ref_txn", 0, 0, 0);
		in_ref_transaction = 0;
	}
	printf("ok\n"); fflush(stdout);
}

static void cmd_transaction_abort(void) {
	if (in_ref_transaction) {
		sqlite3_exec(storage_db(), "ROLLBACK TO ref_txn; RELEASE ref_txn", 0, 0, 0);
		in_ref_transaction = 0;
	}
	printf("ok\n"); fflush(stdout);
}

/* ---- Reflog commands ---- */

static void cmd_reflog_read(const char *refname) {
	char old_hex[OID_HEXSZ + 1], new_hex[OID_HEXSZ + 1];
	sqlite3_stmt *st = storage_reflog_read_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	while (sqlite3_step(st) == SQLITE_ROW) {
		bin2hex(sqlite3_column_blob(st, 0), OID_RAWSZ, old_hex);
		bin2hex(sqlite3_column_blob(st, 1), OID_RAWSZ, new_hex);
		printf("%s %s %s %lld %+05d\t%s\n",
		       old_hex, new_hex,
		       (const char *)sqlite3_column_text(st, 2),
		       (long long)sqlite3_column_int64(st, 3),
		       sqlite3_column_int(st, 4),
		       (const char *)sqlite3_column_text(st, 5));
	}
	printf("\n"); fflush(stdout);
}

static void cmd_reflog_append(const char *args) {
	char refname[4096], old_hex[OID_HEXSZ + 1], new_hex[OID_HEXSZ + 1];
	unsigned char old_oid[OID_RAWSZ], new_oid[OID_RAWSZ];

	const char *p = args;
	const char *sp;

	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(refname, sizeof(refname), "%.*s", (int)(sp - p), p); p = sp + 1;

	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(old_hex, sizeof(old_hex), "%.*s", (int)(sp - p), p); p = sp + 1;

	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(new_hex, sizeof(new_hex), "%.*s", (int)(sp - p), p); p = sp + 1;

	if (hex2bin(old_hex, old_oid, OID_RAWSZ) < 0 ||
	    hex2bin(new_hex, new_oid, OID_RAWSZ) < 0) {
		printf("error bad oid\n"); fflush(stdout); return;
	}

	const char *email_end = strchr(p, '>');
	if (!email_end) { printf("error bad committer\n"); fflush(stdout); return; }
	email_end++;

	char committer[512];
	snprintf(committer, sizeof(committer), "%.*s", (int)(email_end - p), p);

	long long timestamp = 0; int tz = 0; char msg[4096] = "";
	if (*email_end == ' ') {
		char *end;
		timestamp = strtoll(email_end + 1, &end, 10);
		if (*end == ' ') tz = strtol(end + 1, &end, 10);
		if (*end == '\t') snprintf(msg, sizeof(msg), "%s", end + 1);
	}

	sqlite3_stmt *st = storage_reflog_append_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_bind_text(st, 2, refname, -1, SQLITE_STATIC);
	sqlite3_bind_blob(st, 3, old_oid, OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_blob(st, 4, new_oid, OID_RAWSZ, SQLITE_STATIC);
	sqlite3_bind_text(st, 5, committer, -1, SQLITE_STATIC);
	sqlite3_bind_int64(st, 6, timestamp);
	sqlite3_bind_int(st, 7, tz);
	sqlite3_bind_text(st, 8, msg, -1, SQLITE_STATIC);
	sqlite3_step(st);
	printf("ok\n"); fflush(stdout);
}

static void cmd_reflog_exists(const char *refname) {
	sqlite3_stmt *st = storage_reflog_exists_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	printf(sqlite3_step(st) == SQLITE_ROW ? "true\n" : "false\n"); fflush(stdout);
}

static void cmd_reflog_delete(const char *refname) {
	sqlite3_stmt *st = storage_reflog_delete_stmt();
	sqlite3_reset(st);
	sqlite3_bind_text(st, 1, refname, -1, SQLITE_STATIC);
	sqlite3_step(st);
	printf("ok\n"); fflush(stdout);
}

/* ---- Main loop ---- */

int local_main(int argc, char **argv) {
	if (argc < 2) { fprintf(stderr, "usage: git-local-sqlite <gitdir>\n"); return 1; }
	if (storage_open(argv[1]) < 0) { fprintf(stderr, "error: cannot open db\n"); return 1; }

	char line[8192];
	while (fgets(line, sizeof(line), stdin)) {
		size_t len = strlen(line);
		if (len > 0 && line[len - 1] == '\n') line[--len] = 0;

		if (!strcmp(line, "capabilities")) cmd_capabilities();
		else if (!strncmp(line, "info ", 5)) cmd_info(line + 5);
		else if (!strncmp(line, "get ", 4)) cmd_get(line + 4);
		else if (!strncmp(line, "put ", 4)) cmd_put(line + 4);
		else if (!strncmp(line, "have ", 5)) cmd_have(line + 5);
		else if (!strcmp(line, "list-objects")) cmd_list_objects();
		else if (!strcmp(line, "odb-transaction-begin")) cmd_odb_transaction_begin();
		else if (!strcmp(line, "odb-transaction-commit")) cmd_odb_transaction_commit();
		else if (!strncmp(line, "read ", 5)) cmd_read(line + 5);
		else if (!strncmp(line, "list ", 5)) cmd_list(line + 5);
		else if (!strcmp(line, "list")) cmd_list(NULL);
		else if (!strcmp(line, "create")) cmd_create();
		else if (!strcmp(line, "remove")) cmd_remove();
		else if (!strcmp(line, "transaction-begin")) cmd_transaction_begin();
		else if (!strncmp(line, "transaction-update ", 19)) cmd_transaction_update(line + 19);
		else if (!strncmp(line, "transaction-create-symref ", 25)) cmd_transaction_create_symref(line + 25);
		else if (!strncmp(line, "transaction-create ", 19)) cmd_transaction_create(line + 19);
		else if (!strncmp(line, "transaction-delete ", 19)) cmd_transaction_delete(line + 19);
		else if (!strcmp(line, "transaction-prepare")) cmd_transaction_prepare();
		else if (!strcmp(line, "transaction-finish")) cmd_transaction_finish();
		else if (!strcmp(line, "transaction-abort")) cmd_transaction_abort();
		else if (!strncmp(line, "reflog-read ", 12)) cmd_reflog_read(line + 12);
		else if (!strncmp(line, "reflog-append ", 14)) cmd_reflog_append(line + 14);
		else if (!strncmp(line, "reflog-exists ", 14)) cmd_reflog_exists(line + 14);
		else if (!strncmp(line, "reflog-delete ", 14)) cmd_reflog_delete(line + 14);
	}

	storage_close();
	return 0;
}
