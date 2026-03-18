/*
 * git-local-sqlite: local helper protocol handler.
 *
 * Translates hex OIDs from the git protocol into git_oid for
 * storage operations. All storage goes through storage.h.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <git2.h>
#include "storage.h"



static void cmd_capabilities(void) {
	printf("get\ninfo\nput\nhave\nlist-objects\nodb-transaction\n"
	       "read\nlist\ntransaction\ncreate\nremove\n"
	       "reflog-read\nreflog-append\nreflog-exists\nreflog-delete\n\n");
	fflush(stdout);
}

/* ---- ODB commands ---- */

static void cmd_info(const char *hex) {
	git_oid oid;
	if (git_oid_fromstr(&oid, hex) != 0) { printf("missing\n"); fflush(stdout); return; }
	git_object_t type; size_t size; unsigned char *data;
	if (storage_read_object(&oid, &type, &size, &data) < 0) {
		printf("missing\n");
	} else {
		printf("%s %zu\n", git_object_type2string(type), size);
		free(data);
	}
	fflush(stdout);
}

static void cmd_get(const char *hex) {
	git_oid oid;
	if (git_oid_fromstr(&oid, hex) != 0) { printf("missing\n"); fflush(stdout); return; }
	git_object_t type; size_t size; unsigned char *data;
	if (storage_read_object(&oid, &type, &size, &data) < 0) {
		printf("missing\n"); fflush(stdout); return;
	}
	printf("%s %zu\n", git_object_type2string(type), size);
	fflush(stdout);
	if (size > 0) fwrite(data, 1, size, stdout);
	fflush(stdout);
	free(data);
}

static void cmd_put(const char *args) {
	char hex[GIT_OID_SHA1_HEXSIZE + 1], type_str[32];
	unsigned long size;
	if (sscanf(args, "%40s %31s %lu", hex, type_str, &size) != 3) return;
	git_oid oid;
	if (git_oid_fromstr(&oid, hex) != 0) return;

	unsigned char *data = malloc(size ? size : 1);
	if (!data) return;
	size_t got = 0;
	while (got < size) {
		size_t n = fread(data + got, 1, size - got, stdin);
		if (n == 0) break;
		got += n;
	}
	storage_write_object(&oid, git_object_string2type(type_str), data, got);
	free(data);
	printf("%s\n", hex); fflush(stdout);
}

static void cmd_have(const char *hex) {
	git_oid oid;
	if (git_oid_fromstr(&oid, hex) != 0) { printf("false\n"); fflush(stdout); return; }
	printf(storage_object_exists(&oid) ? "true\n" : "false\n"); fflush(stdout);
}

static int print_obj(const git_oid *oid, git_object_t type, size_t size, void *data) {
	(void)data;
	char hex[GIT_OID_SHA1_HEXSIZE + 1];
	git_oid_tostr(hex, sizeof(hex), oid);
	printf("%s %s %zu\n", hex, git_object_type2string(type), size);
	return 0;
}

static void cmd_list_objects(void) {
	storage_obj_list(print_obj, NULL);
	printf("\n"); fflush(stdout);
}

static void cmd_odb_transaction_begin(void) {
	storage_begin();
	printf("ok\n"); fflush(stdout);
}

static void cmd_odb_transaction_commit(void) {
	storage_commit();
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref commands ---- */

static void cmd_read(const char *refname) {
	git_oid oid; char symref[4096];
	if (storage_ref_read(refname, &oid, symref, sizeof(symref)) < 0) {
		printf("missing\n");
	} else if (symref[0]) {
		printf("symref %s\n", symref);
	} else {
		char hex[GIT_OID_SHA1_HEXSIZE + 1];
		git_oid_tostr(hex, sizeof(hex), &oid);
		printf("%s\n", hex);
	}
	fflush(stdout);
}

static int print_ref(const char *name, const git_oid *oid, const char *sym, void *data) {
	(void)data;
	if (sym && *sym)
		printf("%s symref %s\n", name, sym);
	else if (oid) {
		char hex[GIT_OID_SHA1_HEXSIZE + 1];
		git_oid_tostr(hex, sizeof(hex), oid);
		printf("%s %s\n", name, hex);
	}
	return 0;
}

static void cmd_list(const char *prefix) {
	storage_ref_list(prefix, print_ref, NULL);
	printf("\n"); fflush(stdout);
}

static void cmd_create(void) { printf("ok\n"); fflush(stdout); }

static void cmd_remove(void) {
	storage_destroy();
	printf("ok\n"); fflush(stdout);
}

/* ---- Ref transactions ---- */

static int in_txn;

static void cmd_txn_begin(void) {
	if (!in_txn) { storage_savepoint("ref_txn"); in_txn = 1; }
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_update(const char *args) {
	char refname[4096], hex[GIT_OID_SHA1_HEXSIZE + 1];
	if (sscanf(args, "%4095s %40s", refname, hex) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	git_oid oid;
	if (git_oid_fromstr(&oid, hex) != 0) { printf("error bad oid\n"); fflush(stdout); return; }
	storage_ref_write(refname, &oid, NULL);
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_create(const char *args) { cmd_txn_update(args); }

static void cmd_txn_delete(const char *args) {
	char refname[4096];
	if (sscanf(args, "%4095s", refname) < 1) { printf("error bad arguments\n"); fflush(stdout); return; }
	storage_ref_delete(refname);
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_create_symref(const char *args) {
	char refname[4096], target[4096];
	if (sscanf(args, "%4095s %4095s", refname, target) < 2) {
		printf("error bad arguments\n"); fflush(stdout); return;
	}
	storage_ref_write(refname, NULL, target);
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_prepare(void) { printf("ok\n"); fflush(stdout); }

static void cmd_txn_finish(void) {
	if (in_txn) { storage_release("ref_txn"); in_txn = 0; }
	printf("ok\n"); fflush(stdout);
}

static void cmd_txn_abort(void) {
	if (in_txn) { storage_rollback_to("ref_txn"); in_txn = 0; }
	printf("ok\n"); fflush(stdout);
}

/* ---- Reflog commands ---- */

static int print_reflog(const git_oid *old_oid, const git_oid *new_oid,
			const char *committer, long long ts, int tz,
			const char *msg, void *data) {
	(void)data;
	char old_hex[GIT_OID_SHA1_HEXSIZE + 1], new_hex[GIT_OID_SHA1_HEXSIZE + 1];
	git_oid_tostr(old_hex, sizeof(old_hex), old_oid);
	git_oid_tostr(new_hex, sizeof(new_hex), new_oid);
	printf("%s %s %s %lld %+05d\t%s\n", old_hex, new_hex, committer, ts, tz, msg ? msg : "");
	return 0;
}

static void cmd_reflog_read(const char *refname) {
	storage_reflog_read(refname, print_reflog, NULL);
	printf("\n"); fflush(stdout);
}

static void cmd_reflog_append(const char *args) {
	char refname[4096], old_hex[GIT_OID_SHA1_HEXSIZE + 1], new_hex[GIT_OID_SHA1_HEXSIZE + 1];
	const char *p = args, *sp;

	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(refname, sizeof(refname), "%.*s", (int)(sp - p), p); p = sp + 1;
	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(old_hex, sizeof(old_hex), "%.*s", (int)(sp - p), p); p = sp + 1;
	sp = strchr(p, ' '); if (!sp) { printf("error bad args\n"); fflush(stdout); return; }
	snprintf(new_hex, sizeof(new_hex), "%.*s", (int)(sp - p), p); p = sp + 1;

	git_oid old_oid, new_oid;
	if (git_oid_fromstr(&old_oid, old_hex) != 0 || git_oid_fromstr(&new_oid, new_hex) != 0) {
		printf("error bad oid\n"); fflush(stdout); return;
	}

	const char *email_end = strchr(p, '>');
	if (!email_end) { printf("error bad committer\n"); fflush(stdout); return; }
	email_end++;
	char committer[512];
	snprintf(committer, sizeof(committer), "%.*s", (int)(email_end - p), p);

	long long ts = 0; int tz = 0; char msg[4096] = "";
	if (*email_end == ' ') {
		char *end;
		ts = strtoll(email_end + 1, &end, 10);
		if (*end == ' ') tz = strtol(end + 1, &end, 10);
		if (*end == '\t') snprintf(msg, sizeof(msg), "%s", end + 1);
	}

	storage_reflog_append(refname, &old_oid, &new_oid, committer, ts, tz, msg);
	printf("ok\n"); fflush(stdout);
}

static void cmd_reflog_exists(const char *refname) {
	printf(storage_reflog_exists(refname) ? "true\n" : "false\n"); fflush(stdout);
}

static void cmd_reflog_delete(const char *refname) {
	storage_reflog_delete(refname);
	printf("ok\n"); fflush(stdout);
}

/* ---- Main loop ---- */

int local_main(int argc, char **argv) {
	if (argc < 2) { fprintf(stderr, "usage: git-local-sqlite <gitdir>\n"); return 1; }
	git_libgit2_init();
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
		else if (!strcmp(line, "transaction-begin")) cmd_txn_begin();
		else if (!strncmp(line, "transaction-update ", 19)) cmd_txn_update(line + 19);
		else if (!strncmp(line, "transaction-create-symref ", 25)) cmd_txn_create_symref(line + 25);
		else if (!strncmp(line, "transaction-create ", 19)) cmd_txn_create(line + 19);
		else if (!strncmp(line, "transaction-delete ", 19)) cmd_txn_delete(line + 19);
		else if (!strcmp(line, "transaction-prepare")) cmd_txn_prepare();
		else if (!strcmp(line, "transaction-finish")) cmd_txn_finish();
		else if (!strcmp(line, "transaction-abort")) cmd_txn_abort();
		else if (!strncmp(line, "reflog-read ", 12)) cmd_reflog_read(line + 12);
		else if (!strncmp(line, "reflog-append ", 14)) cmd_reflog_append(line + 14);
		else if (!strncmp(line, "reflog-exists ", 14)) cmd_reflog_exists(line + 14);
		else if (!strncmp(line, "reflog-delete ", 14)) cmd_reflog_delete(line + 14);
	}

	storage_close();
	git_libgit2_shutdown();
	return 0;
}
