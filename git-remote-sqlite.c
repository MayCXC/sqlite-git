/*
 * git-remote-sqlite: remote helper for SQLite-backed repos.
 *
 * Git invokes this as: git-remote-sqlite <remote-name> <url>
 * URL format: sqlite:///path/to/dir (database at dir/sqlite.db)
 *
 * Uses the shared storage layer (storage.h) for all SQLite access.
 * Uses libgit2 for local git repo object graph traversal.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <git2.h>
#include "storage.h"

/*
 * Extract referenced OIDs from a git object. Queues hex OIDs
 * for BFS traversal of the object graph.
 */
static void queue_referenced_oids(git_object_t type, const void *data,
				  size_t size, char (*queue)[GIT_OID_SHA1_HEXSIZE + 1],
				  int *tail, int max) {
	if (type == GIT_OBJECT_COMMIT) {
		const char *p = (const char *)data;
		const char *end = p + size;
		while (p < end) {
			if (strncmp(p, "tree ", 5) == 0 && *tail < max) {
				strncpy(queue[(*tail)++], p + 5, 40);
				queue[*tail - 1][40] = '\0';
			} else if (strncmp(p, "parent ", 7) == 0 && *tail < max) {
				strncpy(queue[(*tail)++], p + 7, 40);
				queue[*tail - 1][40] = '\0';
			}
			while (p < end && *p != '\n') p++;
			if (p < end) p++;
			if (p < end && *p == '\n') break;
		}
	} else if (type == GIT_OBJECT_TREE) {
		const unsigned char *p = (const unsigned char *)data;
		const unsigned char *end = p + size;
		while (p < end) {
			while (p < end && *p != ' ') p++;
			if (p >= end) break;
			p++;
			while (p < end && *p != 0) p++;
			if (p >= end) break;
			p++;
			if (p + GIT_OID_SHA1_SIZE > end) break;
			if (*tail < max) {
				git_oid entry;
				memcpy(entry.id, p, GIT_OID_SHA1_SIZE);
				git_oid_tostr(queue[(*tail)++], GIT_OID_SHA1_HEXSIZE + 1, &entry);
			}
			p += GIT_OID_SHA1_SIZE;
		}
	}
}

static void cmd_capabilities(void) {
	printf("fetch\npush\n\n");
	fflush(stdout);
}

/* ---- List refs from SQLite ---- */

static int print_remote_ref(const char *name, const git_oid *oid,
			    const char *symref, void *data) {
	(void)data;
	if (symref && *symref)
		printf("@%s %s\n", symref, name);
	else if (oid) {
		char hex[GIT_OID_SHA1_HEXSIZE + 1];
		git_oid_tostr(hex, sizeof(hex), oid);
		printf("%s %s\n", hex, name);
	}
	return 0;
}

static void cmd_list(void) {
	storage_ref_list(NULL, print_remote_ref, NULL);
	printf("\n");
	fflush(stdout);
}

/* ---- Fetch: copy objects from SQLite to local git repo ---- */

static void cmd_fetch(const char *sha, const char *ref) {
	(void)ref;
	git_repository *repo = NULL;
	if (git_repository_open(&repo, ".") != 0) {
		fprintf(stderr, "git-remote-sqlite: cannot open local repo\n");
		return;
	}
	git_odb *odb = NULL;
	git_repository_odb(&odb, repo);

	/* BFS: walk object graph from sha */
	char (*queue)[GIT_OID_SHA1_HEXSIZE + 1] = malloc(65536 * (GIT_OID_SHA1_HEXSIZE + 1));
	int head = 0, tail = 0;
	strncpy(queue[tail++], sha, GIT_OID_SHA1_HEXSIZE + 1);

	while (head < tail && tail < 65536) {
		git_oid cur_oid;
		if (git_oid_fromstr(&cur_oid, queue[head]) < 0) { head++; continue; }

		/* Skip if already in local repo */
		if (git_odb_exists(odb, &cur_oid)) { head++; continue; }

		/* Read from SQLite */
		git_object_t type; size_t size; unsigned char *data;
		if (storage_read_object(&cur_oid, &type, &size, &data) < 0) { head++; continue; }

		/* Write to local git odb */
		git_oid written;
		git_odb_write(&written, odb, data, size, (git_object_t)type);

		queue_referenced_oids(type, data, size, queue, &tail, 65536);
		free(data);
		head++;
	}

	free(queue);
	git_odb_free(odb);
	git_repository_free(repo);
	printf("\n");
	fflush(stdout);
}

/* ---- Push: copy objects from local git repo to SQLite ---- */

static void cmd_push(const char *refspec) {
	int force = 0;
	if (refspec[0] == '+') { force = 1; refspec++; }

	/* Parse src:dst */
	char src[256] = "", dst[256] = "";
	const char *colon = strchr(refspec, ':');
	if (!colon) return;
	snprintf(src, sizeof(src), "%.*s", (int)(colon - refspec), refspec);
	snprintf(dst, sizeof(dst), "%s", colon + 1);

	/* Delete ref */
	if (!*src) {
		storage_ref_delete(dst);
		printf("ok %s\n", dst);
		fflush(stdout);
		return;
	}

	/* Resolve src to oid */
	git_repository *repo = NULL;
	if (git_repository_open(&repo, ".") != 0) {
		printf("error %s cannot open repo\n", dst);
		fflush(stdout);
		return;
	}
	git_odb *odb = NULL;
	git_repository_odb(&odb, repo);

	git_oid src_oid;
	if (git_reference_name_to_id(&src_oid, repo, src) != 0) {
		if (git_oid_fromstr(&src_oid, src) != 0) {
			printf("error %s cannot resolve '%s'\n", dst, src);
			fflush(stdout);
			git_odb_free(odb);
			git_repository_free(repo);
			return;
		}
	}

	/* BFS: walk object graph and copy to SQLite */
	char hex[GIT_OID_SHA1_HEXSIZE + 1];
	git_oid_tostr(hex, sizeof(hex), &src_oid);

	char (*queue)[GIT_OID_SHA1_HEXSIZE + 1] = malloc(65536 * (GIT_OID_SHA1_HEXSIZE + 1));
	int head = 0, tail = 0;
	strncpy(queue[tail++], hex, GIT_OID_SHA1_HEXSIZE + 1);

	while (head < tail && tail < 65536) {
		git_oid cur_oid;
		git_oid_fromstr(&cur_oid, queue[head]);

		/* Skip if already in SQLite */
		if (storage_object_exists(&cur_oid)) { head++; continue; }

		/* Read from local git odb */
		git_odb_object *obj = NULL;
		if (git_odb_read(&obj, odb, &cur_oid) != 0) { head++; continue; }

		int type = (int)git_odb_object_type(obj);
		const void *data = git_odb_object_data(obj);
		size_t size = git_odb_object_size(obj);

		/* Write to SQLite with compression + delta */
		storage_write_object(&cur_oid, (git_object_t)type, data, size);

		queue_referenced_oids((git_object_t)type, data, size, queue, &tail, 65536);
		git_odb_object_free(obj);
		head++;
	}

	/* Update the ref in SQLite */
	storage_ref_write(dst, &src_oid, NULL);

	free(queue);
	git_odb_free(odb);
	git_repository_free(repo);

	printf("ok %s\n", dst);
	fflush(stdout);
}

/* ---- Main loop ---- */

int remote_main(int argc, char **argv) {
	if (argc < 3) {
		fprintf(stderr, "usage: git-remote-sqlite <remote> <url>\n");
		return 1;
	}

	const char *url = argv[2];
	/* Parse sqlite:///path -> /path, or use url as-is */
	const char *path = url;
	if (strncmp(url, "sqlite://", 9) == 0) path = url + 9;

	git_libgit2_init();

	if (storage_open(path) < 0) {
		fprintf(stderr, "git-remote-sqlite: cannot open %s\n", path);
		return 1;
	}

	char line[4096];
	while (fgets(line, sizeof(line), stdin)) {
		size_t len = strlen(line);
		if (len > 0 && line[len - 1] == '\n') line[--len] = 0;
		if (!len) continue;

		if (!strcmp(line, "capabilities")) {
			cmd_capabilities();
		} else if (!strcmp(line, "list") || !strcmp(line, "list for-push")) {
			cmd_list();
		} else if (!strncmp(line, "fetch ", 6)) {
			/* Collect all fetch lines, process last one */
			char sha[GIT_OID_SHA1_HEXSIZE + 1], ref[256];
			sscanf(line + 6, "%40s %255s", sha, ref);
			/* Read until blank line */
			while (fgets(line, sizeof(line), stdin)) {
				len = strlen(line);
				if (len > 0 && line[len - 1] == '\n') line[--len] = 0;
				if (!len) break;
				if (!strncmp(line, "fetch ", 6))
					sscanf(line + 6, "%40s %255s", sha, ref);
			}
			cmd_fetch(sha, ref);
		} else if (!strncmp(line, "push ", 5)) {
			/* Process push lines until blank */
			cmd_push(line + 5);
			while (fgets(line, sizeof(line), stdin)) {
				len = strlen(line);
				if (len > 0 && line[len - 1] == '\n') line[--len] = 0;
				if (!len) break;
				if (!strncmp(line, "push ", 5))
					cmd_push(line + 5);
			}
			printf("\n");
			fflush(stdout);
		}
	}

	storage_close();
	git_libgit2_shutdown();
	return 0;
}
