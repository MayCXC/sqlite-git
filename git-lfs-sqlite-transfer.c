/*
 * git-lfs-sqlite-transfer: custom LFS transfer adapter for SQLite.
 *
 * Speaks the git-lfs custom transfer protocol on stdin/stdout.
 * Stores/retrieves LFS content via the shared storage layer.
 *
 * Configuration:
 *   git config lfs.customtransfer.sqlite.path git-lfs-sqlite-transfer
 *   git config lfs.customtransfer.sqlite.args /path/to/repo
 *   git config lfs.standalonetransferagent sqlite
 *
 * Protocol: JSON lines on stdin/stdout.
 *   <- {"event":"init","operation":"download","concurrent":true}
 *   -> {}
 *   <- {"event":"download","oid":"sha256hex","size":N}
 *   -> {"event":"complete","oid":"sha256hex"}
 *      or {"event":"complete","oid":"sha256hex","error":{"code":N,"message":"..."}}
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "storage.h"

/* Minimal JSON helpers (no dependency on a JSON library) */

static const char *json_string(const char *json, const char *key, char *out, size_t outlen) {
	char search[256];
	snprintf(search, sizeof(search), "\"%s\":\"", key);
	const char *p = strstr(json, search);
	if (!p) return NULL;
	p += strlen(search);
	const char *end = strchr(p, '"');
	if (!end) return NULL;
	size_t len = end - p;
	if (len >= outlen) len = outlen - 1;
	memcpy(out, p, len);
	out[len] = '\0';
	return out;
}

static long long json_int(const char *json, const char *key) {
	char search[256];
	snprintf(search, sizeof(search), "\"%s\":", key);
	const char *p = strstr(json, search);
	if (!p) return -1;
	return strtoll(p + strlen(search), NULL, 10);
}


int main(int argc, char **argv) {
	if (argc < 2) {
		fprintf(stderr, "usage: git-lfs-sqlite-transfer <gitdir>\n");
		return 1;
	}

	if (storage_open(argv[1]) < 0) {
		fprintf(stderr, "error: cannot open database\n");
		return 1;
	}

	char line[65536];
	while (fgets(line, sizeof(line), stdin)) {
		char event[64] = "", oid_hex[LFS_OID_HEXSZ + 1] = "";
		char operation[32] = "", path[4096] = "";

		json_string(line, "event", event, sizeof(event));

		if (!strcmp(event, "init")) {
			json_string(line, "operation", operation, sizeof(operation));
			printf("{}\n");
			fflush(stdout);

		} else if (!strcmp(event, "download")) {
			json_string(line, "oid", oid_hex, sizeof(oid_hex));
			long long size = json_int(line, "size");
			(void)size;

			unsigned char oid[LFS_OID_RAWSZ];
			if (storage_lfs_oid_from_hex(oid_hex, oid) != 0) {
				printf("{\"event\":\"complete\",\"oid\":\"%s\","
				       "\"error\":{\"code\":400,\"message\":\"bad oid\"}}\n",
				       oid_hex);
				fflush(stdout);
				continue;
			}

			size_t out_size;
			unsigned char *data;
			if (storage_lfs_read(oid, &out_size, &data) == 0) {
				char tmppath[] = "/tmp/lfs-XXXXXX";
				int fd = mkstemp(tmppath);
				if (fd >= 0) {
					FILE *f = fdopen(fd, "wb");
					if (f) {
						fwrite(data, 1, out_size, f);
						fclose(f);
					} else {
						close(fd);
						unlink(tmppath);
					}
				}
				free(data);
				printf("{\"event\":\"complete\",\"oid\":\"%s\",\"path\":\"%s\"}\n",
				       oid_hex, tmppath);
			} else {
				printf("{\"event\":\"complete\",\"oid\":\"%s\","
				       "\"error\":{\"code\":404,\"message\":\"not found\"}}\n",
				       oid_hex);
			}
			fflush(stdout);

		} else if (!strcmp(event, "upload")) {
			json_string(line, "oid", oid_hex, sizeof(oid_hex));
			long long size = json_int(line, "size");
			json_string(line, "path", path, sizeof(path));

			unsigned char oid[LFS_OID_RAWSZ];
			if (storage_lfs_oid_from_hex(oid_hex, oid) != 0 || size < 0) {
				printf("{\"event\":\"complete\",\"oid\":\"%s\","
				       "\"error\":{\"code\":400,\"message\":\"bad oid or size\"}}\n",
				       oid_hex);
				fflush(stdout);
				continue;
			}

			/* Read content from the file git-lfs provides */
			FILE *f = fopen(path, "rb");
			if (f) {
				unsigned char *data = malloc(size ? (size_t)size : 1);
				if (!data) { fclose(f); printf("{\"event\":\"complete\",\"oid\":\"%s\","
					"\"error\":{\"code\":500,\"message\":\"out of memory\"}}\n", oid_hex);
					fflush(stdout); continue; }
				size_t got = fread(data, 1, (size_t)size, f);
				fclose(f);
				storage_lfs_write(oid, data, got);
				free(data);
				printf("{\"event\":\"complete\",\"oid\":\"%s\"}\n", oid_hex);
			} else {
				printf("{\"event\":\"complete\",\"oid\":\"%s\","
				       "\"error\":{\"code\":500,\"message\":\"cannot read file\"}}\n",
				       oid_hex);
			}
			fflush(stdout);

		} else if (!strcmp(event, "terminate")) {
			break;
		}
	}

	storage_close();
	return 0;
}
