#include "git-compat-util.h"
#include "object-file.h"
#include "odb/source-files.h"
#include "odb/source-helper.h"
#include "odb/source-sqlite.h"
#include "odb/source.h"
#include "packfile.h"

static int path_is_sqlite(const char *path) {
	size_t len = strlen(path);
	return len > 3 && !strcmp(path + len - 3, ".db");
}

struct odb_source *odb_source_new(struct object_database *odb,
				  const char *path,
				  bool local)
{
	struct odb_source *source;

	if (path_is_sqlite(path))
		source = &odb_source_sqlite_new(odb, path, local)->base;
	else
		source = &odb_source_files_new(odb, path, local)->base;

	/*
	 * When GIT_ODB_HELPER is set, add a helper source as the first
	 * alternate. The files backend stays as the primary (handles
	 * writes, temp files, packs). The helper gets first chance at
	 * reads since alternates are checked before repreparing.
	 *
	 * To sync objects TO the helper, use git-remote-sqlite push.
	 */
	if (local) {
		const char *helper = getenv("GIT_ODB_HELPER");
		if (helper && *helper) {
			struct odb_source *hsrc =
				&odb_source_helper_new(odb, helper, path, false)->base;
			source->next = hsrc;
		}
	}

	return source;
}

void odb_source_init(struct odb_source *source,
		     struct object_database *odb,
		     enum odb_source_type type,
		     const char *path,
		     bool local)
{
	source->odb = odb;
	source->type = type;
	source->local = local;
	source->path = xstrdup(path);
}

void odb_source_free(struct odb_source *source)
{
	if (!source)
		return;
	source->free(source);
}

void odb_source_release(struct odb_source *source)
{
	if (!source)
		return;
	free(source->path);
}
