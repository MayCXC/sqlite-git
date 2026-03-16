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
	/*
	 * ODB helper takes priority when configured.
	 * The helper receives the object directory path and handles
	 * storage internally (e.g., creates objects.db in the directory).
	 */
	const char *helper = getenv("GIT_ODB_HELPER");
	if (helper && *helper)
		return &odb_source_helper_new(odb, helper, path, local)->base;

	if (path_is_sqlite(path))
		return &odb_source_sqlite_new(odb, path, local)->base;

	return &odb_source_files_new(odb, path, local)->base;
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
