#ifndef ODB_SOURCE_HELPER_H
#define ODB_SOURCE_HELPER_H

#include "odb/source.h"

struct child_process;

struct odb_source_helper {
	struct odb_source base;
	struct child_process *helper;
	FILE *out;
	char *helper_name;
};

struct odb_source_helper *odb_source_helper_new(struct object_database *odb,
						const char *helper_name,
						const char *path,
						bool local);

#endif
