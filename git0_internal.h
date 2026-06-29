/*
** Internal declarations shared between git0 extension source files.
** Not part of the public API (git0.h).
*/

#ifndef GIT0_INTERNAL_H
#define GIT0_INTERNAL_H

#include <sqlite3.h>
#include "oidhex.h"
#include "storage_git.h"

/* git0.c: repo handle management. Returns a caller-owned handle (free with
 * git_repository_free) unless *borrowed is set, which marks the shared
 * storage-backed repo that must not be freed. */
git_repository *git0_repo_open(const char *path, int *borrowed, char **err);

/* git0_vtab.c: table-valued functions */
int git0_register_vtabs(sqlite3 *db);

/* git0_repo.c: self-contained repo functions */
int git0_register_repo(sqlite3 *db);

/* git0_scalar.c: storage-native scalar functions */
int git0_register_storage(sqlite3 *db);

/* git0_backend.c: custom libgit2 ODB+refdb backend */
int git0_register_backend(sqlite3 *db);
git_repository *git0_storage_repo(void);

/* git0_refs_vt.c / git0_objects_vt.c: writable vtabs over the storage ref/object
 * store (CREATE VIRTUAL TABLE <name> USING git0_refs / git0_objects). */
int git0_register_refs_vt(sqlite3 *db);
int git0_register_objects_vt(sqlite3 *db);

#endif /* GIT0_INTERNAL_H */
