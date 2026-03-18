/*
** Internal declarations shared between git0 extension source files.
** Not part of the public API (git0.h).
*/

#ifndef GIT0_INTERNAL_H
#define GIT0_INTERNAL_H

#include <sqlite3.h>
#include <git2.h>

/* git0.c: repo handle management */
git_repository *git0_repo_open(const char *path, char **err);

/* git0_vtab.c: table-valued functions */
int git0_register_vtabs(sqlite3 *db);

/* git0_objects.c: objects virtual table */
void git0_register_objects(sqlite3 *db);

/* git0_refs_vt.c: refs virtual table */
int git0_register_refs_vt(sqlite3 *db);

/* git0_repo.c: self-contained repo functions */
int git0_register_repo(sqlite3 *db);

/* git0_lfs.c: LFS functions */
int git0_register_lfs(sqlite3 *db);

/* git0_storage.c: storage-native scalar functions */
int git0_register_storage(sqlite3 *db);

/* git0_backend.c: custom libgit2 ODB+refdb backend */
int git0_register_backend(sqlite3 *db);
git_repository *git0_storage_repo(void);
void git0_backend_cleanup(void);

#endif /* GIT0_INTERNAL_H */
