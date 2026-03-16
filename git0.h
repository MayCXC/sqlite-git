/*
** git0: SQLite extension exposing git plumbing via libgit2.
**
** Scalar functions for reading/writing git objects, references, and config.
** Table-valued functions for log, tree, diff, refs, status, blame, and more.
*/

#ifndef GIT0_H
#define GIT0_H

#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
#else
  #include "sqlite3.h"
#endif

#define GIT0_VERSION "0.1.0"
#define GIT0_VERSION_MAJOR 0
#define GIT0_VERSION_MINOR 1
#define GIT0_VERSION_PATCH 0

#ifdef GIT0_STATIC
  #define GIT0_API
#else
  #ifdef _WIN32
    #define GIT0_API __declspec(dllexport)
  #else
    #define GIT0_API
  #endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

GIT0_API int sqlite3_git_init(sqlite3 *db, char **pzErrMsg,
                               const sqlite3_api_routines *pApi);

#ifdef __cplusplus
}
#endif

#endif /* GIT0_H */
