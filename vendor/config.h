/* Minimal config.h shim for fossil delta.c standalone build */
#ifndef FOSSIL_CONFIG_H
#define FOSSIL_CONFIG_H

#include <stdlib.h>
#include <stdint.h>

typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef uint64_t sqlite3_uint64;

#define fossil_malloc(n) malloc(n)
#define fossil_free(p) free(p)

#endif
