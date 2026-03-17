/* Minimal delta.h for fossil delta.c standalone build */
#ifndef FOSSIL_DELTA_H
#define FOSSIL_DELTA_H

int delta_create(
  const char *zSrc, unsigned int lenSrc,
  const char *zOut, unsigned int lenOut,
  char *zDelta);

int delta_output_size(const char *zDelta, int lenDelta);

int delta_apply(
  const char *zSrc, int lenSrc,
  const char *zDelta, int lenDelta,
  char *zOut);

int delta_analyze(
  const char *zDelta, int lenDelta,
  int *pnCopy, int *pnInsert);

#endif
