/*
 * pack_name_hash - sortable hash from a pathname.
 *
 * Copied from git-core pack-objects.h (pack_name_hash v1).
 * Creates a sortable number from the last sixteen non-whitespace
 * characters. Last characters count most, so files ending in ".c"
 * sort together and same-name files across directories sort together.
 *
 * Copyright (C) Linus Torvalds, 2005. Licensed under GPL v2.
 */
#ifndef VENDOR_NAME_HASH_H
#define VENDOR_NAME_HASH_H

#include <stdint.h>

static inline uint32_t pack_name_hash(const char *name)
{
	uint32_t c, hash = 0;
	if (!name)
		return 0;
	while ((c = (unsigned char)*name++) != 0) {
		if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
			continue;
		hash = (hash >> 2) + (c << 24);
	}
	return hash;
}

#endif
