/*-------------------------------------------------------------------------
 *
 * ogr_fdw_common.h
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

#include <string.h>
#include <ctype.h>

#define STR_MAX_LEN 256

void ogrStringLaunder (char *str);
