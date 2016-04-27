/*-------------------------------------------------------------------------
 *
 * ogr_fdw_common.h
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

#ifndef _OGR_FDW_COMMON_H
#define _OGR_FDW_COMMON_H 1

#include <string.h>
#include <ctype.h>
#include "stringbuffer.h"

#define STR_MAX_LEN 256

/* Utility macros for string equality */
#define streq(s1,s2) (strcmp((s1),(s2)) == 0)
#define strcaseeq(s1,s2) (strcasecmp((s1),(s2)) == 0)


/* Re-write a string in place with laundering rules */
void ogrStringLaunder(char *str);

OGRErr ogrLayerToSQL (const OGRLayerH ogr_lyr,
               const char *fwd_server, int launder_table_names, int launder_column_names,
			   int use_postgis_geometry, stringbuffer_t *buf);


#endif /* _OGR_FDW_COMMON_H */
