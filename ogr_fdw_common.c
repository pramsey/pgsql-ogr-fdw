/*-------------------------------------------------------------------------
 *
 * ogr_fdw_common.c
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2016, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

#include "ogr_fdw_common.h"

void
ogrStringLaunder (char *str)
{
	int i, j = 0;
	char tmp[STR_MAX_LEN];
	memset(tmp, 0, STR_MAX_LEN);
	
	for(i = 0; str[i]; i++)
	{
		char c = tolower(str[i]);

		/* First character is a numeral, prefix with 'n' */
		if ( i == 0 && (c >= 48 && c <= 57) )
		{
			tmp[j++] = 'n';
		}

		/* Replace non-safe characters w/ _ */
		if ( (c >= 48 && c <= 57) || /* 0-9 */
			 (c >= 65 && c <= 90) || /* A-Z */
			 (c >= 97 && c <= 122)   /* a-z */ )
		{
			/* Good character, do nothing */
		}
		else
		{
			c = '_';
		}
		tmp[j++] = c;

		/* Avoid mucking with data beyond the end of our stack-allocated strings */
		if ( j >= STR_MAX_LEN )
			j = STR_MAX_LEN - 1;
	}
	strncpy(str, tmp, STR_MAX_LEN);
	
}