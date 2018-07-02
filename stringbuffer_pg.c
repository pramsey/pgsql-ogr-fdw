/*-------------------------------------------------------------------------
 *
 * stringbuffer.c
 *		  simple stringbuffer
 *
 * Copyright (c) 2009, Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright (c) 2002 Thamer Alharbash
 *
 *-------------------------------------------------------------------------
 */

/*
 * We need a version of stringbuffer that uses palloc/pfree/repalloc
 * for use inside PgSQL. (We are sharing code in ogr_fdw_common.c 
 * between the commandline utility and the backend module, so we 
 * cannot just depend on the PgSQL standard string handling utility)
 * We rebuild it here, but with the pgsql memory stuff in place
 * of the standard system calls
 */

#include "stringbuffer.h"

void * palloc(size_t sz);
void pfree(void *ptr);
void * repalloc(void *ptr, size_t sz);

#define malloc(sz) palloc(sz)
#define free(ptr) pfree(ptr)
#define realloc(ptr,sz) repalloc(ptr,sz)

#include "stringbuffer.c"

