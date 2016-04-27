/*-------------------------------------------------------------------------
 *
 * stringbuffer.h
 *		  simple stringbuffer
 *
 * Copyright (c) 2009, Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright (c) 2002 Thamer Alharbash
 *
 *-------------------------------------------------------------------------
 */

#ifndef _STRINGBUFFER_H
#define _STRINGBUFFER_H 1

#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <stdio.h>

#define STRINGBUFFER_STARTSIZE 128

typedef struct
{
	size_t capacity;
	char *str_end;
	char *str_start;
}
stringbuffer_t;

extern stringbuffer_t *stringbuffer_create_with_size(size_t size);
extern stringbuffer_t *stringbuffer_create(void);
extern void stringbuffer_init(stringbuffer_t *s);
extern void stringbuffer_release(stringbuffer_t *s);
extern void stringbuffer_destroy(stringbuffer_t *sb);
extern void stringbuffer_clear(stringbuffer_t *sb);
void stringbuffer_set(stringbuffer_t *sb, const char *s);
void stringbuffer_copy(stringbuffer_t *sb, stringbuffer_t *src);
extern void stringbuffer_append(stringbuffer_t *sb, const char *s);
extern void stringbuffer_append_char(stringbuffer_t *s, char c);
extern int stringbuffer_aprintf(stringbuffer_t *sb, const char *fmt, ...);
extern const char *stringbuffer_getstring(stringbuffer_t *sb);
extern char *stringbuffer_getstringcopy(stringbuffer_t *sb);
extern int stringbuffer_getlength(stringbuffer_t *sb);
extern char stringbuffer_lastchar(stringbuffer_t *s);
extern int stringbuffer_trim_trailing_white(stringbuffer_t *s);
extern int stringbuffer_trim_trailing_zeroes(stringbuffer_t *s);

#endif /* _STRINGBUFFER_H */
