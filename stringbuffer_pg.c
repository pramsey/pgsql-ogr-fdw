/*
* We need to use stringbuffer twice: once in the commandline utility
* and once in the pgsql backend. That way the core code for building
* the create table statements can be shared between both parts. This
* is why we don't just use the pgsql string library.
* In order to get a copy of the code that uses pgsql memory management
* functions, we do this little dance here.
*/

#include "stringbuffer.h"

#define USE_PG_MEM

#include "stringbuffer.c"

