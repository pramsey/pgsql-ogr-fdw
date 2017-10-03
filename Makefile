# ogr_fdw/Makefile

MODULE_big = ogr_fdw
OBJS = ogr_fdw.o ogr_fdw_deparse.o ogr_fdw_common.o stringbuffer_pg.o
EXTENSION = ogr_fdw
DATA = ogr_fdw--1.0.sql

REGRESS = ogr_fdw

EXTRA_CLEAN = sql/*.sql expected/*.out

GDAL_CONFIG = gdal-config
GDAL_CFLAGS = $(shell $(GDAL_CONFIG) --cflags)
GDAL_LIBS = $(shell $(GDAL_CONFIG) --libs)

PG_CONFIG = pg_config
REGRESS_OPTS = --encoding=UTF8

PG_CPPFLAGS += $(GDAL_CFLAGS)
LIBS += $(GDAL_LIBS)
SHLIB_LINK := $(LIBS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

PG_VERSION_NUM = $(shell awk '/PG_VERSION_NUM/ { print $$3 }' $(shell $(PG_CONFIG) --includedir-server)/pg_config.h)
HAS_IMPORT_SCHEMA = $(shell [ $(PG_VERSION_NUM) -ge 90500 ] && echo yes)

# order matters, file first, import last
REGRESS = file pgsql
ifeq ($(HAS_IMPORT_SCHEMA),yes)
REGRESS += import
endif

###############################################################
# Build the utility program after PGXS to override the
# PGXS environment

CFLAGS = $(GDAL_CFLAGS)
LIBS = $(GDAL_LIBS)

ogr_fdw_info$(X): ogr_fdw_info.o ogr_fdw_common.o stringbuffer.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

stringbuffer_pg.o: stringbuffer.c stringbuffer.h
	$(CC) $(CFLAGS) -D USE_PG_MEM -c -o $@ $<

clean-exe:
	rm -f ogr_fdw_info$(X) ogr_fdw_info.o stringbuffer.o

install-exe: all
	$(INSTALL_PROGRAM) ogr_fdw_info$(X) '$(DESTDIR)$(bindir)'

all: ogr_fdw_info$(X)

clean: clean-exe

install: install-exe
