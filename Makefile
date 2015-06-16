# ogr_fdw/Makefile

MODULE_big = ogr_fdw
OBJS = ogr_fdw.o ogr_fdw_deparse.o
EXTENSION = ogr_fdw
DATA = ogr_fdw--1.0.sql

REGRESS = ogr_fdw

EXTRA_CLEAN = sql/ogr_fdw.sql expected/ogr_fdw.out

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


###############################################################
# Build the utility program after PGXS to override the
# PGXS environment

CFLAGS = $(GDAL_CFLAGS)
LIBS = $(GDAL_LIBS)

ogr_fdw_info: ogr_fdw_info.o
	$(CC) $(CFLAGS) -o $@ $? $(LIBS)

clean:
	rm -f *.o ogr_fdw_info *.so

all: ogr_fdw_info

install: all
	$(INSTALL_PROGRAM) ogr_fdw_info$(X) '$(DESTDIR)$(bindir)'

