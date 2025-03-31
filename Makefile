# ogr_fdw/Makefile

MODULE_big = ogr_fdw

OBJS = \
	ogr_fdw.o \
	ogr_fdw_deparse.o \
	ogr_fdw_common.o \
	ogr_fdw_func.o \
	stringbuffer_pg.o

EXTENSION = ogr_fdw
DATA = \
	ogr_fdw--1.0--1.1.sql \
	ogr_fdw--1.1.sql

REGRESS = ogr_fdw

EXTRA_CLEAN = sql/*.sql expected/*.out

GDAL_CONFIG = gdal-config
GDAL_CFLAGS = $(shell $(GDAL_CONFIG) --cflags)
GDAL_LIBS  = $(shell $(GDAL_CONFIG) --libs)

# For MacOS
# GDAL_LIBS += -rpath $(shell $(GDAL_CONFIG) --prefix)/lib

PG_CONFIG = pg_config
REGRESS_OPTS = --encoding=UTF8

PG_CPPFLAGS += $(GDAL_CFLAGS)
LIBS += $(GDAL_LIBS)
SHLIB_LINK := $(LIBS)

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

PG_VERSION_NUM = $(shell pg_config --version | cut -f2 -d' ' | awk -F. '{printf "%d%04d", $$1, $$2}')
HAS_IMPORT_SCHEMA = $(shell [ $(PG_VERSION_NUM) -ge 90500 ] && echo yes)


# order matters, file first, import last
REGRESS = file pgsql
ifeq ($(HAS_IMPORT_SCHEMA),yes)
REGRESS += import
endif

# work around pg15 change to regression file variable
# substitution for @abs_srcdir@ until we can drop older
# version support
# https://github.com/postgres/postgres/commit/d1029bb5a26cb84b116b0dee4dde312291359f2a
PG15 := $(shell [ $(PG_VERSION_NUM) -ge 150000 ] && echo yes)
ifeq ($(PG15),yes)

sql/%.sql: input/%.source
	perl -pe 's#\@abs_srcdir\@#$(PWD)#g' < $< > $@

expected/%.out: output/%.source
	perl -pe 's#\@abs_srcdir\@#$(PWD)#g' < $< > $@

SQLFILES := sql/file.sql sql/import.sql sql/pgsql.sql sql/postgis.sql
OUTFILES := expected/file.out expected/import.out expected/pgsql.out expected/postgis.out

installcheck: $(SQLFILES) $(OUTFILES)

endif

###############################################################
# Build the utility program after PGXS to override the
# PGXS environment

CFLAGS = $(GDAL_CFLAGS)
LIBS = $(GDAL_LIBS)

ogr_fdw_info$(X): ogr_fdw_info.o ogr_fdw_common.o stringbuffer.o
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

clean-exe:
	rm -f ogr_fdw_info$(X) ogr_fdw_info.o stringbuffer.o

install-exe: all
	$(INSTALL_PROGRAM) ogr_fdw_info$(X) '$(DESTDIR)$(bindir)'

all: ogr_fdw_info$(X)

clean: clean-exe

install: install-exe
