/*-------------------------------------------------------------------------
 *
 * ogr_fdw_info.c
 *		Commandline utility to read an OGR layer and output a
 *		SQL "create table" statement.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

/* getopt */
#include <unistd.h>

/*
 * OGR library API
 */
#include "ogr_fdw_gdal.h"
#include "ogr_fdw_common.h"

static void usage();
static OGRErr ogrListLayers(const char *source);
static OGRErr ogrGenerateSQL(const char *source, const char *layer);

#define STR_MAX_LEN 256


/* Define this no-op here, so that code */
/* in the ogr_fdw_common module works */
const char * quote_identifier(const char *ident);

const char *
quote_identifier(const char *ident)
{
	return ident;
}


static void
formats()
{
	int i;

	GDALAllRegister();

	printf( "Supported Formats:\n" );
	for ( i = 0; i < GDALGetDriverCount(); i++ )
	{
		GDALDriverH ogr_dr = GDALGetDriver(i);
		int vector = FALSE;
		int createable = TRUE;
		const char *tmpl;

#if GDAL_VERSION_MAJOR >= 2
		char** papszMD = GDALGetMetadata(ogr_dr, NULL);
		vector = CSLFetchBoolean(papszMD, GDAL_DCAP_VECTOR, FALSE);
		createable = CSLFetchBoolean(papszMD, GDAL_DCAP_CREATE, FALSE);
#else
		createable = GDALDatasetTestCapability(ogr_dr, ODrCCreateDataSource);
#endif
		/* Skip raster data sources */
		if ( ! vector ) continue;

		/* Report sources w/ create capability as r/w */
		if( createable )
			tmpl = "  -> \"%s\" (read/write)\n";
		else
			tmpl = "  -> \"%s\" (readonly)\n";

		printf(tmpl, GDALGetDriverShortName(ogr_dr));
	}

	exit(0);
}

static void
usage()
{
	printf(
		"usage: ogr_fdw_info -s <ogr datasource> -l <ogr layer>\n"
		"	   ogr_fdw_info -s <ogr datasource>\n"
		"	   ogr_fdw_info -f\n"
		"\n");
	exit(0);
}

int
main (int argc, char **argv)
{
	int ch;
	char *source = NULL, *layer = NULL;
	OGRErr err = OGRERR_NONE;

	/* If no options are specified, display usage */
	if (argc == 1)
		usage();

	while ((ch = getopt(argc, argv, "h?s:l:f")) != -1) {
		switch (ch) {
			case 's':
				source = optarg;
				break;
			case 'l':
				layer = optarg;
				break;
			case 'f':
				formats();
				break;
			case '?':
			case 'h':
			default:
				usage();
				break;
		}
	}

	if ( source && ! layer )
	{
		err = ogrListLayers(source);
	}
	else if ( source && layer )
	{
		err = ogrGenerateSQL(source, layer);
	}
	else if ( ! source && ! layer )
	{
		usage();
	}

	if ( err != OGRERR_NONE )
	{
		// printf("OGR Error: %s\n\n", CPLGetLastErrorMsg());
	}

	OGRCleanupAll();
	exit(0);
}

static OGRErr
ogrListLayers(const char *source)
{
	GDALDatasetH ogr_ds = NULL;
	int i;

	GDALAllRegister();

#if GDAL_VERSION_MAJOR < 2
	ogr_ds = OGROpen(source, FALSE, NULL);
#else
	ogr_ds = GDALOpenEx(source,
						GDAL_OF_VECTOR|GDAL_OF_READONLY,
						NULL, NULL, NULL);
#endif

	if ( ! ogr_ds )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not connect to source '%s'", source);
		return OGRERR_FAILURE;
	}

	printf("Layers:\n");
	for ( i = 0; i < GDALDatasetGetLayerCount(ogr_ds); i++ )
	{
		OGRLayerH ogr_lyr = GDALDatasetGetLayer(ogr_ds, i);
		if ( ! ogr_lyr )
		{
			return OGRERR_FAILURE;
		}
		printf("  %s\n", OGR_L_GetName(ogr_lyr));
	}
	printf("\n");

	GDALClose(ogr_ds);

	return OGRERR_NONE;
}

static OGRErr
ogrGenerateSQL(const char *source, const char *layer)
{
	OGRErr err;
	GDALDatasetH ogr_ds = NULL;
	GDALDriverH ogr_dr = NULL;
	OGRLayerH ogr_lyr = NULL;
	char server_name[STR_MAX_LEN];
	stringbuffer_t buf;

	GDALAllRegister();

#if GDAL_VERSION_MAJOR < 2
	ogr_ds = OGROpen(source, FALSE, &ogr_dr);
#else
	ogr_ds = GDALOpenEx(source,
						GDAL_OF_VECTOR|GDAL_OF_READONLY,
						NULL, NULL, NULL);
#endif

	if ( ! ogr_ds )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not connect to source '%s'", source);
		return OGRERR_FAILURE;
	}

	if ( ! ogr_dr )
		ogr_dr = GDALGetDatasetDriver(ogr_ds);

	/* There should be a nicer way to do this */
	strcpy(server_name, "myserver");

	ogr_lyr = GDALDatasetGetLayerByName(ogr_ds, layer);
	if ( ! ogr_lyr )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not find layer '%s' in source '%s'", layer, source);
		return OGRERR_FAILURE;
	}

	/* Output SERVER definition */
	printf("\nCREATE SERVER %s\n"
		"  FOREIGN DATA WRAPPER ogr_fdw\n"
		"  OPTIONS (\n"
		"	datasource '%s',\n"
		"	format '%s' );\n",
		server_name, source, GDALGetDriverShortName(ogr_dr));

	stringbuffer_init(&buf);
	err = ogrLayerToSQL(ogr_lyr,
			server_name,
			TRUE, /* launder table names */
			TRUE, /* launder column names */
			TRUE, /* use postgis geometry */
			&buf);

	GDALClose(ogr_ds);

	if ( err != OGRERR_NONE )
	{
		return err;
	}

	printf("\n%s\n", stringbuffer_getstring(&buf));
	stringbuffer_release(&buf);
	return OGRERR_NONE;
}

