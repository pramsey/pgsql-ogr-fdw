/*-------------------------------------------------------------------------
 *
 * ogr_fdw_info.c
 *        Commandline utility to read an OGR layer and output a 
 *        SQL "create table" statement.
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

static void usage();
static OGRErr ogrListLayers(const char *source);
static OGRErr ogrGenerateSQL(const char *source, const char *layer);

#define STR_MAX_LEN 256 
	
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
		"       ogr_fdw_info -s <ogr datasource>\n"
		"       ogr_fdw_info -f\n"
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
	OGRDataSourceH ogr_ds = NULL;
	OGRSFDriverH ogr_dr = NULL;
	int i;
	
	OGRRegisterAll();
	ogr_ds = OGROpen(source, FALSE, &ogr_dr);			

	if ( ! ogr_ds )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not conect to source '%s'", source);
		return OGRERR_FAILURE; 
	}

	printf("Layers:\n");
	for ( i = 0; i < OGR_DS_GetLayerCount(ogr_ds); i++ )
	{
		OGRLayerH ogr_lyr = OGR_DS_GetLayer(ogr_ds, i);
		if ( ! ogr_lyr ) 
		{
			return OGRERR_FAILURE;
		}
		printf("  %s\n", OGR_L_GetName(ogr_lyr));
	}
	printf("\n");
	
	OGR_DS_Destroy(ogr_ds);
	
	return OGRERR_NONE;
}

static void strlaunder (char *str)
{
	int i, j = 0;
	for(i = 0; str[i]; i++)
	{
		char c = tolower(str[i]);
		
		/* First character is a numeral, prefix with 'n' */
		if ( i == 0 && (c >= 48 && c <= 57) )
		{
			str[j++] = 'n';
		}
		
		/* Replace non-safe characters w/ _ */
		if ( (c >= 48 && c <= 57) || /* 0-9 */
			 (c >= 65 && c <= 90) || /* A-Z */
			 (c >= 97 && c <= 122 ) /* a-z */ )
		{
			/* Good character, do nothing */
		}
		else
		{
			c = '_';
		}
		str[j++] = c;
		
		/* Avoid mucking with data beyond the end of our stack-allocated strings */
		if ( j >= STR_MAX_LEN )
			j = STR_MAX_LEN - 1;
	}
}

static OGRErr
ogrGenerateSQL(const char *source, const char *layer)
{	
	OGRDataSourceH ogr_ds = NULL;
	OGRSFDriverH ogr_dr = NULL;
	OGRLayerH ogr_lyr = NULL;
	OGRFeatureDefnH ogr_fd = NULL;
	char server_name[STR_MAX_LEN];
	char layer_name[STR_MAX_LEN];
	int i;
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
	int geom_field_count;
#endif

	OGRRegisterAll();
	ogr_ds = OGROpen(source, FALSE, &ogr_dr);			

	if ( ! ogr_ds )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not connect to source '%s'", source);
		return OGRERR_FAILURE; 
	}

	/* There should be a nicer way to do this */
	strcpy(server_name, "myserver");

	ogr_lyr = OGR_DS_GetLayerByName(ogr_ds, layer);
	if ( ! ogr_lyr )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not find layer '%s' in source '%s'", layer, source);
		return OGRERR_FAILURE; 
	}
	strncpy(layer_name, OGR_L_GetName(ogr_lyr), STR_MAX_LEN);
	strlaunder(layer_name);

	/* Output SERVER definition */
	printf("\nCREATE SERVER %s\n" 
		"  FOREIGN DATA WRAPPER ogr_fdw\n"
		"  OPTIONS (\n"
		"    datasource '%s',\n"
		"    format '%s' );\n\n",
		server_name, OGR_DS_GetName(ogr_ds), OGR_Dr_GetName(ogr_dr));

	
	ogr_fd = OGR_L_GetLayerDefn(ogr_lyr);
	if ( ! ogr_fd )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not read layer definition for layer '%s' in source '%s'", layer, source);
		return OGRERR_FAILURE;
	}

	/* Output TABLE definition */
	printf("CREATE FOREIGN TABLE %s (\n", layer_name);
	printf("  fid bigint");
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
	geom_field_count = OGR_FD_GetGeomFieldCount(ogr_fd);
	if( geom_field_count == 1 )
	{
		printf(",\n  geom geometry");
	}
	else
	{
		for ( i = 0; i < geom_field_count; i++ )
		{
			printf(",\n  geom%d geometry", i + 1);
		}
	}
#else
	if( OGR_L_GetGeomType(ogr_lyr) != wkbNone )
		printf(",\n  geom geometry");
#endif
	
	for ( i = 0; i < OGR_FD_GetFieldCount(ogr_fd); i++ )
	{
		char field_name[STR_MAX_LEN];
		OGRFieldDefnH ogr_fld = OGR_FD_GetFieldDefn(ogr_fd, i);
		OGRFieldType ogr_fld_type = OGR_Fld_GetType(ogr_fld);
		const char *ogr_field_name = OGR_Fld_GetNameRef(ogr_fld);
		strncpy(field_name, ogr_field_name, STR_MAX_LEN);
		strlaunder(field_name);
		printf(",\n  %s ", field_name);
		switch( ogr_fld_type )
		{
			case OFTInteger:
#if GDAL_VERSION_MAJOR >= 2 
				if( OGR_Fld_GetSubType(ogr_fld) == OFSTBoolean )
					printf("boolean");
				else
#endif
				printf("integer");
				break;
			case OFTReal:
				printf("real");
				break;
			case OFTString:
				printf("varchar");
				break;
			case OFTBinary:
				printf("bytea");
				break;
			case OFTDate:
				printf("date");
				break;			
			case OFTTime:
				printf("time");
				break;
			case OFTDateTime:
				printf("timestamp");
				break;
			case OFTIntegerList:
				printf("integer[]");
				break;
			case OFTRealList:
				printf("real[]");
				break;
			case OFTStringList:
				printf("varchar[]");
				break;
				
#if GDAL_VERSION_MAJOR >= 2
            case OFTInteger64:
                printf("bigint");
                break;
#endif
			default:
				CPLError(CE_Failure, CPLE_AppDefined, 
				         "Unsupported GDAL type '%s'", 
				         OGR_GetFieldTypeName(ogr_fld_type));
				return OGRERR_FAILURE;
		}
		
		if ( strncasecmp(field_name, ogr_field_name, STR_MAX_LEN) != 0 )
		{
            printf(" OPTIONS (column_name '%s')", ogr_field_name);
		}
	}
	printf(" )\n  SERVER %s\n", server_name);
	printf("  OPTIONS ( layer '%s' );\n", OGR_L_GetName(ogr_lyr));
	printf("\n");
		
	return OGRERR_NONE;
}

