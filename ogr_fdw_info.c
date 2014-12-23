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
#include "ogr_api.h"
#include "cpl_error.h"

static void usage();
static OGRErr ogrListLayers(const char *source);
static OGRErr ogrGenerateSQL(const char *source, const char *layer);

	
static void
usage()
{
	printf(
		"usage: ogr_fdw_config -s <ogr datasource> -l <ogr layer>\n"
		"       ogr_fdw_config -s <ogr datasource>\n"
		"\n");	
	exit(0);
}

int
main (int argc, char **argv)
{
    int bflag, ch;
	char *source = NULL, *layer = NULL;
	OGRErr err = OGRERR_NONE;

	/* If no options are specified, display usage */
	if (argc == 1)
		usage();

	bflag = 0;
	while ((ch = getopt(argc, argv, "h?s:l:")) != -1) {
		switch (ch) {
			case 's':
				source = optarg;
				break;
			case 'l':
				layer = optarg;
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
	int i;
	for(i = 0; str[i]; i++)
	{
		char c = tolower(str[i]);
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
		str[i] = c;
	}
}

static OGRErr
ogrGenerateSQL(const char *source, const char *layer)
{	
	OGRDataSourceH ogr_ds = NULL;
	OGRSFDriverH ogr_dr = NULL;
	OGRLayerH ogr_lyr = NULL;
	OGRFeatureDefnH ogr_fd = NULL;
	char server_name[256];
	char layer_name[256];
	int i;
	
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
	strncpy(layer_name, OGR_L_GetName(ogr_lyr), 256);
	strlaunder(layer_name);

	/* Output SERVER definition */
	printf("\nCREATE SERVER %s\n" 
		"  FOREIGN DATA WRAPPER ogr_fdw\n"
		"  OPTIONS (\n"
		"    datasource '%s',\n"
		"    format '%s' );\n\n",
		server_name, OGR_DS_GetName(ogr_ds), OGR_Dr_GetName(ogr_dr));


	/* Output TABLE definition */
	printf("CREATE FOREIGN TABLE %s (\n", layer_name);
	printf("  fid integer,\n");
	printf("  geom geometry");
	
	ogr_fd = OGR_L_GetLayerDefn(ogr_lyr);
	if ( ! ogr_fd )
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not read layer definition for layer '%s' in source '%s'", layer, source);
		return OGRERR_FAILURE;
	}
	
	for ( i = 0; i < OGR_FD_GetFieldCount(ogr_fd); i++ )
	{
		char field_name[256];
		OGRFieldDefnH ogr_fld = OGR_FD_GetFieldDefn(ogr_fd, i);
		strncpy(field_name, OGR_Fld_GetNameRef(ogr_fld), 256);
		strlaunder(field_name);
		printf(",\n  %s ", field_name);
		switch( OGR_Fld_GetType(ogr_fld) )
		{
			case OFTInteger:
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
			default:
				CPLError(CE_Failure, CPLE_AppDefined, "Unsupported GDAL type '%s'", OGR_GetFieldTypeName(OGR_Fld_GetType(ogr_fld)));
				return OGRERR_FAILURE;
		}
	}
	printf(" )\n  SERVER %s\n", server_name);
	printf("  OPTIONS ( layer '%s' );\n", OGR_L_GetName(ogr_lyr));
	printf("\n");
		
	return OGRERR_NONE;
}

