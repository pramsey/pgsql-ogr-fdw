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

/* postgresql */
#include "pg_config_manual.h"

/* getopt */
#include <unistd.h>

/*
 * OGR library API
 */
#include "ogr_fdw_gdal.h"
#include "ogr_fdw_common.h"

static void usage();
static OGRErr ogrListLayers(const char* source);
static OGRErr ogrFindLayer(const char* source, int layerno, const char** layer);
static OGRErr ogrGenerateSQL(const char* server, const char* layer, const char* table, const char* source, const char* options);
static int reserved_word(const char* pgcolumn);

static char *
ogr_fdw_strupr(char* str)
{
	int i;
  for (i = 0; i < strlen(str); i++) {
    str[i] = toupper(str[i]);
  }

  return str;
}

static char *
strip_spaces(char* str)
{
	unsigned char *cur = (unsigned char *)str;
	unsigned char *head = cur;
	while (*head != '\0') {
		if (*head != ' ') {
			*cur = *head;
			++cur;
		}
		++head;
	}
	*cur = '\0';

	return str;
}

/* Define this no-op here, so that code */
/* in the ogr_fdw_common module works */
const char* quote_identifier(const char* ident);

static char identifier[NAMEDATALEN+3];

const char*
quote_identifier(const char* ident)
{
	int len = (int)MIN(strlen(ident), NAMEDATALEN - 1);

	if (reserved_word(ident))
	{
		sprintf(identifier,"\"%*s\"", len, ident);
	}
	else
	{
		sprintf(identifier,"%*s", len, ident);
	}
  return identifier;
}

static char config_options[STR_MAX_LEN] = {0};


static void
formats()
{
	int i;

	GDALAllRegister();

	printf("Supported Formats:\n");
	for (i = 0; i < GDALGetDriverCount(); i++)
	{
		GDALDriverH ogr_dr = GDALGetDriver(i);
		int vector = FALSE;
		int createable = TRUE;
		const char* tmpl;

#if GDAL_VERSION_MAJOR >= 2
		char** papszMD = GDALGetMetadata(ogr_dr, NULL);
		vector = CSLFetchBoolean(papszMD, GDAL_DCAP_VECTOR, FALSE);
		createable = CSLFetchBoolean(papszMD, GDAL_DCAP_CREATE, FALSE);
#else
		createable = GDALDatasetTestCapability(ogr_dr, ODrCCreateDataSource);
#endif
		/* Skip raster data sources */
		if (! vector)
		{
			continue;
		}

		/* Report sources w/ create capability as r/w */
		if (createable)
		{
			tmpl = "  -> \"%s\" (read/write)\n";
		}
		else
		{
			tmpl = "  -> \"%s\" (readonly)\n";
		}

		printf(tmpl, GDALGetDriverShortName(ogr_dr));
	}

	exit(0);
}

static void
usage()
{
	printf(
		"usage: ogr_fdw_info -s <ogr datasource> -l <ogr layer name> -i <ogr layer index (numeric)> -t <output table name> -n <output server name> -o <config options>\n"
		"       ogr_fdw_info -s <ogr datasource>\n"
		"usage: ogr_fdw_info -f\n"
		"       Show what input file formats are supported.\n"
		"\n");
	printf(
		"note (1): You can specify either -l (layer name) or -i (layer index)\n"
		"          if you specify both -l will be used\n"
		"note (2): config options are specified as a comma deliminated list without the OGR_<driver>_ prefix\n"
		"          so OGR_XLSX_HEADERS = FORCE OGR_XLSX_FIELD_TYPES = STRING would become:\n"
		"          \"HEADERS = FORCE,FIELD_TYPES = STRING\""
		"\n");
	exit(0);
}

int
main(int argc, char** argv)
{
	int ch;
	char* source = NULL;
	const char* layer = NULL, *server = NULL, *table = NULL, *options = NULL;
	int layer_index = -1;
	OGRErr err = OGRERR_NONE;

	/* If no options are specified, display usage */
	if (argc == 1)
	{
		usage();
	}

	while ((ch = getopt(argc, argv, "hfs:l:t:n:i:o:")) != -1)
	{
		switch (ch)
		{
		case 's':
			source = optarg;
			break;
		case 'l':
			layer = optarg;
			break;
		case 'f':
			formats();
			break;
		case 't':
			table = optarg;
			break;
		case 'n':
			server = optarg;
			break;
		case 'i':
			layer_index = atoi(optarg) - 1;
			break;
		case 'o':
			options = optarg;
			break;
		case '?':
		case 'h':
		default:
			usage();
			break;
		}
	}

	if (source && ! layer && layer_index == -1)
	{
		err = ogrListLayers(source);
	}
	else if (source && (layer || layer_index > -1))
	{
		if (! layer)
		{
			err = ogrFindLayer(source, layer_index, &layer);
		}

		if (err == OGRERR_NONE)
		{
			err = ogrGenerateSQL(server, layer, table, source, options);
		}
	}
	else if (! source && ! layer)
	{
		usage();
	}

	if (err != OGRERR_NONE)
	{
		printf("OGR Error: %s\n\n", CPLGetLastErrorMsg());
		exit(1);
	}

	OGRCleanupAll();
	exit(0);
}

static OGRErr
ogrListLayers(const char* source)
{
	GDALDatasetH ogr_ds = NULL;
	int i;

	GDALAllRegister();

#if GDAL_VERSION_MAJOR < 2
	ogr_ds = OGROpen(source, FALSE, NULL);
#else
	ogr_ds = GDALOpenEx(source,
	                    GDAL_OF_VECTOR | GDAL_OF_READONLY,
	                    NULL, NULL, NULL);
#endif

	if (! ogr_ds)
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not connect to source '%s'", source);
		return OGRERR_FAILURE;
	}
	printf("Format: %s\n\n", GDALGetDriverShortName(GDALGetDatasetDriver(ogr_ds)));

	printf("Layers:\n");
	for (i = 0; i < GDALDatasetGetLayerCount(ogr_ds); i++)
	{
		OGRLayerH ogr_lyr = GDALDatasetGetLayer(ogr_ds, i);
		if (! ogr_lyr)
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
ogrGenerateSQL(const char* server, const char* layer, const char* table, const char* source, const char* options)
{
	OGRErr err;
	GDALDatasetH ogr_ds = NULL;
	GDALDriverH ogr_dr = NULL;
	OGRLayerH ogr_lyr = NULL;
	char server_name[NAMEDATALEN];
	stringbuffer_t buf;

	char **option_iter;
	char **option_list;

	GDALAllRegister();

#if GDAL_VERSION_MAJOR < 2
	ogr_ds = OGROpen(source, FALSE, &ogr_dr);
#else
	ogr_ds = GDALOpenEx(source,
	                    GDAL_OF_VECTOR | GDAL_OF_READONLY,
	                    NULL, NULL, NULL);
#endif

	if (! ogr_ds)
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not connect to source '%s'", source);
		return OGRERR_FAILURE;
	}

	if (! ogr_dr)
		ogr_dr = GDALGetDatasetDriver(ogr_ds);

	strcpy(server_name, server == NULL ? "myserver" : server);

	if (options != NULL) {
		char *p;
		char stripped_config_options[STR_MAX_LEN] = {0};
		char option[NAMEDATALEN];
		const char *short_name = GDALGetDriverShortName(ogr_dr);

		strncpy(stripped_config_options, options, STR_MAX_LEN - 1);
		p = strtok(strip_spaces(stripped_config_options), ",");

		while (p != NULL) {
			if (strcmp(short_name, "XLSX") == 0 || strcmp(short_name, "XLSX") == 0 || strcmp(short_name, "ODS") == 0)
			{
				/* Unify the handling of the options of spreadsheet file options as they are all the same except they have their
				 * Driver Short Name included in the option
				 */
				sprintf(option, "OGR_%s_%s ", short_name, ogr_fdw_strupr(p));
			}
			else {
				sprintf(option, "%s ", ogr_fdw_strupr(p));
			}

			strcat(config_options, option);
			p = strtok(NULL, ",");
		}
	}

	option_list = CSLTokenizeString(config_options);
	for ( option_iter = option_list; option_iter && *option_iter; option_iter++ )
	{
		char *key;
		const char *value;
		value = CPLParseNameValue(*option_iter, &key);
		if (! (key && value))
			CPLError(CE_Failure, CPLE_AppDefined, "bad config option string '%s'", config_options);

		CPLSetConfigOption(key, value);
		CPLFree(key);
	}
	CSLDestroy( option_list );

	ogr_lyr = GDALDatasetGetLayerByName(ogr_ds, layer);
	if (! ogr_lyr)
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not find layer '%s' in source '%s'", layer, source);
		return OGRERR_FAILURE;
	}

	/* Output SERVER definition */
	printf("\nCREATE SERVER %s\n"
	       "  FOREIGN DATA WRAPPER ogr_fdw\n"
	       "  OPTIONS (\n"
	       "    datasource '%s',\n"
	       "    format '%s'",
	       quote_identifier(server_name), source, GDALGetDriverShortName(ogr_dr));

	if (strlen(config_options) > 0)
	{
		printf(",\n    config_options '%s');\n", config_options);
	}
	else
	{
		printf(");\n");
	}

	stringbuffer_init(&buf);
	err = ogrLayerToSQL(ogr_lyr,
	                    server_name,
	                    TRUE, /* launder table names */
	                    TRUE, /* launder column names */
	                    table,/* output table name */
	                    TRUE, /* use postgis geometry */
	                    &buf);

	GDALClose(ogr_ds);

	if (err != OGRERR_NONE)
	{
		return err;
	}

	printf("\n%s\n", stringbuffer_getstring(&buf));
	stringbuffer_release(&buf);
	return OGRERR_NONE;
}

static OGRErr
ogrFindLayer(const char *source, int layerno, const char** layer)
{
	GDALDatasetH ogr_ds = NULL;
	int i;
	char **option_iter;
	char **option_list;

	GDALAllRegister();

	option_list = CSLTokenizeString(config_options);
	for (option_iter = option_list; option_iter && *option_iter; option_iter++)
	{
		char *key;
		const char *value;
		value = CPLParseNameValue(*option_iter, &key);
		if (! (key && value))
			CPLError(CE_Failure, CPLE_AppDefined, "bad config option string '%s'", config_options);

		CPLSetConfigOption(key, value);
		CPLFree(key);
	}
	CSLDestroy(option_list);


	#if GDAL_VERSION_MAJOR < 2
	ogr_ds = OGROpen(source, FALSE, NULL);
	#else
	ogr_ds = GDALOpenEx(source,
	                    GDAL_OF_VECTOR | GDAL_OF_READONLY,
	                    NULL, NULL, NULL);
	#endif

	if (! ogr_ds)
	{
		CPLError(CE_Failure, CPLE_AppDefined, "Could not connect to source '%s'", source);
		return OGRERR_FAILURE;
	}

	for (i = 0; i < GDALDatasetGetLayerCount(ogr_ds); i++)
	{
		if (i == layerno) {
			OGRLayerH ogr_lyr = GDALDatasetGetLayer(ogr_ds, i);
			if (! ogr_lyr)
			{
				return OGRERR_FAILURE;
			}
			*layer = OGR_L_GetName(ogr_lyr);
			return OGRERR_NONE;
		}
	}

	GDALClose(ogr_ds);

	return OGRERR_FAILURE;
}

static int
reserved_word(const char * pgcolumn)
{
	char* reserved[] = {
	"all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "authorization",
	"binary", "both",
	"case", "cast", "check", "collate", "collation", "column", "concurrently", "constraint", "create", "cross", "current_catalog", "current_date", "current_role",
	"current_schema", "current_time", "current_timestamp", "current_user",
	"default", "deferrable", "desc", "distinct", "do",
	"else", "end", "except",
	"false", "fetch", "for", "foreign", "freeze", "from", "full",
	"grant", "group",
	"having",
	"ilike", "in", "initially", "inner", "intersect", "into", "is", "isnull",
	"join",
	"lateral", "leading", "left", "like", "limit", "localtime", "localtimestamp",
	"natural", "not", "notnull", "null",
	"offset", "on", "only", "or", "order", "outer", "overlaps",
	"placing", "primary",
	"references", "returning", "right",
	"select", "session_user", "similar", "some", "symmetric",
	"table", "tablesample", "then", "to", "trailing", "true",
	"union", "unique", "user", "using",
	"variadic", "verbose",
	"when", "where", "window", "with"
	};

	int i;
	for (i = 0; i < sizeof(reserved)/sizeof(reserved[0]); i++)
	{
		if (strcmp(pgcolumn, reserved[i]) == 0)
			return 1;
	}

	return 0;
}
