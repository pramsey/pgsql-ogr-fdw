/*-------------------------------------------------------------------------
 *
 * ogr_fdw.c
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

/*
 * System
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

/*
 * Require PostgreSQL >= 9.3
 */
#if PG_VERSION_NUM < 90300
#error "OGR FDW requires PostgreSQL version 9.3 or higher"
#else

/*
 * Local structures
 */
#include "ogr_fdw.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct OgrFdwOption
{
	const char *optname;
	Oid optcontext;     /* Oid of catalog in which option may appear */
	bool optrequired;   /* Flag mandatory options */
	bool optfound;      /* Flag whether options was specified by user */
};

#define OPT_DRIVER "format"
#define OPT_SOURCE "datasource"
#define OPT_LAYER "layer"
#define OPT_COLUMN "column_name"
#define OPT_CONFIG_OPTIONS "config_options"
#define OPT_OPEN_OPTIONS "open_options"

/*
 * Valid options for ogr_fdw.
 * ForeignDataWrapperRelationId (no options)
 * ForeignServerRelationId (CREATE SERVER options)
 * UserMappingRelationId (CREATE USER MAPPING options)
 * ForeignTableRelationId (CREATE FOREIGN TABLE options)
 */
static struct OgrFdwOption valid_options[] = {

	/* OGR column mapping */
	{OPT_COLUMN, AttributeRelationId, false, false},

	/* OGR datasource options */
	{OPT_SOURCE, ForeignServerRelationId, true, false},
	{OPT_DRIVER, ForeignServerRelationId, false, false},
	{OPT_CONFIG_OPTIONS, ForeignServerRelationId, false, false},
#if GDAL_VERSION_MAJOR >= 2
	{OPT_OPEN_OPTIONS, ForeignServerRelationId, false, false},
#endif
	/* OGR layer options */
	{OPT_LAYER, ForeignTableRelationId, true, false},

	/* EOList marker */
	{NULL, InvalidOid, false, false}
};


/*
 * SQL functions
 */
extern Datum ogr_fdw_handler(PG_FUNCTION_ARGS);
extern Datum ogr_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(ogr_fdw_handler);
PG_FUNCTION_INFO_V1(ogr_fdw_validator);
void _PG_init(void);

/*
 * FDW callback routines
 */
static void ogrGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid);
static void ogrGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid);
static ForeignScan *ogrGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses
#if PG_VERSION_NUM >= 90500
				   ,Plan *outer_plan
#endif
);
static void ogrBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *ogrIterateForeignScan(ForeignScanState *node);
static void ogrReScanForeignScan(ForeignScanState *node);
static void ogrEndForeignScan(ForeignScanState *node);

#if PG_VERSION_NUM >= 90500
static List *ogrImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
#endif
static void ogrStringLaunder (char *str);

/*
 * Helper functions
 */
static OgrFdwPlanState* getOgrFdwPlanState(Oid foreigntableid);
static OgrFdwExecState* getOgrFdwExecState(Oid foreigntableid);
static OgrConnection ogrGetConnectionFromTable(Oid foreigntableid);
static void ogr_fdw_exit(int code, Datum arg);

/* Global to hold GEOMETRYOID */
Oid GEOMETRYOID = InvalidOid;

#define STR_MAX_LEN 256


void
_PG_init(void)
{
	// DefineCustomIntVariable("mysql_fdw.wait_timeout",
	// 						"Server-side wait_timeout",
	// 						"Set the maximum wait_timeout"
	// 						"use to set the MySQL session timeout",
	// 						&wait_timeout,
	// 						WAIT_TIMEOUT,
	// 						0,
	// 						INT_MAX,
	// 						PGC_USERSET,
	// 						0,
	// 						NULL,
	// 						NULL,
	// 						NULL);

	/*
	 * We assume PostGIS is installed in 'public' and if we cannot
	 * find it, we'll treat all geometry from OGR as bytea.
	 */
	// const char *typname = "geometry";
	// Oid namesp = LookupExplicitNamespace("public", false);
	// Oid typoid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(typname), ObjectIdGetDatum(namesp));
	Oid typoid = TypenameGetTypid("geometry");

	if (OidIsValid(typoid) && get_typisdefined(typoid))
	{
		GEOMETRYOID = typoid;
	}
	else
	{
		GEOMETRYOID = BYTEAOID;
	}

	on_proc_exit(&ogr_fdw_exit, PointerGetDatum(NULL));
}

/*
 * ogr_fdw_exit: Exit callback function.
 */
static void
ogr_fdw_exit(int code, Datum arg)
{
	OGRCleanupAll();
}


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
ogr_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = ogrGetForeignRelSize;
	fdwroutine->GetForeignPaths = ogrGetForeignPaths;
	fdwroutine->GetForeignPlan = ogrGetForeignPlan;
	fdwroutine->BeginForeignScan = ogrBeginForeignScan;
	fdwroutine->IterateForeignScan = ogrIterateForeignScan;
	fdwroutine->ReScanForeignScan = ogrReScanForeignScan;
	fdwroutine->EndForeignScan = ogrEndForeignScan;

#if PG_VERSION_NUM >= 90500
	/*  Support functions for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = ogrImportForeignSchema;
#endif

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Given a connection string and (optional) driver string, try to connect
 * with appropriate error handling and reporting. Used in query startup,
 * and in FDW options validation.
 */
static GDALDatasetH
ogrGetDataSource(const char *source, const char *driver, 
                 const char *config_options, const char *open_options)
{
	GDALDatasetH ogr_ds = NULL;
	GDALDriverH ogr_dr = NULL;
	char **open_option_list = NULL;

	if ( config_options )
	{
		const char *option;
		char **option_list = CSLTokenizeString(config_options);

		for ( option = *option_list; *option_list; option_list++ )
		{
			char *key;
			const char *value;
			value = CPLParseNameValue(option, &key);
			if ( ! (key && value) )
				elog(ERROR, "bad config option string '%s'", config_options);
			
			elog(DEBUG1, "GDAL config option '%s' set to '%s'", key, value);
			CPLSetConfigOption(key, value);
			CPLFree(key);
		}
	}

	if ( open_options )
		open_option_list = CSLTokenizeString(open_options);

	/* Cannot search for drivers if they aren't registered */
	/* But don't call for registration if we already have drivers */
	if ( GDALGetDriverCount() <= 0 )
		GDALAllRegister();

	if ( driver )
	{
		ogr_dr = GDALGetDriverByName(driver);
		if ( ! ogr_dr )
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("unable to find format \"%s\"", driver),
					errhint("See the formats list at http://www.gdal.org/ogr_formats.html")));
		}
#if GDAL_VERSION_MAJOR < 2
		ogr_ds = OGR_Dr_Open(ogr_dr, source, false);
#else
		ogr_ds = GDALOpenEx(source,                                         /* file/data source */
		                    GDAL_OF_VECTOR|GDAL_OF_READONLY,                /* open flags */
		                    (const char* const*)CSLAddString(NULL, driver), /* driver */
		                    (const char *const *)open_option_list,          /* open options */
		                    NULL);                                          /* sibling files */
#endif
	}
	/* No driver, try a blind open... */
	else
	{
#if GDAL_VERSION_MAJOR < 2
		ogr_ds = OGROpen(source, false, &ogr_dr);
#else
		ogr_ds = GDALOpenEx(source, 
		                    GDAL_OF_VECTOR|GDAL_OF_READONLY, 
		                    NULL, 
		                    (const char *const *)open_option_list,
		                    NULL);
#endif
	}

	/* Open failed, provide error hint if OGR gives us one. */
	if ( ! ogr_ds )
	{
		const char *ogrerr = CPLGetLastErrorMsg();
		if ( ogrerr && ! streq(ogrerr, "") )
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("unable to connect to data source \"%s\"", source),
				 errhint("%s", ogrerr)));
		}
		else
		{
 			ereport(ERROR,
 				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
 				 errmsg("unable to connect to data source \"%s\"", source)));
		}
	}

	return ogr_ds;
}

static bool
ogrCanReallyCountFast(const OgrConnection *con)
{
	GDALDriverH dr = GDALGetDatasetDriver(con->ds);
	const char *dr_str = GDALGetDriverShortName(dr);

	if ( streq(dr_str, "ESRI Shapefile" ) ||
   	     streq(dr_str, "FileGDB" ) ||
	     streq(dr_str, "OpenFileGDB" ) )
	{
		return true;
	}
	return false;
}

/*
 * Make sure the datasource is cleaned up when we're done
 * with a connection.
 */
static void
ogrFinishConnection(OgrConnection *ogr)
{
	if ( ogr->ds )
	{
		GDALClose(ogr->ds);
	}
	ogr->ds = NULL;
}

static OgrConnection
ogrGetConnectionFromServer(Oid foreignserverid)
{
	ForeignServer *server;
	OgrConnection ogr;
	ListCell *cell;

	/* Null all values */
	memset(&ogr, 0, sizeof(OgrConnection));

	server = GetForeignServer(foreignserverid);

	foreach(cell, server->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (streq(def->defname, OPT_SOURCE))
			ogr.ds_str = defGetString(def);
		if (streq(def->defname, OPT_DRIVER))
			ogr.dr_str = defGetString(def);
		if (streq(def->defname, OPT_CONFIG_OPTIONS))
			ogr.config_options = defGetString(def);
		if (streq(def->defname, OPT_OPEN_OPTIONS))
			ogr.open_options = defGetString(def);
	}

	if ( ! ogr.ds_str )
		elog(ERROR, "FDW table '%s' option is missing", OPT_SOURCE);

	/*
	 * TODO: Connections happen twice for each query, having a
	 * connection pool will certainly make things faster.
	 */

	/*  Connect! */
	ogr.ds = ogrGetDataSource(ogr.ds_str, ogr.dr_str, ogr.config_options, ogr.open_options);

	return ogr;
}


/*
 * Read the options (data source connection from server and
 * layer name from table) from a foreign table and use them
 * to connect to an OGR layer. Return a connection object that
 * has handles for both the datasource and layer.
 */
static OgrConnection
ogrGetConnectionFromTable(Oid foreigntableid)
{
	ForeignTable *table;
	/* UserMapping *mapping; */
	/* ForeignDataWrapper *wrapper; */
	ListCell *cell;
	OgrConnection ogr;

	/* Gather all data for the foreign table. */
	table = GetForeignTable(foreigntableid);
	/* mapping = GetUserMapping(GetUserId(), table->serverid); */

	ogr = ogrGetConnectionFromServer(table->serverid);

	foreach(cell, table->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (streq(def->defname, OPT_LAYER))
			ogr.lyr_str = defGetString(def);
	}

	if ( ! ogr.lyr_str )
		elog(ERROR, "FDW table '%s' option is missing", OPT_LAYER);

	/* Does the layer exist in the data source? */
	ogr.lyr = GDALDatasetGetLayerByName(ogr.ds, ogr.lyr_str);
	if ( ! ogr.lyr )
	{
		const char *ogrerr = CPLGetLastErrorMsg();
		ereport(ERROR, (
				errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				errmsg("unable to connect to %s to \"%s\"", OPT_LAYER, ogr.lyr_str),
				(ogrerr && ! streq(ogrerr, ""))
				? errhint("%s", ogrerr)
				: errhint("Does the layer exist?")
				));
	}

	return ogr;
}

/*
 * Validate the options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses ogr_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
ogr_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	ListCell *cell;
	struct OgrFdwOption *opt;
	const char *source = NULL, *driver = NULL;
	const char *config_options = NULL, *open_options = NULL;

	/* Check that the database encoding is UTF8, to match OGR internals */
	if ( GetDatabaseEncoding() != PG_UTF8 )
	{
		elog(ERROR, "OGR FDW only works with UTF-8 databases");
		PG_RETURN_VOID();
	}

	/* Initialize found state to not found */
	for ( opt = valid_options; opt->optname; opt++ )
	{
		opt->optfound = false;
	}

	/*
	 * Check that only options supported by ogr_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		bool optfound = false;

		for ( opt = valid_options; opt->optname; opt++ )
		{
			if ( catalog == opt->optcontext && streq(opt->optname, def->defname) )
			{
				/* Mark that this user option was found */
				opt->optfound = optfound = true;

				/* Store some options for testing later */
				if ( streq(opt->optname, OPT_SOURCE) )
					source = defGetString(def);
				if ( streq(opt->optname, OPT_DRIVER) )
					driver = defGetString(def);
				if ( streq(opt->optname, OPT_CONFIG_OPTIONS) )
					config_options = defGetString(def);
				if ( streq(opt->optname, OPT_OPEN_OPTIONS) )
					open_options = defGetString(def);

				break;
			}
		}

		if ( ! optfound )
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			const struct OgrFdwOption *opt;
			StringInfoData buf;

			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR, (
				errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				errmsg("invalid option \"%s\"", def->defname),
				buf.len > 0
				? errhint("Valid options in this context are: %s", buf.data)
				: errhint("There are no valid options in this context.")));
		}
	}

	/* Check that all the mandatory options were found */
	for ( opt = valid_options; opt->optname; opt++ )
	{
		/* Required option for this catalog type is missing? */
		if ( catalog == opt->optcontext && opt->optrequired && ! opt->optfound )
		{
			ereport(ERROR, (
					errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
					errmsg("required option \"%s\" is missing", opt->optname)));
		}
	}

	/* Make sure server connection can actually be established */
	if ( catalog == ForeignServerRelationId && source )
	{
		OGRDataSourceH ogr_ds;
		ogr_ds = ogrGetDataSource(source, driver, config_options, open_options);
		if ( ogr_ds )
		{
			GDALClose(ogr_ds);
		}
		OGRCleanupAll();
	}

	PG_RETURN_VOID();
}

/*
 * Initialize an OgrFdwPlanState on the heap.
 */
static OgrFdwPlanState*
getOgrFdwPlanState(Oid foreigntableid)
{
	OgrFdwPlanState *planstate = palloc0(sizeof(OgrFdwPlanState));

	/*  Connect! */
	planstate->ogr = ogrGetConnectionFromTable(foreigntableid);
	planstate->foreigntableid = foreigntableid;

	return planstate;
}

/*
 * Initialize an OgrFdwExecState on the heap.
 */
static OgrFdwExecState*
getOgrFdwExecState(Oid foreigntableid)
{
	OgrFdwExecState *execstate = palloc0(sizeof(OgrFdwExecState));

	/*  Connect! */
	execstate->ogr = ogrGetConnectionFromTable(foreigntableid);
	execstate->foreigntableid = foreigntableid;

	return execstate;
}



/*
 * ogrGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
ogrGetForeignRelSize(PlannerInfo *root,
                     RelOptInfo *baserel,
                     Oid foreigntableid)
{
	/* Initialize the OGR connection */
	OgrFdwPlanState *planstate = getOgrFdwPlanState(foreigntableid);
	List *scan_clauses = baserel->baserestrictinfo;

	/* Set to NULL to clear the restriction clauses in OGR */
	OGR_L_SetIgnoredFields(planstate->ogr.lyr, NULL);
	OGR_L_SetSpatialFilter(planstate->ogr.lyr, NULL);
	OGR_L_SetAttributeFilter(planstate->ogr.lyr, NULL);

	/*
	* The estimate number of rows returned must actually use restrictions.
	* Since OGR can't really give us a fast count with restrictions on
	* (usually involves a scan) and restrictions in the baserel mean we
	* must punt row count estimates.
	*/

	/* TODO: calculate the row width based on the attribute types of the OGR table */

	/*
	* OGR asks drivers to honestly state if they can provide a fast
	* row count, but too many drivers lie. We are only listing drivers
	* we trust in ogrCanReallyCountFast()
	*/

	/* If we can quickly figure how many rows this layer has, then do so */
	if ( scan_clauses == NIL &&
	     OGR_L_TestCapability(planstate->ogr.lyr, OLCFastFeatureCount) == TRUE &&
	     ogrCanReallyCountFast(&(planstate->ogr)) )
	{
		/* Count rows, but don't force a slow count */
		int rows = OGR_L_GetFeatureCount(planstate->ogr.lyr, false);
		/* Only use row count if return is valid (>0) */
		if ( rows >= 0 )
		{
			planstate->nrows = rows;
			baserel->rows = rows;
		}
	}

	/* Save connection state for next calls */
	baserel->fdw_private = (void *) planstate;

	return;
}



/*
 * ogrGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
ogrGetForeignPaths(PlannerInfo *root,
                   RelOptInfo *baserel,
                   Oid foreigntableid)
{
	OgrFdwPlanState *planstate = (OgrFdwPlanState *)(baserel->fdw_private);

	/* TODO: replace this with something that looks at the OGRDriver and */
	/* makes a determination based on that? Better: add connection caching */
	/* so that slow startup doesn't matter so much */
	planstate->startup_cost = 25;

	/* TODO: more research on what the total cost is supposed to mean, */
	/* relative to the startup cost? */
	planstate->total_cost = planstate->startup_cost + baserel->rows;

	/* Built the (one) path we are providing. Providing fancy paths is */
	/* really only possible with back-ends that can properly provide */
	/* explain info on how they complete the query, not for something as */
	/* obtuse as OGR. (So far, have only seen it w/ the postgres_fdw */
	add_path(baserel,
		(Path *) create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
					NULL, /* PathTarget */
#endif
					baserel->rows,
					planstate->startup_cost,
					planstate->total_cost,
					NIL,     /* no pathkeys */
					NULL,    /* no outer rel either */
					NULL  /* no extra plan */
#if PG_VERSION_NUM >= 90500
					,NIL /* no fdw_private list */
#endif
					)
		);   /* no fdw_private data */
}




/*
 * fileGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
ogrGetForeignPlan(PlannerInfo *root,
                  RelOptInfo *baserel,
                  Oid foreigntableid,
                  ForeignPath *best_path,
                  List *tlist,
                  List *scan_clauses
#if PG_VERSION_NUM >= 90500
                  ,Plan *outer_plan
#endif
                  )
{
	Index scan_relid = baserel->relid;
	bool sql_generated;
	StringInfoData sql;
	List *params_list = NULL;
	List *fdw_private;
	OgrFdwPlanState *planstate = (OgrFdwPlanState *)(baserel->fdw_private);

	/*
	 * TODO: Review the columns requested (via params_list) and only pull those back, using
	 * OGR_L_SetIgnoredFields. This is less important than pushing restrictions
	 * down to OGR via OGR_L_SetAttributeFilter (done) and (TODO) OGR_L_SetSpatialFilter.
	 */
	initStringInfo(&sql);
	sql_generated = ogrDeparse(&sql, root, baserel, scan_clauses, &params_list);
	elog(DEBUG1,"OGR SQL: %s", sql.data);

	/*
	 * Here we strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 * Some FDW implementations (mysql_fdw) just pass this full list on to the
	 * make_foreignscan function. postgres_fdw carefully separates local and remote
	 * clauses and only passes the local ones to make_foreignscan, so this
	 * is probably best practice, though re-applying the clauses is probably
	 * the least of our performance worries with this fdw. For now, we just
	 * pass them all to make_foreignscan, see no evil, etc.
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/*
	 * Serialize the data we want to pass to the execution stage.
	 * This is ugly but seems to be the only way to pass our constructed
	 * OGR SQL command to execution.
	 *
	 * TODO: Pass a spatial filter down also.
	 */
	if ( sql_generated )
		fdw_private = list_make2(makeString(sql.data), params_list);
	else
		fdw_private = list_make2(NULL, params_list);

	/*
	 * Clean up our connection
	 */
	ogrFinishConnection(&(planstate->ogr));

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							fdw_private
#if PG_VERSION_NUM >= 90500
							,NIL  /* no scan_tlist */
							,NIL   /* no remote quals */
							,outer_plan
#endif
);


}

static void
ogrCanConvertToPg(OGRFieldType ogr_type, Oid pg_type, const char *colname, const char *tblname)
{
	switch (ogr_type)
	{
		case OFTInteger:
			if ( pg_type == BOOLOID ||  pg_type == INT4OID || pg_type == INT8OID || pg_type == NUMERICOID || pg_type == FLOAT4OID || pg_type == FLOAT8OID || pg_type == TEXTOID || pg_type == VARCHAROID )
				return;
			break;

		case OFTReal:
			if ( pg_type == NUMERICOID || pg_type == FLOAT4OID || pg_type == FLOAT8OID || pg_type == TEXTOID || pg_type == VARCHAROID )
				return;
			break;

		case OFTBinary:
			if ( pg_type == BYTEAOID )
				return;
			break;

		case OFTString:
			if ( pg_type == TEXTOID || pg_type == VARCHAROID )
				return;
			break;

		case OFTDate:
			if ( pg_type == DATEOID || pg_type == TIMESTAMPOID || pg_type == TEXTOID || pg_type == VARCHAROID )
				return;
			break;

		case OFTTime:
			if ( pg_type == TIMEOID || pg_type == TEXTOID || pg_type == VARCHAROID )
				return;
			break;

		case OFTDateTime:
			if ( pg_type == TIMESTAMPOID || pg_type == TEXTOID || pg_type == VARCHAROID )
				return;
			break;

#if GDAL_VERSION_MAJOR >= 2
		case OFTInteger64:
			if ( pg_type == INT8OID || pg_type == NUMERICOID || pg_type == FLOAT8OID || pg_type == TEXTOID || pg_type == VARCHAROID )
				return;
			break;
#endif

		case OFTWideString:
		case OFTIntegerList:
#if GDAL_VERSION_MAJOR >= 2
		case OFTInteger64List:
#endif
		case OFTRealList:
		case OFTStringList:
		case OFTWideStringList:
		{
			ereport(ERROR, (
					errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					errmsg("column \"%s\" of foreign table \"%s\" uses an OGR array, currently unsupported", colname, tblname)
					));
			break;
		}
	}
	ereport(ERROR, (
			errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
			errmsg("column \"%s\" of foreign table \"%s\" converts OGR \"%s\" to \"%s\"",
				colname, tblname,
				OGR_GetFieldTypeName(ogr_type), format_type_be(pg_type))
			));
}

static char*
ogrTypeToPgType(OGRFieldDefnH ogr_fld)
{

	OGRFieldType ogr_type = OGR_Fld_GetType(ogr_fld);
	switch(ogr_type)
	{
		case OFTInteger:
#if GDAL_VERSION_MAJOR >= 2
			if( OGR_Fld_GetSubType(ogr_fld) == OFSTBoolean )
				return "boolean";
			else
#endif
				return "integer";
		case OFTReal:
			return "real";
		case OFTString:
			return "varchar";
		case OFTBinary:
			return "bytea";
		case OFTDate:
			return "date";
		case OFTTime:
			return "time";
		case OFTDateTime:
			return "timestamp";
		case OFTIntegerList:
			return "integer[]";
		case OFTRealList:
			return "real[]";
		case OFTStringList:
			return "varchar[]";
#if GDAL_VERSION_MAJOR >= 2
		case OFTInteger64:
			return "bigint";
#endif
		default:
			elog(ERROR,
			     "Unsupported GDAL type '%s'",
			     OGR_GetFieldTypeName(ogr_type));
	}

}

static void
freeOgrFdwTable(OgrFdwTable *table)
{
	if ( table )
	{
		if ( table->tblname ) pfree(table->tblname);
		if ( table->cols ) pfree(table->cols);
		pfree(table);
	}
}

typedef struct
{
	char *fldname;
	int fldnum;
} OgrFieldEntry;

static int ogrFieldEntryCmpFunc(const void * a, const void * b)
{
	const char *a_name = ((OgrFieldEntry*)a)->fldname;
	const char *b_name = ((OgrFieldEntry*)b)->fldname;

	return strcasecmp(a_name, b_name);
}


/*
 * The execstate holds a foreign table relation id and an OGR connection,
 * this function finds all the OGR fields that match up to columns in the
 * foreign table definition, using columns name match and data type consistency
 * as the criteria for making a match.
 * The results of the matching are stored in the execstate before the function
 * returns.
 */
static void
ogrReadColumnData(OgrFdwExecState *execstate)
{
	Relation rel;
	TupleDesc tupdesc;
	int i;
	OgrFdwTable *tbl;
	OGRFeatureDefnH dfn;
	int ogr_ncols;
	int fid_count = 0;
	int geom_count = 0;
	int ogr_geom_count = 0;
	int field_count = 0;
	OgrFieldEntry *ogr_fields;
	int ogr_fields_count = 0;
	char *tblname = get_rel_name(execstate->foreigntableid);

	/* Blow away any existing table in the state */
	if ( execstate->table )
	{
		freeOgrFdwTable(execstate->table);
		execstate->table = NULL;
	}

	/* Fresh table */
	tbl = palloc0(sizeof(OgrFdwTable));

	/* One column for each PgSQL foreign table column */
	rel = heap_open(execstate->foreigntableid, NoLock);
	tupdesc = rel->rd_att;
	execstate->tupdesc = tupdesc;
	tbl->ncols = tupdesc->natts;
	tbl->cols = palloc0(tbl->ncols * sizeof(OgrFdwColumn));
	tbl->tblname = pstrdup(tblname);

	/* Get OGR metadata ready */
	dfn = OGR_L_GetLayerDefn(execstate->ogr.lyr);
	ogr_ncols = OGR_FD_GetFieldCount(dfn);
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
	ogr_geom_count = OGR_FD_GetGeomFieldCount(dfn);
#else
	ogr_geom_count = ( OGR_FD_GetGeomType(dfn) != wkbNone ) ? 1 : 0;
#endif


	/* Prepare sorted list of OGR column names */
	/* TODO: change this to a hash table, to avoid repeated strcmp */
	/* We will search both the original and laundered OGR field names for matches */
	ogr_fields_count = 2 * ogr_ncols;
	ogr_fields = palloc0(ogr_fields_count * sizeof(OgrFieldEntry));
	for ( i = 0; i < ogr_ncols; i++ )
	{
		char *fldname = pstrdup(OGR_Fld_GetNameRef(OGR_FD_GetFieldDefn(dfn, i)));
		char *fldname_laundered = palloc(STR_MAX_LEN);
		strncpy(fldname_laundered, fldname, STR_MAX_LEN);
		ogrStringLaunder(fldname_laundered);
		ogr_fields[2*i].fldname = fldname;
		ogr_fields[2*i].fldnum = i;
		ogr_fields[2*i+1].fldname = fldname_laundered;
		ogr_fields[2*i+1].fldnum = i;
	}
	qsort(ogr_fields, ogr_fields_count, sizeof(OgrFieldEntry), ogrFieldEntryCmpFunc);


	/* loop through foreign table columns */
	for ( i = 0; i < tbl->ncols; i++ )
	{
		Form_pg_attribute att_tuple = tupdesc->attrs[i];
		OgrFdwColumn col = tbl->cols[i];
		col.pgattnum = att_tuple->attnum;
		col.pgtype = att_tuple->atttypid;
		col.pgtypmod = att_tuple->atttypmod;
		col.pgattisdropped = att_tuple->attisdropped;

		/* Skip filling in any further metadata about dropped columns */
		if ( col.pgattisdropped )
			continue;

		/* Find the appropriate conversion function */
		getTypeInputInfo(col.pgtype, &col.pginputfunc, &col.pginputioparam);
		getTypeBinaryInputInfo(col.pgtype, &col.pgrecvfunc, &col.pgrecvioparam);

		/* Get the PgSQL column name */
		col.pgname = get_relid_attribute_name(rel->rd_id, att_tuple->attnum);

		if ( strcaseeq(col.pgname, "fid") && (col.pgtype == INT4OID || col.pgtype == INT8OID) )
		{
			if ( fid_count >= 1 )
				elog(ERROR, "FDW table '%s' includes more than one FID column", tblname);

			col.ogrvariant = OGR_FID;
			col.ogrfldnum = fid_count++;
		}
		else if ( col.pgtype == GEOMETRYOID )
		{
			/* Stop if there are more geometry columns than we can support */
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
			if ( geom_count >= ogr_geom_count )
				elog(ERROR, "FDW table includes more geometry columns than exist in the OGR source");
#else
			if ( geom_count >= ogr_geom_count )
				elog(ERROR, "FDW table includes more than one geometry column");
#endif
			col.ogrvariant = OGR_GEOMETRY;
			col.ogrfldtype = OFTBinary;
			col.ogrfldnum = geom_count++;
		}
		else
		{
			List *options;
			ListCell *lc;
			OgrFieldEntry *found_entry;

			/* By default, search for the PgSQL column name */
			OgrFieldEntry entry;
			entry.fldname = col.pgname;
			entry.fldnum = 0;

			/*
			 * But, if there is a 'column_name' option for this column, we
			 * want to search for *that* in the OGR layer.
			 */
			options = GetForeignColumnOptions(execstate->foreigntableid, i + 1);
			foreach(lc, options)
			{
				DefElem    *def = (DefElem *) lfirst(lc);

				if ( streq(def->defname, "column_name") )
				{
					entry.fldname = defGetString(def);
					break;
				}
			}

			/* Search PgSQL column name in the OGR column name list */
			found_entry = bsearch(&entry, ogr_fields, ogr_fields_count, sizeof(OgrFieldEntry), ogrFieldEntryCmpFunc);

			/* Column name matched, so save this entry, if the types are consistent */
			if ( found_entry )
			{
				OGRFieldDefnH fld = OGR_FD_GetFieldDefn(dfn, found_entry->fldnum);
				OGRFieldType fldtype = OGR_Fld_GetType(fld);

				/* Error if types mismatched when column names match */
				ogrCanConvertToPg(fldtype, col.pgtype, col.pgname, tblname);

				col.ogrvariant = OGR_FIELD;
				col.ogrfldnum = found_entry->fldnum;
				col.ogrfldtype = fldtype;
				field_count++;
			}
			else
			{
				col.ogrvariant = OGR_UNMATCHED;
			}
		}
		tbl->cols[i] = col;
	}

	elog(DEBUG2, "ogrReadColumnData matched %d FID, %d GEOM, %d FIELDS out of %d PGSQL COLUMNS", fid_count, geom_count, field_count, tbl->ncols);

	/* Clean up */
	
	execstate->table = tbl;
	for( i = 0; i < 2*ogr_ncols; i++ )
		if ( ogr_fields[i].fldname ) pfree(ogr_fields[i].fldname);
	pfree(ogr_fields);
	heap_close(rel, NoLock);

	return;
}


/*
 * ogrBeginForeignScan
 */
static void
ogrBeginForeignScan(ForeignScanState *node, int eflags)
{
	Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;

	/* Initialize OGR connection */
	OgrFdwExecState *execstate = getOgrFdwExecState(foreigntableid);

	/* Read the OGR layer definition and PgSQL foreign table definitions */
	ogrReadColumnData(execstate);

	/* Get private info created by planner functions. */
	execstate->sql = strVal(list_nth(fsplan->fdw_private, 0));
	// execstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);

	if ( execstate->sql && strlen(execstate->sql) > 0 )
	{
		OGRErr err = OGR_L_SetAttributeFilter(execstate->ogr.lyr, execstate->sql);
		if ( err != OGRERR_NONE )
		{
			elog(NOTICE, "unable to set OGR SQL '%s' on layer", execstate->sql);
		}
	}
	else
	{
		OGR_L_SetAttributeFilter(execstate->ogr.lyr, NULL);
	}

	/* Save the state for the next call */
	node->fdw_state = (void *) execstate;

	return;
}

/*
 * Rather than explicitly try and form PgSQL datums, use the type
 * input functions, that accept cstring representations, and convert
 * to the input format. We have to lookup the right input function for
 * each column in the foreign table. This is happening for every
 * column and every row, so probably a performance improvement would
 * be to cache this information once.
 */
static Datum
pgDatumFromCString(const char *cstr, Oid pgtype, int pgtypmod, Oid pginputfunc)
{
	Datum value;
	Datum cdata = CStringGetDatum(cstr);

	/* pgtypmod will be -1 for types w/o typmod  */
	if ( pgtypmod >= 0 )
	{
		/* These functions require a type modifier */
		value = OidFunctionCall3(pginputfunc, cdata,
			ObjectIdGetDatum(InvalidOid),
			Int32GetDatum(pgtypmod));
	}
	else
	{
		/* These functions don't */
		value = OidFunctionCall1(pginputfunc, cdata);
	}

	return value;
}

static inline void
ogrNullSlot(Datum *values, bool *nulls, int i)
{
	values[i] = PointerGetDatum(NULL);
	nulls[i] = true;
}

/*
* The ogrIterateForeignScan is getting a new TupleTableSlot to handle
* for each iteration. Each slot contains an entry for every column in
* in the foreign table, that has to be filled out, either with a value
* or a NULL for columns that either have been deleted or were not requested
* in the query.
*
* The tupledescriptor tells us about the types of each slot.
* For now we assume our slot has exactly the same number of
* records and equivalent types to our OGR layer, and that our
* foreign table's first two columns are an integer primary key
* using int8 as the type, and then a geometry using bytea as
* the type, then everything else.
*/
static OGRErr
ogrFeatureToSlot(const OGRFeatureH feat, TupleTableSlot *slot, TupleDesc tupdesc, const OgrFdwTable *tbl)
{
	int i;
	Datum *values = slot->tts_values;
	bool *nulls = slot->tts_isnull;

	/* Check our assumption that slot and setup data match */
	if ( tbl->ncols != tupdesc->natts )
	{
		elog(ERROR, "FDW metadata table and exec table have mismatching number of columns");
		return OGRERR_FAILURE;
	}

	/* For each pgtable column, get a value from OGR */
	for ( i = 0; i < tbl->ncols; i++ )
	{
		OgrFdwColumn col = tbl->cols[i];
		const char *pgname = col.pgname;
		Oid pgtype = col.pgtype;
		int pgtypmod = col.pgtypmod;
		Oid pginputfunc = col.pginputfunc;
		// Oid pginputioparam = col.pginputioparam;
		int ogrfldnum = col.ogrfldnum;
		OGRFieldType ogrfldtype = col.ogrfldtype;
		OgrColumnVariant ogrvariant = col.ogrvariant;

		/*
		 * Fill in dropped attributes with NULL
		 */
		if ( col.pgattisdropped )
		{
			ogrNullSlot(values, nulls, i);
			continue;
		}

		if ( ogrvariant == OGR_FID )
		{
			long fid = OGR_F_GetFID(feat);

			if ( fid == OGRNullFID )
			{
				ogrNullSlot(values, nulls, i);
			}
			else
			{
				char fidstr[256];
				snprintf(fidstr, 256, "%ld", fid);

				nulls[i] = false;
				values[i] = pgDatumFromCString(fidstr, pgtype, pgtypmod, pginputfunc);
			}
		}
		else if ( ogrvariant == OGR_GEOMETRY )
		{
			int wkbsize;
			int varsize;
			bytea *varlena;
			OGRErr err;

#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
			OGRGeometryH geom = OGR_F_GetGeomFieldRef(feat, ogrfldnum);
#else
			OGRGeometryH geom = OGR_F_GetGeometryRef(feat);
#endif

			/* No geometry ? NULL */
			if ( ! geom )
			{
				/* No geometry column, so make the output null */
				ogrNullSlot(values, nulls, i);
				continue;
			}

			/*
			 * Start by generating standard PgSQL variable length byte
			 * buffer, with WKB filled into the data area.
			 */
			wkbsize = OGR_G_WkbSize(geom);
			varsize = wkbsize + VARHDRSZ;
			varlena = palloc(varsize);
			err = OGR_G_ExportToWkb(geom, wkbNDR, (unsigned char *)VARDATA(varlena));
			SET_VARSIZE(varlena, varsize);

			/* Couldn't create WKB from OGR geometry? error */
			if ( err != OGRERR_NONE )
			{
				return err;
			}

			if ( pgtype == BYTEAOID )
			{
				/*
				 * Nothing special to do for bytea, just send the varlena data through!
				 */
				nulls[i] = false;
				values[i] = PointerGetDatum(varlena);
			}
			else if ( pgtype == GEOMETRYOID )
			{
				/*
				 * For geometry we need to convert the varlena WKB data into a serialized
				 * geometry (aka "gserialized"). For that, we can use the type's "recv" function
				 * which takes in WKB and spits out serialized form.
				 */
				StringInfoData strinfo;

				/*
				 * The "recv" function expects to receive a StringInfo pointer
				 * on the first argument, so we form one of those ourselves by
				 * hand. Rather than copy into a fresh buffer, we'll just use the
				 * existing varlena buffer and point to the data area.
				 */
				strinfo.data = (char *)VARDATA(varlena);
				strinfo.len = wkbsize;
				strinfo.maxlen = strinfo.len;
				strinfo.cursor = 0;

				/*
				 * TODO: We should probably find out the typmod and send
				 * this along to the recv function too, but we can ignore it now
				 * and just have no typmod checking.
				 */
				nulls[i] = false;
				values[i] = OidFunctionCall1(col.pgrecvfunc, PointerGetDatum(&strinfo));
			}
			else
			{
				elog(NOTICE, "conversion to geometry called with column type not equal to bytea or geometry");
				ogrNullSlot(values, nulls, i);
			}
		}
		else if ( ogrvariant == OGR_FIELD )
		{
			/* Ensure that the OGR data type fits the destination Pg column */
			ogrCanConvertToPg(ogrfldtype, pgtype, pgname, tbl->tblname);

			/* Only convert non-null fields */
			if ( OGR_F_IsFieldSet(feat, ogrfldnum) )
			{
				switch(ogrfldtype)
				{
					case OFTBinary:
					{
						/*
						 * Convert binary fields to bytea directly
						 */
						int bufsize;
						GByte *buf = OGR_F_GetFieldAsBinary(feat, ogrfldnum, &bufsize);
						int varsize = bufsize + VARHDRSZ;
						bytea *varlena = palloc(varsize);
						memcpy(VARDATA(varlena), buf, bufsize);
						SET_VARSIZE(varlena, varsize);
						nulls[i] = false;
						values[i] = PointerGetDatum(varlena);
						break;
					}
					case OFTInteger:
					case OFTReal:
					case OFTString:
					{
						/*
						 * Convert numbers and strings via a string representation.
						 * Handling numbers directly would be faster, but require a lot of extra code.
						 * For now, we go via text.
						 */
						const char *cstr = OGR_F_GetFieldAsString(feat, ogrfldnum);
						if ( cstr )
						{
							nulls[i] = false;
							values[i] = pgDatumFromCString(cstr, pgtype, pgtypmod, pginputfunc);
						}
						else
						{
							ogrNullSlot(values, nulls, i);
						}
						break;
					}
					case OFTDate:
					case OFTTime:
					case OFTDateTime:
					{
						/*
						 * OGR date/times have a weird access method, so we use that to pull
						 * out the raw data and turn it into a string for PgSQL's (very
						 * sophisticated) date/time parsing routines to handle.
						 */
						int year, month, day, hour, minute, second, tz;
						char cstr[256];

						OGR_F_GetFieldAsDateTime(feat, ogrfldnum,
						                         &year, &month, &day,
						                         &hour, &minute, &second, &tz);

						if ( ogrfldtype == OFTDate )
						{
							snprintf(cstr, 256, "%d-%02d-%02d", year, month, day);
						}
						else if ( ogrfldtype == OFTTime )
						{
							snprintf(cstr, 256, "%02d:%02d:%02d", hour, minute, second);
						}
						else
						{
							snprintf(cstr, 256, "%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
						}

						nulls[i] = false;
						values[i] = pgDatumFromCString(cstr, pgtype, pgtypmod, pginputfunc);
						break;

					}
					case OFTIntegerList:
					case OFTRealList:
					case OFTStringList:
					{
						/* TODO, map these OGR array types into PgSQL arrays (fun!) */
						elog(ERROR, "unsupported OGR array type \"%s\"", OGR_GetFieldTypeName(ogrfldtype));
						break;
					}
					default:
					{
						elog(ERROR, "unsupported OGR type \"%s\"", OGR_GetFieldTypeName(ogrfldtype));
						break;
					}

				}
			}
			else
			{
				ogrNullSlot(values, nulls, i);
			}
		}
		/* Fill in unmatched columns with NULL */
		else if ( ogrvariant == OGR_UNMATCHED )
		{
			ogrNullSlot(values, nulls, i);
		}
		else
		{
			elog(ERROR, "OGR FDW unsupported column variant in \"%s\", %d", pgname, ogrvariant);
			return OGRERR_FAILURE;
		}

	}

	/* done! */
	return OGRERR_NONE;
}


/*
 * ogrIterateForeignScan
 *		Read next record from OGR and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
ogrIterateForeignScan(ForeignScanState *node)
{
	OgrFdwExecState *execstate = (OgrFdwExecState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	OGRFeatureH feat;

	/*
	 * Clear the slot. If it gets through w/o being filled up, that means
	 * we're all done.
	 */
	ExecClearTuple(slot);

	/*
	 * First time through, reset reading. Then keep reading until
	 * we run out of records, then return a cleared (NULL) slot, to
	 * notify the core we're done.
	 */
	if ( execstate->rownum == 0 )
	{
		OGR_L_ResetReading(execstate->ogr.lyr);
	}

	/* If we rectreive a feature from OGR, copy it over into the slot */
	if ( (feat = OGR_L_GetNextFeature(execstate->ogr.lyr)) )
	{
		/* convert result to arrays of values and null indicators */
		if ( OGRERR_NONE != ogrFeatureToSlot(feat, slot, execstate->tupdesc, execstate->table) )
		{
			const char *ogrerr = CPLGetLastErrorMsg();
			if ( ogrerr && ! streq(ogrerr,"") )
			{
				ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("failure reading OGR data source"),
					 errhint("%s", ogrerr)));
			}
			else
			{
	 			ereport(ERROR,
	 				(errcode(ERRCODE_FDW_ERROR),
	 				 errmsg("failure reading OGR data source")));
			}
		}

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);

		/* increment row count */
		execstate->rownum++;

		/* Release OGR feature object */
		OGR_F_Destroy(feat);
	}

	return slot;
}

/*
 * ogrReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
ogrReScanForeignScan(ForeignScanState *node)
{
	OgrFdwExecState *execstate = (OgrFdwExecState *) node->fdw_state;

	OGR_L_ResetReading(execstate->ogr.lyr);
	execstate->rownum = 0;

	return;
}

/*
 * ogrEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
ogrEndForeignScan(ForeignScanState *node)
{
	OgrFdwExecState *execstate = (OgrFdwExecState *) node->fdw_state;

	ogrFinishConnection( &(execstate->ogr) );

	return;
}


#endif /* PostgreSQL 9.3 version check */

#if PG_VERSION_NUM >= 90500

static void
ogrImportForeignColumn(StringInfoData *buf, const char *ogrcolname, const char *pgtype, bool launder_column_names)
{
	char pgcolname[STR_MAX_LEN];
	strncpy(pgcolname, ogrcolname, STR_MAX_LEN);
	ogrStringLaunder(pgcolname);
	if ( launder_column_names )
	{
		appendStringInfo(buf, ",\n %s %s", pgcolname, pgtype);
		if ( ! strcaseeq(pgcolname, ogrcolname) )
			appendStringInfo(buf, " OPTIONS (column_name '%s')", ogrcolname);
	}
	else
	{
		/* OGR column is PgSQL compliant, we're all good */
		if ( streq(pgcolname, ogrcolname) )
			appendStringInfo(buf, ",\n %s %s", ogrcolname, pgtype);
		/* OGR is mixed case or non-compliant, we need to quote it */
		else
			appendStringInfo(buf, ",\n \"%s\" %s", ogrcolname, pgtype);
	}
}

/*
 * PostgreSQL 9.5 or above.  Import a foreign schema
 */
static List *
ogrImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	List *commands = NIL;
	ForeignServer *server;
	ListCell *lc;
	bool import_all = false;
	bool launder_column_names, launder_table_names;
	StringInfoData buf;
	OgrConnection ogr;
	int i;
	char layer_name[STR_MAX_LEN];
	char table_name[STR_MAX_LEN];

	/* Create workspace for strings */
	initStringInfo(&buf);

	/* Are we importing all layers in the OGR datasource? */
	import_all = streq(stmt->remote_schema, "ogr_all");

	/* Make connection to server */
	memset(&ogr, 0, sizeof(OgrConnection));
	server = GetForeignServer(serverOid);
	ogr = ogrGetConnectionFromServer(serverOid);

	/* Launder by default */
	launder_column_names = launder_table_names = true;

	/* Read user-provided statement laundering options */
	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (streq(def->defname, "launder_column_names"))
			launder_column_names = defGetBoolean(def);
		else if (streq(def->defname, "launder_table_names"))
			launder_table_names = defGetBoolean(def);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
	}

	for ( i = 0; i < GDALDatasetGetLayerCount(ogr.ds); i++ )
	{
		bool import_layer = false;
		OGRLayerH ogr_lyr = GDALDatasetGetLayer(ogr.ds, i);

		if ( ! ogr_lyr )
		{
			elog(DEBUG1, "Skipping OGR layer %d, unable to read layer", i);
			continue;
		}

		/* Layer name is never laundered, since it's the link back to OGR */
		strncpy(layer_name, OGR_L_GetName(ogr_lyr), STR_MAX_LEN);

		/*
		* We need to compare against created table names
		* because PgSQL does an extra check on CREATE FOREIGN TABLE
		*/
		strncpy(table_name, layer_name, STR_MAX_LEN);
		if (launder_table_names)
			ogrStringLaunder(table_name);

		/*
		* Only include if we are importing "ogr_all" or
		* the layer prefix starts with the remote schema
		*/
		import_layer = import_all ||
				( strncmp(layer_name, stmt->remote_schema, strlen(stmt->remote_schema) ) == 0 );

		/* Apply restrictions for LIMIT TO and EXCEPT */
		if (import_layer && (
		    stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
		    stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT ) )
		{
			/* Limited list? Assume we are taking no items */
			if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
				import_layer = false;

			/* Check the list for our items */
			foreach(lc, stmt->table_list)
			{
				RangeVar *rv = (RangeVar *) lfirst(lc);
				/* Found one! */
				if ( streq(rv->relname, table_name) )
				{
					if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
						import_layer = true;
					else
						import_layer = false;

					break;
				}

			}
		}

		if (import_layer)
		{

			OGRFeatureDefnH ogr_fd = OGR_L_GetLayerDefn(ogr_lyr);
			char *pggeomtype;
			int j;

#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
			int geom_field_count = OGR_FD_GetGeomFieldCount(ogr_fd);
#else
			int geom_field_count = (OGR_L_GetGeomType(ogr_lyr) != wkbNone);
#endif

			if ( ! ogr_fd )
				elog(ERROR, "unable to read layer definition for OGR layer `%s`", layer_name);

			resetStringInfo(&buf);

			appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (\n", quote_identifier(table_name));
			appendStringInfoString(&buf, " fid integer");

			/* What column type to use for OGR geometries? */
			if ( GEOMETRYOID == BYTEAOID )
				pggeomtype = "bytea";
			else
				pggeomtype = "geometry";

			geom_field_count = OGR_FD_GetGeomFieldCount(ogr_fd);

			if( geom_field_count == 1 )
			{
				appendStringInfo(&buf, ",\n geom %s", pggeomtype);
			}
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
			else
			{
				for ( j = 0; j < geom_field_count; j++ )
				{
					OGRGeomFieldDefnH gfld = OGR_FD_GetGeomFieldDefn(ogr_fd, j);
					const char *geomfldname = OGR_GFld_GetNameRef(gfld);

					/* TODO reflect geometry type/srid info from OGR */
					if ( geomfldname )
					{
						ogrImportForeignColumn(&buf, geomfldname, pggeomtype, launder_column_names);
					}
					else
					{
						if ( geom_field_count > 1 )
							appendStringInfo(&buf, ",\n geom%d %s", j + 1, pggeomtype);
						else
							appendStringInfo(&buf, ",\n geom %s", pggeomtype);
					}
				}
			}
#endif
			/* Write out attribute fields */
			for ( j = 0; j < OGR_FD_GetFieldCount(ogr_fd); j++ )
			{
				OGRFieldDefnH ogr_fld = OGR_FD_GetFieldDefn(ogr_fd, j);
				char *pgcoltype = ogrTypeToPgType(ogr_fld);
				ogrImportForeignColumn(&buf, OGR_Fld_GetNameRef(ogr_fld), pgcoltype, launder_column_names);
			}

			/*
			 * Add server name and layer-level options.  We specify remote
			 *  layer name as option
			 */
			appendStringInfo(&buf, "\n) SERVER %s\nOPTIONS (",
							 quote_identifier(server->servername));

			appendStringInfoString(&buf, "layer ");
			ogrDeparseStringLiteral(&buf, layer_name);

			appendStringInfoString(&buf, ");");

			elog(DEBUG2, "%s", buf.data);

			commands = lappend(commands, pstrdup(buf.data));
		}
	}

	elog(NOTICE, "Number of tables to be created %d", list_length(commands) );

	/* Clean up */
	pfree(buf.data);

	return commands;
}

#endif /* PGSQL 9.5+ */

static void
ogrStringLaunder (char *str)
{
	int i, j = 0;
	char tmp[STR_MAX_LEN];
	memset(tmp, 0, STR_MAX_LEN);
	
	for(i = 0; str[i]; i++)
	{
		char c = tolower(str[i]);

		/* First character is a numeral, prefix with 'n' */
		if ( i == 0 && (c >= 48 && c <= 57) )
		{
			tmp[j++] = 'n';
		}

		/* Replace non-safe characters w/ _ */
		if ( (c >= 48 && c <= 57) || /* 0-9 */
			 (c >= 65 && c <= 90) || /* A-Z */
			 (c >= 97 && c <= 122)   /* a-z */ )
		{
			/* Good character, do nothing */
		}
		else
		{
			c = '_';
		}
		tmp[j++] = c;

		/* Avoid mucking with data beyond the end of our stack-allocated strings */
		if ( j >= STR_MAX_LEN )
			j = STR_MAX_LEN - 1;
	}
	strncpy(str, tmp, STR_MAX_LEN);
	
}