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
* PostgreSQL
*/
#include "postgres.h"

/*
 * System
 */
#include <sys/stat.h>
#include <unistd.h>


/*
 * Require PostgreSQL >= 9.3
 */
#if PG_VERSION_NUM < 90300
#error "OGR FDW requires PostgreSQL version 9.3 or higher"

#else

/*
 * Definition of stringToQualifiedNameList
 */
#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif

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
	const char* optname;
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
#define OPT_UPDATEABLE "updateable"
#define OPT_CHAR_ENCODING "character_encoding"

#define OGR_FDW_FRMT_INT64	 "%lld"
#define OGR_FDW_CAST_INT64(x)	 (long long)(x)

/*
 * Valid options for ogr_fdw.
 * ForeignDataWrapperRelationId (no options)
 * ForeignServerRelationId (CREATE SERVER options)
 * UserMappingRelationId (CREATE USER MAPPING options)
 * ForeignTableRelationId (CREATE FOREIGN TABLE options)
 *
 * {optname, optcontext, optrequired, optfound}
 */
static struct OgrFdwOption valid_options[] =
{

	/* OGR column mapping */
	{OPT_COLUMN, AttributeRelationId, false, false},

	/* OGR datasource options */
	{OPT_SOURCE, ForeignServerRelationId, true, false},
	{OPT_DRIVER, ForeignServerRelationId, false, false},
	{OPT_UPDATEABLE, ForeignServerRelationId, false, false},
	{OPT_CONFIG_OPTIONS, ForeignServerRelationId, false, false},
	{OPT_CHAR_ENCODING, ForeignServerRelationId, false, false},
#if GDAL_VERSION_MAJOR >= 2
	{OPT_OPEN_OPTIONS, ForeignServerRelationId, false, false},
#endif
	/* OGR layer options */
	{OPT_LAYER, ForeignTableRelationId, true, false},
	{OPT_UPDATEABLE, ForeignTableRelationId, false, false},

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
 * FDW query callback routines
 */
static void ogrGetForeignRelSize(PlannerInfo* root,
                                 RelOptInfo* baserel,
                                 Oid foreigntableid);
static void ogrGetForeignPaths(PlannerInfo* root,
                               RelOptInfo* baserel,
                               Oid foreigntableid);
static ForeignScan* ogrGetForeignPlan(PlannerInfo* root,
                                      RelOptInfo* baserel,
                                      Oid foreigntableid,
                                      ForeignPath* best_path,
                                      List* tlist,
                                      List* scan_clauses
#if PG_VERSION_NUM >= 90500
                                      , Plan* outer_plan
#endif
                                     );
static void ogrBeginForeignScan(ForeignScanState* node, int eflags);
static TupleTableSlot* ogrIterateForeignScan(ForeignScanState* node);
static void ogrReScanForeignScan(ForeignScanState* node);
static void ogrEndForeignScan(ForeignScanState* node);

/*
 * FDW modify callback routines
 */
#if PG_VERSION_NUM >= 140000
static void ogrAddForeignUpdateTargets(PlannerInfo* planinfo,
                                       unsigned int rte_index,
                                       RangeTblEntry* target_rte,
                                       Relation target_relation);
#else
static void ogrAddForeignUpdateTargets(Query* parsetree,
                                       RangeTblEntry* target_rte,
                                       Relation target_relation);
#endif

static void ogrBeginForeignModify(ModifyTableState* mtstate,
                                  ResultRelInfo* rinfo,
                                  List* fdw_private,
                                  int subplan_index,
                                  int eflags);
static TupleTableSlot* ogrExecForeignInsert(EState* estate,
        ResultRelInfo* rinfo,
        TupleTableSlot* slot,
        TupleTableSlot* planSlot);
static TupleTableSlot* ogrExecForeignUpdate(EState* estate,
        ResultRelInfo* rinfo,
        TupleTableSlot* slot,
        TupleTableSlot* planSlot);
static TupleTableSlot* ogrExecForeignDelete(EState* estate,
        ResultRelInfo* rinfo,
        TupleTableSlot* slot,
        TupleTableSlot* planSlot);
static void ogrEndForeignModify(EState* estate,
                                ResultRelInfo* rinfo);
static int ogrIsForeignRelUpdatable(Relation rel);


#if PG_VERSION_NUM >= 90500
static List* ogrImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid serverOid);
#endif

/*
 * Helper functions
 */
static OgrConnection ogrGetConnectionFromTable(Oid foreigntableid, OgrUpdateable updateable);
static void ogr_fdw_exit(int code, Datum arg);
static void ogrReadColumnData(OgrFdwState* state);

/* Global to hold GEOMETRYOID */
Oid GEOMETRYOID = InvalidOid;


#if GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(2,1,0)

static const char* const gdalErrorTypes[] =
{
	"None",
	"AppDefined",
	"OutOfMemory",
	"FileIO",
	"OpenFailed",
	"IllegalArg",
	"NotSupported",
	"AssertionFailed",
	"NoWriteAccess",
	"UserInterrupt",
	"ObjectNull",
	"HttpResponse",
	"AWSBucketNotFound",
	"AWSObjectNotFound",
	"AWSAccessDenied",
	"AWSInvalidCredentials",
	"AWSSignatureDoesNotMatch"
};

/* In theory this function should be declared "static void CPL_STDCALL" */
/* since this is the official signature of error handler callbacks. */
/* That would be needed if both GDAL and ogr_fdw were compiled with Visual */
/* Studio, but with non-Visual Studio compilers, the macro expands to empty, */
/* so if both GDAL and ogr_fdw are compiled with gcc things are fine. In case */
/* of mixes, crashes may occur but there is no clean fix... So let this as a note */
/* in case of future issue... */
static void
ogrErrorHandler(CPLErr eErrClass, int err_no, const char* msg)
{
	const char* gdalErrType = "unknown type";
	if (err_no >= 0 && err_no <
	        (int)sizeof(gdalErrorTypes) / sizeof(gdalErrorTypes[0]))
	{
		gdalErrType = gdalErrorTypes[err_no];
	}
	switch (eErrClass)
	{
	case CE_None:
		elog(NOTICE, "GDAL %s [%d] %s", gdalErrType, err_no, msg);
		break;
	case CE_Debug:
		elog(DEBUG2, "GDAL %s [%d] %s", gdalErrType, err_no, msg);
		break;
	case CE_Warning:
		elog(WARNING, "GDAL %s [%d] %s", gdalErrType, err_no, msg);
		break;
	case CE_Failure:
	case CE_Fatal:
	default:
		elog(ERROR, "GDAL %s [%d] %s", gdalErrType, err_no, msg);
		break;
	}
	return;
}

#endif /* GDAL 2.1.0+ */

void
_PG_init(void)
{
	on_proc_exit(&ogr_fdw_exit, PointerGetDatum(NULL));

#if GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(2,1,0)
	/* Hook up the GDAL error handlers to PgSQL elog() */
	CPLSetErrorHandler(ogrErrorHandler);
	CPLSetCurrentErrorHandlerCatchDebug(true);
#endif
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
 * Given extension oid, lookup installation namespace oid.
 * This side steps search_path issues with
 * TypenameGetTypid encountered in
 * https://github.com/pramsey/pgsql-ogr-fdw/issues/263
 */
static Oid
get_extension_nsp_oid(Oid extOid)
{
	Oid         result;
	SysScanDesc scandesc;
	HeapTuple   tuple;
	ScanKeyData entry[1];

#if PG_VERSION_NUM < 120000
	Relation rel = heap_open(ExtensionRelationId, AccessShareLock);
    ScanKeyInit(&entry[0],
        ObjectIdAttributeNumber,
        BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(extOid));
#else
	Relation rel = table_open(ExtensionRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
		Anum_pg_extension_oid,
		BTEqualStrategyNumber, F_OIDEQ,
		ObjectIdGetDatum(extOid));
#endif /* PG_VERSION_NUM */

	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
                                  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

#if PG_VERSION_NUM < 120000
    heap_close(rel, AccessShareLock);
#else
    table_close(rel, AccessShareLock);
#endif

	return result;
}


/*
 * Get the geometry OID (if postgis is
 * installed) and cache it for quick lookup.
 */
Oid
ogrGetGeometryOid(void)
{
	/* Is value not set yet? */
	if (GEOMETRYOID == InvalidOid)
	{
		const char *extName = "postgis";
		const char *typName = "geometry";
		bool missing_ok = true;
		Oid extOid, extNspOid, typOid;

		/* Got postgis extension? */
		extOid = get_extension_oid(extName, missing_ok);
		if (!OidIsValid(extOid))
		{
			elog(DEBUG2, "%s: lookup of extension '%s' failed", __func__, extName);
			GEOMETRYOID = BYTEAOID;
			return GEOMETRYOID;
		}

		/* Got namespace for extension? */
		extNspOid = get_extension_nsp_oid(extOid);
		if (!OidIsValid(extNspOid))
		{
			elog(DEBUG2, "%s: lookup of namespace for '%s' (%u) failed", __func__, extName, extOid);
			GEOMETRYOID = BYTEAOID;
			return GEOMETRYOID;
		}

		/* Got geometry type in namespace? */
		typOid = GetSysCacheOid2(TYPENAMENSP,
#if PG_VERSION_NUM >= 120000
		            Anum_pg_type_oid,
#endif
		            PointerGetDatum(typName),
		            ObjectIdGetDatum(extNspOid));



		elog(DEBUG2, "%s: lookup of type id for '%s' got %u", __func__, typName, typOid);

		/* Geometry type is good? */
		if (OidIsValid(typOid) && get_typisdefined(typOid))
			GEOMETRYOID = typOid;
		else
			GEOMETRYOID = BYTEAOID;
	}

	return GEOMETRYOID;
}


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
ogr_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine* fdwroutine = makeNode(FdwRoutine);

	/* Read support */
	fdwroutine->GetForeignRelSize = ogrGetForeignRelSize;
	fdwroutine->GetForeignPaths = ogrGetForeignPaths;
	fdwroutine->GetForeignPlan = ogrGetForeignPlan;
	fdwroutine->BeginForeignScan = ogrBeginForeignScan;
	fdwroutine->IterateForeignScan = ogrIterateForeignScan;
	fdwroutine->ReScanForeignScan = ogrReScanForeignScan;
	fdwroutine->EndForeignScan = ogrEndForeignScan;

	/* Write support */
	fdwroutine->AddForeignUpdateTargets = ogrAddForeignUpdateTargets;
	fdwroutine->BeginForeignModify = ogrBeginForeignModify;
	fdwroutine->ExecForeignInsert = ogrExecForeignInsert;
	fdwroutine->ExecForeignUpdate = ogrExecForeignUpdate;
	fdwroutine->ExecForeignDelete = ogrExecForeignDelete;
	fdwroutine->EndForeignModify = ogrEndForeignModify;
	fdwroutine->IsForeignRelUpdatable = ogrIsForeignRelUpdatable;

#if PG_VERSION_NUM >= 90500
	/*  Support functions for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = ogrImportForeignSchema;
#endif

	PG_RETURN_POINTER(fdwroutine);
}

/*
* When attempting a soft open (allowing for failure and retry),
* we might need to call the opening
* routines twice, so all the opening machinery is placed here
* for convenient re-calling.
*/
static OGRErr
ogrGetDataSourceAttempt(OgrConnection* ogr, bool bUpdateable, char** open_option_list)
{
	GDALDriverH ogr_dr = NULL;
#if GDAL_VERSION_MAJOR >= 2
	unsigned int open_flags = GDAL_OF_VECTOR;

	if (bUpdateable)
	{
		open_flags |= GDAL_OF_UPDATE;
	}
	else
	{
		open_flags |= GDAL_OF_READONLY;
	}
#endif

	if (ogr->dr_str)
	{
		ogr_dr = GDALGetDriverByName(ogr->dr_str);
		if (!ogr_dr)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			         errmsg("unable to find format \"%s\"", ogr->dr_str),
			         errhint("See the formats list at http://www.gdal.org/ogr_formats.html")));
		}
#if GDAL_VERSION_MAJOR < 2
		ogr->ds = OGR_Dr_Open(ogr_dr, ogr->ds_str, bUpdateable);
#else
		{
			char** driver_list = CSLAddString(NULL, ogr->dr_str);
			ogr->ds = GDALOpenEx(ogr->ds_str,                  /* file/data source */
			                     open_flags,                            /* open flags */
			                     (const char* const*)driver_list,       /* driver */
			                     (const char* const*)open_option_list,  /* open options */
			                     NULL);                                 /* sibling files */
			CSLDestroy(driver_list);
		}
#endif
	}
	/* No driver, try a blind open... */
	else
	{
#if GDAL_VERSION_MAJOR < 2
		ogr->ds = OGROpen(ogr->ds_str, bUpdateable, &ogr_dr);
#else
		ogr->ds = GDALOpenEx(ogr->ds_str,
		                     open_flags,
		                     NULL,
		                     (const char* const*)open_option_list,
		                     NULL);
#endif
	}
	return ogr->ds ? OGRERR_NONE : OGRERR_FAILURE;
}

/*
 * Given a connection string and (optional) driver string, try to connect
 * with appropriate error handling and reporting. Used in query startup,
 * and in FDW options validation.
 */
static OGRErr
ogrGetDataSource(OgrConnection* ogr, OgrUpdateable updateable)
{
	char** open_option_list = NULL;
	bool bUpdateable = (updateable == OGR_UPDATEABLE_TRUE || updateable == OGR_UPDATEABLE_TRY);
	OGRErr err;

	/* Set the GDAL config options into the environment */
	if (ogr->config_options)
	{
		char** option_iter;
		char** option_list = CSLTokenizeString(ogr->config_options);

		for (option_iter = option_list; option_iter && *option_iter; option_iter++)
		{
			char* key;
			const char* value;
			value = CPLParseNameValue(*option_iter, &key);
			if (!(key && value))
			{
				elog(ERROR, "bad config option string '%s'", ogr->config_options);
			}

			elog(DEBUG1, "GDAL config option '%s' set to '%s'", key, value);
			CPLSetConfigOption(key, value);
			CPLFree(key);
		}
		CSLDestroy(option_list);
	}

	/* Parse the GDAL layer open options */
	if (ogr->open_options)
	{
		open_option_list = CSLTokenizeString(ogr->open_options);
	}

	/* Cannot search for drivers if they aren't registered, */
	/* but don't do registration if we already have drivers loaded */
	if (GDALGetDriverCount() <= 0)
	{
		GDALAllRegister();
	}

	/* First attempt at connection */
	err = ogrGetDataSourceAttempt(ogr, bUpdateable, open_option_list);

	/* Failed on soft updateable attempt, try and fall back to readonly */
	if ((!ogr->ds) && updateable == OGR_UPDATEABLE_TRY)
	{
		err = ogrGetDataSourceAttempt(ogr, false, open_option_list);
		/* Succeeded with readonly connection */
		if (ogr->ds)
		{
			ogr->ds_updateable = ogr->lyr_updateable = OGR_UPDATEABLE_FALSE;
		}
	}

	/* Open failed, provide error hint if OGR gives us one. */
	if (!ogr->ds)
	{
		const char* ogrerrmsg = CPLGetLastErrorMsg();
		if (ogrerrmsg && !streq(ogrerrmsg, ""))
		{
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			         errmsg("unable to connect to data source \"%s\"", ogr->ds_str),
			         errhint("%s", ogrerrmsg)));
		}
		else
		{
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			         errmsg("unable to connect to data source \"%s\"", ogr->ds_str)));
		}
	}

	CSLDestroy(open_option_list);

	return err;
}

static bool
ogrCanReallyCountFast(const OgrConnection* con)
{
	GDALDriverH dr = GDALGetDatasetDriver(con->ds);
	const char* dr_str = GDALGetDriverShortName(dr);

	if (streq(dr_str, "ESRI Shapefile") ||
	    streq(dr_str, "FileGDB") ||
	    streq(dr_str, "OpenFileGDB"))
	{
		return true;
	}
	return false;
}

static void
ogrEreportError(const char* errstr)
{
	const char* ogrerrmsg = CPLGetLastErrorMsg();
	if (ogrerrmsg && !streq(ogrerrmsg, ""))
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR),
		         errmsg("%s", errstr),
		         errhint("%s", ogrerrmsg)));
	}
	else
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR),
		         errmsg("%s", errstr)));
	}
}

/*
 * Make sure the datasource is cleaned up when we're done
 * with a connection.
 */
static void
ogrFinishConnection(OgrConnection* ogr)
{
	elog(DEBUG3, "%s: entered function", __func__);

	if (ogr->lyr && OGR_L_SyncToDisk(ogr->lyr) != OGRERR_NONE)
	{
		elog(NOTICE, "failed to flush writes to OGR data source");
	}

	if (ogr->ds)
	{
		GDALClose(ogr->ds);
	}

	ogr->ds = NULL;
}

static OgrConnection
ogrGetConnectionFromServer(Oid foreignserverid, OgrUpdateable updateable)
{
	ForeignServer* server;
	OgrConnection ogr;
	ListCell* cell;
	OGRErr err;

	elog(DEBUG3, "%s: entered function", __func__);

	/* Null all values */
	memset(&ogr, 0, sizeof(OgrConnection));
	ogr.ds_updateable = OGR_UPDATEABLE_UNSET;
	ogr.lyr_updateable = OGR_UPDATEABLE_UNSET;

	server = GetForeignServer(foreignserverid);

	foreach (cell, server->options)
	{
		DefElem* def = (DefElem*) lfirst(cell);
		if (streq(def->defname, OPT_SOURCE))
		{
			ogr.ds_str = defGetString(def);
		}
		if (streq(def->defname, OPT_DRIVER))
		{
			ogr.dr_str = defGetString(def);
		}
		if (streq(def->defname, OPT_CONFIG_OPTIONS))
		{
			ogr.config_options = defGetString(def);
		}
		if (streq(def->defname, OPT_OPEN_OPTIONS))
		{
			ogr.open_options = defGetString(def);
		}
		if (streq(def->defname, OPT_CHAR_ENCODING))
		{
			ogr.char_encoding = pg_char_to_encoding(defGetString(def));
		}
		if (streq(def->defname, OPT_UPDATEABLE))
		{
			if (defGetBoolean(def))
			{
				ogr.ds_updateable = OGR_UPDATEABLE_TRUE;
			}
			else
			{
				ogr.ds_updateable = OGR_UPDATEABLE_FALSE;
				/* Over-ride the open mode to favour user-defined mode */
				updateable = OGR_UPDATEABLE_FALSE;
			}
		}
	}

	if (!ogr.ds_str)
	{
		elog(ERROR, "FDW table '%s' option is missing", OPT_SOURCE);
	}

	/*
	 * TODO: Connections happen twice for each query, having a
	 * connection pool will certainly make things faster.
	 */

	/*  Connect! */
	err = ogrGetDataSource(&ogr, updateable);
	if (err == OGRERR_FAILURE)
	{
		elog(ERROR, "ogrGetDataSource failed");
	}
	return ogr;
}


/*
 * Read the options (data source connection from server and
 * layer name from table) from a foreign table and use them
 * to connect to an OGR layer. Return a connection object that
 * has handles for both the datasource and layer.
 */
static OgrConnection
ogrGetConnectionFromTable(Oid foreigntableid, OgrUpdateable updateable)
{
	ForeignTable* table;
	/* UserMapping *mapping; */
	/* ForeignDataWrapper *wrapper; */
	ListCell* cell;
	OgrConnection ogr;

	elog(DEBUG3, "%s: entered function", __func__);

	/* Gather all data for the foreign table. */
	table = GetForeignTable(foreigntableid);
	/* mapping = GetUserMapping(GetUserId(), table->serverid); */

	ogr = ogrGetConnectionFromServer(table->serverid, updateable);

	elog(DEBUG3, "%s: ogr.ds_str = %s", __func__, ogr.ds_str);

	foreach (cell, table->options)
	{
		DefElem* def = (DefElem*) lfirst(cell);
		if (streq(def->defname, OPT_LAYER))
		{
			ogr.lyr_str = defGetString(def);
		}
		if (streq(def->defname, OPT_UPDATEABLE))
		{
			if (defGetBoolean(def))
			{
				if (ogr.ds_updateable == OGR_UPDATEABLE_FALSE)
				{
					ereport(ERROR, (
					            errcode(ERRCODE_FDW_ERROR),
					            errmsg("data source \"%s\" is not updateable", ogr.ds_str),
					            errhint("cannot set table '%s' option to true", OPT_UPDATEABLE)
					        ));
				}
				ogr.lyr_updateable = OGR_UPDATEABLE_TRUE;
			}
			else
			{
				ogr.lyr_updateable = OGR_UPDATEABLE_FALSE;
			}
		}
	}

	if (!ogr.lyr_str)
	{
		elog(ERROR, "FDW table '%s' option is missing", OPT_LAYER);
	}

	elog(DEBUG3, "%s: ogr.lyr_str = %s", __func__, ogr.lyr_str);

	/* Does the layer exist in the data source? */
	ogr.lyr = GDALDatasetGetLayerByName(ogr.ds, ogr.lyr_str);
	if (!ogr.lyr)
	{
		const char* ogrerr = CPLGetLastErrorMsg();
		ereport(ERROR, (
		    errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
		    errmsg("unable to connect to %s to \"%s\"", OPT_LAYER, ogr.lyr_str),
		    (ogrerr && ! streq(ogrerr, ""))
		        ? errhint("%s", ogrerr)
		        : errhint("Does the layer exist?")
		    ));
	}

	if (OGR_L_TestCapability(ogr.lyr, OLCStringsAsUTF8))
	{
		ogr.char_encoding = PG_UTF8;
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
	List* options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	ListCell* cell;
	struct OgrFdwOption* opt;
	const char* source = NULL, *driver = NULL;
	const char* config_options = NULL, *open_options = NULL;
	OgrUpdateable updateable = OGR_UPDATEABLE_FALSE;

	/* Initialize found state to not found */
	for (opt = valid_options; opt->optname; opt++)
	{
		opt->optfound = false;
	}

	/*
	 * Check that only options supported by ogr_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach (cell, options_list)
	{
		DefElem* def = (DefElem*) lfirst(cell);
		bool optfound = false;

		for (opt = valid_options; opt->optname; opt++)
		{
			if (catalog == opt->optcontext && streq(opt->optname, def->defname))
			{
				/* Mark that this user option was found */
				opt->optfound = optfound = true;

				/* Store some options for testing later */
				if (streq(opt->optname, OPT_SOURCE))
				{
					source = defGetString(def);
				}
				if (streq(opt->optname, OPT_DRIVER))
				{
					driver = defGetString(def);
				}
				if (streq(opt->optname, OPT_CONFIG_OPTIONS))
				{
					config_options = defGetString(def);
				}
				if (streq(opt->optname, OPT_OPEN_OPTIONS))
				{
					open_options = defGetString(def);
				}
				if (streq(opt->optname, OPT_UPDATEABLE))
				{
					if (defGetBoolean(def))
					{
						updateable = OGR_UPDATEABLE_TRY;
					}
				}

				break;
			}
		}

		if (!optfound)
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			const struct OgrFdwOption* opt;
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
	for (opt = valid_options; opt->optname; opt++)
	{
		/* Required option for this catalog type is missing? */
		if (catalog == opt->optcontext && opt->optrequired && ! opt->optfound)
		{
			ereport(ERROR, (
			    errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    errmsg("required option \"%s\" is missing", opt->optname)));
		}
	}

	/* Make sure server connection can actually be established */
	if (catalog == ForeignServerRelationId && source)
	{
		OgrConnection ogr;
		OGRErr err;

		ogr.ds_str = source;
		ogr.dr_str = driver;
		ogr.config_options = config_options;
		ogr.open_options = open_options;

		err = ogrGetDataSource(&ogr, updateable);
		if (err == OGRERR_FAILURE)
		{
			elog(ERROR, "ogrGetDataSource failed");
		}
		if (ogr.ds)
		{
			GDALClose(ogr.ds);
		}
	}

	PG_RETURN_VOID();
}

/*
 * Initialize an OgrFdwPlanState on the heap.
 */
static OgrFdwState*
getOgrFdwState(Oid foreigntableid, OgrFdwStateType state_type)
{
	OgrFdwState* state;
	size_t size;
	OgrUpdateable updateable = OGR_UPDATEABLE_FALSE;

	switch (state_type)
	{
	case OGR_PLAN_STATE:
		size = sizeof(OgrFdwPlanState);
		updateable = OGR_UPDATEABLE_FALSE;
		break;
	case OGR_EXEC_STATE:
		size = sizeof(OgrFdwExecState);
		updateable = OGR_UPDATEABLE_FALSE;
		break;
	case OGR_MODIFY_STATE:
		updateable = OGR_UPDATEABLE_TRUE;
		size = sizeof(OgrFdwModifyState);
		break;
	default:
		elog(ERROR, "invalid state type");
	}

	state = palloc0(size);
	state->type = state_type;

	/*  Connect! */
	state->ogr = ogrGetConnectionFromTable(foreigntableid, updateable);
	state->foreigntableid = foreigntableid;

	return state;
}



/*
 * ogrGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
ogrGetForeignRelSize(PlannerInfo* root,
                     RelOptInfo* baserel,
                     Oid foreigntableid)
{
	/* Initialize the OGR connection */
	OgrFdwState* state = (OgrFdwState*)getOgrFdwState(foreigntableid, OGR_PLAN_STATE);
	OgrFdwPlanState* planstate = (OgrFdwPlanState*)state;
	List* scan_clauses = baserel->baserestrictinfo;

	elog(DEBUG3, "%s: entered function", __func__);

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
	if (scan_clauses == NIL &&
	        OGR_L_TestCapability(planstate->ogr.lyr, OLCFastFeatureCount) == TRUE &&
	        ogrCanReallyCountFast(&(planstate->ogr)))
	{
		/* Count rows, but don't force a slow count */
		int rows = OGR_L_GetFeatureCount(planstate->ogr.lyr, false);
		/* Only use row count if return is valid (>0) */
		if (rows >= 0)
		{
			planstate->nrows = rows;
			baserel->rows = rows;
		}
	}

	/* Save connection state for next calls */
	baserel->fdw_private = (void*) planstate;

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
ogrGetForeignPaths(PlannerInfo* root,
                   RelOptInfo* baserel,
                   Oid foreigntableid)
{
	OgrFdwPlanState* planstate = (OgrFdwPlanState*)(baserel->fdw_private);

	elog(DEBUG3, "%s: entered function", __func__);

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
	         (Path*) create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
	                 NULL, /* PathTarget */
#endif
	                 baserel->rows,
#if PG_VERSION_NUM >= 180000
	                 0,       /* disabled_nodes */
#endif
	                 planstate->startup_cost,
	                 planstate->total_cost,
	                 NIL,     /* no pathkeys */
	                 NULL,    /* no lateral_relids */
	                 NULL  /* no extra plan */
#if PG_VERSION_NUM >= 170000
	                 , NIL /* no fdw_restrictinfo list */
#endif
#if PG_VERSION_NUM >= 90500
	                 , NIL /* no fdw_private list */
#endif
	                                        )
	        );   /* no fdw_private data */
}

/*
 * Convert an OgrFdwSpatialFilter into a List so it can
 * be safely passed through the fdw_private list.
 */
static List*
ogrSpatialFilterToList(const OgrFdwSpatialFilter* spatial_filter)
{
	List *l = NIL;
	if (spatial_filter)
	{
		l = lappend(l, makeInteger(spatial_filter->ogrfldnum));
		l = lappend(l, makeFloat(psprintf("%.17g", spatial_filter->minx)));
		l = lappend(l, makeFloat(psprintf("%.17g", spatial_filter->miny)));
		l = lappend(l, makeFloat(psprintf("%.17g", spatial_filter->maxx)));
		l = lappend(l, makeFloat(psprintf("%.17g", spatial_filter->maxy)));
	}
	return l;
}

/*
 * Convert the List form back into an OgrFdwSpatialFilter
 * after passing through fdw_private.
 */
static OgrFdwSpatialFilter*
ogrSpatialFilterFromList(const List* lst)
{
	OgrFdwSpatialFilter* spatial_filter;

	if (lst == NIL)
		return NULL;

	Assert(list_length(lst) == 5);

	spatial_filter = palloc(sizeof(OgrFdwSpatialFilter));
	spatial_filter->ogrfldnum = intVal(linitial(lst));
	spatial_filter->minx = floatVal(lsecond(lst));
	spatial_filter->miny = floatVal(lthird(lst));
	spatial_filter->maxx = floatVal(lfourth(lst));
	spatial_filter->maxy = floatVal(list_nth(lst, 4)); /* list_nth counts from zero */
	return spatial_filter;
}

/*
 * fileGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan*
ogrGetForeignPlan(PlannerInfo* root,
                  RelOptInfo* baserel,
                  Oid foreigntableid,
                  ForeignPath* best_path,
                  List* tlist,
                  List* scan_clauses
#if PG_VERSION_NUM >= 90500
                  , Plan* outer_plan
#endif
                 )
{
	Index scan_relid = baserel->relid;
	bool sql_generated;
	StringInfoData sql;
	List* params_list = NULL;
	List* fdw_private;
	OgrFdwPlanState* planstate = (OgrFdwPlanState*)(baserel->fdw_private);
	OgrFdwState* state = (OgrFdwState*)(baserel->fdw_private);
	OgrFdwSpatialFilter* spatial_filter = NULL;
	char* attribute_filter = NULL;

	elog(DEBUG3, "%s: entered function", __func__);

	/* Add in column mapping data to build SQL with the right OGR column names */
	ogrReadColumnData(state);

	/*
	 * TODO: Review the columns requested (via params_list) and only pull those back, using
	 * OGR_L_SetIgnoredFields. This is less important than pushing restrictions
	 * down to OGR via OGR_L_SetAttributeFilter (done) and (TODO) OGR_L_SetSpatialFilter.
	 */
	initStringInfo(&sql);
	sql_generated = ogrDeparse(&sql, root, baserel, scan_clauses, state, &params_list, &spatial_filter);

	/* Extract the OGR SQL from the StringInfoData */
	if (sql_generated && sql.len > 0)
		attribute_filter = sql.data;

	/* Log filters at debug level one as necessary */
	if (attribute_filter)
		elog(DEBUG1, "OGR SQL: %s", attribute_filter);
	if (spatial_filter)
		elog(DEBUG1, "OGR spatial filter (%g %g, %g %g)",
		             spatial_filter->minx, spatial_filter->miny,
		             spatial_filter->maxx, spatial_filter->maxy);
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

	/* Pack the data we want to pass to the execution stage into a List. */
	/* The members of this list must by copyable by PgSQL, which means */
	/* they need to be Lists themselves, or Value nodes, otherwise when */
	/* the plan gets copied the copy might fail. */
	fdw_private = list_make3(makeString(attribute_filter), params_list, ogrSpatialFilterToList(spatial_filter));

	/* Clean up our connection */
	ogrFinishConnection(&(planstate->ogr));

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
	                        scan_clauses,
	                        scan_relid,
	                        NIL,	/* no expressions to evaluate */
	                        fdw_private
#if PG_VERSION_NUM >= 90500
	                        , NIL /* no scan_tlist */
	                        , NIL  /* no remote quals */
	                        , outer_plan
#endif
	                       );


}

static bool
pgCanConvertToOgr(Oid pg_type, OGRFieldType ogr_type)
{
	struct PgOgrMap
	{
		Oid pg;
		OGRFieldType ogr;
	};

	static struct PgOgrMap data[] = {
		{BOOLOID, OFTInteger}, /* 16 */
		{BYTEAOID, OFTBinary}, /* 17 */
		{CHAROID, OFTString},  /* 18 */
		{NAMEOID, OFTString},  /* 19 */
#if GDAL_VERSION_MAJOR >= 2
		{INT8OID, OFTInteger64}, /* 20 */
#else
		{INT8OID, OFTInteger}, /* 20 */
#endif
		{INT2OID, OFTInteger}, /* 21 */
		{INT4OID, OFTInteger}, /* 23 */
		{TEXTOID, OFTString},  /* 25 */
		{FLOAT4OID, OFTReal},  /* 700 */
		{FLOAT8OID, OFTReal},  /* 701 */
		{BOOLARRAYOID, OFTIntegerList}, /* 1000 */
		{CHARARRAYOID, OFTStringList}, /* 1002 */
		{NAMEARRAYOID, OFTStringList}, /* 1003 */
		{INT2ARRAYOID, OFTIntegerList}, /* 1005 */
		{INT4ARRAYOID, OFTIntegerList}, /* 1007 */
		{TEXTARRAYOID, OFTStringList}, /* 1009 */
		{VARCHARARRAYOID, OFTStringList}, /* 1015 */
#if GDAL_VERSION_MAJOR >= 2
		{INT8ARRAYOID, OFTInteger64List}, /* 1016 */
#endif
		{FLOAT4ARRAYOID, OFTRealList}, /* 1021 */
		{FLOAT8ARRAYOID, OFTRealList}, /* 1022 */
		{BPCHAROID, OFTString}, /* 1042 */
		{VARCHAROID, OFTString}, /* 1043 */
		{DATEOID, OFTDate}, /* 1082 */
		{TIMEOID, OFTTime}, /* 1083 */
		{TIMESTAMPOID, OFTDateTime}, /* 1114 */
		{NUMERICOID, OFTReal}, /* 1700 */
		{0, 0}};

	struct PgOgrMap* map = data;
	while (map->pg)
	{
		if (ogr_type == map->ogr) return true;
		else map++;
	}
	return false;

}


static void
pgCheckConvertToOgr(Oid pg_type, OGRFieldType ogr_type, const char *colname, const char *tblname)
{
	if (pgCanConvertToOgr(pg_type, ogr_type))
		return;

	ereport(ERROR, (
	    errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
	    errmsg("column \"%s\" of foreign table \"%s\" converts \"%s\" to OGR \"%s\"",
	        colname, tblname,
	        format_type_be(pg_type), OGR_GetFieldTypeName(ogr_type))
	    ));
}


static bool
ogrCanConvertToPg(OGRFieldType ogr_type, Oid pg_type)
{
	struct OgrPgMap
	{
		OGRFieldType ogr;
		Oid pg[16];
	};

	static struct OgrPgMap data[] =
	{
		{OFTInteger, {BOOLOID, INT4OID, INT8OID, NUMERICOID, FLOAT4OID, FLOAT8OID, TEXTOID, VARCHAROID, 0}},
		{OFTReal, {NUMERICOID, FLOAT4OID, FLOAT8OID, TEXTOID, VARCHAROID, 0}},
		{OFTBinary, {BYTEAOID, 0}},
		{OFTString, {TEXTOID, VARCHAROID, CHAROID, BPCHAROID, JSONBOID, JSONOID, 0}},
		{OFTDate, {DATEOID, TIMESTAMPOID, TEXTOID, VARCHAROID, 0}},
		{OFTTime, {TIMEOID, TEXTOID, VARCHAROID, 0}},
		{OFTDateTime, {TIMESTAMPOID, TEXTOID, VARCHAROID, 0}},
	#if GDAL_VERSION_MAJOR >= 2
		{OFTInteger64, {INT8OID, NUMERICOID, FLOAT8OID, TEXTOID, VARCHAROID, 0}},
		{OFTInteger64List, {INT8ARRAYOID, FLOAT8ARRAYOID, TEXTARRAYOID, VARCHARARRAYOID, 0}},
	#endif
		{OFTRealList, {FLOAT4ARRAYOID, FLOAT8ARRAYOID, TEXTARRAYOID, VARCHARARRAYOID, 0}},
		{OFTStringList, {TEXTARRAYOID, VARCHARARRAYOID, NAMEARRAYOID, CHARARRAYOID, 0}},
		{OFTIntegerList, {BOOLARRAYOID, INT2ARRAYOID, INT4ARRAYOID, INT8ARRAYOID, TEXTARRAYOID, VARCHARARRAYOID, 0}},
		{256, {0}} /* Zero terminate list */
	};

	struct OgrPgMap* map = data;
	while (map->ogr <= OFTMaxType)
	{
		if (ogr_type == map->ogr)
		{
			Oid *typ = map->pg;
			while (*typ)
			{
				if (pg_type == *typ) return true;
				else typ++;
			}
		}
		map++;
	}

	return false;
}

static void
ogrCheckConvertToPg(OGRFieldType ogr_type, Oid pg_type, const char *colname, const char *tblname)
{
	if (ogrCanConvertToPg(ogr_type, pg_type))
		return;

	if (ogr_type == OFTWideString || ogr_type == OFTWideStringList)
	{
		ereport(ERROR, (
		    errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
		    errmsg("column \"%s\" of foreign table \"%s\" uses an OGR OFTWideString, deprecated", colname, tblname)
		    ));
		return;
	}

	ereport(ERROR, (
	    errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
	    errmsg("column \"%s\" of foreign table \"%s\" converts OGR \"%s\" to \"%s\"",
	           colname, tblname,
	           OGR_GetFieldTypeName(ogr_type), format_type_be(pg_type))
	    ));
}


#ifdef OGR_FDW_HEXWKB

static char* hexchr = "0123456789ABCDEF";

static char*
ogrBytesToHex(unsigned char* bytes, size_t size)
{
	char* hex;
	int i;
	if (! bytes || ! size)
	{
		elog(ERROR, "ogrBytesToHex: invalid input");
		return NULL;
	}
	hex = palloc(size * 2 + 1);
	hex[2 * size] = '\0';
	for (i = 0; i < size; i++)
	{
		/* Top four bits to 0-F */
		hex[2 * i] = hexchr[bytes[i] >> 4];
		/* Bottom four bits to 0-F */
		hex[2 * i + 1] = hexchr[bytes[i] & 0x0F];
	}
	return hex;
}

#endif

static void
freeOgrFdwTable(OgrFdwTable* table)
{
	if (table)
	{
		if (table->tblname)
		{
			pfree(table->tblname);
		}
		if (table->cols)
		{
			pfree(table->cols);
		}
		pfree(table);
	}
}

typedef struct
{
	char* fldname;
	int fldnum;
} OgrFieldEntry;

static int
ogrFieldEntryCmpFunc(const void* a, const void* b)
{
	const char* a_name = ((OgrFieldEntry*)a)->fldname;
	const char* b_name = ((OgrFieldEntry*)b)->fldname;

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
ogrReadColumnData(OgrFdwState* state)
{
	Relation rel;
	TupleDesc tupdesc;
	int i;
	OgrFdwTable* tbl;
	OGRFeatureDefnH dfn;
	int ogr_ncols;
	int fid_count = 0;
	int geom_count = 0;
	int ogr_geom_count = 0;
	int field_count = 0;
	OgrFieldEntry* ogr_fields;
	int ogr_fields_count = 0;
	char* tblname = get_rel_name(state->foreigntableid);

	/* Blow away any existing table in the state */
	if (state->table)
	{
		freeOgrFdwTable(state->table);
		state->table = NULL;
	}

	/* Fresh table */
	tbl = palloc0(sizeof(OgrFdwTable));

	/* One column for each PgSQL foreign table column */
#if PG_VERSION_NUM < 120000
	rel = heap_open(state->foreigntableid, NoLock);
#else
	rel = table_open(state->foreigntableid, NoLock);
#endif /* PG_VERSION_NUM */

	tupdesc = rel->rd_att;
	state->tupdesc = tupdesc;
	tbl->ncols = tupdesc->natts;
	tbl->cols = palloc0(tbl->ncols * sizeof(OgrFdwColumn));
	tbl->tblname = pstrdup(tblname);

	/* Get OGR metadata ready */
	dfn = OGR_L_GetLayerDefn(state->ogr.lyr);
	ogr_ncols = OGR_FD_GetFieldCount(dfn);
#if (GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(1,11,0))
	ogr_geom_count = OGR_FD_GetGeomFieldCount(dfn);
#else
	ogr_geom_count = (OGR_FD_GetGeomType(dfn) != wkbNone) ? 1 : 0;
#endif


	/* Prepare sorted list of OGR column names */
	/* TODO: change this to a hash table, to avoid repeated strcmp */
	/* We will search both the original and laundered OGR field names for matches */
	ogr_fields_count = 2 * ogr_ncols;
	ogr_fields = palloc0(ogr_fields_count * sizeof(OgrFieldEntry));
	for (i = 0; i < ogr_ncols; i++)
	{
		char* fldname = pstrdup(OGR_Fld_GetNameRef(OGR_FD_GetFieldDefn(dfn, i)));
		char* fldname_laundered = palloc(STR_MAX_LEN);
		strncpy(fldname_laundered, fldname, STR_MAX_LEN);
		ogrStringLaunder(fldname_laundered);
		ogr_fields[2 * i].fldname = fldname;
		ogr_fields[2 * i].fldnum = i;
		ogr_fields[2 * i + 1].fldname = fldname_laundered;
		ogr_fields[2 * i + 1].fldnum = i;
	}
	qsort(ogr_fields, ogr_fields_count, sizeof(OgrFieldEntry), ogrFieldEntryCmpFunc);


	/* loop through foreign table columns */
	for (i = 0; i < tbl->ncols; i++)
	{
		List* options;
		ListCell* lc;
		OgrFieldEntry* found_entry;
		OgrFieldEntry entry;

		Form_pg_attribute att_tuple = TupleDescAttr(tupdesc, i);

		OgrFdwColumn col = tbl->cols[i];
		col.pgattnum = att_tuple->attnum;
		col.pgtype = att_tuple->atttypid;
		col.pgtypmod = att_tuple->atttypmod;
		col.pgattisdropped = att_tuple->attisdropped;

		/* Skip filling in any further metadata about dropped columns */
		if (col.pgattisdropped)
		{
			continue;
		}

		/* Check for array type */
		col.pgelmtype = get_element_type(col.pgtype);
		if (col.pgelmtype)
		{
			/* Extra type info needed to form the array */
			col.pgisarray = true;
		}
		else
			col.pgelmtype = col.pgtype;

		/* Find the appropriate conversion functions */
		getTypeInputInfo(col.pgelmtype, &col.pginputfunc, &col.pginputioparam);
		getTypeBinaryInputInfo(col.pgelmtype, &col.pgrecvfunc, &col.pgrecvioparam);
		getTypeOutputInfo(col.pgelmtype, &col.pgoutputfunc, &col.pgoutputvarlena);
		getTypeBinaryOutputInfo(col.pgelmtype, &col.pgsendfunc, &col.pgsendvarlena);

		/* Get the PgSQL column name */
#if PG_VERSION_NUM >= 110000
		col.pgname = pstrdup(get_attname(rel->rd_id, att_tuple->attnum, false));
#else
		col.pgname = pstrdup(get_attname(rel->rd_id, att_tuple->attnum));
#endif

		/* Handle FID first */
		if (strcaseeq(col.pgname, "fid") &&
		        (col.pgtype == INT4OID || col.pgtype == INT8OID))
		{
			if (fid_count >= 1)
			{
				elog(ERROR, "FDW table '%s' includes more than one FID column", tblname);
			}

			col.ogrvariant = OGR_FID;
			col.ogrfldnum = fid_count++;
			tbl->cols[i] = col;
			continue;
		}

		/* If the OGR source has geometries, can we match them to Pg columns? */
		/* We'll match to the first ones we find, irrespective of name */
		if (geom_count < ogr_geom_count && col.pgtype == ogrGetGeometryOid())
		{
			col.ogrvariant = OGR_GEOMETRY;
			col.ogrfldtype = OFTBinary;
			col.ogrfldnum = geom_count++;
			tbl->cols[i] = col;
			continue;
		}

		/* Now we search for matches in the OGR fields */

		/* By default, search for the PgSQL column name */
		entry.fldname = col.pgname;
		entry.fldnum = 0;

		/*
		 * But, if there is a 'column_name' option for this column, we
		 * want to search for *that* in the OGR layer.
		 */
		options = GetForeignColumnOptions(state->foreigntableid, i + 1);
		foreach (lc, options)
		{
			DefElem*    def = (DefElem*) lfirst(lc);

			if (streq(def->defname, OPT_COLUMN))
			{
				entry.fldname = defGetString(def);
				break;
			}
		}

		/* Search PgSQL column name in the OGR column name list */
		found_entry = bsearch(&entry, ogr_fields, ogr_fields_count, sizeof(OgrFieldEntry), ogrFieldEntryCmpFunc);

		/* Column name matched, so save this entry, if the types are consistent */
		if (found_entry)
		{
			OGRFieldDefnH fld = OGR_FD_GetFieldDefn(dfn, found_entry->fldnum);
			OGRFieldType fldtype = OGR_Fld_GetType(fld);

			/* Error if types mismatched when column names match */
			ogrCheckConvertToPg(fldtype, col.pgtype, col.pgname, tblname);

			col.ogrvariant = OGR_FIELD;
			col.ogrfldnum = found_entry->fldnum;
			col.ogrfldtype = fldtype;
			field_count++;
		}
		else
		{
			col.ogrvariant = OGR_UNMATCHED;
		}
		tbl->cols[i] = col;
	}

	elog(DEBUG2, "ogrReadColumnData matched %d FID, %d GEOM, %d FIELDS out of %d PGSQL COLUMNS", fid_count, geom_count,
	     field_count, tbl->ncols);

	/* Clean up */

	state->table = tbl;
	for (i = 0; i < 2 * ogr_ncols; i++)
	{
		if (ogr_fields[i].fldname)
		{
			pfree(ogr_fields[i].fldname);
		}
	}
	pfree(ogr_fields);
#if PG_VERSION_NUM < 120000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif /* PG_VERSION_NUM */


	return;
}


/*
 * ogrLookupGeometryFunctionOid
 *
 * Find the procedure Oids of useful functions so we can call
 * them later. In the case where multiple functions have the
 * same signature, in different namespaces (???) we might have
 * problems, but that seems very unlikely.
 *
 * https://github.com/pramsey/pgsql-ogr-fdw/issues/266
 */
static Oid
ogrLookupGeometryFunctionOid(const char* proname)
{
	CatCList   *clist;
	int			i;
	Oid result = InvalidOid;

	/* This only works if PostGIS is installed */
	if (ogrGetGeometryOid() == InvalidOid ||
	    ogrGetGeometryOid() == BYTEAOID)
	{
		return result;
	}

	if (!proname) return result;

	/* Search syscache by name only */
	clist = SearchSysCacheList1(PROCNAMEARGSNSP,
	                            CStringGetDatum(proname));

	if (!clist) return InvalidOid;

	for (i = 0; i < clist->n_members; i++)
	{
		HeapTuple    proctup = &clist->members[i]->tuple;
		Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
		Oid         *proargtypes = procform->proargtypes.values;
		int          pronargs = procform->pronargs;

#if PG_VERSION_NUM >= 120000
		Oid procoid = procform->oid;
#else
		Oid procoid = HeapTupleGetOid(proctup);
#endif

		/* ST_SetSRID(geometry, integer) */
		if (streq(proname, "st_setsrid") &&
		    pronargs == 2 &&
		    proargtypes[0] == ogrGetGeometryOid())
		{
			result = procoid;
			break;
		}
		/* postgis_typmod_srid(typmod) */
		else if (streq(proname, "postgis_typmod_srid") &&
		         pronargs == 1)
		{
			/* postgis_typmod_srid(integer) */
			result = procoid;
			break;
		}
		else
		{
			elog(ERROR, "%s could not find function '%s'",
			     __func__, proname);
		}
	}

	ReleaseSysCacheList(clist);
	return result;
}

/*
 * ogrBeginForeignScan
 */
static void
ogrBeginForeignScan(ForeignScanState* node, int eflags)
{
	OgrFdwState* state;
	OgrFdwExecState* execstate;
	OgrFdwSpatialFilter* spatial_filter;
	Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	ForeignScan* fsplan = (ForeignScan*)node->ss.ps.plan;

	elog(DEBUG3, "%s: entered function", __func__);

	/* Do nothing in EXPLAIN (no ANALYZE) case */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Initialize OGR connection */
	state = getOgrFdwState(foreigntableid, OGR_EXEC_STATE);
	execstate = (OgrFdwExecState*)state;

	/* Read the OGR layer definition and PgSQL foreign table definitions */
	ogrReadColumnData(state);

	/* Collect the procedure Oids for PostGIS functions we might need */
	execstate->setsridfunc = ogrLookupGeometryFunctionOid("st_setsrid");
	execstate->typmodsridfunc = ogrLookupGeometryFunctionOid("postgis_typmod_srid");

	/* Get OGR SQL generated by the deparse step during the planner function. */
	execstate->sql = (char*) strVal(list_nth(fsplan->fdw_private, 0));

	/* TODO: Use the parse step attribute list to restrict requested columns */
	// execstate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);

	/* Get spatial filter generated by the deparse step. */
	spatial_filter = ogrSpatialFilterFromList(list_nth(fsplan->fdw_private, 2));

	if (spatial_filter)
	{
		OGR_L_SetSpatialFilterRectEx(execstate->ogr.lyr,
		                             spatial_filter->ogrfldnum,
		                             spatial_filter->minx,
		                             spatial_filter->miny,
		                             spatial_filter->maxx,
		                             spatial_filter->maxy
		                             );
	}

	if (execstate->sql && strlen(execstate->sql) > 0)
	{
		OGRErr err = OGR_L_SetAttributeFilter(execstate->ogr.lyr, execstate->sql);
		if (err != OGRERR_NONE)
		{
			const char* ogrerr = CPLGetLastErrorMsg();

			if (ogrerr && ! streq(ogrerr, ""))
			{
				ereport(NOTICE,
				        (errcode(ERRCODE_FDW_ERROR),
				         errmsg("unable to set OGR SQL '%s' on layer", execstate->sql),
				         errhint("%s", ogrerr)));
			}
			else
			{
				ereport(NOTICE,
				        (errcode(ERRCODE_FDW_ERROR),
				         errmsg("unable to set OGR SQL '%s' on layer", execstate->sql)));
			}
		}
	}
	else
	{
		OGR_L_SetAttributeFilter(execstate->ogr.lyr, NULL);
	}

	/* Save the state for the next call */
	node->fdw_state = (void*) execstate;

	return;
}

/*
 * Rather than explicitly try and form PgSQL datums, use the type
 * input functions, that accept cstring representations, and convert
 * to the input format. We have to lookup the right input function for
 * each column in the foreign table.
 */
static Datum
pgDatumFromCString(const char* cstr, const OgrFdwColumn *col, int char_encoding, bool *is_null)
{
	size_t cstr_len = cstr ? strlen(cstr) : 0;
	Datum value = (Datum) 0;
	char *cstr_decoded;

	/* Zero length implies NULL for all non-strings */
	if (cstr_len == 0 && (col->ogrfldtype != OFTString && col->ogrfldtype != OFTStringList))
	{
		*is_null = true;
		return value;
	}

	cstr_decoded = char_encoding ?
			pg_any_to_server(cstr, cstr_len, char_encoding) :
			pstrdup(cstr);

	value = OidFunctionCall3(col->pginputfunc,
			    CStringGetDatum(cstr_decoded),
			    ObjectIdGetDatum(InvalidOid),
			    Int32GetDatum(col->pgtypmod));

	/* Free cstr_decoded if it is a copy */
	if (cstr != cstr_decoded)
		pfree(cstr_decoded);

	is_null = false;
	return value;
}



static inline void
ogrNullSlot(Datum* values, bool* nulls, int i)
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
ogrFeatureToSlot(const OGRFeatureH feat, TupleTableSlot* slot, const OgrFdwExecState* execstate)
{
	const OgrFdwTable* tbl = execstate->table;
	int i;
	Datum* values = slot->tts_values;
	bool* nulls = slot->tts_isnull;
	TupleDesc tupdesc = slot->tts_tupleDescriptor;
	int have_typmod_funcs = (execstate->setsridfunc && execstate->typmodsridfunc);

#define CSTR_SZ 256
	char cstr[CSTR_SZ];

	/* Check our assumption that slot and setup data match */
	if (tbl->ncols != tupdesc->natts)
	{
		elog(ERROR, "FDW metadata table and exec table have mismatching number of columns");
		return OGRERR_FAILURE;
	}

	/* For each pgtable column, get a value from OGR */
	for (i = 0; i < tbl->ncols; i++)
	{
		OgrFdwColumn col = tbl->cols[i];
		const char* pgname = col.pgname;
		Oid pgtype = col.pgtype;
		int ogrfldnum = col.ogrfldnum;
		OGRFieldType ogrfldtype = col.ogrfldtype;
		OgrColumnVariant ogrvariant = col.ogrvariant;

		/*
		 * Fill in dropped attributes with NULL
		 */
		if (col.pgattisdropped)
		{
			ogrNullSlot(values, nulls, i);
			continue;
		}

		if (ogrvariant == OGR_FID)
		{
			GIntBig fid = OGR_F_GetFID(feat);
			bool is_null = false;

			if (fid == OGRNullFID)
			{
				ogrNullSlot(values, nulls, i);
			}
			else
			{
				char fidstr[256];
				snprintf(fidstr, 256, OGR_FDW_FRMT_INT64, OGR_FDW_CAST_INT64(fid));

				values[i] = pgDatumFromCString(fidstr, &col, execstate->ogr.char_encoding, &is_null);
				nulls[i] = is_null;
			}
		}
		else if (ogrvariant == OGR_GEOMETRY)
		{
			int wkbsize;
			int varsize;
			bytea* varlena;
			unsigned char* wkb;
			OGRErr err;

#if (GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(1,11,0))
			OGRGeometryH geom = OGR_F_GetGeomFieldRef(feat, ogrfldnum);
#else
			OGRGeometryH geom = OGR_F_GetGeometryRef(feat);
#endif

			/* No geometry ? NULL */
			if (! geom)
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
			wkb = (unsigned char*)VARDATA(varlena);
			err = OGR_G_ExportToWkb(geom, wkbNDR, wkb);
			SET_VARSIZE(varlena, varsize);

			/* Couldn't create WKB from OGR geometry? error */
			if (err != OGRERR_NONE)
			{
				return err;
			}

			if (pgtype == BYTEAOID)
			{
				/*
				 * Nothing special to do for bytea, just send the varlena data through!
				 */
				nulls[i] = false;
				values[i] = PointerGetDatum(varlena);
			}
			else if (pgtype == ogrGetGeometryOid())
			{
				/*
				 * For geometry we need to convert the varlena WKB data into a serialized
				 * geometry (aka "gserialized"). For that, we can use the type's "recv" function
				 * which takes in WKB and spits out serialized form, or the "input" function
				 * that takes in HEXWKB. The "input" function is more lax about geometry
				 * structure errors (unclosed polys, etc).
				 */
#ifdef OGR_FDW_HEXWKB
				char* hexwkb = ogrBytesToHex(wkb, wkbsize);
				/*
				 * Use the input function to convert the WKB from OGR into
				 * a PostGIS internal format.
				 */
				nulls[i] = false;
				values[i] = OidFunctionCall1(col.pginputfunc, PointerGetDatum(hexwkb));
				pfree(hexwkb);
#else
				/*
				 * The "recv" function expects to receive a StringInfo pointer
				 * on the first argument, so we form one of those ourselves by
				 * hand. Rather than copy into a fresh buffer, we'll just use the
				 * existing varlena buffer and point to the data area.
				 *
				 * The "recv" function tests for basic geometry validity,
				 * things like polygon closure, etc. So don't feed it junk.
				 */
				StringInfoData strinfo;
				strinfo.data = (char*)wkb;
				strinfo.len = wkbsize;
				strinfo.maxlen = strinfo.len;
				strinfo.cursor = 0;

				/*
				 * Use the recv function to convert the WKB from OGR into
				 * a PostGIS internal format.
				 */
				nulls[i] = false;
				values[i] = OidFunctionCall1(col.pgrecvfunc, PointerGetDatum(&strinfo));
#endif

				/*
				 * Apply the typmod restriction to the incoming geometry, so it's
				 * not really a restriction anymore, it's more like a requirement.
				 *
				 * TODO: In the case where the OGR input actually *knows* what SRID
				 * it is, we should actually apply *that* and let the restriction run
				 * its usual course.
				 */
				if (have_typmod_funcs && col.pgtypmod >= 0)
				{
					Datum srid = OidFunctionCall1(execstate->typmodsridfunc, Int32GetDatum(col.pgtypmod));
					values[i] = OidFunctionCall2(execstate->setsridfunc, values[i], srid);
				}
			}
			else
			{
				elog(NOTICE, "conversion to geometry called with column type not equal to bytea or geometry");
				ogrNullSlot(values, nulls, i);
			}

		}
		else if (ogrvariant == OGR_FIELD)
		{
#if (GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(2,2,0))
			int field_not_null = OGR_F_IsFieldSet(feat, ogrfldnum) && ! OGR_F_IsFieldNull(feat, ogrfldnum);
#else
			int field_not_null = OGR_F_IsFieldSet(feat, ogrfldnum);
#endif

			/* Ensure that the OGR data type fits the destination Pg column */
			ogrCheckConvertToPg(ogrfldtype, pgtype, pgname, tbl->tblname);

			/* Only convert non-null fields */
			if (!field_not_null)
			{
				ogrNullSlot(values, nulls, i);
				continue;
			}

			switch (ogrfldtype)
			{
			case OFTBinary:
			{
				/*
				 * Convert binary fields to bytea directly
				 */
				int bufsize;
				GByte* buf = OGR_F_GetFieldAsBinary(feat, ogrfldnum, &bufsize);
				int varsize = bufsize + VARHDRSZ;
				bytea* varlena = palloc(varsize);
				memcpy(VARDATA(varlena), buf, bufsize);
				SET_VARSIZE(varlena, varsize);
				nulls[i] = false;
				values[i] = PointerGetDatum(varlena);
				break;
			}
			case OFTInteger:
			case OFTReal:
			case OFTString:
#if GDAL_VERSION_MAJOR >= 2
			case OFTInteger64:
#endif
			{
				/*
				 * Convert numbers and strings via a string representation.
				 * Handling numbers directly would be faster, but require a lot of extra code.
				 * For now, we go via text.
				 */
				const char* cstr_in = OGR_F_GetFieldAsString(feat, ogrfldnum);
				bool is_null = false;
				values[i] = pgDatumFromCString(cstr_in, &col, execstate->ogr.char_encoding, &is_null);
				nulls[i] = is_null;
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
				bool is_null = false;
				OGR_F_GetFieldAsDateTime(feat, ogrfldnum,
				                         &year, &month, &day,
				                         &hour, &minute, &second, &tz);

				if (ogrfldtype == OFTDate)
				{
					snprintf(cstr, CSTR_SZ, "%d-%02d-%02d", year, month, day);
				}
				else if (ogrfldtype == OFTTime)
				{
					snprintf(cstr, CSTR_SZ, "%02d:%02d:%02d", hour, minute, second);
				}
				else
				{
#if (GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(3,7,0))
					const char* tsstr = OGR_F_GetFieldAsISO8601DateTime(feat, ogrfldnum, NULL);
					strncpy(cstr, tsstr, CSTR_SZ);
#else
					snprintf(cstr, CSTR_SZ, "%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
#endif
				}
				values[i] = pgDatumFromCString(cstr, &col, PG_SQL_ASCII, &is_null);
				nulls[i] = is_null;
				break;

			}

#if GDAL_VERSION_MAJOR >= 2
			case OFTInteger64List:
			{
				int ilist_size;
				const int64 *ilist = (int64*)OGR_F_GetFieldAsInteger64List(feat, ogrfldnum, &ilist_size);
				ArrayBuildState *abs = initArrayResult(col.pgelmtype, CurrentMemoryContext, false);
				for (uint32 i = 0; i < ilist_size; i++)
				{
					bool is_null = false;
					snprintf(cstr, CSTR_SZ, OGR_FDW_FRMT_INT64, OGR_FDW_CAST_INT64(ilist[i]));
					abs = accumArrayResult(abs,
					          pgDatumFromCString(cstr, &col, execstate->ogr.char_encoding, &is_null),
					          is_null,
					          col.pgelmtype,
					          CurrentMemoryContext);
				}
				values[i] = makeArrayResult(abs, CurrentMemoryContext);
				nulls[i] = false;
				break;
			}
#endif

			case OFTIntegerList:
			{
				int ilist_size;
				const int *ilist = OGR_F_GetFieldAsIntegerList(feat, ogrfldnum, &ilist_size);
				ArrayBuildState *abs = initArrayResult(col.pgelmtype, CurrentMemoryContext, false);
				for (uint32 i = 0; i < ilist_size; i++)
				{
					bool is_null = false;
					snprintf(cstr, CSTR_SZ, "%d", ilist[i]);
					abs = accumArrayResult(abs,
					          pgDatumFromCString(cstr, &col, execstate->ogr.char_encoding, &is_null),
					          is_null,
					          col.pgelmtype,
					          CurrentMemoryContext);
				}
				values[i] = makeArrayResult(abs, CurrentMemoryContext);
				nulls[i] = false;
				break;
			}

			case OFTRealList:
			{
				int rlist_size;
				const double *rlist = OGR_F_GetFieldAsDoubleList(feat, ogrfldnum, &rlist_size);
				ArrayBuildState *abs = initArrayResult(col.pgelmtype, CurrentMemoryContext, false);
				for (uint32 i = 0; i < rlist_size; i++)
				{
					bool is_null = false;
					snprintf(cstr, CSTR_SZ, "%g", rlist[i]);
					abs = accumArrayResult(abs,
					          pgDatumFromCString(cstr, &col, execstate->ogr.char_encoding, &is_null),
					          is_null,
					          col.pgelmtype,
					          CurrentMemoryContext);
				}
				values[i] = makeArrayResult(abs, CurrentMemoryContext);
				nulls[i] = false;
				break;
			}

			case OFTStringList:
			{
				ArrayBuildState *abs = initArrayResult(col.pgelmtype, CurrentMemoryContext, false);
				char **cstrs = OGR_F_GetFieldAsStringList(feat, ogrfldnum);
				while (*cstrs)
				{
					bool is_null = false;
					abs = accumArrayResult(abs,
					          pgDatumFromCString(*cstrs, &col, execstate->ogr.char_encoding, &is_null),
					          is_null,
					          col.pgelmtype,
					          CurrentMemoryContext);
					cstrs++;
				}
				values[i] = makeArrayResult(abs, CurrentMemoryContext);
				nulls[i] = false;
				break;
			}
			default:
			{
				elog(ERROR, "unsupported OGR type \"%s\"", OGR_GetFieldTypeName(ogrfldtype));
				break;
			}

			} /* !switch */
		}
		/* Fill in unmatched columns with NULL */
		else if (ogrvariant == OGR_UNMATCHED)
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

static void
ogrStaticText(char* text, const char* str)
{
	size_t len = strlen(str);
	memcpy(VARDATA(text), str, len);
	SET_VARSIZE(text, len + VARHDRSZ);
	return;
}

/*
 * EWKB includes a flag that indicates an SRID embedded in the
 * binary. The EWKB has an endian byte, four bytes of type information
 * and then 4 bytes of optional SRID information. If that info is
 * there, we want to over-write it, and remove the SRID flag, to
 * generate more "standard" WKB for OGR to consume.
 */
static size_t
ogrEwkbStripSrid(unsigned char* wkb, size_t wkbsize)
{
	unsigned int type = 0;
	int has_srid = 0;
	size_t newwkbsize = wkbsize;
	memcpy(&type, wkb + 1, 4);
	/* has_z = type & 0x80000000; */
	/* has_m = type & 0x40000000; */
	has_srid = type & 0x20000000;

	/* Flatten SRID flag away */
	type &= 0xDFFFFFFF;
	memcpy(wkb + 1, &type, 4);

	/* If there was an SRID number embedded, overwrite it */
	if (has_srid)
	{
		newwkbsize -= 4; /* no space for SRID number needed */
		memmove(wkb + 5, wkb + 9, newwkbsize - 5);
	}

	return newwkbsize;
}

OGRErr
pgDatumToOgrGeometry (Datum pg_geometry, Oid pgsendfunc, OGRGeometryH* ogr_geometry)
{
	OGRErr err;
	bytea* wkb_bytea = DatumGetByteaP(OidFunctionCall1(pgsendfunc, pg_geometry));
	unsigned char* wkb = (unsigned char*)VARDATA_ANY(wkb_bytea);
	size_t wkbsize = VARSIZE_ANY_EXHDR(wkb_bytea);
	wkbsize = ogrEwkbStripSrid(wkb, wkbsize);
	err = OGR_G_CreateFromWkb(wkb, NULL, ogr_geometry, wkbsize);
	if (wkb_bytea)
		pfree(wkb_bytea);
	return err;
}

static OGRErr
ogrSlotToFeature(const TupleTableSlot* slot, OGRFeatureH feat, const OgrFdwTable* tbl)
{
	int i;
	Datum* values = slot->tts_values;
	bool* nulls = slot->tts_isnull;
	TupleDesc tupdesc = slot->tts_tupleDescriptor;

	int year, month, day, hour, minute, second;

	/* Prepare date-time part tokens for use later */
	char txtyear[STR_MAX_LEN];
	char txtmonth[STR_MAX_LEN];
	char txtday[STR_MAX_LEN];
	char txthour[STR_MAX_LEN];
	char txtminute[STR_MAX_LEN];
	char txtsecond[STR_MAX_LEN];

	ogrStaticText(txtyear, "year");
	ogrStaticText(txtmonth, "month");
	ogrStaticText(txtday, "day");
	ogrStaticText(txthour, "hour");
	ogrStaticText(txtminute, "minute");
	ogrStaticText(txtsecond, "second");

	/* Check our assumption that slot and setup data match */
	if (tbl->ncols != tupdesc->natts)
	{
		elog(ERROR, "FDW metadata table and slot table have mismatching number of columns");
		return OGRERR_FAILURE;
	}

	/* For each pgtable column, set a value on the feature OGR */
	for (i = 0; i < tbl->ncols; i++)
	{
		OgrFdwColumn col = tbl->cols[i];
		const char* pgname = col.pgname;
		Oid pgtype = col.pgtype;
		Oid pgoutputfunc = col.pgoutputfunc;
		int ogrfldnum = col.ogrfldnum;
		OGRFieldType ogrfldtype = col.ogrfldtype;
		OgrColumnVariant ogrvariant = col.ogrvariant;

		/* Skip dropped attributes */
		if (col.pgattisdropped)
		{
			continue;
		}

		/* Skip the FID, we have to treat it as immutable anyways */
		if (ogrvariant == OGR_FID)
		{
			if (nulls[i])
			{
				OGR_F_SetFID(feat, OGRNullFID);
			}
			else
			{
				if (pgtype == INT4OID)
				{
					int32 val = DatumGetInt32(values[i]);
					OGR_F_SetFID(feat, val);
				}
				else if (pgtype == INT8OID)
				{
					int64 val = DatumGetInt64(values[i]);
					OGR_F_SetFID(feat, val);
				}
				else
				{
					elog(ERROR, "unable to handle non-integer fid");
				}
			}
			continue;
		}

		/* TODO: For updates, we should only set the fields that are */
		/*       in the target list, and flag the others as unchanged */
		if (ogrvariant == OGR_GEOMETRY)
		{
			OGRErr err;
			if (nulls[i])
			{
#if (GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(1,11,0))
				err = OGR_F_SetGeomFieldDirectly(feat, ogrfldnum, NULL);
#else
				err = OGR_F_SetGeometryDirectly(feat, NULL);
#endif
				continue;
			}
			else
			{
				OGRGeometryH geom;
				err = pgDatumToOgrGeometry (values[i], col.pgsendfunc, &geom);
				if (err != OGRERR_NONE)
					return err;

#if (GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(1,11,0))
				err = OGR_F_SetGeomFieldDirectly(feat, ogrfldnum, geom);
#else
				err = OGR_F_SetGeometryDirectly(feat, geom);
#endif
			}
		}
		else if (ogrvariant == OGR_FIELD)
		{
			/* Ensure that the OGR data type fits the destination Pg column */
			pgCheckConvertToOgr(pgtype, ogrfldtype, pgname, tbl->tblname);

			/* Skip NULL case */
			if (nulls[i])
			{
				OGR_F_UnsetField(feat, ogrfldnum);
				continue;
			}

			switch (pgtype)
			{
			case BOOLOID:
			{
				int8 val = DatumGetBool(values[i]);
				OGR_F_SetFieldInteger(feat, ogrfldnum, val);
				break;
			}
			case INT2OID:
			{
				int16 val = DatumGetInt16(values[i]);
				OGR_F_SetFieldInteger(feat, ogrfldnum, val);
				break;
			}
			case INT4OID:
			{
				int32 val = DatumGetInt32(values[i]);
				OGR_F_SetFieldInteger(feat, ogrfldnum, val);
				break;
			}
			case INT8OID:
			{
				int64 val = DatumGetInt64(values[i]);
#if GDAL_VERSION_MAJOR >= 2
				OGR_F_SetFieldInteger64(feat, ogrfldnum, val);
#else
				if (val < INT_MAX)
				{
					OGR_F_SetFieldInteger(feat, ogrfldnum, (int32)val);
				}
				else
				{
					elog(ERROR, "unable to coerce int64 into int32 OGR field");
				}
#endif
				break;

			}

			case NUMERICOID:
			{
				Datum d;
				float8 f;

				/* Convert to string */
				d = OidFunctionCall1(pgoutputfunc, values[i]);
				/* Convert back to float8 */
				f = DatumGetFloat8(DirectFunctionCall1(float8in, d));

				OGR_F_SetFieldDouble(feat, ogrfldnum, f);
				break;
			}
			case FLOAT4OID:
			{
				OGR_F_SetFieldDouble(feat, ogrfldnum, DatumGetFloat4(values[i]));
				break;
			}
			case FLOAT8OID:
			{
				OGR_F_SetFieldDouble(feat, ogrfldnum, DatumGetFloat8(values[i]));
				break;
			}

			case TEXTOID:
			case VARCHAROID:
			case NAMEOID:
			case BPCHAROID: /* char(n) */
			{
				bytea* varlena = (bytea*)DatumGetPointer(values[i]);
				size_t varsize = VARSIZE_ANY_EXHDR(varlena);
				char* str = palloc0(varsize + 1);
				memcpy(str, VARDATA_ANY(varlena), varsize);
				OGR_F_SetFieldString(feat, ogrfldnum, str);
				pfree(str);
				break;
			}

			case CHAROID: /* char */
			{
				char str[2];
				str[0] = DatumGetChar(values[i]);
				str[1] = '\0';
				OGR_F_SetFieldString(feat, ogrfldnum, str);
				break;
			}

			case BYTEAOID:
			{
				bytea* varlena = PG_DETOAST_DATUM(values[i]);
				size_t varsize = VARSIZE_ANY_EXHDR(varlena);
				OGR_F_SetFieldBinary(feat, ogrfldnum, varsize, (GByte*)VARDATA_ANY(varlena));
				break;
			}

			case DATEOID:
			{
				/* Convert date to timestamp */
				Datum d = DirectFunctionCall1(date_timestamp, values[i]);

				/* Read out the parts */
				year = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtyear), d)));
				month = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtmonth), d)));
				day = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtday), d)));
				OGR_F_SetFieldDateTime(feat, ogrfldnum, year, month, day, 0, 0, 0, 0);
				break;
			}

			/* TODO: handle time zones explicitly */
			case TIMEOID:
			case TIMETZOID:
			{
				/* Read the parts of the time */
				hour = lround(DatumGetFloat8(DirectFunctionCall2(time_part, PointerGetDatum(txthour), values[i])));
				minute = lround(DatumGetFloat8(DirectFunctionCall2(time_part, PointerGetDatum(txtminute), values[i])));
				second = lround(DatumGetFloat8(DirectFunctionCall2(time_part, PointerGetDatum(txtsecond), values[i])));
				OGR_F_SetFieldDateTime(feat, ogrfldnum, 0, 0, 0, hour, minute, second, 0);
				break;
			}


			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
			{
				Datum d = values[i];
				year = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtyear), d)));
				month = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtmonth), d)));
				day = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtday), d)));
				hour = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txthour), d)));
				minute = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtminute), d)));
				second = lround(DatumGetFloat8(DirectFunctionCall2(timestamp_part, PointerGetDatum(txtsecond), d)));
				OGR_F_SetFieldDateTime(feat, ogrfldnum, year, month, day, hour, minute, second, 0);
				break;
			}

			case BOOLARRAYOID:
			case INT2ARRAYOID:
			case INT4ARRAYOID:
			{
				ArrayType *arr = DatumGetArrayTypeP(values[i]);
				size_t sz = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
				Datum d;
				bool isnull;
				int *ints = palloc(sizeof(int) * sz);
				int num_ints = 0;
				ArrayIterator it = array_create_iterator(arr, 0, NULL);
				while (array_iterate(it,  &d, &isnull))
				{
					if (isnull) continue;
					ints[num_ints++] = DatumGetInt32(d);
				}
				OGR_F_SetFieldIntegerList(feat, ogrfldnum, num_ints, ints);
				pfree(ints);
				break;
			}

			case CHARARRAYOID:
			case NAMEARRAYOID:
			case TEXTARRAYOID:
			case VARCHARARRAYOID:
			{
				ArrayType *arr = DatumGetArrayTypeP(values[i]);
				Datum d;
				bool isnull;
				char** papszList = NULL;
				ArrayIterator it = array_create_iterator(arr, 0, NULL);
				while (array_iterate(it, &d, &isnull))
				{
					char *cstr;
					if (isnull) continue;
					cstr = text_to_cstring(DatumGetTextP(d));
					papszList = CSLAddString(papszList, cstr);
					pfree(cstr);
				}
				OGR_F_SetFieldStringList(feat, ogrfldnum, papszList);
				CSLDestroy(papszList);
				break;
			}

			case FLOAT4ARRAYOID:
			case FLOAT8ARRAYOID:
			{
				ArrayType *arr = DatumGetArrayTypeP(values[i]);
				size_t sz = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
				Datum d;
				bool isnull;
				double *floats = palloc(sizeof(double) * sz);
				int num_floats = 0;
				ArrayIterator it = array_create_iterator(arr, 0, NULL);
				while (array_iterate(it,  &d, &isnull))
				{
					if (isnull) continue;
					floats[num_floats++] = DatumGetFloat8(d);
				}
				OGR_F_SetFieldDoubleList(feat, ogrfldnum, num_floats, floats);
				pfree(floats);
				break;
			}

			/* TODO: array types for string, integer, float */
			default:
			{
				elog(ERROR, "OGR FDW unsupported PgSQL column type in \"%s\", %d", pgname, pgtype);
				return OGRERR_FAILURE;
			}
			}
		}
		/* Fill in unmatched columns with NULL */
		else if (ogrvariant == OGR_UNMATCHED)
		{
			OGR_F_UnsetField(feat, ogrfldnum);
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
static TupleTableSlot*
ogrIterateForeignScan(ForeignScanState* node)
{
	OgrFdwExecState* execstate = (OgrFdwExecState*) node->fdw_state;
	TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
	OGRFeatureH feat;

	elog(DEBUG3, "%s: entered function", __func__);

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
	if (execstate->rownum == 0)
	{
		OGR_L_ResetReading(execstate->ogr.lyr);
	}

	/* If we rectreive a feature from OGR, copy it over into the slot */
	feat = OGR_L_GetNextFeature(execstate->ogr.lyr);
	if (feat)
	{
		/* convert result to arrays of values and null indicators */
		if (OGRERR_NONE != ogrFeatureToSlot(feat, slot, execstate))
		{
			ogrEreportError("failure reading OGR data source");
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
ogrReScanForeignScan(ForeignScanState* node)
{
	OgrFdwExecState* execstate = (OgrFdwExecState*) node->fdw_state;
	elog(DEBUG3, "%s: entered function", __func__);

	OGR_L_ResetReading(execstate->ogr.lyr);
	execstate->rownum = 0;

	return;
}

/*
 * ogrEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
ogrEndForeignScan(ForeignScanState* node)
{
	OgrFdwExecState* execstate = (OgrFdwExecState*) node->fdw_state;
	elog(DEBUG3, "%s: entered function", __func__);
	if (execstate)
	{
		elog(DEBUG2, "OGR FDW processed %d rows from OGR", execstate->rownum);
		ogrFinishConnection(&(execstate->ogr));
	}

	return;
}

/* ======================================================== */
/* WRITE SUPPORT */
/* ======================================================== */

/* if the scanning functions above respected the targetlist,
we would only be getting back the SET target=foo columns in the slots below,
so we would need to add the "fid" to all targetlists (and also disallow fid changing
perhaps).

since we always pull complete tables in the scan functions, the
slots below are basically full tables, in fact they include (?) one entry
for each OGR column, even when the table does not include the column,
just nulling out the entries that are not in the table definition

it might be better to update the scan code to properly manage target lists
first, and then come back here and do things properly

we will need a ogrSlotToFeature to feed into the OGR_L_SetFeature and
OGR_L_CreateFeature functions. Also will use OGR_L_DeleteFeature and
fid value

in ogrGetForeignPlan we get a tlist that includes just the attributes we
are interested in, can use that to pare down the request perhaps
*/

static int
ogrGetFidColumn(const TupleDesc td)
{
	int i;
	for (i = 0; i < td->natts; i++)
	{
		Form_pg_attribute att_tuple = TupleDescAttr(td, i);
		NameData attname = att_tuple->attname;
		Oid atttypeid = att_tuple->atttypid;

		if ((atttypeid == INT4OID || atttypeid == INT8OID) &&
		        strcaseeq("fid", attname.data))
		{
			return i;
		}
	}
	return -1;
}

/*
 * ogrAddForeignUpdateTargets
 *
 * For now we no-op this callback, as we are making the presence of
 * "fid" in the FDW table definition a requirement for any update.
 * It might be possible to add nonexisting "junk" columns? In which case
 * there could always be a virtual fid travelling with the queries,
 * and the FDW table itself wouldn't need such a column?
 */

#if PG_VERSION_NUM >= 140000
static void
ogrAddForeignUpdateTargets(PlannerInfo* planinfo,
                           unsigned int rte_index,
                           RangeTblEntry* target_rte,
                           Relation target_relation)
{
	Query* parsetree = planinfo->parse;
	Form_pg_attribute att;
	Var* var;
	TupleDesc tupdesc = target_relation->rd_att;
	int fid_column = ogrGetFidColumn(tupdesc);

	elog(DEBUG3, "%s: entered function", __func__);

	if (fid_column < 0)
	{
		elog(ERROR, "table '%s' does not have a 'fid' column", RelationGetRelationName(target_relation));
	}

	att = TupleDescAttr(tupdesc, fid_column);

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
	              att->attnum,
	              att->atttypid,
	              att->atttypmod,
	              att->attcollation,
	              0);

	add_row_identity_var(planinfo, var, rte_index, "fid");
}
#else /* PG_VERSION_NUM < 140000 */

static void
ogrAddForeignUpdateTargets(Query* parsetree,
                           RangeTblEntry* target_rte,
                           Relation target_relation)
{
	ListCell* cell;
	Form_pg_attribute att;
	Var* var;
	TargetEntry* tle;
	TupleDesc tupdesc = target_relation->rd_att;
	int fid_column = ogrGetFidColumn(tupdesc);

	elog(DEBUG3, "%s: entered function", __func__);

	if (fid_column < 0)
	{
		elog(ERROR, "table '%s' does not have a 'fid' column", RelationGetRelationName(target_relation));
	}

#if PG_VERSION_NUM >= 110000
	att = &tupdesc->attrs[fid_column];
#else
	att = tupdesc->attrs[fid_column];
#endif
	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
	              att->attnum,
	              att->atttypid,
	              att->atttypmod,
	              att->attcollation,
	              0);

	/* Wrap it in a resjunk TLE with the right name ... */
	tle = makeTargetEntry((Expr*)var,
	                      list_length(parsetree->targetList) + 1,
	                      pstrdup(NameStr(att->attname)),
	                      true);

	parsetree->targetList = lappend(parsetree->targetList, tle);

	foreach (cell, parsetree->targetList)
	{
		TargetEntry* target = (TargetEntry*) lfirst(cell);
		elog(DEBUG4, "parsetree->targetList %s:%d", target->resname, target->resno);
	}
}
#endif



/*
 * ogrBeginForeignModify
 * For now the only thing we'll do here is set up the connection
 * and pass that on to the next functions.
 */
static void
ogrBeginForeignModify(ModifyTableState* mtstate,
                      ResultRelInfo* rinfo,
                      List* fdw_private,
                      int subplan_index,
                      int eflags)
{
	Oid foreigntableid;
	OgrFdwState* state;

	elog(DEBUG3, "%s: entered function", __func__);

	foreigntableid = RelationGetRelid(rinfo->ri_RelationDesc);
	state = getOgrFdwState(foreigntableid, OGR_MODIFY_STATE);

	/* Read the OGR layer definition and PgSQL foreign table definitions */
	ogrReadColumnData(state);

	/* Save OGR connection, etc, for later */
	rinfo->ri_FdwState = state;
	return;
}

/*
 * ogrExecForeignUpdate
 * Find out what the fid is, get the OGR feature for that FID,
 * and then update the values on that feature.
 */
static TupleTableSlot*
ogrExecForeignUpdate(EState* estate,
                     ResultRelInfo* rinfo,
                     TupleTableSlot* slot,
                     TupleTableSlot* planSlot)
{
	OgrFdwModifyState* modstate = rinfo->ri_FdwState;
	TupleDesc td = slot->tts_tupleDescriptor;
	Relation rel = rinfo->ri_RelationDesc;
	Oid foreigntableid = RelationGetRelid(rel);
	int fid_column;
	Oid fid_type;
	Datum fid_datum;
	int64 fid;
	OGRFeatureH feat;
	OGRErr err;
	Form_pg_attribute attrs;

	elog(DEBUG3, "%s: entered function", __func__);

	/* Is there a fid column? */
	fid_column = ogrGetFidColumn(td);
	if (fid_column < 0)
	{
		elog(ERROR, "cannot find 'fid' column in table '%s'", get_rel_name(foreigntableid));
	}

#if PG_VERSION_NUM >= 120000
	slot_getallattrs(slot);
#endif

	/* What is the value of the FID for this record? */
	fid_datum = slot->tts_values[fid_column];
	attrs = TupleDescAttr(td, fid_column);
	fid_type = attrs->atttypid;

	if (fid_type == INT8OID)
	{
		fid = DatumGetInt64(fid_datum);
	}
	else
	{
		fid = DatumGetInt32(fid_datum);
	}

	elog(DEBUG2, "ogrExecForeignUpdate fid=" OGR_FDW_FRMT_INT64, OGR_FDW_CAST_INT64(fid));

	/* Get the OGR feature for this fid */
	feat = OGR_L_GetFeature(modstate->ogr.lyr, fid);

	/* If we found a feature, then copy data from the slot onto the feature */
	/* and then back into the layer */
	if (! feat)
	{
		ogrEreportError("failure reading OGR feature");
	}

	err = ogrSlotToFeature(slot, feat, modstate->table);
	if (err != OGRERR_NONE)
	{
		ogrEreportError("failure populating OGR feature");
	}

	err = OGR_L_SetFeature(modstate->ogr.lyr, feat);
	if (err != OGRERR_NONE)
	{
		ogrEreportError("failure writing back OGR feature");
	}

	OGR_F_Destroy(feat);

	/* TODO: slot handling? what happens with RETURNING clauses? */

	return slot;
}

// typedef struct TupleTableSlot
// {
//     NodeTag     type;
//     bool        tts_isempty;    /* true = slot is empty */
//     bool        tts_shouldFree; /* should pfree tts_tuple? */
//     bool        tts_shouldFreeMin;      /* should pfree tts_mintuple? */
//     bool        tts_slow;       /* saved state for slot_deform_tuple */
//     HeapTuple   tts_tuple;      /* physical tuple, or NULL if virtual */
//     TupleDesc   tts_tupleDescriptor;    /* slot's tuple descriptor */
//     MemoryContext tts_mcxt;     /* slot itself is in this context */
//     Buffer      tts_buffer;     /* tuple's buffer, or InvalidBuffer */
//     int         tts_nvalid;     /* # of valid values in tts_values */
//     Datum      *tts_values;     /* current per-attribute values */
//     bool       *tts_isnull;     /* current per-attribute isnull flags */
//     MinimalTuple tts_mintuple;  /* minimal tuple, or NULL if none */
//     HeapTupleData tts_minhdr;   /* workspace for minimal-tuple-only case */
//     long        tts_off;        /* saved state for slot_deform_tuple */
// } TupleTableSlot;

// typedef struct tupleDesc
// {
//     int         natts;          /* number of attributes in the tuple */
//     Form_pg_attribute *attrs;
//     /* attrs[N] is a pointer to the description of Attribute Number N+1 */
//     TupleConstr *constr;        /* constraints, or NULL if none */
//     Oid         tdtypeid;       /* composite type ID for tuple type */
//     int32       tdtypmod;       /* typmod for tuple type */
//     bool        tdhasoid;       /* tuple has oid attribute in its header */
//     int         tdrefcount;     /* reference count, or -1 if not counting */
// }   *TupleDesc;
//

// typedef struct ResultRelInfo
// {
//     NodeTag     type;
//     Index       ri_RangeTableIndex;
//     Relation    ri_RelationDesc;
//     int         ri_NumIndices;
//     RelationPtr ri_IndexRelationDescs;
//     IndexInfo **ri_IndexRelationInfo;
//     TriggerDesc *ri_TrigDesc;
//     FmgrInfo   *ri_TrigFunctions;
//     List      **ri_TrigWhenExprs;
//     Instrumentation *ri_TrigInstrument;
//     struct FdwRoutine *ri_FdwRoutine;
//     void       *ri_FdwState;
//     List       *ri_WithCheckOptions;
//     List       *ri_WithCheckOptionExprs;
//     List      **ri_ConstraintExprs;
//     JunkFilter *ri_junkFilter;
//     ProjectionInfo *ri_projectReturning;
//     ProjectionInfo *ri_onConflictSetProj;
//     List       *ri_onConflictSetWhere;
// } ResultRelInfo;

// typedef struct TargetEntry
// 	{
// 	    Expr        xpr;
// 	    Expr       *expr;           /* expression to evaluate */
// 	    AttrNumber  resno;          /* attribute number (see notes above) */
// 	    char       *resname;        /* name of the column (could be NULL) */
// 	    Index       ressortgroupref;/* nonzero if referenced by a sort/group
// 	                                 * clause */
// 	    Oid         resorigtbl;     /* OID of column's source table */
// 	    AttrNumber  resorigcol;     /* column's number in source table */
// 	    bool        resjunk;        /* set to true to eliminate the attribute from
// 	                                 * final target list */
// 	} TargetEntry;

// TargetEntry *
// makeTargetEntry(Expr *expr,
//                 AttrNumber resno,
//                 char *resname,
//                 bool resjunk)

// Var *
// makeVar(Index varno,
//         AttrNumber varattno,
//         Oid vartype,
//         int32 vartypmod,
//         Oid varcollid,
//         Index varlevelsup)

// typedef struct Var
// {
//     Expr        xpr;
//     Index       varno;          /* index of this var's relation in the range
//                                  * table, or INNER_VAR/OUTER_VAR/INDEX_VAR */
//     AttrNumber  varattno;       /* attribute number of this var, or zero for
//                                  * all */
//     Oid         vartype;        /* pg_type OID for the type of this var */
//     int32       vartypmod;      /* pg_attribute typmod value */
//     Oid         varcollid;      /* OID of collation, or InvalidOid if none */
//     Index       varlevelsup;    /* for subquery variables referencing outer
//                                  * relations; 0 in a normal var, >0 means N
//                                  * levels up */
//     Index       varnoold;       /* original value of varno, for debugging */
//     AttrNumber  varoattno;      /* original value of varattno */
//     int         location;       /* token location, or -1 if unknown */
// } Var;

static TupleTableSlot*
ogrExecForeignInsert(EState* estate,
                     ResultRelInfo* rinfo,
                     TupleTableSlot* slot,
                     TupleTableSlot* planSlot)
{
	OgrFdwModifyState* modstate = rinfo->ri_FdwState;
	OGRFeatureDefnH ogr_fd = OGR_L_GetLayerDefn(modstate->ogr.lyr);
	OGRFeatureH feat = OGR_F_Create(ogr_fd);
	int fid_column;
	OGRErr err;
	GIntBig fid;

	elog(DEBUG3, "%s: entered function", __func__);

#if PG_VERSION_NUM >= 120000
	/*
	* PgSQL 12 passes an unpopulated slot to us, and for now
	* we force it to populate itself and then read directly
	* from it. For future, using the slot_getattr() infra
	* would be cleaner, but version dependent.
	*/
	slot_getallattrs(slot);
#endif

	/* Copy the data from the slot onto the feature */
	if (!feat)
	{
		ogrEreportError("failure creating OGR feature");
	}

	err = ogrSlotToFeature(slot, feat, modstate->table);
	if (err != OGRERR_NONE)
	{
		ogrEreportError("failure populating OGR feature");
	}

	err = OGR_L_CreateFeature(modstate->ogr.lyr, feat);
	if (err != OGRERR_NONE)
	{
		ogrEreportError("failure writing OGR feature");
	}

	fid = OGR_F_GetFID(feat);
	OGR_F_Destroy(feat);

	/* Update the FID for RETURNING slot */
	fid_column = ogrGetFidColumn(slot->tts_tupleDescriptor);
	if (fid_column >= 0)
	{
		slot->tts_values[fid_column] = Int64GetDatum(fid);
		slot->tts_isnull[fid_column] = false;
		slot->tts_nvalid++;
	}

	return slot;
}



static TupleTableSlot*
ogrExecForeignDelete(EState* estate,
                     ResultRelInfo* rinfo,
                     TupleTableSlot* slot,
                     TupleTableSlot* planSlot)
{
	OgrFdwModifyState* modstate = rinfo->ri_FdwState;
	TupleDesc td = planSlot->tts_tupleDescriptor;
	Relation rel = rinfo->ri_RelationDesc;
	Oid foreigntableid = RelationGetRelid(rel);
	int fid_column;
	Oid fid_type;
	Datum fid_datum;
	int64 fid;
	OGRErr err;
	Form_pg_attribute attrs;

	elog(DEBUG3, "%s: entered function", __func__);

	/* Is there a fid column? */
	fid_column = ogrGetFidColumn(td);
	if (fid_column < 0)
	{
		elog(ERROR, "cannot find 'fid' column in table '%s'", get_rel_name(foreigntableid));
	}

#if PG_VERSION_NUM >= 120000
	slot_getallattrs(planSlot);
#endif

	/* What is the value of the FID for this record? */
	fid_datum = planSlot->tts_values[fid_column];
	attrs = TupleDescAttr(td, fid_column);
	fid_type = attrs->atttypid;

	if (fid_type == INT8OID)
	{
		fid = DatumGetInt64(fid_datum);
	}
	else
	{
		fid = DatumGetInt32(fid_datum);
	}

	elog(DEBUG2, "ogrExecForeignDelete fid=" OGR_FDW_FRMT_INT64, OGR_FDW_CAST_INT64(fid));

	/* Delete the OGR feature for this fid */
	err = OGR_L_DeleteFeature(modstate->ogr.lyr, fid);

	if (err != OGRERR_NONE)
	{
		return NULL;
	}
	else
	{
		return slot;
	}
}

static void
ogrEndForeignModify(EState* estate, ResultRelInfo* rinfo)
{
	OgrFdwModifyState* modstate = rinfo->ri_FdwState;

	elog(DEBUG3, "%s: entered function", __func__);

	ogrFinishConnection(&(modstate->ogr));

	return;
}

static int
ogrIsForeignRelUpdatable(Relation rel)
{
	const int readonly = 0;
	int foreign_rel_updateable = 0;
	TupleDesc td = RelationGetDescr(rel);
	OgrConnection ogr;
	Oid foreigntableid = RelationGetRelid(rel);

	elog(DEBUG3, "%s: entered function", __func__);

	/* Before we say "yes"... */
	/*  Does the foreign relation have a "fid" column? */
	/* Is that column an integer? */
	if (ogrGetFidColumn(td) < 0)
	{
		elog(NOTICE, "no \"fid\" column in foreign table '%s'", get_rel_name(foreigntableid));
		return readonly;
	}

	/*   Is it backed by a writable OGR driver? */
	/*   Can we open the relation in read/write mode? */
	ogr = ogrGetConnectionFromTable(foreigntableid, OGR_UPDATEABLE_TRY);

	/* Something in the open process set the readonly flags */
	/* Perhaps user has manually set the foreign table option to readonly */
	if (ogr.ds_updateable == OGR_UPDATEABLE_FALSE ||
	    ogr.lyr_updateable == OGR_UPDATEABLE_FALSE)
	{
		return readonly;
	}

	/* No data source or layer objects? Readonly */
	if (!(ogr.ds && ogr.lyr))
	{
		return readonly;
	}

	if (OGR_L_TestCapability(ogr.lyr, OLCRandomWrite))
	{
		foreign_rel_updateable |= (1 << CMD_UPDATE);
	}

	if (OGR_L_TestCapability(ogr.lyr, OLCSequentialWrite))
	{
		foreign_rel_updateable |= (1 << CMD_INSERT);
	}

	if (OGR_L_TestCapability(ogr.lyr, OLCDeleteFeature))
	{
		foreign_rel_updateable |= (1 << CMD_DELETE);
	}

	ogrFinishConnection(&ogr);

	return foreign_rel_updateable;
}

#if PG_VERSION_NUM >= 90500

/*
 * PostgreSQL 9.5 or above.  Import a foreign schema
 */
static List*
ogrImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid serverOid)
{
	List* commands = NIL;
	ForeignServer* server;
	ListCell* lc;
	bool import_all = false;
	bool launder_column_names, launder_table_names;
	OgrConnection ogr;
	int i;
	char layer_name[STR_MAX_LEN];
	char table_name[STR_MAX_LEN];

	elog(DEBUG3, "%s: entered function", __func__);

	/* Are we importing all layers in the OGR datasource? */
	import_all = streq(stmt->remote_schema, "ogr_all");

	/* Make connection to server */
	server = GetForeignServer(serverOid);
	ogr = ogrGetConnectionFromServer(serverOid, OGR_UPDATEABLE_FALSE);

	/* Launder by default */
	launder_column_names = launder_table_names = true;

	/* Read user-provided statement laundering options */
	foreach (lc, stmt->options)
	{
		DefElem* def = (DefElem*) lfirst(lc);

		if (streq(def->defname, "launder_column_names"))
		{
			launder_column_names = defGetBoolean(def);
		}
		else if (streq(def->defname, "launder_table_names"))
		{
			launder_table_names = defGetBoolean(def);
		}
		else
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			         errmsg("invalid option \"%s\"", def->defname)));
	}

	for (i = 0; i < GDALDatasetGetLayerCount(ogr.ds); i++)
	{
		bool import_layer = false;
		OGRLayerH ogr_lyr = GDALDatasetGetLayer(ogr.ds, i);

		if (! ogr_lyr)
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
		{
			ogrStringLaunder(table_name);
		}

		/*
		* Only include if we are importing "ogr_all" or
		* the layer prefix starts with the remote schema
		*/
		import_layer = import_all ||
		               (strncmp(layer_name, stmt->remote_schema, strlen(stmt->remote_schema)) == 0);

		/* Apply restrictions for LIMIT TO and EXCEPT */
		if (import_layer && (
		            stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
		            stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT))
		{
			/* Limited list? Assume we are taking no items */
			if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
			{
				import_layer = false;
			}

			/* Check the list for our items */
			foreach (lc, stmt->table_list)
			{
				RangeVar* rv = (RangeVar*) lfirst(lc);
				/* Found one! */
				if (streq(rv->relname, table_name))
				{
					if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
					{
						import_layer = true;
					}
					else
					{
						import_layer = false;
					}

					break;
				}

			}
		}

		if (import_layer)
		{
			OGRErr err;
			stringbuffer_t buf;
			stringbuffer_init(&buf);

			err = ogrLayerToSQL(ogr_lyr,
			                    server->servername,
			                    launder_table_names,
			                    launder_column_names,
			                    NULL,
			                    ogrGetGeometryOid() != BYTEAOID,
			                    &buf
			                   );

			if (err != OGRERR_NONE)
			{
				elog(ERROR, "unable to generate IMPORT SQL for '%s'", table_name);
			}

			commands = lappend(commands, pstrdup(stringbuffer_getstring(&buf)));
			stringbuffer_release(&buf);
		}
	}

	elog(NOTICE, "Number of tables to be created %d", list_length(commands));

	ogrFinishConnection(&ogr);

	return commands;
}

#endif /* PostgreSQL 9.5+ for ogrImportForeignSchema */



#endif /* PostgreSQL 9.3+ version check */

