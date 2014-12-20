/*-------------------------------------------------------------------------
 *
 * ogr_fdw.c
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/*
 * System
 */
#include <sys/stat.h>
#include <unistd.h>
/*
 * PostgreSQL
 */
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
/*
 * OGR library API
 */
#include "ogr_api.h"
#include "cpl_error.h"
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
	Oid optcontext;		/* Oid of catalog in which option may appear */
	bool optrequired;   /* Flag mandatory options */
	bool optfound;      /* Flag whether options was specified by user */
};

#define OPT_DRIVER "format"
#define OPT_SOURCE "datasource"
#define OPT_LAYER "layer"

/*
 * Valid options for ogr_fdw.
 * ForeignDataWrapperRelationId (no options)
 * ForeignServerRelationId (CREATE SERVER options)
 * UserMappingRelationId (CREATE USER MAPPING options)
 * ForeignTableRelationId (CREATE FOREIGN TABLE options)
 */
static struct OgrFdwOption valid_options[] = {

	/* OGR datasource options */
	{OPT_SOURCE, ForeignServerRelationId, true, false},
	{OPT_DRIVER, ForeignServerRelationId, false, false},
	
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
				   List *scan_clauses);
static void ogrBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *ogrIterateForeignScan(ForeignScanState *node);
static void ogrReScanForeignScan(ForeignScanState *node);
static void ogrEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static OgrFdwPlanState* getOgrFdwPlanState(Oid foreigntableid);
static OgrFdwExecState* getOgrFdwExecState(Oid foreigntableid);
static OgrConnection ogrGetConnection(Oid foreigntableid);
static void ogr_fdw_exit(int code, Datum arg);

/* Global to hold GEOMETRYOID */
static Oid GEOMETRYOID = InvalidOid;


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
	const char *typname = "geometry";
	Oid namesp = LookupExplicitNamespace("public", false);
	Oid typoid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(typname), ObjectIdGetDatum(namesp));

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

	PG_RETURN_POINTER(fdwroutine);
}

static OGRDataSourceH
ogrGetDataSource(const char *source, const char *driver)
{
	OGRDataSourceH ogr_ds = NULL;
	OGRSFDriverH ogr_dr = NULL;
	
	/* Cannot search for drivers if they aren't registered */
	OGRRegisterAll();
	
	if ( driver )
	{
		ogr_dr = OGRGetDriverByName(driver);
		if ( ! ogr_dr )
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("unable to find format \"%s\"", driver),
					errhint("See the formats list at http://www.gdal.org/ogr_formats.html")));
		}
		ogr_ds = OGR_Dr_Open(ogr_dr, source, false);
	}
	/* No driver, try a blind open... */
	else
	{
		ogr_ds = OGROpen(source, false, &ogr_dr);			
	}

	/* Open failed, provide error hint if OGR gives us one. */
	if ( ! ogr_ds )
	{
		const char *ogrerr = CPLGetLastErrorMsg();
		if ( ogrerr && strcmp(ogrerr,"") != 0 )
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

static OgrConnection
ogrGetConnection(Oid foreigntableid)
{
	ForeignTable *table;
	ForeignServer *server;
	/* UserMapping *mapping; */
	/* ForeignDataWrapper *wrapper; */
	List *options;
	ListCell *cell;
	OgrConnection ogr;

	/* Null all values */
	memset(&ogr, 0, sizeof(OgrConnection));

	/* Gather all data for the foreign table. */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	/* mapping = GetUserMapping(GetUserId(), table->serverid); */

	options = NIL;
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_SOURCE) == 0)
			ogr.ds_str = defGetString(def);
		if (strcmp(def->defname, OPT_DRIVER) == 0)
			ogr.dr_str = defGetString(def);
		if (strcmp(def->defname, OPT_LAYER) == 0)
			ogr.lyr_str = defGetString(def);
	}
	
	/*  Connect! */
	ogr.ds = ogrGetDataSource(ogr.ds_str, ogr.dr_str);
	
	/* Does the layer exist in the data source? */
	ogr.lyr = OGR_DS_GetLayerByName(ogr.ds, ogr.lyr_str);
	if ( ! ogr.lyr )
	{
		const char *ogrerr = CPLGetLastErrorMsg();
		ereport(ERROR, (
				errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				errmsg("unable to connect to %s to \"%s\"", OPT_LAYER, ogr.lyr_str),
				(ogrerr && strcmp(ogrerr,"") != 0)
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
	const char *source = NULL, *layer = NULL, *driver = NULL;

	/* TODO, check that the database encoding is UTF8, to match OGR internals */
	/* GetDatabaseEncoding() == "UTF8" ? */

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
			if ( catalog == opt->optcontext && strcmp(opt->optname, def->defname) == 0)
			{
				/* Mark that this user option was found */
				opt->optfound = optfound = true;

				/* Store some options for testing later */
				if ( strcmp(opt->optname, OPT_SOURCE) == 0 )
					source = defGetString(def);
				if ( strcmp(opt->optname, OPT_LAYER) == 0 )
					layer = defGetString(def);
				if ( strcmp(opt->optname, OPT_DRIVER) == 0 )
					driver = defGetString(def);
				
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
	
	/* Check that all the mandatory options where found */
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
		ogr_ds = ogrGetDataSource(source, driver);
		if ( ogr_ds )
		{
			OGR_DS_Destroy(ogr_ds);			
		}
		OGRCleanupAll();
	}
	
	PG_RETURN_VOID();
}

static OgrFdwPlanState* 
getOgrFdwPlanState(Oid foreigntableid)
{
	OgrFdwPlanState *planstate = palloc(sizeof(OgrFdwPlanState));
	
	/* Zero out the state */
	memset(planstate, 0, sizeof(OgrFdwPlanState));

	/*  Connect! */
	planstate->ogr = ogrGetConnection(foreigntableid);
	planstate->foreigntableid = foreigntableid;	
	
	return planstate;
}

static OgrFdwExecState* 
getOgrFdwExecState(Oid foreigntableid)
{
	OgrFdwExecState *execstate = palloc(sizeof(OgrFdwExecState));
	
	/* Zero out the state */
	memset(execstate, 0, sizeof(OgrFdwExecState));

	/*  Connect! */
	execstate->ogr = ogrGetConnection(foreigntableid);
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

	/* Set to NULL to clear the restriction clauses in OGR */
	OGR_L_SetIgnoredFields(planstate->ogr.lyr, NULL);
	OGR_L_SetSpatialFilter(planstate->ogr.lyr, NULL);
	OGR_L_SetAttributeFilter(planstate->ogr.lyr, NULL);

	/* If we can quickly figure how many rows this layer has, then do so */
	if ( OGR_L_TestCapability(planstate->ogr.lyr, OLCFastFeatureCount) )
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
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
ogrGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	OgrFdwPlanState *planstate = (OgrFdwPlanState *)(baserel->fdw_private);
	
    /*
	 * Estimate costs first. 
	 */

	/* TODO: replace this with something that looks at the OGRDriver and */
	/* makes a determination based on that? */
	planstate->startup_cost = 25;
	
	/* TODO: more research on what the total cost is supposed to mean, */
	/* relative to the startup cost? */
	planstate->total_cost = planstate->startup_cost + baserel->rows;


	add_path(baserel, 
		(Path *) create_foreignscan_path(root, baserel,
					baserel->rows,
					planstate->startup_cost,
					planstate->total_cost,
					NIL,     /* no pathkeys */
					NULL,    /* no outer rel either */
					NIL));   /* no fdw_private data */

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
				   List *scan_clauses)
{
	Index		scan_relid = baserel->relid;

	/*
	 * We are not pushing down WHERE clauses to the OGR library yet,
	 * So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 * 
	 * TODO: Actually traverse the clauses and add OGR_L_SetSpatialFilter
	 * and OGR_L_SetAttributeFilter calls to force some clauses down into
	 * the OGR level.
	 * 
	 * TODO: Review the columns requested and only pull those back, using
	 * OGR_L_SetIgnoredFields. This is less important than pushing
	 *
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* 
	 * Clean up the plan state, we're done w/ it for now. 
	 */
	

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							best_path->fdw_private);
}


static void
ogrCanConvertToPg(OGRFieldType ogr_type, Oid pg_type, const char *colname, const char *tblname)
{
	switch (ogr_type)
	{
		case OFTInteger:
			if ( pg_type == INT4OID || pg_type == INT8OID || pg_type == NUMERICOID || pg_type == FLOAT4OID || pg_type == FLOAT8OID || pg_type == TEXTOID || pg_type == VARCHAROID )
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

		case OFTWideString:
		case OFTIntegerList:
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


static void
ogrReadColumnData(OgrFdwExecState *execstate)
{
	Relation rel;
	TupleDesc tupdesc;
	// int i;
	// int j;
	// OgrFdwTable *tbl;
	// OGRFeatureDefnH dfn;

	/* Blow away any existing table in the state */
	// if ( execstate->table )
	// {
	// 	for ( i = 0; i < execstate->table->ncols; i++ )
	// 	{
	// 		if ( execstate->table->cols[i] )
	// 			pfree(execstate->table->cols[i]);
	// 	}
	// 	pfree(execstate->table->cols);
	// 	pfree(execstate->table);
	// }
	
	/* Fresh table */
	// tbl = palloc(sizeof(OgrFdwTable));
	// memset(tbl, 0, sizeof(OgrFdwTable));

	/* Allocate column list */
	// dfn = OGR_L_GetLayerDefn(execstate->ogr.lyr);
	// tbl->ncols = OGR_FD_GetFieldCount(dfn);
	// tbl->cols = palloc((2 + tbl->ncols) * sizeof(OgrFdwColumn*));
	
	
	/*
	 * Fill in information from PgSQL foreign table definition 
	 */

	rel = heap_open(execstate->foreigntableid, NoLock);
	tupdesc = rel->rd_att;
	execstate->tupdesc = tupdesc;

	// tbl->npgcols = tupdesc->natts;

	/* loop through foreign table columns */
	// for ( i = 0; i < tupdesc->natts; i++ )
	// {
	// 	Form_pg_attribute att_tuple = tupdesc->attrs[i];
	//
	// 	/* get PostgreSQL column number and type */
	// 	if ( j < tbl->ncols )
	// 	{
	// 		OgrFdwColumn *col = tbl->cols[j];
	// 		col->pgattisdropped = att_tuple->attisdropped;
	// 		col->pgattnum = att_tuple->attnum;
	// 		col->pgtype = att_tuple->atttypid;
	// 		col->pgtypmod = att_tuple->atttypmod;
	// 		/* Check to make sure we can convert from the OGR to the PostgreSQL type */
	// 		ogrCanConvertToPg(col->ogrtype, col->pgtype,
	// 			col->pgname, get_rel_name(execstate->foreigntableid));
	// 	}
	// 	j++;
	// }
	//
	// execstate->table = tbl;

	heap_close(rel, NoLock);	

	/* 
	 * Fill in information from OGR as base definition 
	 */
	
	/* FID column entry */
	// tbl->cols[0] = palloc(sizeof(OgrFdwColumn));
	// memset(tbl->cols[0], 0, sizeof(OgrFdwColumn));
	// tbl->cols[0]->ogrvariant = OGR_FID;
	//
	// /* Geometry column entry */
	// tbl->cols[1] = palloc(sizeof(OgrFdwColumn));
	// memset(tbl->cols[1], 0, sizeof(OgrFdwColumn));
	// tbl->cols[1]->ogrvariant = OGR_GEOMETRY;
	//
	// /* Field entries */
	// for ( i = 0; i < tbl->ncols; i++ )
	// {
	// 	OGRFieldDefnH flddef = OGR_FD_GetFieldDefn(dfn, i);
	// 	OgrFdwColumn *col = palloc(sizeof(OgrFdwColumn));
	// 	memset(col, 0, sizeof(OgrFdwColumn));
	// 	col->ogrtype = OGR_Fld_GetType(flddef);
	// 	col->ogrvariant = OGR_FIELD;
	//
	// 	/* Save it */
	// 	tbl->cols[i+2] = col;
	// }
	//

}



/*
 * ogrBeginForeignScan
 */
static void
ogrBeginForeignScan(ForeignScanState *node, int eflags)
{
	Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);

	/* Initialize OGR connection */
	OgrFdwExecState *execstate = getOgrFdwExecState(foreigntableid);
	
	/* Read the OGR layer definition and PgSQL foreign table definitions */
	ogrReadColumnData(execstate);
	
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
pgDatumFromCString(const char *cstr, Oid pgtype, int pgtypmod)
{
	Datum cdata;
	Datum value;
	Oid inputfunc;
	Oid inputioparam;

	/* Find the appropriate conversion function */
	getTypeInputInfo(pgtype, &inputfunc, &inputioparam);
	cdata = CStringGetDatum(cstr);
	
	/* pgtypmod will be -1 for types w/o typmod  */
	if ( pgtypmod >= 0 )
	{
		/* These functions require a type modifier */
		value = OidFunctionCall3(inputfunc, cdata,
			ObjectIdGetDatum(InvalidOid),
			Int32GetDatum(pgtypmod));
	}
	else
	{
		/* These functions don't */
		value = OidFunctionCall1(inputfunc, cdata);
	}

	return value;
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
ogrFeatureToSlot(OGRFeatureH feat, TupleTableSlot *slot, TupleDesc tupdesc)
{
	int i, j;
	Datum *values = slot->tts_values;
	bool *nulls = slot->tts_isnull;
	int ogr_nfields = OGR_F_GetFieldCount(feat);
	OGRFeatureDefnH ogr_feat_defn = OGR_F_GetDefnRef(feat);
	int ogr_geom_field_count;
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
	ogr_geom_field_count = OGR_FD_GetGeomFieldCount(ogr_feat_defn);
#else
	ogr_geom_field_count = ( OGR_FD_GetGeomType(ogr_feat_defn) != wkbNone ) ? 1 : 0;
#endif
	
	/*
	 * We have to read several "non-field" fields (FID and Geometry/Geometries) before 
	 * we get to "real" OGR fields in the field definition 
	 */
	j = -(1 + ogr_geom_field_count);
	for ( i = 0; i < tupdesc->natts; i++ )
	{
		Form_pg_attribute att_tuple = tupdesc->attrs[i];
		Oid pgtype = att_tuple->atttypid;
		int pgtypemod = att_tuple->atttypmod;
		const char *pgcolname = att_tuple->attname.data;
		const char *pgtblname = get_rel_name(att_tuple->attrelid);

		/* 
		 * Fill in dropped attributes with NULL 
		 */
		if ( att_tuple->attisdropped )
		{
			nulls[i] = true;
			values[i] = PointerGetDatum(NULL);
			continue;
		}
		
		/* 
		 * First non-dropped column is FID 
		 */
		if ( j == -(1 + ogr_geom_field_count) )
		{
			long fid = OGR_F_GetFID(feat);
			
			if ( fid == OGRNullFID )
			{
				nulls[i] = true;
				values[i] = PointerGetDatum(NULL);
			}
			else
			{
				char fidstr[256];
				snprintf(fidstr, 256, "%ld", fid);

				nulls[i] = false;
				values[i] = pgDatumFromCString(fidstr, pgtype, pgtypemod);
			}
			
			/* Move on to next OGR column */
			j++;
			continue;				
		}

		/* 
		 * Second non-dropped column is Geometry 
		 */
		if ( j < 0 )
		{
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
			OGRGeometryH geom = OGR_F_GetGeomFieldRef(feat, j + ogr_geom_field_count);
#else
			OGRGeometryH geom = OGR_F_GetGeometryRef(feat);
#endif
	
			/* No geometry ? NULL */
			if ( ! geom )
			{
				/* No geometry column, so make the output null */
				nulls[i] = true;
				values[i] = PointerGetDatum(NULL);
			}
			else
			{
				/* 
				 * Start by generating standard PgSQL variable length byte
				 * buffer, with WKB filled into the data area.
				 */
				int wkbsize = OGR_G_WkbSize(geom);
				int varsize = wkbsize + VARHDRSZ;
				bytea *varlena = palloc(varsize);
				OGRErr err = OGR_G_ExportToWkb(geom, wkbNDR, (unsigned char *)VARDATA(varlena));
				SET_VARSIZE(varlena, varsize);

				if ( err != OGRERR_NONE )
				{
					return err;
				}
				else
				{
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
						
						Oid recvfunction;
						Oid ioparam;
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
						 * Given a type oid (geometry in this case), 
						 * look up the "recv" function that takes in
						 * binary input and outputs the serialized form.
						 */
						getTypeBinaryInputInfo(pgtype, &recvfunction, &ioparam);
				
						/* 
						 * TODO: We should probably find out the typmod and send
						 * this along to the recv function too, but we can ignore it now
						 * and just have no typmod checking.
						 */
						nulls[i] = false;
						values[i] = OidFunctionCall1(recvfunction, PointerGetDatum(&strinfo));
					}
					else 
					{
						elog(NOTICE, "conversion to geometry called with column type not equal to bytea or geometry");
						nulls[i] = true;
						values[i] = PointerGetDatum(NULL);
					}
				}
			}

			j++;
			continue;					
		}			

		/*
		 * All the rest of the columns come from the OGR fields.
		 * It's possible that there are more foreign table columns than
		 * there are OGR fields, and in that case we'll just fill them
		 * in with NULLs.
		 */
		if ( j >= 0 && j < ogr_nfields )
		{
			OGRFieldDefnH flddfn = OGR_F_GetFieldDefnRef(feat, j);
			OGRFieldType ogrtype = OGR_Fld_GetType(flddfn);
			
			/* Ensure that the OGR data type fits the destination Pg column */
			ogrCanConvertToPg(ogrtype, pgtype, pgcolname, pgtblname);
			
			/* Only convert non-null fields */
			if ( OGR_F_IsFieldSet(feat, j) )
			{
				switch(ogrtype)
				{
					case OFTBinary:
					{
						/* 
						 * Convert binary fields to bytea directly 
						 */
						int bufsize;
						GByte *buf = OGR_F_GetFieldAsBinary(feat, j, &bufsize);
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
						const char *cstr = OGR_F_GetFieldAsString(feat, j);
						if ( cstr )
						{
							nulls[i] = false;
							values[i] = pgDatumFromCString(cstr, pgtype, pgtypemod);
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
						
						OGR_F_GetFieldAsDateTime(feat, j, 
						                         &year, &month, &day,
												 &hour, &minute, &second, &tz);
						
						if ( ogrtype == OFTDate )
						{
							snprintf(cstr, 256, "%d-%02d-%02d", year, month, day);
						}
						else if ( ogrtype == OFTTime )
						{
							snprintf(cstr, 256, "%02d:%02d:%02d", hour, minute, second);
						}
						else 
						{
							snprintf(cstr, 256, "%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
						}
						
						nulls[i] = false;
						values[i] = pgDatumFromCString(cstr, pgtype, pgtypemod);
						break;
						
					}
					case OFTIntegerList:
					case OFTRealList:
					case OFTStringList:
					{
						/* TODO, map these OGR array types into PgSQL arrays (fun!) */
						elog(ERROR, "unsupported OGR array type \"%s\"", OGR_GetFieldTypeName(ogrtype));
						break;
					}
					default:
					{
						elog(ERROR, "unsupported OGR type \"%s\"", OGR_GetFieldTypeName(ogrtype));
						break;
					}
					
				}				
			}
			else
			{
				nulls[i] = true;
				values[i] = PointerGetDatum(NULL);				
			}
		}
		else 
		{
			elog(NOTICE, "handling more pgsql fields (%d) than there are ogr fields (%d)", i, ogr_nfields);
			nulls[i] = true;
			values[i] = PointerGetDatum(NULL);							
		}
			
		/* Incrememnt our OGR column counter */
		j++;
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
		if ( OGRERR_NONE != ogrFeatureToSlot(feat, slot, execstate->tupdesc) )
		{
			const char *ogrerr = CPLGetLastErrorMsg();
			if ( ogrerr && strcmp(ogrerr,"") != 0 )
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

	OGR_DS_Destroy(execstate->ogr.ds);

	return;
}



