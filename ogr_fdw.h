/*-------------------------------------------------------------------------
 *
 * ogr_fdw.h
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */


/*
 * PostgreSQL
 */
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
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


#define streq(s1,s2) (strcmp((s1),(s2)) == 0)
#define strcaseeq(s1,s2) (strcasecmp((s1),(s2)) == 0)

typedef enum 
{
	OGR_UNMATCHED,
	OGR_GEOMETRY,
	OGR_FID,
	OGR_FIELD
} OgrColumnVariant;

typedef struct OgrFdwColumn
{
	int pgattnum;            /* PostgreSQL attribute number */
	int pgattisdropped;      /* PostgreSQL attribute dropped? */
	char *pgname;            /* PostgreSQL column name */
	Oid pgtype;              /* PostgreSQL data type */
	int pgtypmod;            /* PostgreSQL type modifier */
	Oid pginputfunc;         /* PostgreSQL function to convert cstring to type */
	Oid pginputioparam;
	Oid pgrecvfunc;          /* PostgreSQL function to convert binary to type */
	Oid pgrecvioparam;

	OgrColumnVariant ogrvariant;
	int ogrfldnum;
	OGRFieldType ogrfldtype;

	// int used;                /* is the column used in the query? */
	// int pkey;                /* nonzero for primary keys, later set to the resjunk attribute number */
	// char *val;               /* buffer for OGR to return results in (LOB locator for LOBs) */
	// size_t val_size;           /* allocated size in val */
	// int val_null;          /* indicator for NULL value */
} OgrFdwColumn;

typedef struct OgrFdwTable
{
	int ncols;
	char *tblname;
	OgrFdwColumn *cols;
} OgrFdwTable;

typedef struct OgrConnection
{
	char *ds_str;       /* datasource connection string */
	char *dr_str;       /* driver (format) name */
	char *lyr_str;      /* layer name */
	OGRDataSourceH ds;  /* OGR data source handle */
	OGRLayerH lyr;      /* OGR layer handle */
} OgrConnection;

typedef struct OgrFdwPlanState
{
	Oid foreigntableid; 
	OgrConnection ogr;   /* connection object */
	int nrows;           /* estimate of number of rows in file */
	Cost startup_cost; 
	Cost total_cost;
	bool *pushdown_clauses;
} OgrFdwPlanState;

typedef struct OgrFdwExecState
{
	Oid foreigntableid; 
	OgrConnection ogr;     /* connection object */
	char *sql;             /* OGR SQL for attribute filter */
	OgrFdwTable *table;
	TupleDesc tupdesc;
	int rownum;            /* how many rows have we read thus far? */
} OgrFdwExecState;

/* Shared function signatures */
bool ogrDeparse(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, List *exprs, List **param);

void ogrDeparseStringLiteral(StringInfo buf, const char *val);

/* Shared global value of the Geometry OId */
extern Oid GEOMETRYOID;




