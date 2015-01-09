/*-------------------------------------------------------------------------
 *
 * ogr_fdw.h
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

typedef enum 
{
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

	int ograttnum;
	OgrColumnVariant ogrvariant;
	OGRFieldType ogrtype;

	int used;                /* is the column used in the query? */
	int pkey;                /* nonzero for primary keys, later set to the resjunk attribute number */
	char *val;               /* buffer for OGR to return results in (LOB locator for LOBs) */
	size_t val_size;           /* allocated size in val */
	int val_null;          /* indicator for NULL value */
} OgrFdwColumn;

typedef struct OgrFdwTable
{
	OGRFeatureDefnH fdfn; 
	int npgcols;   /* number of columns (including dropped) in the PostgreSQL foreign table */
	int ncols;
	OgrFdwColumn **cols;
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
	// OgrFdwTable *table;
	TupleDesc tupdesc;
	int rownum;            /* how many rows have we read thus far? */
} OgrFdwExecState;

/* Shared function signatures */
void ogrDeparse(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, List *exprs, List **param);

