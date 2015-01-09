/*-------------------------------------------------------------------------
 *
 * ogr_fdw_deparse.c
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "ogr_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
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


typedef struct OgrDeparseCtx
{
	PlannerInfo *root;        /* global planner state */
	RelOptInfo *foreignrel;   /* the foreign relation we are planning for */
	StringInfo buf;           /* output buffer to append to */
	List **params_list;       /* exprs that will become remote Params */
} OgrDeparseCtx;

/* Local function signatures */
// static void ogrDeparseExpr(Expr *node, OgrDeparseCtx *context);
// static void ogrDeparseOpExpr(OpExpr* node, OgrDeparseCtx *context);


static char *
ogrStringFromDatum(Datum datum, Oid type)
{
	StringInfoData result;
	regproc typoutput;
	HeapTuple tuple;
	char *str, *p;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for type %u", type);
	}
	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
	ReleaseSysCache(tuple);

	initStringInfo(&result);

	/* render the constant in OGR SQL */
	switch (type)
	{
		case TEXTOID:
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case NAMEOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));

			/* Don't return a zero length string, return an empty string */
			if (str[0] == '\0')
				return "''";

			/* quote string with ' */
			appendStringInfoChar(&result, '\'');
			for (p=str; *p; ++p)
			{
				/* Escape single quotes as doubled '' */
				if (*p == '\'')
					appendStringInfoChar(&result, "\'");
				appendStringInfoChar(&result, *p);
			}
			appendStringInfoChar(&result, '\'');
			break;
		case INT8OID:
		case INT2OID:
		case INT4OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			appendStringInfoString(&result, DatumGetCString(OidFunctionCall1(typoutput, datum)));
			break;
		case INTERVALOID:
			elog(ERROR, "could not convert interval to OGR query form");
			return NULL;
			break;			
		default:
			elog(ERROR, "could convert unknown type (%d) to OGR query form", type);
			return NULL;
	}

	return result.data;
}

static bool 
ogrDeparseConst(Const* constant, OgrDeparseCtx *context)
{
	if (constant->constisnull)
	{
		appendStringInfoString(context->buf, "NULL");
	}
	else
	{
		/* get a string representation of the value */
		char *c = ogrStringFromDatum(constant->constvalue, constant->consttype);
		if ( c == NULL )
		{
			return false;
		}
		else
		{
			appendStringInfoString(context->buf, c);
		}
	}
	return true;
}




static bool 
ogrOperatorIsSupported(const char *opname)
{
	const *ogrOperators[10] = { "<", ">", "<=", ">=", "<>", "=", "!=", "&&", "AND", "OR" };
	int i;
	for ( i = 0; i < 10; i++ )
	{
		if ( strcasecmp(opname, ogrOperators[i]) == 0 )
			return true;
	}
	return false;
}


static vool
ogrDeparseOpExpr(OpExpr* node, OgrDeparseCtx *context)
{
	StringInfo buf = context->buf;
	HeapTuple tuple;
	Form_pg_operator form;
	char oprkind;
	char *opname;
	ListCell *arg;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;
	opname = NameStr(form->oprname);

	/* Don't deparse expressions we cannot support */
	if ( ! ogrOperatorIsSupported(opname) )
		return false;

	/* TODO: Maybe here coerce some operators into OGR equivalents (!= to <>) */
	/* TODO: When a && operator is found, we need to do special handling to send the box back up to OGR for SetSpatialFilter */

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
	       (oprkind == 'l' && list_length(node->args) == 1) ||
	       (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the operator expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if ( oprkind == 'r' || oprkind == 'b' )
	{
		arg = list_head(node->args);
		/* recurse for nested operations */
		ogrDeparseExpr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Operator symbol */
	appendStringInfoString(buf, opname);
	
	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		/* recurse for nested operations */
		ogrDeparseExpr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
	return true;
	
}

static bool
ogrDeparseExpr(Expr *node, OgrDeparseCtx *context)
{
	if ( node == NULL )
		return;

	switch ( nodeTag(node) )
	{
		case T_OpExpr:
			return ogrDeparseOpExpr((OpExpr *) node, context);
		case T_Const:
		case T_Var:
		case T_Param:
		case T_ArrayRef:
		case T_FuncExpr:
		case T_DistinctExpr:
		case T_ScalarArrayOpExpr:
		case T_RelabelType:
		case T_BoolExpr:
		case T_NullTest:
		case T_ArrayExpr:
		default:
			elog(ERROR, "unsupported expression type for deparse: %d", (int) nodeTag(node));
			return false;
	}
	
}


bool
ogrDeparse(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, List *exprs, List **params)
{
	OgrDeparseCtx context;
	ListCell *lc;
	bool first = true;

	/* initialize result list to empty */
	if (params)
		*params = NIL;

	/* Set up context struct for recursion */
	context.buf = buf;
	context.root = root;
	context.foreignrel = foreignrel;
	context.paramslist = params;

	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition */
		if ( ! first )
		{
			appendStringInfoString(buf, " AND ");
			is_first = false;
		}

		appendStringInfoChar(buf, '(');
		ogrDeparseExpr(ri->clause, &context);
		appendStringInfoChar(buf, ')');
	}	
	
	return true;	
}



