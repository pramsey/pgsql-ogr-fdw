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
static bool ogrDeparseExpr(Expr *node, OgrDeparseCtx *context);
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
					appendStringInfoChar(&result, '\'');
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
ogrDeparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo *root)
{
	RangeTblEntry *rte;
	char *colname = NULL;
	// List *options;
	// ListCell *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	/*
	 * MySQL FDW uses foreign column options to map remote columns to local columns
	 * OGR could do that or could just try and match up names, but we'll need a correspondance
	 * structure to do that.
	*/
	// options = GetForeignColumnOptions(rte->relid, varattno);
	// foreach(lc, options)
	// {
	// 	DefElem *def = (DefElem *) lfirst(lc);
	//
	// 	if (strcmp(def->defname, "column_name") == 0)
	// 	{
	// 		colname = defGetString(def);
	// 		break;
	// 	}
	// }

	/*
	 * For now we hope that all local column names match remote column names.
	 * This will be true if users use ogr_fdw_info tool, but otherwise might 
	 * not be.
	 */
	
	/* TODO: Handle case of mapping columns to OGR columns that don't share their name */

	if (colname == NULL)
		colname = get_relid_attribute_name(rte->relid, varattno);

	appendStringInfoString(buf, quote_identifier(colname));
	return true;
}

static bool
ogrDeparseParam(Param *node, OgrDeparseCtx *context)
{
	elog(ERROR, "got into ogrDeparseParam code");
	return false;
	// if (context->params_list)
	// {
	// 	int pindex = 0;
	// 	ListCell *lc;
	//
	// 	/* find its index in params_list */
	// 	foreach(lc, *context->params_list)
	// 	{
	// 		pindex++;
	// 		if (equal(node, (Node *) lfirst(lc)))
	// 			break;
	// 	}
	// 	if (lc == NULL)
	// 	{
	// 		/* not in list, so add it */
	// 		pindex++;
	// 		*context->params_list = lappend(*context->params_list, node);
	// 	}
	//
	// 	mysql_print_remote_param(pindex, node->paramtype, node->paramtypmod, context);
	// }
	// else
	// {
	// 	mysql_print_remote_placeholder(node->paramtype, node->paramtypmod, context);
	// }
}


static bool
ogrDeparseVar(Var *node, OgrDeparseCtx *context)
{
	if (node->varno == context->foreignrel->relid && node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		ogrDeparseColumnRef(context->buf, node->varno, node->varattno, context->root);
	}
	else
	{
		elog(ERROR, "got to param handling section of ogrDeparseVar");
		return false;
		// /* Treat like a Param */
		// if (context->params_list)
		// {
		// 	int pindex = 0;
		// 	ListCell *lc;
		//
		// 	/* find its index in params_list */
		// 	foreach(lc, *context->params_list)
		// 	{
		// 		pindex++;
		// 		if (equal(node, (Node *) lfirst(lc)))
		// 			break;
		// 	}
		// 	if (lc == NULL)
		// 	{
		// 		/* not in list, so add it */
		// 		pindex++;
		// 		*context->params_list = lappend(*context->params_list, node);
		// 	}
		//
		// 	mysql_print_remote_param(pindex, node->vartype, node->vartypmod, context);
		// }
		// else
		// {
		// 	mysql_print_remote_placeholder(node->vartype, node->vartypmod, context);
		// }
	}
	return true;
}


static bool 
ogrOperatorIsSupported(const char *opname)
{
	const char * ogrOperators[10] = { "<", ">", "<=", ">=", "<>", "=", "!=", "&&", "AND", "OR" };
	int i;
	for ( i = 0; i < 10; i++ )
	{
		if ( strcasecmp(opname, ogrOperators[i]) == 0 )
			return true;
	}
	return false;
}


static bool
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
		return false;

	switch ( nodeTag(node) )
	{
		case T_OpExpr:
			return ogrDeparseOpExpr((OpExpr *) node, context);
		case T_Const:
			return ogrDeparseConst((Const *) node, context);
		case T_Var:
			return ogrDeparseVar((Var *) node, context);
		case T_Param:
			return ogrDeparseParam((Param *) node, context);
		case T_BoolExpr:
			elog(ERROR, "unsupported expression type, T_BoolExpr");
			return false;
		case T_NullTest:
			elog(ERROR, "unsupported expression type, T_NullTest");
			return false;
		case T_ArrayRef:
			elog(ERROR, "unsupported expression type, T_ArrayRef");
			return false;
		case T_ArrayExpr:
			elog(ERROR, "unsupported expression type, T_ArrayExpr");
			return false;
		case T_FuncExpr:
			elog(ERROR, "unsupported expression type, T_FuncExpr");
			return false;
		case T_DistinctExpr:
			elog(ERROR, "unsupported expression type, T_DistinctExpr");
			return false;
		case T_ScalarArrayOpExpr:
			elog(ERROR, "unsupported expression type, T_ScalarArrayOpExpr");
			return false;
		case T_RelabelType:
			elog(ERROR, "unsupported expression type, T_RelabelType");
			return false;
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
	context.params_list = params;

	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition */
		if ( ! first )
		{
			appendStringInfoString(buf, " AND ");
			first = false;
		}

		appendStringInfoChar(buf, '(');
		ogrDeparseExpr(ri->clause, &context);
		appendStringInfoChar(buf, ')');
	}	
	
	return true;	
}



