/*-------------------------------------------------------------------------
 *
 * ogr_fdw_deparse.c
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2015, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 * Convert parse tree to a QueryExpression as described at 
 * http://gdal.org/ogr_sql.html
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
	OGRGeometryH geom;        /* if filter contains a geometry constant, it resides here */
	OgrFdwState *state;       /* to convert local column names to OGR names */
} OgrDeparseCtx;

/* Local function signatures */
static bool ogrDeparseExpr(Expr *node, OgrDeparseCtx *context);
// static void ogrDeparseOpExpr(OpExpr* node, OgrDeparseCtx *context);

static void setStringInfoLength(StringInfo str, int len)
{
	str->len = len;
	str->data[len] = '\0';
}

static char *
ogrStringFromDatum(Datum datum, Oid type)
{
	StringInfoData result;
	regproc typoutput;
	HeapTuple tuple;
	char *str, *p;

	/* Special handling for boolean */
	if ( type == BOOLOID )
	{
		if ( datum )
			return "1=1";
		else
			return "1=0";
	}

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for type %u", type);
	}
	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
	ReleaseSysCache(tuple);

	initStringInfo(&result);

	/* Special handling to convert a geometry to a bbox needed here */
	if ( type == GEOMETRYOID )
	{
		elog(ERROR, "got a GEOMETRY!");
		return NULL;
	}

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

			/* wrap string with ' */
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
		default:
			elog(DEBUG1, "could not convert type (%d) to OGR query form", type);
			return NULL;
	}

	return result.data;
}

static bool
ogrDeparseConst(Const* constant, OgrDeparseCtx *context)
{
	/* TODO: Can OGR do anythign w/ NULL? */
	if (constant->constisnull)
	{
		appendStringInfoString(context->buf, "NULL");
	}
	/* Use geometry as a spatial filter? */
	else if ( constant->consttype == GEOMETRYOID )
	{
		/*
		 * For geometry we need to convert the gserialized constant into 
		 * an OGRGeometry for the OGR spatial filter.
		 * For that, we can use the type's "send" function
		 * which takes in gserialized and spits out EWKB.
		 */
		Oid sendfunction;
		bool typeIsVarlena;
		Datum wkbdatum;
		char *gser;
		char *wkb;
		int wkb_size;
		OGRGeometryH ogrgeom;
		OGRErr err;

		/*
		 * Given a type oid (geometry in this case),
		 * look up the "send" function that takes in
		 * serialized input and outputs the binary (WKB) form.
		 */
		getTypeBinaryOutputInfo(constant->consttype, &sendfunction, &typeIsVarlena);
		wkbdatum = OidFunctionCall1(sendfunction, constant->constvalue);
		
		/* 
		 * Convert the WKB into an OGR geometry 
		 */
		gser = DatumGetPointer(wkbdatum);
		wkb = VARDATA(gser);
		wkb_size = VARSIZE(gser) - VARHDRSZ;
		err = OGR_G_CreateFromWkb((unsigned char *)wkb, NULL, &ogrgeom, wkb_size);
		
		/* 
		 * Save the result
		 */
		if ( err != OGRERR_NONE )
		{
			if ( ! context->geom )
				context->geom = ogrgeom;
			else
				elog(WARNING, "got two geometries in OGR FDW query, only using the first");
		}
		/* 
		 * geometry doesn't play a role in the deparsed SQL 
		 */
		return false;
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
ogrDeparseParam(Param *node, OgrDeparseCtx *context)
{
	elog(DEBUG3, "got into ogrDeparseParam code");
	return false;
}

static bool
ogrIsLegalVarName(const char *varname)
{
	size_t len = strlen(varname);
	int i;
		
	for ( i = 0; i < len; i++ )
	{
		char c = varname[i];
			
		/* First char must be a-zA-Z */
		if ( i == 0 && ! ((c>=97&&c<=122)||(c>=65&&c<=90)) )
			return false;
		
		/* All other chars must be 0-9a-zA-Z_ */
		if ( ! ((c>=97&&c<=122)||(c>=65&&c<=90)||(c>=48&&c<=59)||(c==96)) )
			return false;

	}
	return true;
}

static bool
ogrDeparseVar(Var *node, OgrDeparseCtx *context)
{
	StringInfoData *buf = context->buf;
	
	if (node->varno == context->foreignrel->relid && node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		int i;
		OgrFdwTable *table = context->state->table;
		OGRLayerH lyr = context->state->ogr.lyr;
		bool done = false;

		/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
		Assert(!IS_SPECIAL_VARNO(node->varno));

		/* TODO: Handle case of mapping columns to OGR columns that don't share their name */
		/* TODO: Lookup OGR column name by going from varattno -> OGR via a table/OGR map */

		for ( i = 0; i < table->ncols; i++ )
		{
			if ( table->cols[i].pgattnum == node->varattno )
			{
				const char *fldname = NULL;
				
				if ( table->cols[i].ogrvariant == OGR_FID )
				{
					fldname = OGR_L_GetFIDColumn(lyr);
					if ( ! fldname || strlen(fldname) == 0 )
						fldname = "fid";
				}
				else if ( table->cols[i].ogrvariant == OGR_FIELD )
				{
					OGRFeatureDefnH fd = OGR_L_GetLayerDefn(lyr);
					OGRFieldDefnH fld = OGR_FD_GetFieldDefn(fd, table->cols[i].ogrfldnum);
					fldname = OGR_Fld_GetNameRef(fld);
				}
				
				if ( fldname )
				{
					if ( ogrIsLegalVarName(fldname) )
						appendStringInfoString(buf, fldname);
					else
						appendStringInfo(buf, "\"%s\"", fldname);
					
					done = true;
				}
			}
		}
		
		return done;
	}
	else
	{
		elog(ERROR, "got to param handling section of ogrDeparseVar");
		return false;
	}
	
	return true;
}

static int ogrOperatorCmpFunc(const void * a, const void * b)
{
	return strcasecmp(*(const char**)a, *(const char**)b);
}

static bool 
ogrOperatorIsSupported(const char *opname)
{
	/* IMPORTANT */
	/* This array MUST be in sorted order or the bsearch will fail */
	static const char * ogrOperators[10] = { "!=", "&&", "<", "<=", "<>", "=", ">", ">=", "~~", "~~*" };

	elog(DEBUG3, "ogrOperatorIsSupported got operator '%s'", opname);
	
	if ( bsearch(&opname, ogrOperators, 10, sizeof(char*), ogrOperatorCmpFunc) )
		return true;
	else
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
	bool result = true;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;
	opname = NameStr(form->oprname);

	/* Don't deparse expressions we cannot support */
	if ( ! ogrOperatorIsSupported(opname) )
	{
		ReleaseSysCache(tuple);
		return false;
	}

	/* TODO: When a && operator is found, we need to do special */
	/*       handling to send the box back up to OGR for SetSpatialFilter */

	/* Overlaps operator is special case: if one side is a constant, */
	/* then we can pass it as a spatial filter to OGR */
	if ( strcmp("&&", opname) == 0 )
	{
		// Expr *r_arg = lfirst(list_head(node->args));
		// Expr *l_arg = lfirst(list_tail(node->args));
		// Const *constant;

		elog(DEBUG1, "whoa, dude, found a && operator");

		/* Specifically, we need a Geometry Const on one side and a Var */
		/* column on the other side that is from the FDW relation */
		/* Both of those implies and OGR spatial filter can be reasonably */
		/* set. */

		// if ( nodeTag(r_arg) == T_Const )
		// 	constant = (Const*)r_arg;
		// else if ( nodeTag(l_arg) == T_Const)
		// 	constant = (Const*)l_arg;
		// else
		// 	return false;
		
		// if ( constant->consttype != GEOMETRYOID )

		ReleaseSysCache(tuple);

		return false;
	}

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
		result &= ogrDeparseExpr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Special case, the 'LIKE' operator is converted to ~~ */
	/* by PgSQL, so we have to convert it back here */
	/* All OGR string comparisons are case insensitive, so we just */
	/* use 'ILIKE' all the time. */
	if ( streq(opname, "~~") || streq(opname, "~~*") )
		opname = "ILIKE";
	
	/* Operator symbol */
	appendStringInfoString(buf, opname);
	
	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		/* recurse for nested operations */
		result &= ogrDeparseExpr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
	return result;
	
}

static bool
ogrDeparseBoolExpr(BoolExpr *node, OgrDeparseCtx *context)
{
	const char *op = NULL;		/* keep compiler quiet */
	ListCell *lc;
	bool first = true;
	bool result = true;
	int len_save_all, len_save_part;
	int boolop = node->boolop;
	int result_total = 0;
	StringInfo buf = context->buf;

	switch (boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;

		/* OGR SQL cannot handle "NOT" */
		case NOT_EXPR:
			return false;
	}

	len_save_all = buf->len;

	appendStringInfoChar(buf, '(');
	foreach(lc, node->args)
	{
		
		len_save_part = buf->len;
		
		/* Connect expressions and parenthesize each condition */
		if ( ! first )
			appendStringInfo(buf, " %s ", op);

		/* Unparse the expression, if possible */
		result = ogrDeparseExpr((Expr *) lfirst(lc), context);
		result_total += result;
		
		/* We can backtrack just this term for AND expressions */
		if ( boolop == AND_EXPR && ! result )
			setStringInfoLength(buf, len_save_part);

		/* We have to drop the whole thing if we can't get every part of an OR expression */
		if ( boolop == OR_EXPR && ! result )
			break;
		
		/* Don't flip the "first" bit until we get a good expression */
		if ( first && result )
			first = false;
	}
	appendStringInfoChar(buf, ')');
	
	/* We have to drop the whole thing if we can't get every part of an OR expression */
	if ( boolop == OR_EXPR && ! result )
		setStringInfoLength(buf, len_save_all);
	
	return result_total > 0;
}


static bool
ogrDeparseRelabelType(RelabelType *node, OgrDeparseCtx *context)
{
	if (node->relabelformat != COERCE_IMPLICIT_CAST)
		elog(WARNING, "Received a non-implicit relabel expression but did not handle it");

	return ogrDeparseExpr(node->arg, context);
}

static bool
ogrDeparseNullTest(NullTest *node, OgrDeparseCtx *context)
{
	StringInfo buf = context->buf;

    appendStringInfoChar(buf, '(');
    ogrDeparseExpr(node->arg, context);
    if (node->nulltesttype == IS_NULL)
        appendStringInfoString(buf, " IS NULL)");
    else
        appendStringInfoString(buf, " IS NOT NULL)");

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
			/* Handle "OR" and "NOT" queries */
			return ogrDeparseBoolExpr((BoolExpr *) node, context);
		case T_NullTest:
			/* Handle "IS NULL" queries */
			return ogrDeparseNullTest((NullTest *) node, context);
		case T_RelabelType:
			return ogrDeparseRelabelType((RelabelType *) node, context);
		case T_ScalarArrayOpExpr:
			/* TODO: Handle this to support the "IN" operator */
			elog(NOTICE, "unsupported OGR FDW expression type, T_ScalarArrayOpExpr");
			return false;
		case T_ArrayRef:
			elog(NOTICE, "unsupported OGR FDW expression type, T_ArrayRef");
			return false;
		case T_ArrayExpr:
			elog(NOTICE, "unsupported OGR FDW expression type, T_ArrayExpr");
			return false;
		case T_FuncExpr:
			elog(NOTICE, "unsupported OGR FDW expression type, T_FuncExpr");
			return false;
		case T_DistinctExpr:
			elog(NOTICE, "unsupported OGR FDW expression type, T_DistinctExpr");
			return false;
		default:
			elog(NOTICE, "unsupported OGR FDW expression type for deparse: %d", (int) nodeTag(node));
			return false;
	}
	
}


bool
ogrDeparse(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, List *exprs, OgrFdwState *state, List **params)
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
	context.geom = NULL;
	context.state = state;
	// context.geom_op = NULL;
	// context.geom_func = NULL;

	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		int len_save = buf->len;
		bool result;

		/* Connect expressions with "AND" and parenthesize each condition */
		if ( ! first )
		{
			appendStringInfoString(buf, " AND ");
		}

		/* Unparse the expression, if possible */
		// appendStringInfoChar(buf, '(');
		result = ogrDeparseExpr(ri->clause, &context);
		// appendStringInfoChar(buf, ')');

		if ( ! result )
		{
			/* Couldn't unparse some portion of the expression, so rewind the stringinfo */
			setStringInfoLength(buf, len_save);
		}
		
		/* Don't flip the "first" bit until we get a good expression */
		if ( first && result )
			first = false;
	}	
	
	return true;	
}




