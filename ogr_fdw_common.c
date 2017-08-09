/*-------------------------------------------------------------------------
 *
 * ogr_fdw_common.c
 *		  foreign-data wrapper for GIS data access.
 *
 * Copyright (c) 2014-2016, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

#include "ogr_fdw_gdal.h"
#include "ogr_fdw_common.h"
#include "stringbuffer.h"

/* Prototype for function that must be defined in PostgreSQL (it is) */
/* and in ogr_fdw_info (it is) */
const char * quote_identifier(const char *ident);


/*
 * Append a SQL string literal representing "val" to buf.
 */
static void
ogrDeparseStringLiteral(stringbuffer_t *buf, const char *val)
{
	const char *valptr;

	/*
	 * Rather than making assumptions about the remote server's value of
	 * standard_conforming_strings, always use E'foo' syntax if there are any
	 * backslashes.  This will fail on remote servers before 8.1, but those
	 * are long out of support.
	 */
	if ( strchr(val, '\\') != NULL )
	{
		stringbuffer_append_char(buf, 'E');
	}
	stringbuffer_append_char(buf, '\'');
	for ( valptr = val; *valptr; valptr++ )
	{
		char ch = *valptr;
		if ( ch == '\'' || ch == '\\' )
		{
			stringbuffer_append_char(buf, ch);
		}
		stringbuffer_append_char(buf, ch);
	}
	stringbuffer_append_char(buf, '\'');
}

void
ogrStringLaunder(char *str)
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

static char *
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
			CPLError(CE_Failure, CPLE_AssertionFailed, 
			         "unsupported GDAL type '%s'", 
			         OGR_GetFieldTypeName(ogr_type));
			return NULL;
	}
	return NULL;
}

static void
ogrGeomTypeToPgGeomType(stringbuffer_t *buf, OGRwkbGeometryType gtype)
{
	switch(wkbFlatten(gtype))
	{
	    case wkbUnknown:
			stringbuffer_append(buf, "Geometry");
			break;
	    case wkbPoint:
			stringbuffer_append(buf, "Point");
			break;
		case wkbLineString:
			stringbuffer_append(buf, "LineString");
			break;
		case wkbPolygon:
			stringbuffer_append(buf, "Polygon");
			break;
		case wkbMultiPoint:
			stringbuffer_append(buf, "MultiPoint");
			break;
		case wkbMultiLineString:
			stringbuffer_append(buf, "MultiLineString");
			break;
		case wkbMultiPolygon:
			stringbuffer_append(buf, "MultiPolygon");
			break;
		case wkbGeometryCollection:
			stringbuffer_append(buf, "GeometryCollection");
			break;
#if GDAL_VERSION_MAJOR >= 2
		case wkbCircularString:
			stringbuffer_append(buf, "CircularString");
			break;
		case wkbCompoundCurve:
			stringbuffer_append(buf, "CompoundCurve");
			break;
		case wkbCurvePolygon:
			stringbuffer_append(buf, "CurvePolygon");
			break;
		case wkbMultiCurve:
			stringbuffer_append(buf, "MultiCurve");
			break;
		case wkbMultiSurface:
			stringbuffer_append(buf, "MultiSurface");
			break;
#endif
		case wkbNone:
			CPLError(CE_Failure, CPLE_AssertionFailed, "Cannot handle OGR geometry type wkbNone");
		default:
			CPLError(CE_Failure, CPLE_AssertionFailed, "Cannot handle OGR geometry type '%d'", gtype);
	}

#if GDAL_VERSION_MAJOR >= 2 
	if ( wkbHasZ(gtype) )
#else
	if ( gtype & wkb25DBit )
#endif
		stringbuffer_append(buf, "Z");
	
#if GDAL_VERSION_MAJOR >= 2 && GDAL_VERSION_MINOR >= 1
	if ( wkbHasM(gtype) )
		stringbuffer_append(buf, "M");
#endif

	return;
}

static OGRErr
ogrColumnNameToSQL (const char *ogrcolname, const char *pgtype, int launder_column_names, stringbuffer_t *buf)
{
	char pgcolname[STR_MAX_LEN];
	strncpy(pgcolname, ogrcolname, STR_MAX_LEN);
	ogrStringLaunder(pgcolname);

	if ( launder_column_names )
	{
		stringbuffer_aprintf(buf, ",\n  %s %s", quote_identifier(pgcolname), pgtype);
		if ( ! strcaseeq(pgcolname, ogrcolname) )
        {
            stringbuffer_append(buf, " OPTIONS (column_name ");
            ogrDeparseStringLiteral(buf, ogrcolname);
            stringbuffer_append(buf, ")");
        }
	}
	else
	{
		/* OGR column is PgSQL compliant, we're all good */
		if ( streq(pgcolname, ogrcolname) )
			stringbuffer_aprintf(buf, ",\n  %s %s", quote_identifier(ogrcolname), pgtype);
		/* OGR is mixed case or non-compliant, we need to quote it */
		else
			stringbuffer_aprintf(buf, ",\n  \"%s\" %s", ogrcolname, pgtype);
	}
	return OGRERR_NONE;
}

OGRErr
ogrLayerToSQL (const OGRLayerH ogr_lyr, const char *fdw_server, 
			   int launder_table_names, int launder_column_names,
			   int use_postgis_geometry, stringbuffer_t *buf)
{
	int geom_field_count, i;
	char table_name[STR_MAX_LEN];
	OGRFeatureDefnH ogr_fd = OGR_L_GetLayerDefn(ogr_lyr);
	stringbuffer_t gbuf;
	
	stringbuffer_init(&gbuf);

	if ( ! ogr_fd )
	{
		CPLError(CE_Failure, CPLE_AssertionFailed, "unable to get OGRFeatureDefnH from OGRLayerH");
		return OGRERR_FAILURE;
	}

#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
	geom_field_count = OGR_FD_GetGeomFieldCount(ogr_fd);
#else
	geom_field_count = (OGR_L_GetGeomType(ogr_lyr) != wkbNone);
#endif
	
	/* Process table name */
	strncpy(table_name, OGR_L_GetName(ogr_lyr), STR_MAX_LEN);
	if (launder_table_names)
		ogrStringLaunder(table_name);
	
	/* Create table */
	stringbuffer_aprintf(buf, "CREATE FOREIGN TABLE %s (\n", quote_identifier(table_name));
	
	/* For now, every table we auto-create will have a FID */
	stringbuffer_append(buf, "  fid bigint");
	
	/* Handle all geometry columns in the OGR source */
	for ( i = 0; i < geom_field_count; i++ )
	{
		int srid = 0;
#if GDAL_VERSION_MAJOR >= 2 || GDAL_VERSION_MINOR >= 11
		OGRGeomFieldDefnH gfld = OGR_FD_GetGeomFieldDefn(ogr_fd, i);
		OGRwkbGeometryType gtype = OGR_GFld_GetType(gfld);
		const char *geomfldname = OGR_GFld_GetNameRef(gfld);
		OGRSpatialReferenceH gsrs = OGR_GFld_GetSpatialRef(gfld);
#else
		OGRwkbGeometryType gtype = OGR_FD_GetGeomType(ogr_fd);
		const char *geomfldname = "geom";
		OGRSpatialReferenceH gsrs = OGR_L_GetSpatialRef(ogr_lyr);
#endif
		/* Skip geometry type we cannot handle */
		if ( gtype == wkbNone )
			continue;

		/* Clear out our geometry type buffer */
		stringbuffer_clear(&gbuf);

		/* PostGIS geometry type has lots of complex stuff */
		if ( use_postgis_geometry )
		{	
			/* Add geometry type info */
			stringbuffer_append(&gbuf, "Geometry(");
			ogrGeomTypeToPgGeomType(&gbuf, gtype);
			
			/* See if we have an EPSG code to work with */
			if ( gsrs )
			{
				const char *charAuthType;
				const char *charSrsCode;
				OSRAutoIdentifyEPSG(gsrs);
				charAuthType = OSRGetAttrValue(gsrs, "AUTHORITY", 0);
				charSrsCode = OSRGetAttrValue(gsrs, "AUTHORITY", 1);
				if ( charAuthType && strcaseeq(charAuthType, "EPSG") &&
				     charSrsCode && atoi(charSrsCode) > 0 )
				{
					srid = atoi(charSrsCode);
				}
			}
		
			/* Add EPSG number, if figured it out */
			if ( srid )
			{
				stringbuffer_aprintf(&gbuf, ",%d)", srid);
			}
			else
			{
				stringbuffer_append(&gbuf, ")");
			}
		}
		/* Bytea is simple */
		else
		{
			stringbuffer_append(&gbuf, "bytea");
		}
	
		/* Use geom field name if we have it */
		if ( geomfldname && strlen(geomfldname) > 0 )
		{
			ogrColumnNameToSQL(geomfldname, stringbuffer_getstring(&gbuf), launder_column_names, buf);
		}
		/* Or a numbered generic name if we don't */
		else if ( geom_field_count > 1 )
		{
			stringbuffer_aprintf(buf, ",\n  geom%d %s", i, stringbuffer_getstring(&gbuf));
		}
		/* Or just a generic name */
		else
		{
			stringbuffer_aprintf(buf, ",\n  geom %s", stringbuffer_getstring(&gbuf));
		}
	}

	/* Write out attribute fields */
	for ( i = 0; i < OGR_FD_GetFieldCount(ogr_fd); i++ )
	{
		OGRFieldDefnH ogr_fld = OGR_FD_GetFieldDefn(ogr_fd, i);
		ogrColumnNameToSQL(OGR_Fld_GetNameRef(ogr_fld), 
		                   ogrTypeToPgType(ogr_fld), 
		                   launder_column_names, buf);
	}

	/*
	 * Add server name and layer-level options.  We specify remote
	 * layer name as option
	 */
	stringbuffer_aprintf(buf, "\n) SERVER %s\nOPTIONS (", quote_identifier(fdw_server));
	stringbuffer_append(buf, "layer ");
	ogrDeparseStringLiteral(buf, OGR_L_GetName(ogr_lyr));
	stringbuffer_append(buf, ");\n");
	
	return OGRERR_NONE;
}

