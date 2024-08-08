
/*-------------------------------------------------------------------------
 *
 * ogr_fdw_func.c
 *          Helper functions for OGR FDW
 *
 * Copyright (c) 2020, Paul Ramsey <pramsey@cleverelephant.ca>
 *
 *-------------------------------------------------------------------------
 */

#include "ogr_fdw.h"
#include "ogr_fdw_gdal.h"
#include "ogr_fdw_common.h"

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
// #include <catalog/pg_type_d.h>
#include <utils/array.h>
#include <utils/builtins.h>


/**
*/
Datum ogr_fdw_drivers(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ogr_fdw_drivers);
Datum ogr_fdw_drivers(PG_FUNCTION_ARGS)
{

	/* Array building */
	size_t arr_nelems = 0;
	Datum *arr_elems;
	ArrayType *arr;
	Oid elem_type = TEXTOID;
	int16 elem_len;
	bool elem_byval;
	char elem_align;
	int num_drivers;
	int i;

	if (GDALGetDriverCount() <= 0)
		GDALAllRegister();

#if (GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(1,11,0))
	num_drivers = GDALGetDriverCount();
#else
	num_drivers = OGRGetDriverCount();
#endif

	if (num_drivers < 1)
		PG_RETURN_NULL();

 	arr_elems = palloc0(num_drivers * sizeof(Datum));
    get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

	for (i = 0; i < num_drivers; i++) {
#if GDAL_VERSION_MAJOR <= 1
		OGRSFDriverH hDriver = OGRGetDriver(i);
		text *txtName = cstring_to_text(OGR_Dr_GetName(hDriver));
		arr_elems[arr_nelems++] = PointerGetDatum(txtName);
#else
		GDALDriverH hDriver = GDALGetDriver(i);
		if (GDALGetMetadataItem(hDriver, GDAL_DCAP_VECTOR, NULL) != NULL) {
			const char *strName = OGR_Dr_GetName(hDriver);
			text *txtName = cstring_to_text(strName);
			arr_elems[arr_nelems++] = PointerGetDatum(txtName);
		}
#endif
	}

	arr = construct_array(arr_elems, arr_nelems, elem_type, elem_len, elem_byval, elem_align);
	PG_RETURN_ARRAYTYPE_P(arr);
}

/**
*/
Datum ogr_fdw_version(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ogr_fdw_version);
Datum ogr_fdw_version(PG_FUNCTION_ARGS)
{
	const char *gdal_ver = GDAL_RELEASE_NAME;
	const char *ogr_fdw_ver = OGR_FDW_RELEASE_NAME;
	char ver_str[256];
	snprintf(ver_str, sizeof(ver_str), "OGR_FDW=\"%s\" GDAL=\"%s\"", ogr_fdw_ver, gdal_ver);
	PG_RETURN_TEXT_P(cstring_to_text(ver_str));
}

/**
*/
Datum ogr_fdw_table_sql(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ogr_fdw_table_sql);
Datum ogr_fdw_table_sql(PG_FUNCTION_ARGS)
{
	char *serverName;
	char *layerName;
	char *tableName = NULL;
	bool launderColumnNames;
	bool launderTableName;

	ForeignServer* server;
	OgrConnection ogr;
	OGRErr err;
	OGRLayerH ogrLayer;
	stringbuffer_t buf;

	// because we accept NULL for tableName, we need to make the function
	// non-STRICT and check all the arguments for NULL
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(3) || PG_ARGISNULL(4))
	{
		PG_RETURN_NULL();
	}
	if (!PG_ARGISNULL(2))
	{
		tableName = text_to_cstring(PG_GETARG_TEXT_PP(2));
	}

	serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	layerName = text_to_cstring(PG_GETARG_TEXT_PP(1));
	launderColumnNames = PG_GETARG_BOOL(3);
	launderTableName = PG_GETARG_BOOL(4);

	server = GetForeignServerByName(serverName, false);
	ogr = ogrGetConnectionFromServer(server->serverid, OGR_UPDATEABLE_FALSE);

	ogrLayer = GDALDatasetGetLayerByName(ogr.ds, layerName);
	if (ogrLayer)
	{
		stringbuffer_init(&buf);
		err = ogrLayerToSQL(ogrLayer,
							serverName,
							launderTableName,
							launderColumnNames,
							tableName,
							ogrGetGeometryOid() != BYTEAOID,
							&buf);

		if (err != OGRERR_NONE)
		{
			ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				errmsg("Cannot generate SQL for: %s", layerName),
				errhint("GDAL Error %d: %s", CPLGetLastErrorNo(), CPLGetLastErrorMsg())));
		}

		ogrFinishConnection(&ogr);

		PG_RETURN_TEXT_P(cstring_to_text(stringbuffer_getstring(&buf)));
	}
	else
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
			errmsg("Unable to find OGR Layer: %s", layerName)));
	}
}


/**
*/
Datum ogr_fdw_layers(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(ogr_fdw_layers);
Datum ogr_fdw_layers(PG_FUNCTION_ARGS)
{
	ForeignServer* server = GetForeignServerByName(text_to_cstring(PG_GETARG_TEXT_PP(0)), false);
	OgrConnection ogr = ogrGetConnectionFromServer(server->serverid, OGR_UPDATEABLE_FALSE);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	for (int i = 0; i < GDALDatasetGetLayerCount(ogr.ds); i++)
	{
		OGRLayerH ogrLayer = GDALDatasetGetLayer(ogr.ds, i);

		if (!ogrLayer)
		{
			ereport(WARNING,
				(errcode(ERRCODE_FDW_ERROR),
				errmsg("Skipping OGR layer %d, unable to read layer", i),
				errhint("GDAL Error %d: %s", CPLGetLastErrorNo(), CPLGetLastErrorMsg())));
			continue;
		}

		Datum		values[1];
		bool		nulls[1];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(OGR_L_GetName(ogrLayer));
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	ogrFinishConnection(&ogr);

	return (Datum) 0;
}
