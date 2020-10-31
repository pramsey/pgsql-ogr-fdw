
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

	for (int i = 0; i < num_drivers; i++) {
#if GDAL_VERSION_MAJOR < 2
		GDALDriverH hDriver = GDALGetDriver(i);
		if (GDALGetMetadataItem(hDriver, GDAL_DCAP_VECTOR, NULL) != NULL) {
			const char *strName = OGR_Dr_GetName(hDriver);
			text *txtName = cstring_to_text(strName);
			arr_elems[arr_nelems++] = PointerGetDatum(txtName);
		}
#else
		OGRSFDriverH hDriver = OGRGetDriver(i);
		text *txtName = cstring_to_text(OGR_Dr_GetName(hDriver));
		arr_elems[arr_nelems++] = PointerGetDatum(txtName);
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
	text* ver_txt = cstring_to_text(ver_str);
	PG_RETURN_TEXT_P(ver_txt);
}


