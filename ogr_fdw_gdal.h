
#ifndef _OGR_FDW_GDAL_H
#define _OGR_FDW_GDAL_H 1

/*
 * OGR library API
 */
#include "gdal.h"
#include "ogr_api.h"
#include "ogr_srs_api.h"
#include "cpl_error.h"
#include "cpl_string.h"

/* 
 * As far as possible code is GDAL2 compliant, and these
 * mappings are used to convert to GDAL1-style function
 * names. For GDALDatasetH opening, there are specific
 * code blocks to handle version differences between 
 * GDALOpenEx() and OGROpen()
 */
#if GDAL_VERSION_MAJOR < 2

/* Redefine variable types */
#define GDALDatasetH OGRDataSourceH
#define GDALDriverH OGRSFDriverH

/* Rename GDAL2 functions to OGR equivs */
#define GDALGetDriverCount() OGRGetDriverCount()
#define GDALGetDriver(i) OGRGetDriver(i)
#define GDALAllRegister() OGRRegisterAll()
#define GDALGetDriverByName(name) OGRGetDriverByName(name)
#define GDALClose(ds) OGR_DS_Destroy(ds)
#define GDALDatasetGetLayerByName(ds,name) OGR_DS_GetLayerByName(ds,name)
#define GDALDatasetGetLayerCount(ds) OGR_DS_GetLayerCount(ds)
#define GDALDatasetGetLayer(ds,i) OGR_DS_GetLayer(ds,i)
#define GDALGetDriverShortName(dr) OGR_Dr_GetName(dr)
#define GDALGetDatasetDriver(ds) OGR_DS_GetDriver(ds)
#define GDALDatasetTestCapability(ds,cap) OGR_Dr_TestCapability(ds,cap)

#endif /* GDAL 1 support */

#endif /* _OGR_FDW_GDAL_H */