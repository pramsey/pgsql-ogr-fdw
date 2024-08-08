/* ogr_fdw/ogr_fdw--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ogr_fdw" to load this file. \quit

CREATE FUNCTION ogr_fdw_handler()
	RETURNS fdw_handler
	AS 'MODULE_PATHNAME'
	LANGUAGE 'c' STRICT;

CREATE FUNCTION ogr_fdw_validator(text[], oid)
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE 'c' STRICT;

CREATE FOREIGN DATA WRAPPER ogr_fdw
	HANDLER ogr_fdw_handler
	VALIDATOR ogr_fdw_validator;

CREATE OR REPLACE FUNCTION ogr_fdw_version()
	RETURNS text
	AS 'MODULE_PATHNAME', 'ogr_fdw_version'
	LANGUAGE 'c'
	IMMUTABLE STRICT
	PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ogr_fdw_drivers()
	RETURNS text[]
	AS 'MODULE_PATHNAME', 'ogr_fdw_drivers'
	LANGUAGE 'c'
	IMMUTABLE STRICT
	PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ogr_fdw_table_sql(server_name text, layer_name text, table_name text DEFAULT NULL, launder_column_names boolean DEFAULT TRUE, launder_table_name boolean DEFAULT TRUE)
	RETURNS text
	AS 'MODULE_PATHNAME', 'ogr_fdw_table_sql'
	LANGUAGE 'c'
	STABLE
	PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ogr_fdw_layers(server_name text)
	RETURNS TABLE (layer_name text)
	AS 'MODULE_PATHNAME', 'ogr_fdw_layers'
	LANGUAGE 'c'
	IMMUTABLE STRICT
	PARALLEL SAFE;
