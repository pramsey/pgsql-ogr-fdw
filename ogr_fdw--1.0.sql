/* ogr_fdw/ogr_fdw--1.0.sql */

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
