# PostgreSQL OGR Foreign Data Wrapper

## Motivation

OGR is the vector half of the [GDAL](http://www.gdal.org/) spatial data access library. It allows access to a [large number of GIS data formats](http://www.gdal.org/ogr_formats.html) using a [simple C API](http://www.gdal.org/ogr__api_8h.html) for data reading and writing. Since OGR exposes a simple table structure and PostgreSQL [foreign data wrappers](https://wiki.postgresql.org/wiki/Foreign_data_wrappers) allow access to table structures, the fit seems pretty perfect. 

## Limitations

This implementation currently has the following limitations:

* **PostgreSQL 9.3+** This wrapper does not support the FDW implementations in older versions of PostgreSQL.
* **Tables are read-only.** Foreign data wrappers support read/write and many OGR drivers support read/write, so this limitation can be removed given some development time.
* **Only non-spatial query restrictions are pushed down to the OGR driver.** PostgreSQL foreign data wrappers support delegating portions of the SQL query to the underlying data source, in this case OGR. This implementation currently pushes down only non-spatial query restrictions, and only for the small subset of comparison operators (>, <, <=, >=, =) supported by OGR.
* **Spatial restrictions are not pushed down.** OGR can handle basic bounding box restrictions and even (for some drivers) more explicit intersection restrictions, but those are not passed to the OGR driver yet.
* **OGR connections every time** Rather than pooling OGR connections, each query makes (and disposes of) two new ones, which seems to be the largest performance drag at the moment for restricted (small) queries.
* **All columns are retrieved every time.** PostgreSQL foreign data wrappers don't require all columns all the time, and some efficiencies can be gained by only requesting the columns needed to fulfill a query. This would be a minimal efficiency improvement, but can be removed given some development time, since the OGR API supports returning a subset of columns.

## Basic Operation

In order to access geometry data from OGR, the PostGIS extension has to be installed: if it is not installed, geometry will be represented as bytea columns, with well-known binary (WKB) values.

To build the wrapper, make sure you have the GDAL library and development packages (is `gdal-config` on your path?) installed, as well as the PostgreSQL development packages (is `pg_config` on your path?)

Build the wrapper with `make` and `make install`. Now you are ready to create a foreign table.

First install the `postgis` and `ogr_fdw` extensions in your database.

    -- Install the required extensions
    CREATE EXTENSION postgis;
    CREATE EXTENSION ogr_fdw;

For a test data set, copy the `pt_two` example shape file from the `data` directory to a location where the PostgreSQL server can read it (like `/tmp/test/` for example). 

Use the `ogr_fdw_info` tool to read an OGR data source and output a server and table definition for a particular layer. (You can write these manually, but the utility makes it a little more foolproof.)

    > ogr_fdw_info -s /tmp/test
    
    Layers:
      pt_two
    
    > ogr_fdw_info -s /tmp/test -l pt_two
    
    CREATE SERVER myserver
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource '/tmp/test',
        format 'ESRI Shapefile' );

    CREATE FOREIGN TABLE pt_two (
      fid integer,
      geom geometry,
      name varchar,
      age integer,
      height real,
      birthdate date )
      SERVER myserver
      OPTIONS ( layer 'pt_two' );
    
Copy the `CREATE SERVER` and `CREATE FOREIGN SERVER` SQL commands into the database and you'll have your foreign table definition.

                 Foreign table "public.pt_two"
      Column  |       Type        | Modifiers | FDW Options 
    ----------+-------------------+-----------+-------------
     fid      | integer           |           | 
     geom     | geometry          |           | 
     name     | character varying |           | 
     age      | integer           |           | 
     height   | real              |           | 
     birthday | date              |           | 
    Server: tmp_shape
    FDW Options: (layer 'pt_two')

And you can query the table directly, even though it's really just a shape file.

    > SELECT * FROM pt_two;

     fid |                    geom                    | name  | age | height |  birthday  
    -----+--------------------------------------------+-------+-----+--------+------------
       0 | 0101000000C00497D1162CB93F8CBAEF08A080E63F | Peter |  45 |    5.6 | 1965-04-12
       1 | 010100000054E943ACD697E2BFC0895EE54A46CF3F | Paul  |  33 |   5.84 | 1971-03-25

## Examples

### WFS FDW

Since we can access any OGR data source as a table, how about a public WFS server?

    CREATE EXTENSION postgis;
    CREATE EXTENSION ogr_fdw;

    CREATE SERVER opengeo
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource 'WFS:http://demo.opengeo.org/geoserver/wfs',
        format 'WFS' );

    CREATE FOREIGN TABLE topp_states (
      fid integer,
      geom geometry,
      gml_id varchar,
      state_name varchar,
      state_fips varchar,
      sub_region varchar,
      state_abbr varchar,
      land_km real,
      water_km real,
      persons real,
      families real,
      houshold real,
      male real,
      female real,
      workers real,
      drvalone real,
      carpool real,
      pubtrans real,
      employed real,
      unemploy real,
      service real,
      manual real,
      p_male real,
      p_female real,
      samp_pop real )
      SERVER opengeo
      OPTIONS ( layer 'topp:states' );

### FGDB FDW

Unzip the `Querying.zip` file from the `data` directory to get a `Querying.gdb` file, and put it somewhere public (like `/tmp`). Now run the `ogr_fdw_info` tool on it to get a table definition.

    CREATE SERVER fgdbtest
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource '/tmp/Querying.gdb',
        format 'OpenFileGDB' );

    CREATE FOREIGN TABLE cities (
      fid integer,
      geom geometry,
      city_fips varchar,
      city_name varchar,
      state_fips varchar,
      state_name varchar,
      state_city varchar,
      type varchar,
      capital varchar,
      elevation integer,
      pop1990 integer,
      popcat integer )
      SERVER fgdbtest
      OPTIONS ( layer 'Cities' );

Query away!

### PostgreSQL FDW

Wraparound action! Handy for testing. Connect your database back to your database and watch the fur fly.

    CREATE TABLE typetest ( 
      id integer, 
      geom geometry(point, 4326),
      num real, 
      name varchar, 
      clock time, 
      calendar date, 
      tstmp timestamp 
    );

    INSERT INTO typetest 
      VALUES (1, 'SRID=4326;POINT(-126 46)', 4.5, 'Paul', '09:34:23', 'June 1, 2013', '12:34:56 December 14, 1823');
    INSERT INTO typetest 
      VALUES (2, 'SRID=4326;POINT(-126 46)', 4.8, 'Peter', '14:34:53', 'July 12, 2011', '1:34:12 December 24, 1923');

    CREATE SERVER wraparound
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource 'Pg:dbname=fdw user=postgres',
        format 'PostgreSQL' );

    CREATE FOREIGN TABLE typetest_fdw (
      fid integer,
      geom geometry,
      id integer,
      num real,
      name varchar,
      clock time,
      calendar date,
      tstmp timestamp )
      SERVER wraparound
      OPTIONS ( layer 'typetest' );

    SELECT * FROM typetest_fdw;
    
### Using IMPORT FOREIGN SCHEMA (for PostgreSQL 9.5+ only)

## Importing links to all tables
If you want to import all tables use the special schema called  *ogr_all*

	CREATE SCHEMA fgdball;
	IMPORT FOREIGN SCHEMA ogr_all 
		FROM server fgdbtest INTO fgdball;

## Importing subset of tables using prefixes		
Not all ogr data sources have a concept of schema, so we use the remote_schema as a prefix.
Note this is case sensitive, so make sure casing matches your layer names.

For example the following will only import tables that start with *CitiesIn*. As long as you quote, you can handle 
true schemaed databases such as SQL server or PostgreSQL by using something like *"dbo."*

	CREATE SCHEMA fgdbcityinf;
	IMPORT FOREIGN SCHEMA "CitiesIn"
		FROM server fgdbtest INTO fgdbcityinf;
		
## Preserving case and special characters in column names and table names
By default, when IMPORT FOREIGN SCHEMA is run on an ogr foreign data server, the table names and column names are laundered
(meaning all upper case is converted to lowercase and special characters such as spaces are replaced with _).

This is not desirable in all cases. You can override this behavior with 2 IMPORT FOREIGN SCHEMA options specific to ogr fdw servers.

These are `launder_column_names` and `launder_tables_names`.

To preserve casing and other funky characters in both column names and table names you can do the following:

	CREATE SCHEMA fgdbcitypreserve;
	IMPORT FOREIGN SCHEMA ogr_all
		FROM server fgdbtest INTO fgdbpreserve 
		OPTIONS(launder_table_names 'false', launder_column_names 'false') ;
		
		
## Importing subset of layers using LIMIT and EXCEPT
Note: LIMIT TO /EXCEPT should contain resulting table names (NOT the layer names)
In the default case, the table names are laundered should not have mixed case or weird characters.

	CREATE SCHEMA fgdbcitysub;
	-- import only layer called Cities
	IMPORT FOREIGN SCHEMA ogr_all 
    		LIMIT TO(cities) 
		FROM server fgdbtest INTO fgdbcitysub ;
		
	-- import only layers not called Cities or Countries
	IMPORT FOREIGN SCHEMA ogr_all 
        EXCEPT (cities, countries)
		FROM server fgdbtest INTO fgdbcitysub;
		
	-- With table laundering turned off, need to use exact layer names
	DROP SCHEMA IF EXISTS fgdbcitysub CASCADE;
	
	IMPORT FOREIGN SCHEMA ogr_all 
    		LIMIT TO("Cities") 
		FROM server fgdbtest INTO fgdbcitysub OPTIONS(launder_table_names 'false') ;
		
		

Enjoy!
