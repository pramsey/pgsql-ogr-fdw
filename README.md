Travis: [![Build Status](https://secure.travis-ci.org/pramsey/pgsql-ogr-fdw.png)](http://travis-ci.org/pramsey/pgsql-ogr-fdw)

# PostgreSQL OGR Foreign Data Wrapper

## Motivation

OGR is the vector half of the [GDAL](http://www.gdal.org/) spatial data access library. It allows access to a [large number of GIS data formats](http://www.gdal.org/ogr_formats.html) using a [simple C API](http://www.gdal.org/ogr__api_8h.html) for data reading and writing. Since OGR exposes a simple table structure and PostgreSQL [foreign data wrappers](https://wiki.postgresql.org/wiki/Foreign_data_wrappers) allow access to table structures, the fit seems pretty perfect.

## Limitations

This implementation currently has the following limitations:

* **PostgreSQL 9.3+** This wrapper does not support the FDW implementations in older versions of PostgreSQL.
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

    > ogr_fdw_info -f

    Supported Formats:
      -> "PCIDSK" (read/write)
      -> "netCDF" (read/write)
      ...
      -> "HTTP" (readonly)


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
      geom geometry(Point, 4326),
      name varchar,
      age integer,
      height real,
      birthdate date )
      SERVER myserver
      OPTIONS (layer 'pt_two');

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
      OPTIONS (layer 'topp:states');

### FGDB FDW

Unzip the `Querying.zip` file from the `data` directory to get a `Querying.gdb` file, and put it somewhere public (like `/tmp`). Now run the `ogr_fdw_info` tool on it to get a table definition.

    CREATE SERVER fgdbtest
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource '/tmp/Querying.gdb',
        format 'OpenFileGDB' );

    CREATE FOREIGN TABLE cities (
      fid integer,
      geom geometry(Point, 4326),
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
      OPTIONS (layer 'Cities');

Query away!

### PostgreSQL FDW

Wraparound action! Handy for testing. Connect your database back to your database and watch the fur fly.

    CREATE TABLE typetest (
      fid serial primary key,
      geom geometry(Point, 4326),
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
      geom geometry(Point, 4326),
      num real,
      name varchar,
      clock time,
      calendar date,
      tstmp timestamp )
      SERVER wraparound
      OPTIONS (layer 'typetest');

    SELECT * FROM typetest_fdw;

## Advanced Features

### Writeable FDW Tables

If the OGR driver you are using supports it, you can insert/update/delete records from your FDW tables.

For file-backed drivers, the user under which `postgres` runs will need read/write access to the file being altered. For database-backed drivers, your connection needs a user with read/write permissions to the database.

By default, servers and tables are updateable if the OGR driver supports it, but you can turn off updateability at a server or table level using the `updateable` option:

    ALTER SERVER myserver
      OPTIONS (ADD updatable 'false');

    ALTER FOREIGN TABLE mytable
      OPTIONS (ADD updatable 'false');

Writeable tables only work if you have included a `fid` column in your table definition. By default, tables imported by `IMPORT FOREIGN SCHEMA` or using the example SQL code from `ogr_fdw_info` include a `fid` column.

### Column Name Mapping

You can create an FDW table with any subset of columns from the OGR source you like, just by using the same column names as the source:

    CREATE FOREIGN TABLE typetest_fdw_partial (
      clock time,
      name varchar
      )
      SERVER wraparound
      OPTIONS (layer 'typetest');

You can also explicitly map remote column names to different local names using the `column_name` option:

    CREATE FOREIGN TABLE typetest_fdw_mapped (
      fid bigint,
      supertime time OPTIONS (column_name 'clock'),
      thebestnamething varchar OPTIONS (column_name 'name')
      )
      SERVER wraparound
      OPTIONS (layer 'typetest');


### Automatic Foreign Table Creation

**This feature is only available with PostgreSQL 9.5 and higher**

You can use the PostgreSQL `IMPORT FOREIGN SCHEMA` command to [import table definitions from an OGR data source](http://www.postgresql.org/docs/9.5/static/sql-importforeignschema.html).

#### Import All Tables

If you want to import all tables in the OGR data source use the special schema called "ogr_all".

	CREATE SCHEMA fgdball;

	IMPORT FOREIGN SCHEMA ogr_all
		FROM SERVER fgdbtest
        INTO fgdball;

#### Import a Subset of Tables

Not all OGR data sources have a concept of schema, so we use the remote schema string as a prefix to match OGR layers. The matching is case sensitive, so make sure casing matches your layer names.

For example, the following will only import tables that start with *CitiesIn*. As long as you quote, you can handle true schemaed databases such as SQL Server or PostgreSQL by using something like *"dbo."*

	CREATE SCHEMA fgdbcityinf;

	IMPORT FOREIGN SCHEMA "CitiesIn"
		FROM SERVER fgdbtest
        INTO fgdbcityinf;

You can also use PostgreSQL clauses `LIMIT TO` and `EXCEPT` to restrict the tables you are importing.

	CREATE SCHEMA fgdbcitysub;

	-- import only layer called Cities
	IMPORT FOREIGN SCHEMA ogr_all
        LIMIT TO(cities)
		FROM server fgdbtest
        INTO fgdbcitysub ;

	-- import only layers not called Cities or Countries
	IMPORT FOREIGN SCHEMA ogr_all
        EXCEPT (cities, countries)
		FROM server fgdbtest
        INTO fgdbcitysub;

	-- With table laundering turned off, need to use exact layer names
	DROP SCHEMA IF EXISTS fgdbcitysub CASCADE;

    -- import with un-laundered table name
	IMPORT FOREIGN SCHEMA ogr_all
    	LIMIT TO("Cities")
		FROM server fgdbtest
        INTO fgdbcitysub
        OPTIONS (launder_table_names 'false') ;


#### Mixed Case and Special Characters

In general, PostgreSQL prefers table names with [simple numbers and letters](http://www.postgresql.org/docs/9.5/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS), no punctuation or special characters.

By default, when `IMPORT FOREIGN SCHEMA` is run on an OGR foreign data server, the table names and column names are "laundered" -- all upper case is converted to lowercase and special characters such as spaces and punctuation are replaced with "_".

Laundering is not desirable in all cases. You can override this behavior with two `IMPORT FOREIGN SCHEMA` options specific to `ogr_fdw` servers: `launder_column_names` and `launder_table_names`.

To preserve casing and other funky characters in both column names and table names, do the following:

	CREATE SCHEMA fgdbcitypreserve;

	IMPORT FOREIGN SCHEMA ogr_all
		FROM SERVER fgdbtest
        INTO fgdbpreserve
		OPTIONS (
            launder_table_names 'false',
            launder_column_names 'false'
            ) ;


###  GDAL Options

The behavior of your GDAL/OGR connection can be altered by passing GDAL `config_options` to the connection when you set up the server. Most GDAL/OGR drivers have some specific behaviours that are controlled by configuration options. For example, the "[ESRI Shapefile](http://www.gdal.org/drv_shapefile.html)" driver includes a `SHAPE_ENCODING` option that controls the character encoding applied to text data.

Since many Shapefiles are encoded using LATIN1, and most PostgreSQL databases are encoded in UTF-8, it is useful to specify the encoding to get proper handling of special characters like accents.

    CREATE SERVER myserver_latin1
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource '/tmp/test',
        format 'ESRI Shapefile',
        config_options 'SHAPE_ENCODING=LATIN1' );

Multiple config options can be passed at one time by supplying a **space-separated** list of options.

If you are using GDAL 2.0 or higher, you can also pass "open options" to your OGR foreign data wrapper, using the `open_options` parameter. In GDAL 2.0, the global `SHAPE_ENCODING` option has been superceded by a driver-specific `ENCODING` option, which can be called like this:

    CREATE SERVER myserver_latin1
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource '/tmp/test',
        format 'ESRI Shapefile',
        open_options 'ENCODING=LATIN1' );

### GDAL Debugging

If you are getting odd behavior and you want to see what GDAL is doing behind the scenes, enable debug logging in your server:

    CREATE SERVER myserver_latin1
      FOREIGN DATA WRAPPER ogr_fdw
      OPTIONS (
        datasource '/tmp/test',
        format 'ESRI Shapefile',
        config_options 'SHAPE_ENCODING=LATIN1 CPL_DEBUG=ON'
        );

GDAL-level messages will be logged at the PostgreSQL **DEBUG2** level, so to see them when running a query, alter your `client_min_messages` setting.

    SET client_min_messages = debug2;

Once you've figured out your issue, don't forget to remove the `CPL_DEBUG` option from your server definition, and set your messages back to **NOTICE** level.

    SET client_min_messages = notice;
    ALTER SERVER myserver_latin1 OPTIONS (SET config_options 'SHAPE_ENCODING=LATIN1');
    

