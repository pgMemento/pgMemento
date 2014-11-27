-- INDEX_SCHEMA.sql
--
-- Author:      Felix Kunde <fkunde@virtualcitysystems.de>
--
--              This skript is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- If pgMemento has been used to restore tables as BASE TABLEs they do not include
-- PRIMARY KEYs, FOREIGN KEYs and INDEXes. This script provides functions to
-- add those elements by querying information on recent contraints (as 
-- constraint metadata is yet not logged by pgMemento). 
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                       | Author
-- 0.2.0     2014-05-22   some intermediate version           FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   fkey_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public', 
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   fkey_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public') 
*     RETURNS SETOF VOID
*   index_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public', 
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   index_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public') 
*     RETURNS SETOF VOID
*   pkey_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public', 
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   pkey_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public') 
*     RETURNS SETOF VOID
***********************************************************/

/**********************************************************
* PKEY TABLE STATE
*
* If a table state is produced as a base table it will not have
* a primary key. The primary key might be reconstruced by
* querying the recent primary key of the table. If no primary
* can be redefined the audit_id column will be used.
***********************************************************/
-- define a primary key for a produced table
CREATE OR REPLACE FUNCTION pgmemento.pkey_table_state( 
  table_name TEXT,
  target_schema_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  pkey_columns TEXT := '';
BEGIN
  -- rebuild primary key columns to index produced tables
  EXECUTE 'SELECT array_to_string(array_agg(pga.attname),'','') FROM pg_index pgi, pg_class pgc, pg_attribute pga 
             WHERE pgc.oid = $1::regclass 
             AND pgi.indrelid = pgc.oid 
             AND pga.attrelid = pgc.oid 
             AND pga.attnum = ANY(pgi.indkey) AND pgi.indisprimary' 
               INTO pkey_columns USING '"' || original_schema_name || '".' || table_name;

  IF length(pkey_columns) = 0 THEN
    RAISE NOTICE 'Table ''%'' has no primary key defined. Column ''audit_id'' will be used as primary key.', table_name;
    pkey_columns := 'audit_id';
  END IF;

  EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I_PK PRIMARY KEY (' || pkey_columns || ')', target_schema_name, table_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform pkey_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.pkey_schema_state(
  target_schema_name TEXT, 
  original_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.pkey_table_state(tablename, schemaname, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, original_schema_name;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* FKEY TABLE STATE
*
* If multiple table states are produced as tables they are not
* referenced which each other. Foreign key relations might be
* reconstruced by querying the recent foreign keys of the table.
***********************************************************/
-- define foreign keys between produced tables
CREATE OR REPLACE FUNCTION pgmemento.fkey_table_state( 
  table_name TEXT,
  target_schema_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  fkey RECORD;
BEGIN
  -- rebuild foreign key contraints
  FOR fkey IN EXECUTE 'SELECT tc.constraint_name AS fkey_name, kcu.column_name AS fkey_column, ccu.table_name AS ref_table, ccu.column_name AS ref_column
                        FROM information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
                          WHERE constraint_type = ''FOREIGN KEY'' AND tc.table_schema = $1 AND tc.table_name=$2' 
                          USING original_schema_name, table_name LOOP
    BEGIN
      -- test query
      EXECUTE format('SELECT 1 FROM %I.%I a, %I.%I b WHERE a.%I = b.%I LIMIT 1',
                        target_schema_name, table_name, target_schema_name, fkey.ref_table, fkey.fkey_column, fkey.ref_column);

      -- recreate foreign key of original table
      EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I.%I ON UPDATE CASCADE ON DELETE RESTRICT',
                        target_schema_name, table_name, fkey.fkey_name, fkey.fkey_column, target_schema_name, fkey.ref_table, fkey.ref_column);

      EXCEPTION
        WHEN OTHERS THEN
          RAISE NOTICE 'Could not recreate foreign key constraint ''%'' on table ''%'': %', fkey.fkey_name, table_name, SQLERRM;
          NULL;
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- perform fkey_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.fkey_schema_state(
  target_schema_name TEXT, 
  original_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.fkey_table_state(tablename, schemaname, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, original_schema_name;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* INDEX TABLE STATE
*
* If a produced table shall be used for queries indexes will 
* be necessary in order to guarantee high performance. Indexes
* might be reconstruced by querying recent indexes of the table.
***********************************************************/
-- define index(es) on columns of a produced table
CREATE OR REPLACE FUNCTION pgmemento.index_table_state( 
  table_name TEXT,
  target_schema_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  idx RECORD;
  dim INTEGER;
BEGIN  
  -- rebuild user defined indexes
  FOR idx IN EXECUTE 'SELECT pgc.relname AS idx_name, pgam.amname AS idx_type, array_to_string(
                      ARRAY(SELECT pg_get_indexdef(pgi.indexrelid, k + 1, true) FROM generate_subscripts(pgi.indkey, 1) as k ORDER BY k)
                      , '','') as idx_columns
                      FROM pg_index pgi
                      JOIN pg_class pgc ON pgc.oid = pgi.indexrelid
                      JOIN pg_am pgam ON pgam.oid = pgc.relam
                        AND pgi.indrelid = $1::regclass
                        AND pgi.indisprimary = ''f''' 
                        USING '"' || original_schema_name || '".' || table_name LOOP
    BEGIN
      -- reset dim variable
      dim := 0;	  
	  
	  -- test query
      EXECUTE format('SELECT ' || idx.idx_columns || ' FROM %I.%I LIMIT 1', target_schema_name, table_name);

	  -- if a gist index has been found, it can be a spatial index of the PostGIS extension
      IF idx.idx_type = 'gist' THEN
        BEGIN		  
		  -- query view 'geometry_columns' view to get the dimension of possible spatial column
          EXECUTE 'SELECT coord_dimension FROM geometry_columns 
                     WHERE f_table_schema = $1 AND f_table_name = $2 AND f_geometry_column = $3'
                       INTO dim USING original_schema_name, table_name, idx.idx_columns;

          EXCEPTION
            WHEN OTHERS THEN
              RAISE NOTICE 'An error occurred when querying the PostGIS table ''geometry_columns'': %', SQLERRM;
              NULL;
        END;
      END IF;

      -- recreate the index
      IF dim = 3 THEN
        EXECUTE format('CREATE INDEX %I ON %I.%I USING GIST(%I gist_geometry_ops_nd)', idx.idx_name, target_schema_name, table_name, idx.idx_columns);
      ELSE
        EXECUTE format('CREATE INDEX %I ON %I.%I USING ' || idx.idx_type || '(' || idx.idx_columns || ')', idx.idx_name, target_schema_name, table_name);
      END IF;

      EXCEPTION
        WHEN OTHERS THEN
          RAISE NOTICE 'Could not recreate index ''%'' on table ''%'': %', idx.idx_name, table_name, SQLERRM;
          NULL;
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- perform index_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.index_schema_state(
  target_schema_name TEXT, 
  original_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.index_table_state(tablename, schemaname, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, original_schema_name;
END;
$$
LANGUAGE plpgsql;