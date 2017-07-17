-- SCHEMA_MANAGEMENT.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This skript is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- If pgMemento has been used to restore tables as BASE TABLEs they do not include
-- PRIMARY KEYs, FOREIGN KEYs, INDEXes, SEQUENCEs and DEFAULT values for columns. 
-- This script provides procedures to add those elements by querying information 
-- on recent contraints (as such metadata is yet not logged by pgMemento).
-- Moreover, recreated tables can be moved or copied to another schema or they
-- can just be dropped. This could be useful when choosing a restored state as to
-- be the new production state.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                   | Author
-- 0.2.1     2016-02-14   removed unnecessary plpgsql code                FKun
-- 0.2.0     2015-06-06   added procedures and renamed file               FKun
-- 0.1.0     2014-11-26   initial commit as INDEX_SCHEMA.sql              FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   default_values_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public',
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   default_values_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public') 
*     RETURNS SETOF VOID
*   drop_schema_state(table_name TEXT, target_schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_state(table_name TEXT, target_schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   fkey_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public', 
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   fkey_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public') 
*     RETURNS SETOF VOID
*   index_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public', 
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   index_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public') 
*     RETURNS SETOF VOID
*   move_schema_state(target_schema_name TEXT, source_schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}',
*     copy_data INTEGER DEFAULT 1) RETURNS SETOF void AS
*   move_table_state(table_name TEXT, target_schema_name TEXT, source_schema_name TEXT, copy_data INTEGER DEFAULT 1
*     RETURNS SETOF VOID
*   pkey_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public', 
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   pkey_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public') 
*     RETURNS SETOF VOID
*   sequence_schema_state(target_schema_name TEXT, original_schema_name TEXT DEFAULT 'public')
*     RETURNS SETOF VOID
***********************************************************/

/**********************************************************
* PKEY TABLE STATE
*
* If a table state is produced as a base table it will not have
* a primary key. The primary key might be reconstructed by
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
  SELECT array_to_string(array_agg(pga.attname),',') INTO pkey_columns
    FROM pg_index pgi, pg_class pgc, pg_attribute pga 
      WHERE pgc.oid = ($3 || '.' || $1)::regclass 
        AND pgi.indrelid = pgc.oid 
        AND pga.attrelid = pgc.oid 
        AND pga.attnum = ANY(pgi.indkey) AND pgi.indisprimary;

  IF pkey_columns IS NULL THEN
    RAISE NOTICE 'Table ''%'' has no primary key defined. Column ''audit_id'' will be used as primary key.', $1;
    pkey_columns := 'audit_id';
  END IF;

  EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I_PK PRIMARY KEY (' || pkey_columns || ')', $2, $1, $1);
END;
$$
LANGUAGE plpgsql STRICT;

-- perform pkey_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.pkey_schema_state(
  target_schema_name TEXT, 
  original_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT pgmemento.pkey_table_state(tablename, $1, schemaname)
  FROM pg_tables 
    WHERE schemaname = $2
      AND tablename <> ALL (COALESCE($3,'{}')); 
$$
LANGUAGE sql;


/**********************************************************
* FKEY TABLE STATE
*
* If multiple table states are produced as tables they are not
* referenced which each other. Foreign key relations might be
* reconstructed by querying the recent foreign keys of the table.
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
  -- rebuild foreign key constraints
  FOR fkey IN 
    SELECT tc.constraint_name AS fkey_name, kcu.column_name AS fkey_column, ccu.table_name AS ref_table, ccu.column_name AS ref_column
      FROM information_schema.table_constraints AS tc 
      JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
        WHERE constraint_type = 'FOREIGN KEY' 
          AND tc.table_schema = $3
          AND tc.table_name = $1 
  LOOP
    BEGIN
      -- test query
      EXECUTE format('SELECT 1 FROM %I.%I a, %I.%I b WHERE a.%I = b.%I LIMIT 1',
                        $2, $1, $2, fkey.ref_table, fkey.fkey_column, fkey.ref_column);

      -- recreate foreign key of original table
      EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I.%I ON UPDATE CASCADE ON DELETE RESTRICT',
                        $2, $1, fkey.fkey_name, fkey.fkey_column, $2, fkey.ref_table, fkey.ref_column);

      EXCEPTION
        WHEN OTHERS THEN
          RAISE NOTICE 'Could not recreate foreign key constraint ''%'' on table ''%'': %', fkey.fkey_name, $1, SQLERRM;
          NULL;
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform fkey_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.fkey_schema_state(
  target_schema_name TEXT, 
  original_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT pgmemento.fkey_table_state(tablename, $1, schemaname)
  FROM pg_tables 
    WHERE schemaname = $2
      AND tablename <> ALL (COALESCE($3,'{}'));
$$
LANGUAGE sql;


/**********************************************************
* INDEX TABLE STATE
*
* If a produced table shall be used for queries indexes will 
* be necessary in order to guarantee high performance. Indexes
* might be reconstructed by querying recent indexes of the table.
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
  FOR idx IN 
    SELECT pgc.relname AS idx_name, pgam.amname AS idx_type, 
      array_to_string(ARRAY(
        SELECT pg_get_indexdef(pgi.indexrelid, k + 1, true) 
          FROM generate_subscripts(pgi.indkey, 1) as k ORDER BY k)
      , ',') as idx_columns
    FROM pg_index pgi
    JOIN pg_class pgc ON pgc.oid = pgi.indexrelid
    JOIN pg_am pgam ON pgam.oid = pgc.relam
      AND pgi.indrelid = ($3 || '.' || $1)::regclass
      AND pgi.indisprimary = 'f'
  LOOP
    BEGIN
      -- reset dim variable
      dim := 0;	  
	  
	  -- test query
      EXECUTE format('SELECT ' || idx.idx_columns || ' FROM %I.%I LIMIT 1', $2, $1);

	  -- if a gist index has been found, it can be a spatial index of the PostGIS extension
      IF idx.idx_type = 'gist' THEN
        BEGIN		  
		  -- query view 'geometry_columns' view to get the dimension of possible spatial column
          SELECT coord_dimension INTO dim FROM geometry_columns 
            WHERE f_table_schema = $3 
              AND f_table_name = $1 
              AND f_geometry_column = idx.idx_columns;

          EXCEPTION
            WHEN OTHERS THEN
              RAISE NOTICE 'An error occurred when querying the PostGIS table ''geometry_columns'': %', SQLERRM;
              NULL;
        END;
      END IF;

      -- recreate the index
      IF dim = 3 THEN
        EXECUTE format('CREATE INDEX %I ON %I.%I USING GIST(%I gist_geometry_ops_nd)', idx.idx_name, $2, $1, idx.idx_columns);
      ELSE
        EXECUTE format('CREATE INDEX %I ON %I.%I USING ' || idx.idx_type || '(' || idx.idx_columns || ')', idx.idx_name, $2, $1);
      END IF;

      EXCEPTION
        WHEN OTHERS THEN
          RAISE NOTICE 'Could not recreate index ''%'' on table ''%'': %', idx.idx_name, $1, SQLERRM;
          NULL;
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform index_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.index_schema_state(
  target_schema_name TEXT, 
  original_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT pgmemento.index_table_state(tablename, $1, schemaname)
  FROM pg_tables 
    WHERE schemaname = $2
      AND tablename <> ALL (COALESCE($3,'{}'));
$$
LANGUAGE sql;


/**********************************************************
* SEQUENCE SCHEMA STATE
*
* Adds sequences to the created target schema by querying the 
* recent sequences of the source schema. This is only necessary
* if new data will be inserted in a previous database state.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.sequence_schema_state( 
  target_schema_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  seq TEXT;
  seq_value INTEGER;
BEGIN
  -- copy or move sequences
  FOR seq IN 
    SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = $2 
  LOOP
    SELECT nextval($2 || '.' || seq) INTO seq_value;
    IF seq_value > 1 THEN
      seq_value = seq_value - 1;
    END IF;
    EXECUTE format('CREATE SEQUENCE %I.%I START ' || seq_value, $1, seq);
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;


/**********************************************************
* DEFAULT VALUES TABLE STATE
*
* Recreate the default values for columns of a given table. 
* This is only necessary if new data will be inserted in a
* previous database state.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.default_values_table_state( 
  table_name TEXT,
  target_schema_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  default_v RECORD;
BEGIN
  FOR default_v IN 
    SELECT column_name, column_default FROM information_schema.columns
      WHERE (table_schema, table_name) = ($3, $1) 
        AND column_default IS NOT NULL
  LOOP
    BEGIN
      -- alter default values of tables
      EXECUTE format('ALTER TABLE %I.%I
                        ALTER COLUMN %I SET DEFAULT ' || default_v.column_default,
                        $2, $1, default_v.column_name);
    END;
  END LOOP;
END
$$
LANGUAGE plpgsql STRICT;

-- perform default_values_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.default_values_schema_state(
  target_schema_name TEXT, 
  original_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT pgmemento.default_values_table_state(tablename, $1, schemaname)
  FROM pg_tables 
    WHERE schemaname = $2
      AND tablename <> ALL (COALESCE($3,'{}')); 
$$
LANGUAGE sql;


/**********************************************************
* MOVE (or COPY) TABLE STATE
*
* Allows for moving or copying tables to another schema. 
* This can be useful when resetting the production state
* by using an already restored state. In this case the 
* content of the production schema should be removed and 
* the content of the restored state would be moved.
* Triggers for tables would have to be created again.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.move_table_state(
  table_name TEXT,
  target_schema_name TEXT,
  source_schema_name TEXT,
  copy_data INTEGER DEFAULT 1
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $4 <> 0 THEN
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I', $2, $1, $3, $1);
  ELSE
    EXECUTE format('ALTER TABLE %I.%I SET SCHEMA %I', $3, $1, $2);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.move_schema_state(
  target_schema_name TEXT, 
  source_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}',
  copy_data INTEGER DEFAULT 1
  ) RETURNS SETOF void AS
$$
DECLARE
  seq VARCHAR(30);
  seq_value INTEGER;
BEGIN
  -- create new schema
  EXECUTE format('CREATE SCHEMA %I', $1);

  -- copy or move sequences
  FOR seq IN 
    SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = $2 
  LOOP
    IF $4 <> 0 THEN
      SELECT nextval($2 || '.' || seq) INTO seq_value;
      IF seq_value > 1 THEN
        seq_value = seq_value - 1;
      END IF;
      EXECUTE format('CREATE SEQUENCE %I.%I START ' || seq_value, $1, seq);
    ELSE
      EXECUTE format('ALTER SEQUENCE %I.%I SET SCHEMA %I', $2, seq, $1);
    END IF;
  END LOOP;

  -- copy or move tables
  PERFORM pgmemento.move_table_state(tablename, $1, schemaname, $4) FROM pg_tables 
    WHERE schemaname = source_schema_name AND tablename <> ALL (COALESCE($3,'{}'));
 
  -- remove old schema if data were not copied but moved
  IF $4 = 0 THEN
    EXECUTE format('DROP SCHEMA %I CASCADE', source_schema_name);
  END IF;
END
$$
LANGUAGE plpgsql;


/**********************************************************
* DROP TABLE STATE
*
* Drops a schema or table state e.g. if it is of no more use.
* Note: The database schema itself is not dropped.
***********************************************************/
-- truncate and drop table and all depending objects
CREATE OR REPLACE FUNCTION pgmemento.drop_table_state(
  table_name TEXT,
  target_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  fkey TEXT;
BEGIN
  -- dropping depending references to given table
  FOR fkey IN
    SELECT constraint_name AS fkey_name FROM information_schema.table_constraints 
      WHERE constraint_type = 'FOREIGN KEY' 
        AND table_schema = $2 
        AND table_name = $1
  LOOP
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', $2, $1, fkey);
  END LOOP;

  -- hit the log_truncate_trigger
  EXECUTE format('TRUNCATE TABLE %I.%I CASCADE', $2, $1);

  -- dropping the table
  EXECUTE format('DROP TABLE %I.%I CASCADE', $2, $1);
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_state(
  target_schema_name TEXT,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT pgmemento.drop_table_state(tablename, schemaname)
  FROM pg_tables 
    WHERE schemaname = $1
      AND tablename <> ALL (COALESCE($2,'{}'));
$$
LANGUAGE sql;