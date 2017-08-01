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
-- 0.3.0     2017-07-27   avoid querying the information_schema           FKun
--                        removed default_values_* functions
-- 0.2.1     2016-02-14   removed unnecessary plpgsql code                FKun
-- 0.2.0     2015-06-06   added procedures and renamed file               FKun
-- 0.1.0     2014-11-26   initial commit as INDEX_SCHEMA.sql              FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
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
  SELECT
    string_agg(pga.attname,', ') INTO pkey_columns
  FROM
    pg_index pgi,
    pg_class pgc,
    pg_attribute pga 
  WHERE
    pgc.oid = ($3 || '.' || $1)::regclass 
    AND pgi.indrelid = pgc.oid 
    AND pga.attrelid = pgc.oid 
    AND pga.attnum = ANY(pgi.indkey)
    AND pgi.indisprimary;

  IF pkey_columns IS NULL THEN
    RAISE NOTICE 'Table ''%'' has no primary key defined. Column ''audit_id'' will be used as primary key.', $1;
    pkey_columns := 'audit_id';
  END IF;

  EXECUTE format(
    'ALTER TABLE %I.%I ADD PRIMARY KEY (' || pkey_columns || ')',
    $2, $1, $1);
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
SELECT
  pgmemento.pkey_table_state(c.relname, $1, n.nspname)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = $2
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($3,'{}')); 
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
    SELECT
      c.conname AS fkey_name,
      a.attname AS fkey_column,
      t.relname AS ref_table,
      a_ref.attname AS ref_column,
      CASE c.confupdtype
        WHEN 'a' THEN 'no action'
        WHEN 'r' THEN 'restrict'
        WHEN 'c' THEN 'cascade'
        WHEN 'n' THEN 'set null'
        WHEN 'd' THEN 'set default'
	  END AS on_up,
      CASE c.confdeltype
        WHEN 'a' THEN 'no action'
        WHEN 'r' THEN 'restrict'
        WHEN 'c' THEN 'cascade'
        WHEN 'n' THEN 'set null'
        WHEN 'd' THEN 'set default'
	  END AS on_del,
      CASE c.confmatchtype
        WHEN 'f' THEN 'full'
        WHEN 'p' THEN 'partial'
        WHEN 'u' THEN 'simple'
      END AS mat
    FROM
      pg_constraint c
    JOIN
      pg_attribute a
      ON a.attrelid = c.conrelid
      AND a.attnum = ANY (c.conkey)
    JOIN
      pg_attribute a_ref
      ON a_ref.attrelid = c.confrelid
      AND a_ref.attnum = ANY (c.confkey)
    JOIN
      pg_class t
      ON t.oid = a_ref.attrelid
    WHERE
      c.conrelid = ($3 || '.' || $1)::regclass::oid
      AND c.contype = 'f'
  LOOP
    BEGIN
      -- test query
      EXECUTE format(
        'SELECT 1 FROM %I.%I a, %I.%I b WHERE a.%I = b.%I LIMIT 1',
        $2, $1, $2, fkey.ref_table, fkey.fkey_column, fkey.ref_column);

      -- recreate foreign key of original table
      EXECUTE format(
        'ALTER TABLE %I.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I.%I ON UPDATE %I ON DELETE %I MATCH %I',
        $2, $1, fkey.fkey_name, fkey.fkey_column, $2, fkey.ref_table, fkey.ref_column, fkey.on_up, fkey.on_del, fkey.mat);

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
SELECT
  pgmemento.fkey_table_state(c.relname, $1, n.nspname)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = $2
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($3,'{}')); 
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
  stmt TEXT;
BEGIN
  -- rebuild user defined indexes
  FOR stmt IN 
    SELECT
      replace(pg_get_indexdef(c.oid),' ON ', format(' ON %I.', $2))
    FROM
      pg_index i
    JOIN
      pg_class c
      ON c.oid = i.indexrelid
    WHERE
      i.indrelid = ($3 || '.' || $1)::regclass
      AND i.indisprimary = 'f'
  LOOP
    BEGIN
      EXECUTE stmt;

      EXCEPTION
        WHEN OTHERS THEN
          RAISE NOTICE 'Could not recreate index ''%'' on table ''%'': %', idx.idx_name, $1, SQLERRM;
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
SELECT
  pgmemento.index_table_state(c.relname, $1, n.nspname)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = $2
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($3,'{}')); 
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
    SELECT
      c.relname
    FROM
      pg_class c,
      pg_namespace n
    WHERE
      c.relnamespace = n.oid
      AND n.nspname = $2
      AND relkind = 'S'
  LOOP
    SELECT nextval($2 || '.' || seq) INTO seq_value;
    IF seq_value > 1 THEN
      seq_value = seq_value - 1;
    END IF;
    EXECUTE format(
      'CREATE SEQUENCE %I.%I START ' || seq_value,
      $1, seq);
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;


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
    EXECUTE format(
      'CREATE TABLE %I.%I AS SELECT * FROM %I.%I',
      $2, $1, $3, $1);
  ELSE
    EXECUTE format(
      'ALTER TABLE %I.%I SET SCHEMA %I',
      $3, $1, $2);
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
    SELECT
      c.relname
    FROM
      pg_class c,
      pg_namespace n
    WHERE
      c.relnamespace = n.oid
      AND n.nspname = $2
      AND relkind = 'S'
  LOOP
    IF $4 <> 0 THEN
      SELECT nextval($2 || '.' || seq) INTO seq_value;
      IF seq_value > 1 THEN
        seq_value = seq_value - 1;
      END IF;
      EXECUTE format(
        'CREATE SEQUENCE %I.%I START ' || seq_value,
        $1, seq);
    ELSE
      EXECUTE format(
        'ALTER SEQUENCE %I.%I SET SCHEMA %I',
        $2, seq, $1);
    END IF;
  END LOOP;

  -- copy or move tables
  PERFORM
    pgmemento.move_table_state(c.relname, $1, n.nspname, $4)
  FROM
    pg_class c,
    pg_namespace n
  WHERE
    c.relnamespace = n.oid
    AND n.nspname = $2
    AND c.relkind = 'r'
    AND c.relname <> ALL (COALESCE($3,'{}')); 
 
  -- remove old schema if data were not copied but moved
  IF $4 = 0 THEN
    EXECUTE format(
      'DROP SCHEMA %I CASCADE',
      $2);
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
    SELECT
      conname
    FROM
      pg_constraint
    WHERE
      conrelid = ($2 || '.' || $1)::regclass::oid
      AND contype = 'f'
  LOOP
    EXECUTE format(
      'ALTER TABLE %I.%I DROP CONSTRAINT %I',
      $2, $1, fkey);
  END LOOP;

  -- hit the log_truncate_trigger
  EXECUTE format(
    'TRUNCATE TABLE %I.%I CASCADE',
    $2, $1);

  -- dropping the table
  EXECUTE format(
    'DROP TABLE %I.%I CASCADE',
    $2, $1);
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_state(
  target_schema_name TEXT,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.drop_table_state(c.relname, n.nspname)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = $1
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($2,'{}')); 
$$
LANGUAGE sql;