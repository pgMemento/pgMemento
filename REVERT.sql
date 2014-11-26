-- REVERT.sql
--
-- Author:      Felix Kunde <fkunde@virtualcitysystems.de>
--
--              This skript is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides functions to
-- 
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
*   drop_table(table_name TEXT, target_schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_relations(table_name TEXT, target_schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   recreate_schema_state(schema_name TEXT, target_schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') 
*     RETURNS SETOF VOID
*   recreate_table_state(table_name TEXT, schema_name TEXT, target_schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
***********************************************************/

/**********************************************************
* RECREATE SCHEMA STATE
*
* If a schema state shall be recreated as the actual database
* the recent tables are truncated and dropped first and the
* the former state is rebuild from the schema that contains
* the former state.
*
* NOTE: In order to rebuild primary keys, foreign keys and 
*       indexes corresponding functions must have been executed
*       on target schema.
***********************************************************/
-- drop foreign key contraints
CREATE OR REPLACE FUNCTION pgmemento.drop_table_relations(
  table_name TEXT,
  target_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  fkey TEXT;
BEGIN
  FOR fkey IN EXECUTE 'SELECT constraint_name AS fkey_name FROM information_schema.table_constraints 
                         WHERE constraint_type = ''FOREIGN KEY'' AND table_schema = $1 AND table_name= $2'
                          USING target_schema_name, table_name LOOP
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', target_schema_name, table_name, fkey);
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- truncate and drop table and all depending objects
CREATE OR REPLACE FUNCTION pgmemento.drop_table(
  table_name TEXT,
  target_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- trigger the log_truncate_trigger
  EXECUTE format('TRUNCATE TABLE %I.%I', target_schema_name, table_name);

  -- dropping the table
  EXECUTE format('DROP TABLE %I.%I CASCADE', target_schema_name, table_name);
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pgmemento.recreate_schema_state(
  schema_name TEXT,
  target_schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- drop foreign keys in target schema
  EXECUTE 'SELECT pgmemento.drop_table_relations(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;

  -- drop tables in target schema
  EXECUTE 'SELECT pgmemento.drop_table(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;

  -- copy tables of chosen schema into target schema
  EXECUTE 'SELECT pgmemento.recreate_table_state(tablename, schemaname, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables, target_schema_name;

  -- create primary keys for tables in target schema
  EXECUTE 'SELECT pgmemento.pkey_table_state(tablename, schemaname, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, schema_name;

  -- create foreign keys for tables in target schema
  EXECUTE 'SELECT pgmemento.fkey_table_state(tablename, schemaname, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, schema_name;

  -- index tables in target schema
  EXECUTE 'SELECT pgmemento.index_table_state(tablename, schemaname, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, schema_name;

  -- activate loggin triggers in target schema 
  EXECUTE 'SELECT pgmemento.create_table_log_trigger(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;

  -- fill pgmemento_log table with entries from new tables in target schema
  EXECUTE 'SELECT pgmemento.log_table_state(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- recreate table state into the schema used as the recent database state
CREATE OR REPLACE FUNCTION pgmemento.recreate_table_state(
  table_name TEXT,
  schema_name TEXT,
  target_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I', target_schema_name, table_name, schema_name, table_name);
END;
$$
LANGUAGE plpgsql;