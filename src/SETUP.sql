-- SETUP.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides functions to set up pgMemento for a schema in an 
-- PostgreSQL 9.4+ database.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                       | Author
-- 0.3.0     2016-04-14   new log tables for ddl changes                      FKun
-- 0.2.4     2016-04-05   more constraints on log tables (+ new ID column)    FKun
-- 0.2.3     2016-03-17   work with time zones and renamed column in          FKun
--                        table_templates table
-- 0.2.2     2016-03-09   fallbacks for adding columns and triggers           FKun 
-- 0.2.1     2016-02-14   removed unnecessary plpgsql and dynamic sql code    FKun
-- 0.2.0     2015-02-21   new table structure, more triggers and JSONB        FKun
-- 0.1.0     2014-11-26   initial commit                                      FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* PGMEMENTO SCHEMA
*   Addtional schema that contains the log tables and
*   all functions to enable versioning of the database.
*
* TABLES:
*   audit_column_log
*   audit_table_log
*   row_log
*   table_event_log
*   transaction_log
*
* INDEXES:
*   column_log_column_idx
*   column_log_range_idx
*   column_log_relid_idx
*   row_log_audit_idx
*   row_log_changes_idx
*   row_log_event_idx
*   table_event_log_op_idx
*   table_event_log_txid_idx
*   table_event_table_idx
*   table_log_idx
*   table_log_range_idx
*   transaction_log_date_idx
*   transaction_log_txid_idx
*
* FUNCTIONS:
*   create_schema_audit(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_audit_id(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_log_trigger(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   create_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   create_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_schema_audit(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_audit_id(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_log_trigger(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*
* TRIGGER FUNCTIONS
*   log_delete() RETURNS trigger
*   log_insert() RETURNS trigger
*   log_tansactions RETURNS trigger
*   log_truncate() RETURNS trigger
*   log_update() RETURNS trigger
*
***********************************************************/
DROP SCHEMA IF EXISTS pgmemento CASCADE;
CREATE SCHEMA pgmemento;

/***********************************************************
CREATE TABLES

***********************************************************/
-- transaction metadata is logged into the transaction_log table
DROP TABLE IF EXISTS pgmemento.transaction_log CASCADE;
CREATE TABLE pgmemento.transaction_log
(
  id SERIAL,
  txid BIGINT NOT NULL,
  stmt_date TIMESTAMP WITH TIME ZONE NOT NULL,
  user_name TEXT,
  client_name TEXT
);

ALTER TABLE pgmemento.transaction_log
  ADD CONSTRAINT transaction_log_pk PRIMARY KEY (id),
  ADD CONSTRAINT transaction_log_unique_txid UNIQUE (txid);

-- event on tables are logged into the table_event_log table
DROP TABLE IF EXISTS pgmemento.table_event_log CASCADE;
CREATE TABLE pgmemento.table_event_log
(
  id SERIAL,
  transaction_id BIGINT NOT NULL,
  op_id SMALLINT NOT NULL,
  table_operation VARCHAR(8),
  table_relid OID NOT NULL
);

ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_pk PRIMARY KEY (id);

-- all row changes are logged into the row_log table
DROP TABLE IF EXISTS pgmemento.row_log CASCADE;
CREATE TABLE pgmemento.row_log
(
  id BIGSERIAL,
  event_id INTEGER NOT NULL,
  audit_id BIGINT NOT NULL,
  changes JSONB
);

ALTER TABLE pgmemento.row_log
  ADD CONSTRAINT row_log_pk PRIMARY KEY (id);

-- liftime of audited tables is logged in the audit_table_log table
CREATE TABLE pgmemento.audit_table_log (
  relid OID,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  txid_range numrange  
);

ALTER TABLE pgmemento.audit_table_log
  ADD CONSTRAINT audit_table_log_pk PRIMARY KEY (relid);

-- lifetime of columns of audited tables is logged in the audit_column_log table
CREATE TABLE pgmemento.audit_column_log (
  id SERIAL,
  table_relid OID NOT NULL,
  column_name TEXT NOT NULL,
  ordinal_position INTEGER,
  column_default TEXT,
  is_nullable VARCHAR(3),
  data_type TEXT,
  data_type_name TEXT,
  char_max_length INTEGER,
  numeric_precision INTEGER,
  numeric_precision_radix INTEGER,
  numeric_scale INTEGER,
  datetime_precision INTEGER,
  interval_type TEXT,
  txid_range numrange
);  

ALTER TABLE pgmemento.audit_column_log
  ADD CONSTRAINT audit_column_log_pk PRIMARY KEY (id);

-- create foreign key constraints
ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_txid_fk FOREIGN KEY (transaction_id)
    REFERENCES pgmemento.transaction_log (txid) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE pgmemento.row_log
  ADD CONSTRAINT row_log_table_fk FOREIGN KEY (event_id)
    REFERENCES pgmemento.table_event_log (id) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE pgmemento.audit_column_log
  ADD CONSTRAINT audit_column_log_fk FOREIGN KEY (table_relid)
    REFERENCES pgmemento.audit_table_log (relid) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE;

-- create indexes on all columns that are queried later
DROP INDEX IF EXISTS transaction_log_txid_idx;
DROP INDEX IF EXISTS transaction_log_date_idx;
DROP INDEX IF EXISTS table_event_log_txid_idx;
DROP INDEX IF EXISTS table_event_log_op_idx;
DROP INDEX IF EXISTS table_event_table_idx;
DROP INDEX IF EXISTS row_log_event_idx;
DROP INDEX IF EXISTS row_log_audit_idx;
DROP INDEX IF EXISTS row_log_changes_idx;
DROP INDEX IF EXISTS table_log_idx;
DROP INDEX IF EXISTS table_log_range_idx;
DROP INDEX IF EXISTS column_log_relid_idx;
DROP INDEX IF EXISTS column_log_column_idx;
DROP INDEX IF EXISTS column_log_range_idx;

CREATE INDEX transaction_log_txid_idx ON pgmemento.transaction_log USING BTREE (txid);
CREATE INDEX transaction_log_date_idx ON pgmemento.transaction_log USING BTREE (stmt_date);
CREATE INDEX table_event_log_txid_idx ON pgmemento.table_event_log USING BTREE (transaction_id);
CREATE INDEX table_event_log_op_idx ON pgmemento.table_event_log USING BTREE (op_id);
CREATE INDEX table_event_table_idx ON pgmemento.table_event_log USING BTREE (table_relid);
CREATE INDEX row_log_event_idx ON pgmemento.row_log USING BTREE (event_id);
CREATE INDEX row_log_audit_idx ON pgmemento.row_log USING BTREE (audit_id);
CREATE INDEX row_log_changes_idx ON pgmemento.row_log USING GIN (changes);
CREATE INDEX table_log_idx ON pgmemento.audit_table_log USING BTREE (table_name, schema_name);
CREATE INDEX table_log_range_idx ON pgmemento.audit_table_log USING GIST (txid_range);
CREATE INDEX column_log_relid_idx ON pgmemento.audit_column_log USING BTREE (table_relid);
CREATE INDEX column_log_column_idx ON pgmemento.audit_column_log USING BTREE (column_name);
CREATE INDEX column_log_range_idx ON pgmemento.audit_column_log USING GIST (txid_range);


/***********************************************************
CREATE SEQUENCE

***********************************************************/
DROP SEQUENCE IF EXISTS pgmemento.audit_id_seq;
CREATE SEQUENCE pgmemento.audit_id_seq
	INCREMENT BY 1
	MINVALUE 0
	MAXVALUE 2147483647
	START WITH 1
	CACHE 1
	NO CYCLE
	OWNED BY NONE;


/**********************************************************
* ENABLE/DISABLE PGMEMENTO
*
* Enables/disables pgMemento for a specified table/schema.
***********************************************************/
-- create pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.create_table_audit( 
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger(table_name, schema_name);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id(table_name, schema_name);
END;
$$
LANGUAGE plpgsql;

-- perform create_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.create_table_audit(tablename, schemaname) FROM pg_tables 
    WHERE schemaname = schema_name AND tablename <> ALL (COALESCE(except_tables,'{}'));
$$
LANGUAGE sql;

-- drop pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public' 
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- drop audit_id column
  PERFORM pgmemento.drop_table_audit_id(table_name, schema_name);

  -- drop log trigger
  PERFORM pgmemento.drop_table_log_trigger(table_name, schema_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.drop_table_audit(tablename, schemaname) FROM pg_tables 
    WHERE schemaname = schema_name AND tablename <> ALL (COALESCE(except_tables,'{}'));
$$
LANGUAGE sql;


/**********************************************************
* LOGGING TRIGGER
*
* Define trigger on a table to fire events when
*  - a statement is executed
*  - rows are inserted, updated or deleted 
*  - the table is truncated
***********************************************************/
-- create logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.create_table_log_trigger( 
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_trigger
      WHERE tgrelid = (schema_name || '.' || table_name)::regclass::oid
        AND tgname = 'log_transaction_trigger'
    ) THEN
    RETURN;
  ELSE
    /*
      statement level triggers
    */
    -- first trigger to be fired on each transaction
    EXECUTE format(
      'CREATE TRIGGER log_transaction_trigger
         BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %I.%I
         FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_transaction()',
      schema_name, table_name);

    -- second trigger to be fired before truncate events 
    EXECUTE format(
      'CREATE TRIGGER log_truncate_trigger 
         BEFORE TRUNCATE ON %I.%I
         FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_truncate()',
      schema_name, table_name);

    /*
      row level triggers
    */
    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER log_insert_trigger
         AFTER INSERT ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_insert()',
      schema_name, table_name);

    -- trigger to be fired after update events
    EXECUTE format(
      'CREATE TRIGGER log_update_trigger
         AFTER UPDATE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_update()',
      schema_name, table_name);

    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER log_delete_trigger
         AFTER DELETE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_delete()',
      schema_name, table_name);
  END IF;
END;
$$
LANGUAGE plpgsql;

-- perform create_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_log_trigger(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.create_table_log_trigger(tablename, schemaname) FROM pg_tables 
    WHERE schemaname = schema_name AND tablename <> ALL (COALESCE(except_tables,'{}'));
$$
LANGUAGE sql;

-- drop logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_log_trigger(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public' 
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('DROP TRIGGER IF EXISTS log_delete_trigger ON %I.%I', schema_name, table_name);
  EXECUTE format('DROP TRIGGER IF EXISTS log_update_trigger ON %I.%I', schema_name, table_name);
  EXECUTE format('DROP TRIGGER IF EXISTS log_insert_trigger ON %I.%I', schema_name, table_name);
  EXECUTE format('DROP TRIGGER IF EXISTS log_truncate_trigger ON %I.%I', schema_name, table_name);
  EXECUTE format('DROP TRIGGER IF EXISTS log_transaction_trigger ON %I.%I', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_log_trigger(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.drop_table_log_trigger(tablename, schemaname) FROM pg_tables 
    WHERE schemaname = schema_name AND tablename <> ALL (COALESCE(except_tables,'{}'));
$$
LANGUAGE sql;


/**********************************************************
* AUDIT ID COLUMN
*
* Add an extra column 'audit_id' to a table to trace 
* changes on rows over time.
***********************************************************/
-- add column 'audit_id' to a table
CREATE OR REPLACE FUNCTION pgmemento.create_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- add 'audit_id' column to table if it does not exist, yet
  IF NOT EXISTS (
    SELECT 1 FROM pg_attribute
      WHERE attrelid = (schema_name || '.' || table_name)::regclass
        AND attname = 'audit_id'
        AND NOT attisdropped
    ) THEN
    EXECUTE format(
      'ALTER TABLE %I.%I ADD COLUMN audit_id BIGINT DEFAULT nextval(''pgmemento.audit_id_seq''::regclass)',
      schema_name, table_name);
  END IF;

  -- add index for 'audit_id' column if it does not exist, yet
  IF NOT EXISTS (
    SELECT 1 FROM pg_index pgi, pg_attribute pga
      WHERE pgi.indrelid = (schema_name || '.' || table_name)::regclass
        AND pga.attrelid = pgi.indrelid
        AND pga.attnum = ANY(pgi.indkey)
        AND pga.attname = 'audit_id'
    ) THEN
    EXECUTE format(
      'CREATE INDEX %I ON %I.%I (audit_id)', table_name || '_audit_idx',
      schema_name, table_name);
  END IF;	
END;
$$
LANGUAGE plpgsql;

-- perform create_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit_id(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.create_table_audit_id(tablename, schemaname) FROM pg_tables 
    WHERE schemaname = schema_name AND tablename <> ALL (COALESCE(except_tables,'{}'));
$$
LANGUAGE sql;

-- drop column 'audit_id' from a table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- drop index on 'audit_id' column if it exists
  EXECUTE format(
    'DROP INDEX IF EXISTS %I',
    table_name || '_audit_idx');

  -- drop 'audit_id' column if it exists
  IF EXISTS (
    SELECT 1 FROM pg_attribute
      WHERE attrelid = (schema_name || '.' || table_name)::regclass::oid
        AND attname = 'audit_id'
        AND attislocal = 't'
        AND NOT attisdropped
    ) THEN
    EXECUTE format(
      'ALTER TABLE %I.%I DROP COLUMN audit_id',
      schema_name, table_name);
  ELSE
    RETURN;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit_id(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.drop_table_audit_id(tablename, schemaname) FROM pg_tables 
    WHERE schemaname = schema_name AND tablename <> ALL (COALESCE(except_tables,'{}'));
$$
LANGUAGE sql;


/**********************************************************
* TRIGGER PROCEDURE log_transaction
*
* Procedure that is called when a log_transaction_trigger is fired.
* Metadata of each transaction is written to the transaction_log table.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_transaction() RETURNS trigger AS
$$
DECLARE
  operation_id SMALLINT;
BEGIN
  BEGIN
    -- try to log corresponding transaction
    INSERT INTO pgmemento.transaction_log (txid, stmt_date, user_name, client_name)
      VALUES (txid_current(), statement_timestamp(), current_user, inet_client_addr());

    EXCEPTION
      WHEN unique_violation THEN
	    NULL;
  END;

  -- assign id for operation type
  CASE TG_OP
    WHEN 'INSERT' THEN operation_id := 1;
	WHEN 'UPDATE' THEN operation_id := 2;
    WHEN 'DELETE' THEN operation_id := 3;
	WHEN 'TRUNCATE' THEN operation_id := 4;
  END CASE;

  -- try to log corresponding table event
  INSERT INTO pgmemento.table_event_log
    (transaction_id, op_id, table_operation, table_relid) 
  VALUES
    (txid_current(), operation_id, TG_OP, TG_RELID);

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* TRIGGER PROCEDURE log_truncate
*
* Procedure that is called when a log_truncate_trigger is fired.
* Table pgmemento.row_log is filled up with entries of truncated table.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_truncate() RETURNS trigger AS
$$
DECLARE
  e_id INTEGER;
BEGIN
  -- get corresponding table event as it has already been logged
  -- by the log_transaction_trigger in advance
  SELECT id INTO e_id
    FROM pgmemento.table_event_log 
      WHERE transaction_id = txid_current() 
        AND table_relid = TG_RELID
        AND op_id = 4
    ORDER BY id DESC LIMIT 1;

  -- log the whole content of the truncated table in the row_log table
  EXECUTE format(
    'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
       SELECT $1, audit_id, row_to_json(%I)::jsonb AS content FROM %I.%I',
    TG_TABLE_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME) USING e_id;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* TRIGGER PROCEDURE log_insert
*
* Procedure that is called when a log_insert_trigger is fired.
* Table pgmemento.row_log is filled up with inserted entries
* without specifying the content.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_insert() RETURNS trigger AS
$$
DECLARE
  e_id INTEGER;
BEGIN
  -- get corresponding table event as it has already been logged
  -- by the log_transaction_trigger in advance
  SELECT id INTO e_id 
    FROM pgmemento.table_event_log 
      WHERE transaction_id = txid_current() 
        AND table_relid = TG_RELID
        AND op_id = 1
    ORDER BY id LIMIT 1;

  -- log inserted row ('changes' column can be left blank)
  INSERT INTO pgmemento.row_log (event_id, audit_id)
    VALUES (e_id, NEW.audit_id);
			 
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* TRIGGER PROCEDURE log_update
*
* Procedure that is called when a log_update_trigger is fired.
* Table pgmemento.row_log is filled up with updated entries
* but logging only the difference between OLD and NEW.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_update() RETURNS trigger AS
$$
DECLARE
  e_id INTEGER;
  json_diff JSONB;
BEGIN
  -- get corresponding table event as it has already been logged
  -- by the log_transaction_trigger in advance
  SELECT id INTO e_id 
    FROM pgmemento.table_event_log 
      WHERE transaction_id = txid_current() 
        AND table_relid = TG_RELID
        AND op_id = 2
    ORDER BY id LIMIT 1;

  -- log values of updated columns for the processed row
  -- therefore, a diff between OLD and NEW is necessary
  WITH json_diff AS (
    SELECT COALESCE(
      (SELECT ('{' || string_agg(to_json(key) || ':' || value, ',') || '}') 
         FROM jsonb_each(row_to_json(OLD)::jsonb)
           WHERE NOT ('{' || to_json(key) || ':' || value || '}')::jsonb <@ row_to_json(NEW)::jsonb
      ),
      '{}')::jsonb AS delta
    )
    INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
      SELECT e_id, NEW.audit_id, delta FROM json_diff;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* TRIGGER PROCEDURE log_delete
*
* Procedure that is called when a log_delete_trigger is fired.
* Table pgmemento.row_log is filled up with deleted entries
* including the complete row as JSONB.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_delete() RETURNS trigger AS
$$
DECLARE
  e_id INTEGER;
BEGIN
  -- get corresponding table event as it has already been logged
  -- by the log_transaction_trigger in advance
  SELECT id INTO e_id
    FROM pgmemento.table_event_log 
      WHERE transaction_id = txid_current() 
        AND table_relid = TG_RELID
        AND op_id = 3
    ORDER BY id DESC LIMIT 1;

  -- log content of the entire row in the row_log table
  INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
    VALUES (e_id, OLD.audit_id, row_to_json(OLD)::jsonb);

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;