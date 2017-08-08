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
-- PostgreSQL 9.5+ database.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                       | Author
-- 0.5.3     2017-07-26   Improved queries for views                          FKun
-- 0.5.2     2017-07-25   UNIQUE constraint for audit_id column, new op_ids   FKun
--                        new column order in audit_column_log
-- 0.5.1     2017-07-18   add functions un/register_audit_table               FKun
-- 0.5.0     2017-07-12   simplified schema for audit_column_log              FKun
-- 0.4.2     2017-04-10   included parts from other scripts                   FKun
-- 0.4.1     2017-03-15   empty JSONB diffs are not logged anymore            FKun
--                        updated schema for DDL log tables
-- 0.4.0     2017-03-05   updated JSONB functions                             FKun
-- 0.3.0     2016-04-14   new log tables for ddl changes (removed             FKun
--                        table_templates table)
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
* VIEWS:
*   audit_tables
*   audit_tables_dependency
*
* INDEXES:
*   column_log_column_idx
*   column_log_range_idx
*   column_log_table_idx
*   row_log_audit_idx
*   row_log_changes_idx
*   row_log_event_idx
*   table_event_log_unique_idx
*   table_log_idx
*   table_log_range_idx
*   transaction_log_date_idx
*   transaction_log_txid_idx
*
* FUNCTIONS:
*   create_schema_audit(schema_name TEXT DEFAULT 'public', log_state INTEGER DEFAULT 1, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_audit_id(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_log_trigger(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public', log_state INTEGER DEFAULT 1) RETURNS SETOF VOID
*   create_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   create_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_schema_audit(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_audit_id(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_log_trigger(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   get_txid_bounds_to_table(table_name TEXT, schema_name TEXT DEFAULT 'public', OUT txid_min BIGINT, OUT txid_max BIGINT) RETURNS RECORD
*   log_schema_state(schemaname TEXT DEFAULT 'public') RETURNS SETOF VOID
*   log_table_state(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   register_audit_table(audit_table_name TEXT, audit_schema_name TEXT DEFAULT 'public') RETURNS INTEGER
*   unregister_audit_table(audit_table_name TEXT, audit_schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
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
  table_operation VARCHAR(12),
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
  id SERIAL,
  relid OID,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  txid_range numrange  
);

ALTER TABLE pgmemento.audit_table_log
  ADD CONSTRAINT audit_table_log_pk PRIMARY KEY (id);

-- lifetime of columns of audited tables is logged in the audit_column_log table
CREATE TABLE pgmemento.audit_column_log (
  id SERIAL,
  audit_table_id INTEGER NOT NULL,
  column_name TEXT NOT NULL,
  ordinal_position INTEGER,
  data_type TEXT,
  column_default TEXT,
  not_null BOOLEAN,
  txid_range numrange
); 

ALTER TABLE pgmemento.audit_column_log
  ADD CONSTRAINT audit_column_log_pk PRIMARY KEY (id);

-- create foreign key constraints
ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_txid_fk
    FOREIGN KEY (transaction_id)
    REFERENCES pgmemento.transaction_log (txid)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;

ALTER TABLE pgmemento.row_log
  ADD CONSTRAINT row_log_table_fk 
    FOREIGN KEY (event_id)
    REFERENCES pgmemento.table_event_log (id)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;

ALTER TABLE pgmemento.audit_column_log
  ADD CONSTRAINT audit_column_log_fk
    FOREIGN KEY (audit_table_id)
    REFERENCES pgmemento.audit_table_log (id)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;

-- create indexes on all columns that are queried later
DROP INDEX IF EXISTS transaction_log_txid_idx;
DROP INDEX IF EXISTS transaction_log_date_idx;
DROP INDEX IF EXISTS table_event_log_unique_idx;
DROP INDEX IF EXISTS row_log_event_idx;
DROP INDEX IF EXISTS row_log_audit_idx;
DROP INDEX IF EXISTS row_log_changes_idx;
DROP INDEX IF EXISTS table_log_idx;
DROP INDEX IF EXISTS table_log_range_idx;
DROP INDEX IF EXISTS column_log_table_idx;
DROP INDEX IF EXISTS column_log_column_idx;
DROP INDEX IF EXISTS column_log_range_idx;

CREATE INDEX transaction_log_txid_idx ON pgmemento.transaction_log USING BTREE (txid);
CREATE INDEX transaction_log_date_idx ON pgmemento.transaction_log USING BTREE (stmt_date);
CREATE UNIQUE INDEX table_event_log_unique_idx ON pgmemento.table_event_log USING BTREE (transaction_id, table_relid, op_id);
CREATE INDEX row_log_event_idx ON pgmemento.row_log USING BTREE (event_id);
CREATE INDEX row_log_audit_idx ON pgmemento.row_log USING BTREE (audit_id);
CREATE INDEX row_log_changes_idx ON pgmemento.row_log USING GIN (changes);
CREATE INDEX table_log_idx ON pgmemento.audit_table_log USING BTREE (table_name, schema_name);
CREATE INDEX table_log_range_idx ON pgmemento.audit_table_log USING GIST (txid_range);
CREATE INDEX column_log_table_idx ON pgmemento.audit_column_log USING BTREE (audit_table_id);
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


/***********************************************************
* GET TXID BOUNDS TO TABLE
*
* A helper function to get highest and lowest logged
* transaction id to an audited table 
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_txid_bounds_to_table(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  OUT txid_min BIGINT,
  OUT txid_max BIGINT
  ) RETURNS RECORD AS
$$
SELECT
  min(transaction_id) AS txid_min,
  max(transaction_id) AS txid_max
FROM
  pgmemento.table_event_log 
WHERE
  table_relid = ($2 || '.' || $1)::regclass::oid;
$$
LANGUAGE sql STABLE STRICT;


/***********************************************************
* AUDIT_TABLES VIEW
*
* A view that shows the user at which transaction auditing
* has been started.
***********************************************************/
CREATE OR REPLACE VIEW pgmemento.audit_tables AS
  SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    b.txid_min,
    b.txid_max,
    CASE WHEN tg.tgenabled IS NOT NULL AND tg.tgenabled <> 'D' THEN
      TRUE
    ELSE
      FALSE
    END AS tg_is_active
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON c.relnamespace = n.oid
  JOIN
    pg_attribute a
    ON a.attrelid = c.oid
  JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(c.relname, n.nspname)
    ) b ON (true)
  LEFT JOIN (
    SELECT
      tgrelid,
      tgenabled
    FROM
      pg_trigger
    WHERE
      tgname = 'log_transaction_trigger'::name
    ) AS tg
    ON c.oid = tg.tgrelid
  WHERE
    n.nspname <> 'pgmemento'
    AND n.nspname NOT LIKE 'pg_temp%'
    AND a.attname = 'audit_id'
    AND c.relkind = 'r'
  ORDER BY
    schemaname,
    tablename;


/***********************************************************
* AUDIT_TABLES_DEPENDENCY VIEW
*
* This view is essential for reverting transactions.
* pgMemento can only log one INSERT/UPDATE/DELETE event per
* table per transaction which maps all changed rows to this
* one event even though it belongs to a subsequent one. 
* Therefore, knowledge about table dependencies is required
* to not violate foreign keys.
***********************************************************/
CREATE OR REPLACE VIEW pgmemento.audit_tables_dependency AS
  WITH RECURSIVE table_dependency(
    parent_oid,
    child_oid,
    table_name,
    schema_name,
    depth
  ) AS (
    SELECT DISTINCT ON (c.conrelid)
      c.confrelid AS parent_oid,
      c.conrelid AS child_oid,
      a.table_name,
      n.nspname AS schema_name,
      1 AS depth
    FROM
      pg_constraint c
    JOIN
      pg_namespace n
      ON n.oid = c.connamespace
    JOIN pgmemento.audit_table_log a
      ON a.relid = c.conrelid
     AND a.schema_name = n.nspname
    WHERE
      c.contype = 'f'
      AND c.conrelid <> c.confrelid
      AND upper(a.txid_range) IS NULL
      AND lower(a.txid_range) IS NOT NULL
    UNION ALL
      SELECT DISTINCT ON (c.conrelid)
        c.confrelid AS parent_oid,
        c.conrelid AS child_oid,
        a.table_name,
        n.nspname AS schema_name,
        d.depth + 1 AS depth
      FROM
        pg_constraint c
      JOIN
        pg_namespace n
        ON n.oid = c.connamespace
      JOIN pgmemento.audit_table_log a
        ON a.relid = c.conrelid
       AND a.schema_name = n.nspname
      JOIN table_dependency d
        ON d.child_oid = c.confrelid
      WHERE
        c.contype = 'f'
        AND d.child_oid <> c.conrelid
        AND upper(a.txid_range) IS NULL
        AND lower(a.txid_range) IS NOT NULL
  )
  SELECT
    schema_name AS schemaname,
    table_name AS tablename,
    depth
  FROM (
    SELECT
      schema_name,
      table_name,
      max(depth) AS depth
    FROM
      table_dependency
    GROUP BY
      schema_name,
      table_name
    UNION ALL
      SELECT
        atl.schema_name,
        atl.table_name,
        0 AS depth 
      FROM
        pgmemento.audit_table_log atl
      LEFT JOIN
        table_dependency d
        ON d.child_oid = atl.relid
      WHERE
        d.child_oid IS NULL
  ) td
  ORDER BY
    schemaname,
    depth,
    tablename;


/**********************************************************
* UN/REGISTER TABLE
*
* Function to un/register information of audited table in
* audit_table_log and corresponding columns in audit_column_log
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.unregister_audit_table(
  audit_table_name TEXT,
  audit_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  tab_id INTEGER;
BEGIN
  -- update txid_range for removed table in audit_table_log table
  UPDATE
    pgmemento.audit_table_log
  SET
    txid_range = numrange(lower(txid_range), txid_current(), '[)') 
  WHERE
    table_name = $1
    AND schema_name = $2
    AND upper(txid_range) IS NULL
    AND lower(txid_range) IS NOT NULL
  RETURNING
    id INTO tab_id;

  IF tab_id IS NOT NULL THEN
    -- update txid_range for removed columns in audit_column_log table
    UPDATE
      pgmemento.audit_column_log
    SET
      txid_range = numrange(lower(txid_range), txid_current(), '[)') 
    WHERE
      audit_table_id = tab_id
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.register_audit_table( 
  audit_table_name TEXT,
  audit_schema_name TEXT DEFAULT 'public'
  ) RETURNS INTEGER AS
$$
DECLARE
  tab_id INTEGER;
BEGIN
  -- first check if table is audited
  IF NOT EXISTS (
    SELECT
      1
    FROM
      pgmemento.audit_tables
    WHERE
      tablename = $1
      AND schemaname = $2
  ) THEN
    RETURN NULL;
  ELSE
    -- check if affected table exists in 'audit_table_log' (with open range)
    SELECT
      id INTO tab_id
    FROM
      pgmemento.audit_table_log 
    WHERE
      table_name = $1
      AND schema_name = $2
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;

    IF tab_id IS NULL THEN
      -- check if table exists in 'audit_table_log' with another name (and open range)
      -- if so, unregister first before making new inserts
      PERFORM
        pgmemento.unregister_audit_table(table_name, schema_name)
      FROM
        pgmemento.audit_table_log 
      WHERE
        relid = ($2 || '.' || $1)::regclass::oid
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL;

      -- now register table and corresponding columns in audit tables
      INSERT INTO pgmemento.audit_table_log
        (relid, schema_name, table_name, txid_range)
      VALUES 
        (($2 || '.' || $1)::regclass::oid, $2, $1, numrange(txid_current(), NULL, '[)'))
      RETURNING id INTO tab_id;

      -- insert columns of new audited table into 'audit_column_log'
      INSERT INTO pgmemento.audit_column_log 
        (id, audit_table_id, column_name, ordinal_position, column_default, not_null, data_type, txid_range)
      (
        SELECT 
          nextval('pgmemento.audit_column_log_id_seq') AS id,
          tab_id AS audit_table_id,
          a.attname AS column_name,
          a.attnum AS ordinal_position,
          d.adsrc AS column_default,
          a.attnotnull AS not_null,
          substr(
            format_type(a.atttypid, a.atttypmod),
            position('.' IN format_type(a.atttypid, a.atttypmod))+1,
            length(format_type(a.atttypid, a.atttypmod))
          ) AS data_type,
          numrange(txid_current(), NULL, '[)') AS txid_range
        FROM
          pg_attribute a
        LEFT JOIN
          pg_attrdef d
          ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
        WHERE
          a.attrelid = ($2 || '.' || $1)::regclass
          AND a.attname <> 'audit_id'
          AND a.attnum > 0
          AND NOT a.attisdropped
          ORDER BY a.attnum
      );
    END IF;
  END IF;

  RETURN tab_id;
END;
$$
LANGUAGE plpgsql STRICT;


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
    SELECT
      1
    FROM
      pg_trigger
    WHERE
      tgrelid = ($2 || '.' || $1)::regclass::oid
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
         $2, $1);

    -- second trigger to be fired before truncate events 
    EXECUTE format(
      'CREATE TRIGGER log_truncate_trigger 
         BEFORE TRUNCATE ON %I.%I
         FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_truncate()',
         $2, $1);

    /*
      row level triggers
    */
    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER log_insert_trigger
         AFTER INSERT ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_insert()',
         $2, $1);

    -- trigger to be fired after update events
    EXECUTE format(
      'CREATE TRIGGER log_update_trigger
         AFTER UPDATE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_update()',
         $2, $1);

    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER log_delete_trigger
         AFTER DELETE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_delete()',
         $2, $1);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform create_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_log_trigger(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_log_trigger(c.relname, $1)
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

-- drop logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_log_trigger(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public' 
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('DROP TRIGGER IF EXISTS log_delete_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_update_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_insert_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_truncate_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_transaction_trigger ON %I.%I', $2, $1);
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_log_trigger(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.drop_table_log_trigger(c.relname, $1)
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
    SELECT
      1
    FROM
      pg_attribute
    WHERE
      attrelid = ($2 || '.' || $1)::regclass
      AND attname = 'audit_id'
      AND NOT attisdropped
  ) THEN
    EXECUTE format(
      'ALTER TABLE %I.%I ADD COLUMN audit_id BIGINT DEFAULT nextval(''pgmemento.audit_id_seq''::regclass) UNIQUE NOT NULL',
      $2, $1);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform create_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit_id(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_audit_id(c.relname, $1)
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

-- drop column 'audit_id' from a table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- drop 'audit_id' column if it exists
  IF EXISTS (
    SELECT
      1
    FROM
      pg_attribute
    WHERE
      attrelid = ($2 || '.' || $1)::regclass::oid
      AND attname = 'audit_id'
      AND attislocal = 't'
      AND NOT attisdropped
  ) THEN
    EXECUTE format(
      'ALTER TABLE %I.%I DROP COLUMN audit_id',
      $2, $1);

    -- update audit_table_log and audit_column_log
    PERFORM pgmemento.unregister_audit_table($1, $2);
  ELSE
    RETURN;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit_id(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.drop_table_audit_id(c.relname, $1)
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
  -- try to log corresponding transaction
  INSERT INTO pgmemento.transaction_log 
    (txid, stmt_date, user_name, client_name)
  VALUES 
    (txid_current(), statement_timestamp(), current_user, inet_client_addr())
  ON CONFLICT (txid)
    DO NOTHING;

  -- assign id for operation type
  CASE TG_OP
    WHEN 'INSERT' THEN operation_id := 3;
    WHEN 'UPDATE' THEN operation_id := 4;
    WHEN 'DELETE' THEN operation_id := 7;
    WHEN 'TRUNCATE' THEN operation_id := 8;
  END CASE;

  -- try to log corresponding table event
  -- on conflict do nothing
  INSERT INTO pgmemento.table_event_log 
    (transaction_id, op_id, table_operation, table_relid) 
  VALUES
    (txid_current(), operation_id, TG_OP, TG_RELID)
  ON CONFLICT (transaction_id, table_relid, op_id)
    DO NOTHING;

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
  SELECT
    id INTO e_id
  FROM
    pgmemento.table_event_log 
  WHERE
    transaction_id = txid_current() 
    AND table_relid = TG_RELID
    AND op_id = 8;

  -- log the whole content of the truncated table in the row_log table
  EXECUTE format(
    'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
       SELECT $1, audit_id, to_jsonb(%I) AS content FROM %I.%I',
       TG_TABLE_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME
    ) USING e_id;

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
  SELECT
    id INTO e_id
  FROM
    pgmemento.table_event_log 
  WHERE
    transaction_id = txid_current() 
    AND table_relid = TG_RELID
    AND op_id = 3;

  -- log inserted row ('changes' column can be left blank)
  INSERT INTO pgmemento.row_log
    (event_id, audit_id)
  VALUES
    (e_id, NEW.audit_id);
			 
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
  jsonb_diff JSONB;
BEGIN
  -- get corresponding table event as it has already been logged
  -- by the log_transaction_trigger in advance
  SELECT
    id INTO e_id
  FROM
    pgmemento.table_event_log 
  WHERE
    transaction_id = txid_current() 
    AND table_relid = TG_RELID
    AND op_id = 4;

  -- log values of updated columns for the processed row
  -- therefore, a diff between OLD and NEW is necessary
  SELECT COALESCE(
    (SELECT
       ('{' || string_agg(to_json(key) || ':' || value, ',') || '}') 
     FROM
       jsonb_each(to_jsonb(OLD))
     WHERE
       NOT ('{' || to_json(key) || ':' || value || '}')::jsonb <@ to_jsonb(NEW)
    ),
    '{}')::jsonb INTO jsonb_diff;

  IF jsonb_diff <> '{}'::jsonb THEN
    INSERT INTO pgmemento.row_log
      (event_id, audit_id, changes)
    VALUES 
      (e_id, NEW.audit_id, jsonb_diff);
  END IF;

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
  SELECT
    id INTO e_id
  FROM
    pgmemento.table_event_log 
  WHERE
    transaction_id = txid_current() 
    AND table_relid = TG_RELID
    AND op_id = 7;

  -- log content of the entire row in the row_log table
  INSERT INTO pgmemento.row_log
    (event_id, audit_id, changes)
  VALUES
    (e_id, OLD.audit_id, to_jsonb(OLD));

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* LOG TABLE STATE
*
* Log table content in the audit_log table (as inserted values)
* to have a baseline for table versioning.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_table_state(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  is_empty INTEGER := 0;
  e_id INTEGER;
  pkey_columns TEXT := '';
BEGIN
  -- first, check if table is not empty
  EXECUTE format(
    'SELECT 1 FROM %I.%I LIMIT 1',
    $2, $1)
    INTO is_empty;

  IF is_empty <> 0 THEN
    RAISE NOTICE 'Log existing data in table %.% as inserted', $1, $2;

    -- fill transaction_log table 
    INSERT INTO pgmemento.transaction_log
      (txid, stmt_date, user_name, client_name)
    VALUES 
      (txid_current(), statement_timestamp(), current_user, inet_client_addr())
    ON CONFLICT (txid)
      DO NOTHING;

    -- fill table_event_log table  
    INSERT INTO pgmemento.table_event_log
      (transaction_id, op_id, table_operation, table_relid) 
    VALUES
      (txid_current(), 3, 'INSERT', ($2 || '.' || $1)::regclass::oid)
    ON CONFLICT (transaction_id, table_relid, op_id)
      DO NOTHING
      RETURNING id INTO e_id;

    -- fill row_log table
    IF e_id IS NOT NULL THEN
      -- get the primary key columns
      SELECT
        array_to_string(array_agg(pga.attname),',') INTO pkey_columns
      FROM
        pg_index pgi,
        pg_class pgc,
        pg_attribute pga 
      WHERE
        pgc.oid = ($2 || '.' || $1)::regclass::oid
        AND pgi.indrelid = pgc.oid 
        AND pga.attrelid = pgc.oid 
        AND pga.attnum = ANY(pgi.indkey)
        AND pgi.indisprimary;

      IF pkey_columns IS NOT NULL THEN
        pkey_columns := ' ORDER BY ' || pkey_columns;
      ELSE
        pkey_columns := ' ORDER BY audit_id';
      END IF;

      EXECUTE format(
        'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
           SELECT $1, audit_id, NULL::jsonb AS changes FROM %I.%I' || pkey_columns,
           $2, $1) USING e_id;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform log_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.log_schema_state(
  schemaname TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.log_table_state(a.table_name, a.schema_name)
FROM
  pgmemento.audit_table_log a,
  pgmemento.audit_tables_dependency d
WHERE
  a.schema_name = d.schemaname
  AND a.table_name = d.tablename
  AND a.schema_name = $1
  AND d.schemaname = $1
  AND upper(a.txid_range) IS NULL
  AND lower(a.txid_range) IS NOT NULL
ORDER BY
  d.depth;
$$
LANGUAGE sql STRICT;


/**********************************************************
* ENABLE/DISABLE PGMEMENTO
*
* Enables/disables pgMemento for a specified table/schema.
***********************************************************/
-- create pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.create_table_audit( 
  table_name TEXT,
  schema_name TEXT DEFAULT 'public',
  log_state INTEGER DEFAULT 1
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger($1, $2);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id($1, $2);

  -- log existing table content as inserted
  IF $3 = 1 THEN
    PERFORM pgmemento.log_table_state($1, $2);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform create_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit(
  schema_name TEXT DEFAULT 'public',
  log_state INTEGER DEFAULT 1,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_audit(c.relname, $1, $2)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = $1
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($3,'{}')); 
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
  PERFORM pgmemento.drop_table_audit_id($1, $2);

  -- drop log trigger
  PERFORM pgmemento.drop_table_log_trigger($1, $2);
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.drop_table_audit(c.relname, $1)
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