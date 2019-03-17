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
-- 0.6.8     2019-02-14   ADD AUDIT_ID event gets its own op_id               FKun
--                        new helper function trim_outer_quotes
-- 0.6.7     2018-11-19   new log events for adding and dropping audit_id     FKun 
-- 0.6.6     2018-11-10   rename log_table_state to log_table_baseline        FKun
--                        new option for drop_table_audit to drop all logs
-- 0.6.5     2018-11-05   get_txid_bounds_to_table function now takes OID     FKun
-- 0.6.4     2018-11-01   reflect range bounds change in audit tables         FKun
-- 0.6.3     2018-10-26   fixed delta creation for UPDATEs with JSON types    FKun
-- 0.6.2     2018-10-25   log_state argument changed to boolean               FKun
-- 0.6.1     2018-07-23   moved schema parts in its own file                  FKun
-- 0.6.0     2018-07-14   additional columns in transaction_log table and     FKun
--                        better handling for internal txid cycles
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
* VIEWS:
*   audit_tables
*   audit_tables_dependency
*
* FUNCTIONS:
*   column_array_to_column_list(columns TEXT[]) RETURNS TEXT
*   create_schema_audit(schema_name TEXT DEFAULT 'public'::text, log_state BOOLEAN DEFAULT TRUE,
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_audit_id(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_log_trigger(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, log_state BOOLEAN DEFAULT TRUE) RETURNS SETOF VOID
*   create_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   create_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   drop_schema_audit(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_audit_id(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_log_trigger(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   drop_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   drop_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   get_txid_bounds_to_table(table_oid OID, OUT txid_min INTEGER, OUT txid_max INTEGER) RETURNS RECORD
*   log_schema_baseline(schemaname TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   log_table_baseline(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   log_table_event(event_txid BIGINT, table_oid OID, op_type TEXT) RETURNS INTEGER
*   log_table_state(e_id INTEGER, columns TEXT[], table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   register_audit_table(audit_table_name TEXT, audit_schema_name TEXT DEFAULT 'public'::text) RETURNS INTEGER
*   trim_outer_quotes(quoted_string TEXT) RETURNS TEXT
*   unregister_audit_table(audit_table_name TEXT, audit_schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*  
* TRIGGER FUNCTIONS
*   log_delete() RETURNS trigger
*   log_insert() RETURNS trigger
*   log_tansaction() RETURNS trigger
*   log_truncate() RETURNS trigger
*   log_update() RETURNS trigger
*
***********************************************************/

/***********************************************************
* GET TXID BOUNDS TO TABLE
*
* A helper function to get highest and lowest logged
* transaction id to an audited table 
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_txid_bounds_to_table(
  table_oid OID,
  OUT txid_min INTEGER,
  OUT txid_max INTEGER
  ) RETURNS RECORD AS
$$
SELECT
  min(transaction_id) AS txid_min,
  max(transaction_id) AS txid_max
FROM
  pgmemento.table_event_log 
WHERE
  table_relid = $1;
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
    COALESCE(bounds.txid_min, bounds_old.txid_min) AS txid_min,
    COALESCE(bounds.txid_max, bounds_old.txid_max) AS txid_max,
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
   AND n.nspname <> 'pgmemento'
   AND n.nspname NOT LIKE 'pg_temp%'
  JOIN
    pg_attribute a
    ON a.attrelid = c.oid
   AND a.attname = 'audit_id'
  JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(c.oid)
    ) bounds ON (true)
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
  LEFT JOIN
    pgmemento.audit_table_log atl
    ON atl.table_name = c.relname
   AND atl.schema_name = n.nspname
   AND upper(atl.txid_range) IS NULL
  LEFT JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(atl.relid)
    ) bounds_old ON (true)
  WHERE
    c.relkind = 'r'
  ORDER BY
    schemaname,
    tablename;

COMMENT ON VIEW pgmemento.audit_tables IS 'Lists which tables are audited by pgMemento (a.k.a. have an audit_id column)';
COMMENT ON COLUMN pgmemento.audit_tables.schemaname IS 'The schema the audited table belongs to';
COMMENT ON COLUMN pgmemento.audit_tables.tablename IS 'Name of the audited table';
COMMENT ON COLUMN pgmemento.audit_tables.txid_min IS 'The minimal transaction ID referenced to the audited table in the table_event_log';
COMMENT ON COLUMN pgmemento.audit_tables.txid_max IS 'The maximal transaction ID referenced to the audited table in the table_event_log';
COMMENT ON COLUMN pgmemento.audit_tables.tg_is_active IS 'Flag, that shows if logging is activated for the table or not';

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
    child_oid AS relid,
    schema_name AS schemaname,
    table_name AS tablename,
    depth
  FROM (
    SELECT
      child_oid,
      schema_name,
      table_name,
      max(depth) AS depth
    FROM
      table_dependency
    GROUP BY
      child_oid,
      schema_name,
      table_name
    UNION ALL
      SELECT
        atl.relid,
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
        AND upper(atl.txid_range) IS NULL
        AND lower(atl.txid_range) IS NOT NULL
  ) td
  ORDER BY
    schemaname,
    depth,
    tablename;

COMMENT ON VIEW pgmemento.audit_tables_dependency IS 'Lists the dependencies between audited tables which is important for reverts';
COMMENT ON COLUMN pgmemento.audit_tables_dependency.relid IS 'The OID of the table';
COMMENT ON COLUMN pgmemento.audit_tables_dependency.schemaname IS 'The schema name the table belongs to';
COMMENT ON COLUMN pgmemento.audit_tables_dependency.tablename IS 'The name of the table';
COMMENT ON COLUMN pgmemento.audit_tables_dependency.depth IS 'The depth of foreign key references';


/**********************************************************
* TRIM_OUTER_QUOTES
*
* Helper function to support auditing quoted tables
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.trim_outer_quotes(quoted_string TEXT) RETURNS TEXT AS
$$
SELECT
  CASE WHEN length(btrim($1, '"')) < length($1)
  THEN replace(substr($1, 2, length($1) - 2),'""','"')
  ELSE replace($1,'""','"')
  END;
$$
LANGUAGE sql;


/**********************************************************
* UN/REGISTER TABLE
*
* Function to un/register information of audited table in
* audit_table_log and corresponding columns in audit_column_log
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.unregister_audit_table(
  audit_table_name TEXT,
  audit_schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
DECLARE
  tab_id INTEGER;
BEGIN
  -- update txid_range for removed table in audit_table_log table
  UPDATE
    pgmemento.audit_table_log
  SET
    txid_range = numrange(lower(txid_range), current_setting('pgmemento.' || txid_current())::numeric, '(]')
  WHERE
    table_name = pgmemento.trim_outer_quotes($1)
    AND schema_name = pgmemento.trim_outer_quotes($2)
    AND upper(txid_range) IS NULL
    AND lower(txid_range) IS NOT NULL
  RETURNING
    id INTO tab_id;

  IF tab_id IS NOT NULL THEN
    -- update txid_range for removed columns in audit_column_log table
    UPDATE
      pgmemento.audit_column_log
    SET
      txid_range = numrange(lower(txid_range), current_setting('pgmemento.' || txid_current())::numeric, '(]') 
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
  audit_schema_name TEXT DEFAULT 'public'::text
  ) RETURNS INTEGER AS
$$
DECLARE
  tab_id INTEGER;
  old_table_name TEXT;
  old_schema_name TEXT;
BEGIN
  -- first check if table is audited
  IF NOT EXISTS (
    SELECT
      1
    FROM
      pgmemento.audit_tables
    WHERE
      tablename = pgmemento.trim_outer_quotes($1)
      AND schemaname = pgmemento.trim_outer_quotes($2)
  ) THEN
    RETURN NULL;
  ELSE
    -- check if affected table exists in 'audit_table_log' (with open range)
    SELECT
      id INTO tab_id
    FROM
      pgmemento.audit_table_log 
    WHERE
      table_name = pgmemento.trim_outer_quotes($1)
      AND schema_name = pgmemento.trim_outer_quotes($2)
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;

    IF tab_id IS NULL THEN
      -- check if table exists in 'audit_table_log' with another name (and open range)
      SELECT
        table_name,
        schema_name
      INTO
        old_table_name,
        old_schema_name
      FROM
        pgmemento.audit_table_log 
      WHERE
        relid = ($2 || '.' || $1)::regclass::oid
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL;

      -- if so, unregister first before making new inserts
      IF old_table_name IS NOT NULL AND old_schema_name IS NOT NULL THEN
        PERFORM pgmemento.unregister_audit_table(old_table_name, old_schema_name);
      END IF;

      -- now register table and corresponding columns in audit tables
      INSERT INTO pgmemento.audit_table_log
        (relid, schema_name, table_name, txid_range)
      VALUES 
        (($2 || '.' || $1)::regclass::oid, pgmemento.trim_outer_quotes($2), pgmemento.trim_outer_quotes($1), numrange(current_setting('pgmemento.' || txid_current())::numeric, NULL, '(]'))
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
          numrange(current_setting('pgmemento.' || txid_current())::numeric, NULL, '(]') AS txid_range
        FROM
          pg_attribute a
        LEFT JOIN
          pg_attrdef d
          ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
        WHERE
          a.attrelid = ($2 || '.' || $1)::regclass::oid
          AND a.attname <> 'audit_id'
          AND a.attnum > 0
          AND NOT a.attisdropped
          ORDER BY a.attnum
      );

      -- rename unique constraint for audit_id column
      IF old_table_name IS NOT NULL AND old_schema_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
          pgmemento.trim_outer_quotes($2), pgmemento.trim_outer_quotes($1), old_table_name || '_audit_id_key', pgmemento.trim_outer_quotes($1) || '_audit_id_key');
      END IF;
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
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
DECLARE
  tablename TEXT := pgmemento.trim_outer_quotes($1);
  schemaname TEXT := pgmemento.trim_outer_quotes($2);
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
         schemaname, tablename);

    -- second trigger to be fired before truncate events 
    EXECUTE format(
      'CREATE TRIGGER log_truncate_trigger 
         BEFORE TRUNCATE ON %I.%I
         FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_truncate()',
         schemaname, tablename);

    /*
      row level triggers
    */
    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER log_insert_trigger
         AFTER INSERT ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_insert()',
         schemaname, tablename);

    -- trigger to be fired after update events
    EXECUTE format(
      'CREATE TRIGGER log_update_trigger
         AFTER UPDATE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_update()',
         schemaname, tablename);

    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER log_delete_trigger
         AFTER DELETE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_delete()',
         schemaname, tablename);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform create_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_log_trigger(
  schema_name TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_log_trigger(quote_ident(c.relname), $1)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = pgmemento.trim_outer_quotes($1)
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($2,'{}'));
$$
LANGUAGE sql;

-- drop logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_log_trigger(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text 
  ) RETURNS SETOF VOID AS
$$
DECLARE
  schemaname TEXT := pgmemento.trim_outer_quotes($2);
BEGIN
  EXECUTE format('DROP TRIGGER IF EXISTS log_delete_trigger ON %I.%I', schemaname, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_update_trigger ON %I.%I', schemaname, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_insert_trigger ON %I.%I', schemaname, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_truncate_trigger ON %I.%I', schemaname, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS log_transaction_trigger ON %I.%I', schemaname, $1);
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_log_trigger(
  schema_name TEXT DEFAULT 'public'::text,
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
  AND n.nspname = pgmemento.trim_outer_quotes($1)
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
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- log as 'add column' event, as it is not done by event triggers
  PERFORM pgmemento.log_table_event(txid_current(),($2 || '.' || $1)::regclass::oid, 'ADD AUDIT_ID');

  -- add 'audit_id' column to table if it does not exist, yet
  IF NOT EXISTS (
    SELECT
      1
    FROM
      pg_attribute
    WHERE
      attrelid = ($2 || '.' || $1)::regclass::oid
      AND attname = 'audit_id'
      AND NOT attisdropped
  ) THEN
    EXECUTE format(
      'ALTER TABLE %I.%I ADD COLUMN audit_id BIGINT DEFAULT nextval(''pgmemento.audit_id_seq''::regclass) UNIQUE NOT NULL',
      pgmemento.trim_outer_quotes($2), pgmemento.trim_outer_quotes($1));
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform create_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit_id(
  schema_name TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_audit_id(quote_ident(c.relname), $1)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = pgmemento.trim_outer_quotes($1)
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($2,'{}'));
$$
LANGUAGE sql;

-- drop column 'audit_id' from a table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
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
      'ALTER TABLE %I.%I DROP CONSTRAINT %I, DROP COLUMN audit_id',
      pgmemento.trim_outer_quotes($2), pgmemento.trim_outer_quotes($1), pgmemento.trim_outer_quotes($1) || '_audit_id_key');
  ELSE
    RETURN;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit_id(
  schema_name TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.drop_table_audit_id(quote_ident(c.relname), $1)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = pgmemento.trim_outer_quotes($1)
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($2,'{}'));
$$
LANGUAGE sql;


/**********************************************************
* LOG TABLE STATE
*
* Function to log the whole content of a table or only
* for given columns.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.column_array_to_column_list(columns TEXT[]) RETURNS TEXT AS
$$
SELECT
  array_to_string(array_agg(format('%L, %I', pgmemento.trim_outer_quotes(k), pgmemento.trim_outer_quotes(v))), ', ')
FROM
  unnest($1) k,
  unnest($1) v
WHERE
  k = v;
$$
LANGUAGE sql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.log_table_state(
  e_id INTEGER,
  columns TEXT[],
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $2 IS NOT NULL AND array_length($2, 1) IS NOT NULL THEN
    -- log content of given columns
    EXECUTE format(
      'INSERT INTO pgmemento.row_log(event_id, audit_id, changes)
         SELECT $1, audit_id, jsonb_build_object('||pgmemento.column_array_to_column_list($2)||') AS content FROM %I.%I ORDER BY audit_id',
         pgmemento.trim_outer_quotes($4), pgmemento.trim_outer_quotes($3)) USING $1;
  ELSE
    -- log content of entire table 
    EXECUTE format(
      'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
         SELECT $1, audit_id, to_jsonb(%I) AS content FROM %I.%I ORDER BY audit_id',
         pgmemento.trim_outer_quotes($3), pgmemento.trim_outer_quotes($4), pgmemento.trim_outer_quotes($3)) USING $1;
  END IF;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* LOG TABLE EVENT
*
* Function that write information of ddl and dml events into
* transaction_log and table_event_log and returns the event ID
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_table_event(
  event_txid BIGINT,
  table_oid OID,
  op_type TEXT
  ) RETURNS INTEGER AS
$$
DECLARE
  session_info_text TEXT;
  session_info_obj JSONB;
  transaction_log_id INTEGER;
  operation_id SMALLINT;
  table_event_log_id INTEGER;
BEGIN
  -- retrieve session_info set by client
  BEGIN
    session_info_text := current_setting('pgmemento.session_info');

    IF session_info_text IS NULL OR session_info_text = '' THEN
      session_info_obj := NULL;
    ELSE
      session_info_obj := session_info_text::jsonb;
    END IF;

    EXCEPTION
      WHEN invalid_text_representation THEN
        BEGIN
          session_info_obj := to_jsonb(current_setting('pgmemento.session_info'));
        END;
      WHEN others THEN
        session_info_obj := NULL;
  END;

  -- try to log corresponding transaction
  INSERT INTO pgmemento.transaction_log 
    (txid, stmt_date, process_id, user_name, client_name, client_port, application_name, session_info)
  VALUES 
    ($1, transaction_timestamp(), pg_backend_pid(), current_user, inet_client_addr(), inet_client_port(),
     current_setting('application_name'), session_info_obj
    )
  ON CONFLICT (txid, stmt_date)
    DO NOTHING
  RETURNING id
  INTO transaction_log_id;

  IF transaction_log_id IS NOT NULL THEN
    PERFORM set_config('pgmemento.' || $1, transaction_log_id::text, TRUE);
  ELSE
    transaction_log_id := current_setting('pgmemento.' || $1)::int;
  END IF;

  -- assign id for operation type
  CASE $3
    WHEN 'CREATE TABLE' THEN operation_id := 1;
    WHEN 'RENAME TABLE' THEN operation_id := 12;
    WHEN 'ADD COLUMN' THEN operation_id := 2;
    WHEN 'ADD AUDIT_ID' THEN operation_id := 21;
    WHEN 'RENAME COLUMN' THEN operation_id := 22;
    WHEN 'INSERT' THEN operation_id := 3;
    WHEN 'UPDATE' THEN operation_id := 4;
    WHEN 'ALTER COLUMN' THEN operation_id := 5;
    WHEN 'DROP COLUMN' THEN operation_id := 6;
    WHEN 'DELETE' THEN operation_id := 7;
    WHEN 'TRUNCATE' THEN operation_id := 8;
    WHEN 'DROP AUDIT_ID' THEN operation_id := 8;
    WHEN 'DROP TABLE' THEN operation_id := 9;
  END CASE;

  -- try to log corresponding table event
  -- on conflict do nothing
  INSERT INTO pgmemento.table_event_log 
    (transaction_id, op_id, table_operation, table_relid) 
  VALUES
    (transaction_log_id, operation_id, $3, $2)
  ON CONFLICT (transaction_id, table_relid, op_id)
    DO NOTHING
  RETURNING id
  INTO table_event_log_id;

  /* too many variables have a bad side effect
  IF table_event_log_id IS NOT NULL THEN
    PERFORM set_config('pgmemento.' || $1 || '_' || $2 || '_' || operation_id, table_event_log_id::text, TRUE);
  ELSE
    table_event_log_id := current_setting('pgmemento.' || $1 || '_' || $2 || '_' || operation_id)::int;
  END IF;
  */

  RETURN table_event_log_id;
END;
$$
LANGUAGE plpgsql;

/**********************************************************
* TRIGGER PROCEDURE log_transaction
*
* Procedure that is called when a log_transaction_trigger is fired.
* Metadata of each transaction is written to the transaction_log table.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_transaction() RETURNS trigger AS
$$
BEGIN
  PERFORM pgmemento.log_table_event(txid_current(), TG_RELID, TG_OP);
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
    transaction_id = current_setting('pgmemento.' || txid_current())::int
    AND table_relid = TG_RELID
    AND op_id = 8;

  -- log the whole content of the truncated table in the row_log table
  PERFORM pgmemento.log_table_state(
    e_id,
    --current_setting('pgmemento.' || txid_current() || '_' || TG_RELID || '_' || 8)::int,
    '{}'::text[], TG_TABLE_NAME, TG_TABLE_SCHEMA
  );

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
    transaction_id = current_setting('pgmemento.' || txid_current())::int
    AND table_relid = TG_RELID
    AND op_id = 3;

  -- log inserted row ('changes' column can be left blank)
  INSERT INTO pgmemento.row_log
    (event_id, audit_id)
  VALUES
    (e_id,
     --current_setting('pgmemento.' || txid_current() || '_' || TG_RELID || '_' || 3)::int, 
     NEW.audit_id);

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
    transaction_id = current_setting('pgmemento.' || txid_current())::int
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
      to_jsonb(NEW) ->> key IS DISTINCT FROM to_jsonb(OLD) ->> key
    ),
    '{}')::jsonb INTO jsonb_diff;

  IF jsonb_diff <> '{}'::jsonb THEN
    INSERT INTO pgmemento.row_log
      (event_id, audit_id, changes)
    VALUES 
      (e_id,
       --current_setting('pgmemento.' || txid_current() || '_' || TG_RELID || '_' || 4)::int, 
       NEW.audit_id, jsonb_diff);
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
    transaction_id = current_setting('pgmemento.' || txid_current())::int
    AND table_relid = TG_RELID
    AND op_id = 7;

  -- log content of the entire row in the row_log table
  INSERT INTO pgmemento.row_log
    (event_id, audit_id, changes)
  VALUES
    (e_id,
     --current_setting('pgmemento.' || txid_current() || '_' || TG_RELID || '_' || 7)::int, 
     OLD.audit_id, to_jsonb(OLD));

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* LOG TABLE BASELINE
*
* Log table content in the row_log table (as inserted values)
* to have a baseline for table versioning.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_table_baseline(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
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
    pgmemento.trim_outer_quotes($2), pgmemento.trim_outer_quotes($1))
    INTO is_empty;

  IF is_empty <> 0 THEN
    RAISE NOTICE 'Log existing data in table %.% as inserted', $1, $2;
    e_id := pgmemento.log_table_event(txid_current(), ($2 || '.' || $1)::regclass::oid, 'INSERT');

    -- fill row_log table
    IF e_id IS NOT NULL THEN
      -- get the primary key columns
      SELECT
        array_to_string(array_agg('t.' || pga.attname),',') INTO pkey_columns
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
        pkey_columns := ' ORDER BY t.audit_id';
      END IF;

      EXECUTE format(
        'INSERT INTO pgmemento.row_log (event_id, audit_id, changes) '
         || 'SELECT $1, t.audit_id, NULL::jsonb AS changes FROM %I.%I t '
         || 'LEFT JOIN pgmemento.row_log r ON r.audit_id = t.audit_id '
         || 'WHERE r.audit_id IS NULL' || pkey_columns,
         pgmemento.trim_outer_quotes($2), pgmemento.trim_outer_quotes($1)) USING e_id;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform log_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.log_schema_baseline(
  schemaname TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.log_table_baseline(a.table_name, a.schema_name)
FROM
  pgmemento.audit_table_log a,
  pgmemento.audit_tables_dependency d
WHERE
  a.schema_name = d.schemaname
  AND a.table_name = d.tablename
  AND a.schema_name = pgmemento.trim_outer_quotes($1)
  AND d.schemaname = pgmemento.trim_outer_quotes($1)
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
  schema_name TEXT DEFAULT 'public'::text,
  log_state BOOLEAN DEFAULT TRUE
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger($1, $2);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id($1, $2);

  -- log existing table content as inserted
  IF $3 THEN
    PERFORM pgmemento.log_table_baseline($1, $2);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform create_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit(
  schema_name TEXT DEFAULT 'public'::text,
  log_state BOOLEAN DEFAULT TRUE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_audit(quote_ident(c.relname), $1, $2)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = pgmemento.trim_outer_quotes($1)
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($3,'{}')); 
$$
LANGUAGE sql;

-- drop pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  keep_log BOOLEAN DEFAULT TRUE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  e_id INTEGER;
BEGIN
  -- first drop log trigger
  PERFORM pgmemento.drop_table_log_trigger($1, $2);

  -- then either keep the audit trail for table or delete everything
  IF $3 THEN
    -- log event as event triggers will walk around anything related to the audit_id
    e_id := pgmemento.log_table_event(txid_current(),($2 || '.' || $1)::regclass::oid, 'DROP AUDIT_ID');

    -- log the whole content of the table to keep the reference between audit_id and table rows
    PERFORM pgmemento.log_table_state(e_id, '{}'::text[], $1, $2);

    -- update audit_table_log and audit_column_log
    PERFORM pgmemento.unregister_audit_table($1, $2);
  ELSE
    -- remove all logs related to given table
    PERFORM pgmemento.delete_audit_table_log(($2 || '.' || $1)::regclass::oid);
  END IF;

  -- drop audit_id column
  PERFORM pgmemento.drop_table_audit_id($1, $2);
END;
$$
LANGUAGE plpgsql STRICT;

-- perform drop_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit(
  schema_name TEXT DEFAULT 'public'::text,
  keep_log BOOLEAN DEFAULT TRUE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.drop_table_audit(quote_ident(c.relname), $1, $2)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = pgmemento.trim_outer_quotes($1)
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($3,'{}'));
$$
LANGUAGE sql;
