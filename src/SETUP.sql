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
-- 0.7.4     2020-02-29   added option to also log new data in row_log        FKun
-- 0.7.3     2020-02-09   reflect changes on schema and triggers              FKun
-- 0.7.2     2020-02-08   new get_table_oid function to replace trimming      FKun
-- 0.7.1     2019-04-21   introduce new event RECREATE TABLE with op_id       FKun
-- 0.7.0     2019-03-23   reflect schema changes in UDFs and VIEWs            FKun
-- 0.6.9     2019-03-23   Audit views list tables even on relid mismatch      FKun
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
*     include_new BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_audit_id(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_log_trigger(schema_name TEXT DEFAULT 'public'::text, include_new BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public'::text,
*     log_state BOOLEAN DEFAULT TRUE, include_new BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   create_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   create_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, include_new BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   drop_schema_audit(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_audit_id(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_log_trigger(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   drop_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   drop_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   get_operation_id(operation TEXT) RETURNS SMALLINT
*   get_table_oid(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS OID
*   get_txid_bounds_to_table(table_log_id INTEGER, OUT txid_min INTEGER, OUT txid_max INTEGER) RETURNS RECORD
*   log_new_table_state(columns TEXT[], table_name TEXT, schema_name TEXT DEFAULT 'public'::text, table_event_key TEXT) RETURNS SETOF VOID
*   log_old_table_state(columns TEXT[], table_name TEXT, schema_name TEXT DEFAULT 'public'::text, table_event_key TEXT) RETURNS SETOF VOID
*   log_schema_baseline(schemaname TEXT DEFAULT 'public'::text, include_new BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   log_table_baseline(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, include_new BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   log_table_event(event_txid BIGINT, tablename TEXT, schemaname TEXT, op_type TEXT) RETURNS TEXT
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
  table_log_id INTEGER,
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
  table_log_id = $1;
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
    bounds.txid_min,
    bounds.txid_max,
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
  LEFT JOIN
    pgmemento.audit_table_log atl
    ON atl.table_name = c.relname
   AND atl.schema_name = n.nspname
   AND upper(atl.txid_range) IS NULL
  LEFT JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(atl.log_id)
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
    table_log_id,
    table_name,
    schema_name,
    depth
  ) AS (
    SELECT DISTINCT ON (ct.conrelid)
      ct.confrelid AS parent_oid,
      ct.conrelid AS child_oid,
      a.log_id AS table_log_id,
      a.table_name,
      n.nspname AS schema_name,
      1 AS depth
    FROM
      pg_class c
    JOIN
      pg_namespace n
      ON n.oid = c.relnamespace
    JOIN
      pg_constraint ct
      ON ct.conrelid = c.oid
    JOIN pgmemento.audit_table_log a
      ON a.table_name = c.relname
     AND a.schema_name = n.nspname
     AND upper(a.txid_range) IS NULL
     AND lower(a.txid_range) IS NOT NULL
    WHERE
      ct.contype = 'f'
      AND ct.conrelid <> ct.confrelid
    UNION ALL
      SELECT DISTINCT ON (ct.conrelid)
        ct.confrelid AS parent_oid,
        ct.conrelid AS child_oid,
        a.log_id AS table_log_id,
        a.table_name,
        n.nspname AS schema_name,
        d.depth + 1 AS depth
      FROM
        pg_class c
      JOIN
        pg_namespace n
        ON n.oid = c.relnamespace
      JOIN
        pg_constraint ct
        ON ct.conrelid = c.oid
      JOIN pgmemento.audit_table_log a
        ON a.table_name = c.relname
       AND a.schema_name = n.nspname
       AND upper(a.txid_range) IS NULL
       AND lower(a.txid_range) IS NOT NULL
      JOIN table_dependency d
        ON d.child_oid = ct.confrelid
      WHERE
        ct.contype = 'f'
        AND d.child_oid <> ct.conrelid
  )
  SELECT
    child_oid AS relid,
    table_log_id,
    schema_name AS schemaname,
    table_name AS tablename,
    depth
  FROM (
    SELECT
      child_oid,
      table_log_id,
      schema_name,
      table_name,
      max(depth) AS depth
    FROM
      table_dependency
    GROUP BY
      child_oid,
      table_log_id,
      schema_name,
      table_name
    UNION ALL
      SELECT
        atl.relid,
        atl.log_id AS table_log_id,
        atl.schema_name,
        atl.table_name,
        0 AS depth
      FROM
        pgmemento.audit_table_log atl
      LEFT JOIN
        table_dependency d
        ON d.table_log_id = atl.log_id
      WHERE
        d.table_log_id IS NULL
        AND upper(atl.txid_range) IS NULL
        AND lower(atl.txid_range) IS NOT NULL
  ) td
  ORDER BY
    schemaname,
    depth,
    tablename;

COMMENT ON VIEW pgmemento.audit_tables_dependency IS 'Lists the dependencies between audited tables which is important for reverts';
COMMENT ON COLUMN pgmemento.audit_tables_dependency.relid IS 'The OID of the table';
COMMENT ON COLUMN pgmemento.audit_tables_dependency.table_log_id IS 'The tracing log ID from audit_table_log';
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
* GET_OPERATION_iD
*
* Helper function to return id for triggered operation
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_operation_id(operation TEXT) RETURNS SMALLINT AS
$$
SELECT (CASE $1
  WHEN 'CREATE TABLE' THEN 1
  WHEN 'RECREATE TABLE' THEN 1
  WHEN 'RENAME TABLE' THEN 12
  WHEN 'ADD COLUMN' THEN 2
  WHEN 'ADD AUDIT_ID' THEN 21
  WHEN 'RENAME COLUMN' THEN 22
  WHEN 'INSERT' THEN 3
  WHEN 'UPDATE' THEN 4
  WHEN 'ALTER COLUMN' THEN 5
  WHEN 'DROP COLUMN' THEN 6
  WHEN 'DELETE' THEN 7
  WHEN 'TRUNCATE' THEN 8
  WHEN 'DROP AUDIT_ID' THEN 8
  WHEN 'DROP TABLE' THEN 9
  ELSE NULL
END)::smallint;
$$
LANGUAGE sql IMMUTABLE STRICT;

/**********************************************************
* GET_TABLE_OID
*
* Returns the OID for schema.table / "schema"."table"
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_table_oid(
  table_name TEXT, 
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS OID AS
$$
DECLARE
  table_oid OID;
BEGIN
  table_oid := ($2 || '.' || $1)::regclass::oid;
  RETURN table_oid;

  EXCEPTION
    WHEN others THEN
      table_oid := (quote_ident($2) || '.' || quote_ident($1))::regclass::oid;
      RETURN table_oid;
END;
$$
LANGUAGE plpgsql STRICT;


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
  table_log_id INTEGER;
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
      BEGIN
        -- check if table exists in 'audit_table_log' with another name (and open range)
        table_log_id := current_setting('pgmemento.' || quote_ident($2) || '.' || quote_ident($1))::int;

        IF NOT EXISTS (
         SELECT
           1
         FROM
           pgmemento.table_event_log
         WHERE
           transaction_id = current_setting('pgmemento.' || txid_current())::int
           AND table_name = $1
           AND schema_name = $2
           AND op_id = 1
           AND table_operation = 'RECREATE TABLE'
        ) THEN
          SELECT
            table_name,
            schema_name
          INTO
            old_table_name,
            old_schema_name
          FROM
            pgmemento.audit_table_log
          WHERE
            log_id = table_log_id
            AND upper(txid_range) IS NULL
            AND lower(txid_range) IS NOT NULL;
        END IF;

        EXCEPTION
          WHEN others THEN
            table_log_id := nextval('pgmemento.table_log_id_seq');
      END;

      -- if so, unregister first before making new inserts
      IF old_table_name IS NOT NULL AND old_schema_name IS NOT NULL THEN
        PERFORM pgmemento.unregister_audit_table(old_table_name, old_schema_name);
      END IF;

      -- now register table and corresponding columns in audit tables
      INSERT INTO pgmemento.audit_table_log
        (log_id, relid, schema_name, table_name, txid_range)
      VALUES
        (table_log_id, pgmemento.get_table_oid($1, $2), $2, $1, numrange(current_setting('pgmemento.' || txid_current())::numeric, NULL, '(]'))
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
          pg_get_expr(d.adbin, d.adrelid, TRUE) AS column_default,
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
          a.attrelid = pgmemento.get_table_oid($1, $2)
          AND a.attname <> 'audit_id'
          AND a.attnum > 0
          AND NOT a.attisdropped
          ORDER BY a.attnum
      );

      -- rename unique constraint for audit_id column
      IF old_table_name IS NOT NULL AND old_schema_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
          $2, $1, old_table_name || '_audit_id_key', $1 || '_audit_id_key');
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
  schema_name TEXT DEFAULT 'public'::text,
  include_new BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF EXISTS (
    SELECT
      1
    FROM
      pg_trigger
    WHERE
      tgrelid = pgmemento.get_table_oid($1, $2)
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
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_insert(%s)',
         $2, $1, CASE WHEN $3 THEN 'log' ELSE '' END);

    -- trigger to be fired after update events
    EXECUTE format(
      'CREATE TRIGGER log_update_trigger
         AFTER UPDATE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_update(%s)',
         $2, $1, CASE WHEN $3 THEN 'log' ELSE '' END);

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
  schema_name TEXT DEFAULT 'public'::text,
  include_new BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_log_trigger(c.relname, $1, $2)
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

-- drop logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_log_trigger(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
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
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- log as 'add column' event, as it is not done by event triggers
  PERFORM pgmemento.log_table_event(txid_current(), $1, $2, 'ADD AUDIT_ID');

  -- add 'audit_id' column to table if it does not exist, yet
  IF NOT EXISTS (
    SELECT
      1
    FROM
      pg_attribute
    WHERE
      attrelid = pgmemento.get_table_oid($1, $2)
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
  schema_name TEXT DEFAULT 'public'::text,
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
      attrelid = pgmemento.get_table_oid($1, $2)
      AND attname = 'audit_id'
      AND attislocal = 't'
      AND NOT attisdropped
  ) THEN
    EXECUTE format(
      'ALTER TABLE %I.%I DROP CONSTRAINT %I, DROP COLUMN audit_id',
      $2, $1, $1 || '_audit_id_key');
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
* LOG TABLE STATE
*
* Function to log the whole content of a table or only
* for given columns.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.column_array_to_column_list(columns TEXT[]) RETURNS TEXT AS
$$
SELECT
  array_to_string(array_agg(format('%L, %I', k, v)), ', ')
FROM
  unnest($1) k,
  unnest($1) v
WHERE
  k = v;
$$
LANGUAGE sql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.log_old_table_state(
  columns TEXT[],
  tablename TEXT,
  schemaname TEXT,
  table_event_key TEXT
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $1 IS NOT NULL AND array_length($1, 1) IS NOT NULL THEN
    -- log content of given columns
    EXECUTE format(
      'INSERT INTO pgmemento.row_log(audit_id, event_key, old_data)
         SELECT audit_id, $1, jsonb_build_object('||pgmemento.column_array_to_column_list($1)||') AS content
           FROM %I.%I ORDER BY audit_id
       ON CONFLICT (audit_id, event_key)
       DO NOTHING',
       $3, $2) USING $4;
  ELSE
    -- log content of entire table
    EXECUTE format(
      'INSERT INTO pgmemento.row_log (audit_id, event_key, old_data)
         SELECT audit_id, $1, to_jsonb(%I) AS content
           FROM %I.%I ORDER BY audit_id
       ON CONFLICT (audit_id, event_key)
       DO NOTHING',
       $2, $3, $2) USING $4;
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.log_new_table_state(
  columns TEXT[],
  tablename TEXT,
  schemaname TEXT,
  table_event_key TEXT
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $1 IS NOT NULL AND array_length($1, 1) IS NOT NULL THEN
    -- log content of given columns
    EXECUTE format(
      'INSERT INTO pgmemento.row_log(audit_id, event_key, new_data)
         SELECT audit_id, $1, jsonb_build_object('||pgmemento.column_array_to_column_list($1)||') AS content
           FROM %I.%I ORDER BY audit_id
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET new_data = excluded.new_data',
       $3, $2) USING $4;
  ELSE
    -- log content of entire table
    EXECUTE format(
      'INSERT INTO pgmemento.row_log (audit_id, event_key, new_data)
         SELECT audit_id, $1, to_jsonb(%I) AS content
           FROM %I.%I ORDER BY audit_id
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET new_data = excluded.new_data',
       $2, $3, $2) USING $4;
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
  tablename TEXT,
  schemaname TEXT,
  op_type TEXT
  ) RETURNS TEXT AS
$$
DECLARE
  atl_log_id INTEGER;
  session_info_text TEXT;
  session_info_obj JSONB;
  txid_ts TIMESTAMP WITH TIME ZONE;
  stmt_ts TIMESTAMP WITH TIME ZONE := statement_timestamp();
  operation_id SMALLINT := pgmemento.get_operation_id($4);
  transaction_log_id INTEGER;
  table_event_key TEXT;
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
    (txid, txid_time, process_id, user_name, client_name, client_port, application_name, session_info)
  VALUES
    ($1, transaction_timestamp(), pg_backend_pid(), current_user, inet_client_addr(), inet_client_port(),
     current_setting('application_name'), session_info_obj
    )
  ON CONFLICT (txid_time, txid)
    DO NOTHING
  RETURNING id, txid_time
  INTO transaction_log_id, txid_ts;

  IF transaction_log_id IS NOT NULL THEN
    PERFORM set_config('pgmemento.' || $1, transaction_log_id::text, TRUE);
  ELSE
    transaction_log_id := current_setting('pgmemento.' || $1)::int;
    txid_ts := transaction_timestamp();
  END IF;

  -- try to log corresponding table event
  -- on conflict do nothing
  INSERT INTO pgmemento.table_event_log
    (transaction_id, stmt_time, op_id, table_operation, table_name, schema_name, event_key)
  VALUES
    (transaction_log_id, stmt_ts, operation_id, $4, $2, $3,
     concat_ws(';', extract(epoch from txid_ts), extract(epoch from stmt_ts), $1, operation_id, $2, $3)) 
  ON CONFLICT (event_key)
    DO NOTHING
  RETURNING event_key
  INTO table_event_key;

  RETURN table_event_key;
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
  PERFORM pgmemento.log_table_event(txid_current(), TG_TABLE_NAME, TG_TABLE_SCHEMA, TG_OP);
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
BEGIN
  -- log the whole content of the truncated table in the row_log table
  PERFORM
    pgmemento.log_old_table_state('{}'::text[], TG_TABLE_NAME, TG_TABLE_SCHEMA, event_key)
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.' || txid_current())::int
    AND table_name = TG_TABLE_NAME
    AND schema_name = TG_TABLE_SCHEMA
    AND op_id = 8;

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
BEGIN
  -- log inserted row ('old_data' column can be left blank)
  IF TG_ARGV[0] IS NULL THEN
    INSERT INTO pgmemento.row_log
      (audit_id, event_key)
    VALUES
      (NEW.audit_id,
       concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA));
  ELSE
    -- log complete new row as JSONB
    INSERT INTO pgmemento.row_log
      (audit_id, event_key, new_data)
    VALUES
      (NEW.audit_id,
       concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA),
       to_json(NEW));
  END IF;

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
  jsonb_diff_old JSONB;
  jsonb_diff_new JSONB;
BEGIN
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
    '{}')::jsonb INTO jsonb_diff_old;

  IF TG_ARGV[0] IS NOT NULL THEN
    -- switch the diff to only get the new values
    SELECT COALESCE(
      (SELECT
         ('{' || string_agg(to_json(key) || ':' || value, ',') || '}')
       FROM
         jsonb_each(to_jsonb(NEW))
       WHERE
         to_jsonb(OLD) ->> key IS DISTINCT FROM to_jsonb(NEW) ->> key
      ),
      '{}')::jsonb INTO jsonb_diff_new;    
  END IF;

  IF jsonb_diff_old <> '{}'::jsonb OR jsonb_diff_new <> '{}'::jsonb THEN
    INSERT INTO pgmemento.row_log
      (audit_id, event_key, old_data, new_data)
    VALUES
      (NEW.audit_id,
       concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA),
       jsonb_diff_old, jsonb_diff_new)
    ON CONFLICT (audit_id, event_key)
    DO UPDATE SET new_data = excluded.new_data;
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
BEGIN
  -- log content of the entire row in the row_log table
  INSERT INTO pgmemento.row_log
    (audit_id, event_key, old_data)
  VALUES
    (OLD.audit_id,
     concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA),
     to_jsonb(OLD));

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
  schema_name TEXT DEFAULT 'public'::text,
  include_new BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  is_empty INTEGER := 0;
  table_event_key TEXT;
  pkey_columns TEXT := '';
BEGIN
  -- first, check if table is not empty
  EXECUTE format('SELECT 1 FROM %I.%I LIMIT 1', $2, $1) INTO is_empty;

  IF is_empty <> 0 THEN
    RAISE NOTICE 'Log existing data in table %.% as inserted', $1, $2;
    table_event_key := pgmemento.log_table_event(txid_current(), $1, $2, 'INSERT');

    -- fill row_log table
    IF table_event_key IS NOT NULL THEN
      -- get the primary key columns
      SELECT
        array_to_string(array_agg('t.' || pga.attname),',') INTO pkey_columns
      FROM
        pg_index pgi,
        pg_class pgc,
        pg_attribute pga
      WHERE
        pgc.oid = pgmemento.get_table_oid($1, $2)
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
        'INSERT INTO pgmemento.row_log (audit_id, event_key'
         || CASE WHEN $3 THEN ', new_data' ELSE '' END
         || ') '
         || 'SELECT t.audit_id, $1'
         || CASE WHEN $3 THEN ', to_json(t.*) ' ELSE ' ' END
         || 'FROM %I.%I t '
         || 'LEFT JOIN pgmemento.row_log r ON r.audit_id = t.audit_id '
         || 'WHERE r.audit_id IS NULL' || pkey_columns
         || ' ON CONFLICT (audit_id, event_key) DO NOTHING',
         $2, $1) USING table_event_key;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform log_table_baseline on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.log_schema_baseline(
  schemaname TEXT DEFAULT 'public'::text,
  include_new BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.log_table_baseline(a.table_name, a.schema_name, $2)
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
  schema_name TEXT DEFAULT 'public'::text,
  log_state BOOLEAN DEFAULT TRUE,
  include_new BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger($1, $2, $4);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id($1, $2);

  -- log existing table content as inserted
  IF $3 THEN
    PERFORM pgmemento.log_table_baseline($1, $2, $4);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform create_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit(
  schema_name TEXT DEFAULT 'public'::text,
  log_state BOOLEAN DEFAULT TRUE,
  include_new BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_audit(c.relname, $1, $2, $3)
FROM
  pg_class c,
  pg_namespace n
WHERE
  c.relnamespace = n.oid
  AND n.nspname = $1
  AND c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($4,'{}'));
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
  table_event_key TEXT;
BEGIN
  -- first drop log trigger
  PERFORM pgmemento.drop_table_log_trigger($1, $2);

  -- log event as event triggers will walk around anything related to the audit_id
  table_event_key := pgmemento.log_table_event(txid_current(), $1, $2, 'DROP AUDIT_ID');

  -- update audit_table_log and audit_column_log
  PERFORM pgmemento.unregister_audit_table($1, $2);

  -- then either keep the audit trail for table or delete everything
  IF $3 THEN
    -- log the whole content of the table to keep the reference between audit_id and table rows
    PERFORM pgmemento.log_old_table_state('{}'::text[], $1, $2, table_event_key);
  ELSE
    -- remove all logs related to given table
    PERFORM pgmemento.delete_audit_table_log($1, $2);
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
  pgmemento.drop_table_audit(c.relname, $1, $2)
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
