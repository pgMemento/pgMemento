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
-- 0.7.14    2022-09-20   use column_array_to_column_list with to_jsonb       ekeuus
--                        because jsonb_build_object has arg limit of 100
-- 0.7.13    2021-12-23   concat jsonb logs on upsert                         FKun
-- 0.7.12    2021-12-23   session variables must start with letter in Pg14    ol-teuto
-- 0.7.11    2021-03-28   exclude audit_tables with empty txid_range          FKun
-- 0.7.10    2020-04-19   change signature for drop audit functions and       FKun
--                        define new REINIT TABLE event
-- 0.7.9     2020-04-13   remove txid from log_table_event                    FKun
-- 0.7.8     2020-03-29   make logging of old data configurable, too          FKun
-- 0.7.7     2020-03-23   allow configurable audit_id column                  FKun
-- 0.7.6     2020-03-21   new function log_transaction to do writes and       FKun
--                        renamed trigger function to log_statement
-- 0.7.5     2020-03-07   set SECURITY DEFINER where log tables are touched   FKun
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
*   create_schema_audit(schemaname TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_state BOOLEAN DEFAULT FALSE, log_new_data BOOLEAN DEFAULT FALSE, trigger_create_table BOOLEAN DEFAULT FALSE,
*     except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_audit_id(schemaname TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_log_trigger(schemaname TEXT DEFAULT 'public'::text, log_old_data BOOLEAN DEFAULT TRUE,
*     log_new_data BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_table_audit(tablename TEXT, schemaname TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_old_data BOOLEAN DEFAULT TRUE, log_new_data BOOLEAN DEFAULT FALSE ,log_state BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   create_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text) RETURNS SETOF VOID
*   create_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_old_data BOOLEAN DEFAULT TRUE, log_new_data BOOLEAN DEFAULT FALSE, log_state BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   drop_schema_audit(schema_name TEXT DEFAULT 'public'::text, log_state BOOLEAN DEFAULT TRUE, drop_log BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_audit_id(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_log_trigger(schema_name TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_state BOOLEAN DEFAULT TRUE, drop_log BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   drop_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text) RETURNS SETOF VOID
*   drop_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   get_operation_id(operation TEXT) RETURNS SMALLINT
*   get_table_oid(table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS OID
*   get_txid_bounds_to_table(table_log_id INTEGER, OUT txid_min INTEGER, OUT txid_max INTEGER) RETURNS RECORD
*   log_new_table_state(columns TEXT[], table_name TEXT, schema_name TEXT DEFAULT 'public'::text, table_event_key TEXT,
*     audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text) RETURNS SETOF VOID
*   log_old_table_state(columns TEXT[], table_name TEXT, schema_name TEXT DEFAULT 'public'::text, table_event_key TEXT,
      audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text) RETURNS SETOF VOID
*   log_schema_baseline(audit_schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
*   log_table_baseline(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_new_data BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   log_table_event(event_txid BIGINT, tablename TEXT, schemaname TEXT, op_type TEXT) RETURNS TEXT
*   log_transaction(current_txid BIGINT) RETURNS INTEGER
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
    atl.audit_id_column,
    atl.log_old_data,
    atl.log_new_data,
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
  JOIN
    pgmemento.audit_schema_log asl
    ON asl.schema_name = n.nspname
   AND upper(asl.txid_range) IS NULL
   AND lower(asl.txid_range) IS NOT NULL
  JOIN (
    SELECT DISTINCT ON (log_id)
      log_id,
      table_name,
      schema_name,
      audit_id_column,
      log_old_data,
      log_new_data
    FROM
      pgmemento.audit_table_log
    WHERE
      upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL
    ORDER BY
      log_id, id
    ) atl
    ON atl.table_name = c.relname
   AND atl.schema_name = n.nspname
  JOIN
    pg_attribute a
    ON a.attrelid = c.oid
   AND a.attname = atl.audit_id_column
  JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(atl.log_id)
    ) bounds ON (true)
  LEFT JOIN (
    SELECT
      tgrelid,
      tgenabled
    FROM
      pg_trigger
    WHERE
      tgname = 'pgmemento_transaction_trigger'::name
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
COMMENT ON COLUMN pgmemento.audit_tables.audit_id_column IS 'Name of the audit_id column added to the audited table';
COMMENT ON COLUMN pgmemento.audit_tables.log_old_data IS 'Flag that shows if old values are logged for audited table';
COMMENT ON COLUMN pgmemento.audit_tables.log_new_data IS 'Flag that shows if new values are logged for audited table';
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
  WHEN 'REINIT TABLE' THEN 11
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
  WHEN 'DROP AUDIT_ID' THEN 81
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
    txid_range = numrange(lower(txid_range), current_setting('pgmemento.t' || txid_current())::numeric, '(]')
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
      txid_range = numrange(lower(txid_range), current_setting('pgmemento.t' || txid_current())::numeric, '(]')
    WHERE
      audit_table_id = tab_id
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

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
  audit_id_column_name TEXT;
  log_data_settings TEXT;
BEGIN
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

  IF tab_id IS NOT NULL THEN
    RETURN tab_id;
  END IF;

  BEGIN
    -- check if table exists in 'audit_table_log' with another name (and open range)
    table_log_id := current_setting('pgmemento.' || quote_ident($2) || '.' || quote_ident($1))::int;

    IF NOT EXISTS (
      SELECT
        1
      FROM
        pgmemento.table_event_log
      WHERE
        transaction_id = current_setting('pgmemento.t' || txid_current())::int
        AND table_name = $1
        AND schema_name = $2
        AND ((op_id = 1 AND table_operation = 'RECREATE TABLE')
         OR op_id = 11)  -- REINIT TABLE event
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

  -- get audit_id_column name which was set in create_table_audit_id or in event trigger when renaming the table
  audit_id_column_name := current_setting('pgmemento.' || $2 || '.' || $1 || '.audit_id.t' || txid_current());

  -- get logging behavior which was set in create_table_audit_id or in event trigger when renaming the table
  log_data_settings := current_setting('pgmemento.' || $2 || '.' || $1 || '.log_data.t' || txid_current());

  -- now register table and corresponding columns in audit tables
  INSERT INTO pgmemento.audit_table_log
    (log_id, relid, schema_name, table_name, audit_id_column, log_old_data, log_new_data, txid_range)
  VALUES
    (table_log_id, pgmemento.get_table_oid($1, $2), $2, $1, audit_id_column_name,
     CASE WHEN split_part(log_data_settings, ',' ,1) = 'old=true' THEN TRUE ELSE FALSE END,
     CASE WHEN split_part(log_data_settings, ',' ,2) = 'new=true' THEN TRUE ELSE FALSE END,
     numrange(current_setting('pgmemento.t' || txid_current())::numeric, NULL, '(]'))
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
      numrange(current_setting('pgmemento.t' || txid_current())::numeric, NULL, '(]') AS txid_range
    FROM
      pg_attribute a
    LEFT JOIN
      pg_attrdef d
      ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
    WHERE
      a.attrelid = pgmemento.get_table_oid($1, $2)
      AND a.attname <> audit_id_column_name
      AND a.attnum > 0
      AND NOT a.attisdropped
      ORDER BY a.attnum
  );

  -- rename unique constraint for audit_id column
  IF old_table_name IS NOT NULL AND old_schema_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
      $2, $1, old_table_name || '_' || audit_id_column_name || '_key', $1 || '_' || audit_id_column_name || '_key');
  END IF;

  RETURN tab_id;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;


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
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE
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
      AND tgname = 'pgmemento_transaction_trigger'
  ) THEN
    RETURN;
  ELSE
    /*
      statement level triggers
    */
    -- first trigger to be fired on each transaction
    EXECUTE format(
      'CREATE TRIGGER pgmemento_transaction_trigger
         BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %I.%I
         FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_statement()',
         $2, $1);

    -- second trigger to be fired before truncate events if old data shall be logged
    IF $4 THEN
      EXECUTE format(
        'CREATE TRIGGER pgmemento_truncate_trigger
           BEFORE TRUNCATE ON %I.%I
           FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_truncate(%L)',
           $2, $1, $3);
    END IF;

    /*
      row level triggers
    */
    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER pgmemento_insert_trigger
         AFTER INSERT ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_insert(%L, %s, %s)',
         $2, $1, $3, CASE WHEN $4 THEN 'true' ELSE 'false' END, CASE WHEN $5 THEN 'true' ELSE 'false' END);

    -- trigger to be fired after update events
    EXECUTE format(
      'CREATE TRIGGER pgmemento_update_trigger
         AFTER UPDATE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_update(%L, %s, %s)',
         $2, $1, $3, CASE WHEN $4 THEN 'true' ELSE 'false' END, CASE WHEN $5 THEN 'true' ELSE 'false' END);

    -- trigger to be fired after insert events
    EXECUTE format(
      'CREATE TRIGGER pgmemento_delete_trigger
         AFTER DELETE ON %I.%I
         FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_delete(%L, %s)',
         $2, $1, $3, CASE WHEN $4 THEN 'true' ELSE 'false' END);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

-- perform create_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_log_trigger(
  schemaname TEXT DEFAULT 'public'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_log_trigger(c.relname, $1, s.default_audit_id_column, $2, $3)
FROM
  pg_class c
JOIN
  pg_namespace n
  ON c.relnamespace = n.oid
 AND n.nspname = $1
JOIN
  pgmemento.audit_schema_log s
  ON s.schema_name = n.nspname
 AND upper(s.txid_range) IS NULL
WHERE
  c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($4,'{}'::text[]));
$$
LANGUAGE sql
SECURITY DEFINER;

-- drop logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_log_trigger(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('DROP TRIGGER IF EXISTS pgmemento_delete_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS pgmemento_update_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS pgmemento_insert_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS pgmemento_truncate_trigger ON %I.%I', $2, $1);
  EXECUTE format('DROP TRIGGER IF EXISTS pgmemento_transaction_trigger ON %I.%I', $2, $1);
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

-- perform drop_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_log_trigger(
  schema_name TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pgmemento.audit_tables
     WHERE schemaname = $1
       AND tablename <> ALL (COALESCE($2,'{}'::text[]))
       AND tg_is_active
  ) THEN
    PERFORM
      pgmemento.drop_table_log_trigger(tablename, $1)
    FROM
      pgmemento.audit_tables
    WHERE
      schemaname = $1
      AND tablename <> ALL (COALESCE($2,'{}'::text[]))
      AND tg_is_active;

    PERFORM pgmemento.stop($1, $2);
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


/**********************************************************
* AUDIT ID COLUMN
*
* Add an extra audit column to a table to trace changes on
* rows over time.
***********************************************************/
-- add audit column to a table
CREATE OR REPLACE FUNCTION pgmemento.create_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- log as 'add column' event, as it is not done by event triggers
  PERFORM pgmemento.log_table_event($1, $2, 'ADD AUDIT_ID');

  -- add audit column to table
  -- throws exception if it already exist
  EXECUTE format(
    'ALTER TABLE %I.%I ADD COLUMN %I BIGINT DEFAULT nextval(''pgmemento.audit_id_seq''::regclass) UNIQUE NOT NULL',
    $2, $1, $3);
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

-- perform create_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit_id(
  schemaname TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.create_table_audit_id(c.relname, $1, s.default_audit_id_column)
FROM
  pg_class c
JOIN
  pg_namespace n
  ON c.relnamespace = n.oid
 AND n.nspname = $1
JOIN
  pgmemento.audit_schema_log s
  ON s.schema_name = n.nspname
 AND upper(s.txid_range) IS NULL
WHERE
  c.relkind = 'r'
  AND c.relname <> ALL (COALESCE($2,'{}'::text[]));
$$
LANGUAGE sql
SECURITY DEFINER;

-- drop audit column from a table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- drop audit column if it exists
  IF EXISTS (
    SELECT
      1
    FROM
      pg_attribute
    WHERE
      attrelid = pgmemento.get_table_oid($1, $2)
      AND attname = $3
      AND attislocal = 't'
      AND NOT attisdropped
  ) THEN
    EXECUTE format(
      'ALTER TABLE %I.%I DROP CONSTRAINT %I, DROP COLUMN %I',
      $2, $1, $1 || '_' || audit_id_column_name || '_key', $3);
  ELSE
    RETURN;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

-- perform drop_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit_id(
  schema_name TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.drop_table_audit_id(tablename, $1, audit_id_column)
FROM
  pgmemento.audit_tables
WHERE
  schemaname = $1
  AND tablename <> ALL (COALESCE($2,'{}'::text[]));
$$
LANGUAGE sql
SECURITY DEFINER;


/**********************************************************
* LOG TABLE STATE
*
* Function to log the whole content of a table or only
* for given columns.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.column_array_to_column_list(columns TEXT[]) RETURNS TEXT AS
$$
  SELECT
    'SELECT d FROM (SELECT ' || array_to_string(array_agg(format('%I', k)), ', ') || ') d'
  FROM
    unnest($1) k
$$
LANGUAGE sql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.log_old_table_state(
  columns TEXT[],
  tablename TEXT,
  schemaname TEXT,
  table_event_key TEXT,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $1 IS NOT NULL AND array_length($1, 1) IS NOT NULL THEN
    -- log content of given columns
    EXECUTE format(
      'INSERT INTO pgmemento.row_log AS r (audit_id, event_key, old_data)
         SELECT %I, $1, to_jsonb(('||pgmemento.column_array_to_column_list($1)||')) AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET
         old_data = COALESCE(excluded.old_data, ''{}''::jsonb) || COALESCE(r.old_data, ''{}''::jsonb)',
       $5, $3, $2, $5) USING $4;
  ELSE
    -- log content of entire table
    EXECUTE format(
      'INSERT INTO pgmemento.row_log (audit_id, event_key, old_data)
         SELECT %I, $1, to_jsonb(%I) AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO NOTHING',
       $5, $2, $3, $2, $5) USING $4;
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pgmemento.log_new_table_state(
  columns TEXT[],
  tablename TEXT,
  schemaname TEXT,
  table_event_key TEXT,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $1 IS NOT NULL AND array_length($1, 1) IS NOT NULL THEN
    -- log content of given columns
    EXECUTE format(
      'INSERT INTO pgmemento.row_log AS r (audit_id, event_key, new_data)
         SELECT %I, $1, to_jsonb(('||pgmemento.column_array_to_column_list($1)||')) AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET new_data = COALESCE(r.new_data, ''{}''::jsonb) || COALESCE(excluded.new_data, ''{}''::jsonb)',
       $5, $3, $2, $5) USING $4;
  ELSE
    -- log content of entire table
    EXECUTE format(
      'INSERT INTO pgmemento.row_log r (audit_id, event_key, new_data)
         SELECT %I, $1, to_jsonb(%I) AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET COALESCE(r.new_data, ''{}''::jsonb) || COALESCE(excluded.new_data, ''{}''::jsonb)',
       $5, $2, $3, $2, $5) USING $4;
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


/**********************************************************
* LOG TRANSACTION
*
* Function that write information of ddl and dml events into
* transaction_log and returns the transaction ID
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_transaction(current_txid BIGINT) RETURNS INTEGER AS
$$
DECLARE
  session_info_text TEXT;
  session_info_obj JSONB;
  transaction_log_id INTEGER;
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
      WHEN undefined_object THEN
        session_info_obj := NULL;
      WHEN invalid_text_representation THEN
        BEGIN
          session_info_obj := to_jsonb(current_setting('pgmemento.session_info'));
        END;
      WHEN others THEN
        RAISE NOTICE 'Unable to parse session info: %', session_info_text;
        session_info_obj := NULL;
  END;

  -- try to log corresponding transaction
  INSERT INTO pgmemento.transaction_log
    (txid, txid_time, process_id, user_name, client_name, client_port, application_name, session_info)
  VALUES
    ($1, transaction_timestamp(), pg_backend_pid(), session_user, inet_client_addr(), inet_client_port(),
     current_setting('application_name'), session_info_obj
    )
  ON CONFLICT (txid_time, txid)
    DO NOTHING
  RETURNING id
  INTO transaction_log_id;

  IF transaction_log_id IS NOT NULL THEN
    PERFORM set_config('pgmemento.t' || $1, transaction_log_id::text, TRUE);
  ELSE
    transaction_log_id := current_setting('pgmemento.t' || $1)::int;
  END IF;

  RETURN transaction_log_id;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

/**********************************************************
* LOG TABLE EVENT
*
* Function that write information of ddl and dml events into
* transaction_log and table_event_log and returns the event ID
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_table_event(
  tablename TEXT,
  schemaname TEXT,
  op_type TEXT
  ) RETURNS TEXT AS
$$
DECLARE
  txid_log_id INTEGER;
  stmt_ts TIMESTAMP WITH TIME ZONE := statement_timestamp();
  operation_id SMALLINT := pgmemento.get_operation_id($3);
  table_event_key TEXT;
BEGIN
  -- try to log corresponding transaction
  txid_log_id := pgmemento.log_transaction(txid_current());

  -- try to log corresponding table event
  -- on conflict do nothing
  INSERT INTO pgmemento.table_event_log
    (transaction_id, stmt_time, op_id, table_operation, table_name, schema_name, event_key)
  VALUES
    (txid_log_id, stmt_ts, operation_id, $3, $1, $2,
     concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from stmt_ts), txid_current(), operation_id, $1, $2))
  ON CONFLICT (event_key)
    DO NOTHING
  RETURNING event_key
  INTO table_event_key;

  RETURN table_event_key;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


/**********************************************************
* TRIGGER PROCEDURE log_statement
*
* Procedure that is called when a pgmemento_transaction_trigger
* is fired. Metadata of each transaction is written to the
* transaction_log table.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_statement() RETURNS trigger AS
$$
BEGIN
  PERFORM pgmemento.log_table_event(TG_TABLE_NAME, TG_TABLE_SCHEMA, TG_OP);
  RETURN NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
    pgmemento.log_old_table_state('{}'::text[], TG_TABLE_NAME, TG_TABLE_SCHEMA, event_key, TG_ARGV[0])
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.t' || txid_current())::int
    AND table_name = TG_TABLE_NAME
    AND schema_name = TG_TABLE_SCHEMA
    AND op_id = 8;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
  new_audit_id BIGINT;
BEGIN
  EXECUTE 'SELECT $1.' || TG_ARGV[0] USING NEW INTO new_audit_id;

  -- log inserted row ('old_data' column can be left blank)
  INSERT INTO pgmemento.row_log
    (audit_id, event_key, new_data)
  VALUES
    (new_audit_id,
     concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA),
     CASE WHEN TG_ARGV[2] = 'true' THEN to_json(NEW) ELSE NULL END);

  RETURN NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
  new_audit_id BIGINT;
  jsonb_diff_old JSONB;
  jsonb_diff_new JSONB;
BEGIN
  EXECUTE 'SELECT $1.' || TG_ARGV[0] USING NEW INTO new_audit_id;

  -- log values of updated columns for the processed row
  -- therefore, a diff between OLD and NEW is necessary
  IF TG_ARGV[1] = 'true' THEN
    SELECT COALESCE(
      (SELECT
         ('{' || string_agg(to_json(key) || ':' || value, ',') || '}')
       FROM
         jsonb_each(to_jsonb(OLD))
       WHERE
         to_jsonb(NEW) ->> key IS DISTINCT FROM to_jsonb(OLD) ->> key
      ),
      '{}')::jsonb INTO jsonb_diff_old;
  END IF;

  IF TG_ARGV[2] = 'true' THEN
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
    -- log delta, on conflict concat logs, for old_data oldest should overwrite, for new_data vice versa
    INSERT INTO pgmemento.row_log AS r
      (audit_id, event_key, old_data, new_data)
    VALUES
      (new_audit_id,
       concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA),
       jsonb_diff_old, jsonb_diff_new)
    ON CONFLICT (audit_id, event_key)
    DO UPDATE SET
      old_data = COALESCE(excluded.old_data, '{}'::jsonb) || COALESCE(r.old_data, '{}'::jsonb),
      new_data = COALESCE(r.new_data, '{}'::jsonb) || COALESCE(excluded.new_data, '{}'::jsonb);
  END IF;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
  old_audit_id BIGINT;
BEGIN
  EXECUTE 'SELECT $1.' || TG_ARGV[0] USING OLD INTO old_audit_id;

  -- log content of the entire row in the row_log table
  INSERT INTO pgmemento.row_log
    (audit_id, event_key, old_data)
  VALUES
    (old_audit_id,
     concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA),
     CASE WHEN TG_ARGV[1] = 'true' THEN to_json(OLD) ELSE NULL END);

  RETURN NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


/**********************************************************
* LOG TABLE BASELINE
*
* Log table content in the row_log table (as inserted values)
* to have a baseline for table versioning.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_table_baseline(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_new_data BOOLEAN DEFAULT FALSE
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
    table_event_key := pgmemento.log_table_event($1, $2, 'INSERT');

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
        pkey_columns := ' ORDER BY t.' || $3;
      END IF;

      EXECUTE format(
        'INSERT INTO pgmemento.row_log (audit_id, event_key'
         || CASE WHEN $4 THEN ', new_data' ELSE '' END
         || ') '
         || 'SELECT t.' || $3 || ', $1'
         || CASE WHEN $4 THEN ', to_json(t.*) ' ELSE ' ' END
         || 'FROM %I.%I t '
         || 'LEFT JOIN pgmemento.row_log r ON r.audit_id = t.' || $3
         || ' WHERE r.audit_id IS NULL' || pkey_columns
         || ' ON CONFLICT (audit_id, event_key) DO NOTHING',
         $2, $1) USING table_event_key;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

-- perform log_table_baseline on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.log_schema_baseline(
  audit_schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.log_table_baseline(a.table_name, a.schema_name, a.audit_id_column, a.log_new_data)
FROM
  pgmemento.audit_schema_log s,
  pgmemento.audit_table_log a,
  pgmemento.audit_tables_dependency d
WHERE
  s.schema_name = $1
  AND s.schema_name = a.schema_name
  AND a.schema_name = d.schemaname
  AND a.table_name = d.tablename
  AND upper(a.txid_range) IS NULL
  AND lower(a.txid_range) IS NOT NULL
ORDER BY
  d.depth;
$$
LANGUAGE sql STRICT
SECURITY DEFINER;


/**********************************************************
* ENABLE/DISABLE PGMEMENTO
*
* Enables/disables pgMemento for a specified tabl
e/schema.
***********************************************************/
-- create pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.create_table_audit(
  tablename TEXT,
  schemaname TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE,
  log_state BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  except_tables TEXT[] DEFAULT '{}';
BEGIN
  -- check if pgMemento is already initialized for schema
  IF NOT EXISTS (
    SELECT 1
      FROM pgmemento.audit_schema_log
     WHERE schema_name = $2
       AND upper(txid_range) IS NULL
  ) THEN
    SELECT
      array_agg(c.relname)
    INTO
      except_tables
    FROM
      pg_class c
    JOIN
      pg_namespace n
      ON c.relnamespace = n.oid
    WHERE
      n.nspname = $2
      AND c.relname <> $1
      AND c.relkind = 'r';

    PERFORM pgmemento.create_schema_audit($2, $3, $4, $5, $6, FALSE, except_tables);
    RETURN;
  END IF;

  -- remember audit_id_column when registering table in audit_table_log later
  PERFORM set_config('pgmemento.' || $2 || '.' || $1 || '.audit_id.t' || txid_current(), $3, TRUE);

  -- remember logging behavior when registering table in audit_table_log later
  PERFORM set_config('pgmemento.' || $2 || '.' || $1 || '.log_data.t' || txid_current(),
    CASE WHEN log_old_data THEN 'old=true,' ELSE 'old=false,' END ||
    CASE WHEN log_new_data THEN 'new=true' ELSE 'new=false' END, TRUE);

  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger($1, $2, $3, $4, $5);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id($1, $2, $3);

  -- log existing table content as inserted
  IF $6 THEN
    PERFORM pgmemento.log_table_baseline($1, $2, $3, $5);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

-- perform create_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit(
  schemaname TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE,
  log_state BOOLEAN DEFAULT FALSE,
  trigger_create_table BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  current_txid_range numrange;
BEGIN
  -- check if schema is already audited
  SELECT txid_range INTO current_txid_range
    FROM pgmemento.audit_schema_log
   WHERE schema_name = $1;

  -- if not initialize pgMemento, this will also call create_schema_audit
  IF current_txid_range IS NULL THEN
    PERFORM pgmemento.init($1, $2, $3, $4, $5, $6, $7);
    RETURN;
  ELSE
    IF upper(current_txid_range) IS NOT NULL THEN
      RAISE NOTICE 'Schema has been audited before. pgMemento will only be started.';
      PERFORM pgmemento.start($1, $2, $3, $4, $6, $7);
    END IF;
  END IF;

  PERFORM
    pgmemento.create_table_audit(c.relname, $1, $2, $3, $4, $5)
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON c.relnamespace = n.oid
  LEFT JOIN pgmemento.audit_tables at
    ON at.tablename = c.relname
   AND at.schemaname = n.nspname
   AND NOT at.tg_is_active
  WHERE
    n.nspname = $1
    AND c.relkind = 'r'
    AND c.relname <> ALL (COALESCE($7,'{}'::text[]))
    AND at.tg_is_active IS NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

-- drop pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_state BOOLEAN DEFAULT TRUE,
  drop_log BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  table_event_key TEXT;
BEGIN
  -- first drop log trigger
  PERFORM pgmemento.drop_table_log_trigger($1, $2);

  -- log the whole content of the table to keep the reference between audit_id and table rows
  IF $4 THEN
    -- log event as event triggers will walk around anything related to the audit_id
    table_event_key := pgmemento.log_table_event($1, $2, 'TRUNCATE');

    -- log the whole content of the table to keep the reference between audit_id and table rows
    PERFORM pgmemento.log_old_table_state('{}'::text[], $1, $2, table_event_key, $3);
  END IF;

  -- log event as event triggers will walk around anything related to the audit_id
  table_event_key := pgmemento.log_table_event($1, $2, 'DROP AUDIT_ID');

  -- update audit_table_log and audit_column_log
  PERFORM pgmemento.unregister_audit_table($1, $2);

  -- remove all logs related to given table
  IF $5 THEN
    PERFORM pgmemento.delete_audit_table_log($1, $2);
  END IF;

  -- drop audit_id column
  PERFORM pgmemento.drop_table_audit_id($1, $2, $3);
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

-- perform drop_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit(
  schema_name TEXT DEFAULT 'public'::text,
  log_state BOOLEAN DEFAULT TRUE,
  drop_log BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pgmemento.audit_tables
     WHERE schemaname = $1
       AND tablename <> ALL (COALESCE($4,'{}'::text[]))
  ) THEN
    PERFORM
      pgmemento.drop_table_audit(tablename, $1, audit_id_column, $2, $3)
    FROM
      pgmemento.audit_tables
    WHERE
      schemaname = $1
      AND tablename <> ALL (COALESCE($4,'{}'::text[]));

    PERFORM pgmemento.drop($1, $2, $3, $4);
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;
