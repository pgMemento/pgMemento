-- UPGRADE_v061_to_v07.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script upgrades a pgMemento extension of v0.6.1 to v0.7. All functions
-- will be replaced and tables will be altered (see changelog for more details)
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.3.0     2020-04-25   copy audit_tables for migration                FKun
-- 0.2.0     2020-04-04   finalizing script for v0.7                     FKun
-- 0.1.0     2019-06-09   initial commit                                 FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'Updgrade pgMemento from v0.6.1 to v0.7.3 ...'

SELECT EXISTS (
  SELECT
    1
  FROM
    pg_event_trigger
  WHERE
    evtname = 'table_create_post_trigger'
) AS trigger_create_table_enabled \gset

\echo
\echo 'Drop event triggers'
SELECT pgmemento.drop_schema_event_trigger();

\echo 'Create table from audit_tables view'
CREATE TABLE IF NOT EXISTS pgmemento.audit_tables_copy AS
  SELECT schemaname, tablename, tg_is_active
    FROM pgmemento.audit_tables;

\echo
\echo 'Close all open ranges for removed/renamed tables'
WITH logged_tables AS (
    SELECT c.oid AS table_oid,
           n.nspname AS schemaname,
           c.relname AS tablename,
           bounds.txid_min,
           atl.id AS log_table_id,
           atl.schema_name AS log_schemaname,
           atl.table_name AS log_tablename,
           atl.txid_max AS log_txid_max,
           COALESCE(c.relkind, 'r') AS relkind,
           CASE WHEN c.relname IS NOT NULL THEN TRUE ELSE FALSE END AS table_exists,
           CASE WHEN atl.table_name IS NOT NULL THEN TRUE ELSE FALSE END AS log_table_exists,
           CASE WHEN c.relname IS DISTINCT FROM atl.table_name THEN TRUE ELSE FALSE END AS table_changed
      FROM pg_class c
      JOIN pg_namespace n
        ON c.relnamespace = n.oid
       AND n.nspname <> 'pgmemento'
       AND n.nspname NOT LIKE 'pg_temp%'
      JOIN pg_attribute a
        ON a.attrelid = c.oid
       AND a.attname = 'audit_id'
      JOIN LATERAL (
           SELECT * FROM pgmemento.get_txid_bounds_to_table(c.oid)
           ) bounds ON (true)
 FULL JOIN (
           SELECT a.id, a.relid, a.table_name, a.schema_name, bounds_old.txid_max
             FROM pgmemento.audit_table_log a
             JOIN LATERAL (
                  SELECT * FROM pgmemento.get_txid_bounds_to_table(a.relid)
                  ) bounds_old ON (true)
            WHERE upper(a.txid_range) IS NULL
           ) atl
        ON atl.relid = c.oid
       AND atl.schema_name = n.nspname
), insert_missing AS (
  INSERT INTO pgmemento.audit_table_log (relid, schema_name, table_name, txid_range)
    SELECT table_oid, schemaname, tablename, numrange(txid_min, NULL, '(]')
      FROM logged_tables
     WHERE relkind = 'r' AND (NOT log_table_exists
        OR (table_changed AND table_exists))
)
UPDATE pgmemento.audit_table_log atl
   SET txid_range = numrange(lower(atl.txid_range), t.log_txid_max, '(]')
  FROM (
    SELECT log_table_id, log_txid_max
      FROM logged_tables
     WHERE relkind = 'r' AND log_table_exists AND table_changed
    ) t
 WHERE atl.id = t.log_table_id;

\echo
\echo 'Upgrade log tables'
\i ctl/UPGRADE.sql

\echo
\echo 'Remove views'
DROP VIEW IF EXISTS pgmemento.audit_tables CASCADE;
DROP VIEW IF EXISTS pgmemento.audit_tables_dependency CASCADE;

\echo
\echo 'Drop functions'
DROP FUNCTION IF EXISTS pgmemento.audit_table_check(
  IN tid INTEGER, IN tab_name TEXT, IN tab_schema TEXT,
  OUT log_tab_oid OID, OUT log_tab_name TEXT, OUT log_tab_schema TEXT, OUT log_tab_id INTEGER,
  OUT recent_tab_name TEXT, OUT recent_tab_schema TEXT, OUT recent_tab_id INTEGER);

DROP FUNCTION IF EXISTS pgmemento.delete_audit_table_log(table_oid OID);

DROP FUNCTION IF EXISTS pgmemento.delete_table_event_log(tid INTEGER, table_oid OID);

DROP FUNCTION IF EXISTS pgmemento.delete_table_event_log(table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.get_column_list_by_txid_range(
  start_from_tid INTEGER, end_at_tid INTEGER, table_oid OID,
  OUT column_name TEXT, OUT column_count INTEGER, OUT data_type TEXT, OUT ordinal_position INTEGER, OUT txid_range numrange);

DROP FUNCTION IF EXISTS pgmemento.pkey_table_state(table_name TEXT, target_schema_name TEXT, original_schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.restore_record_definition(start_from_tid INTEGER, end_at_tid INTEGER, table_oid OID);

DROP FUNCTION IF EXISTS pgmemento.restore_record_definition(tid INTEGER, table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.recover_audit_version(tid INTEGER, aid BIGINT, changes JSONB, table_op INTEGER, table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.get_txid_bounds_to_table(table_oid OID, OUT txid_min INTEGER, OUT txid_max INTEGER);

DROP FUNCTION IF EXISTS pgmemento.log_table_event(event_txid BIGINT, table_oid OID, op_type TEXT);

DROP FUNCTION IF EXISTS pgmemento.log_table_state(e_id INTEGER, columns TEXT[], table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.log_schema_baseline(schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.log_table_baseline(table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.create_schema_audit(schema_name TEXT, log_state BOOLEAN, except_tables TEXT[]);

DROP FUNCTION IF EXISTS pgmemento.create_table_audit(table_name TEXT, schema_name TEXT, log_state BOOLEAN);

DROP FUNCTION IF EXISTS pgmemento.create_table_audit_id(table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.create_schema_audit_id(schema_name TEXT, except_tables TEXT[]);

DROP FUNCTION IF EXISTS pgmemento.create_schema_log_trigger(schema_name TEXT, except_tables TEXT[]);

DROP FUNCTION IF EXISTS pgmemento.create_table_log_trigger(table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.drop_schema_audit(schema_name TEXT, keep_log BOOLEAN, except_tables TEXT[]);

DROP FUNCTION IF EXISTS pgmemento.drop_table_audit(table_name TEXT, schema_name TEXT, keep_log BOOLEAN);

DROP FUNCTION IF EXISTS pgmemento.drop_table_audit_id(table_name TEXT, schema_name TEXT);

DROP AGGREGATE IF EXISTS pgmemento.jsonb_merge(jsonb);

\echo
\echo 'Recreate functions'
\i src/SETUP.sql
\i src/LOG_UTIL.sql
\i src/DDL_LOG.sql
\i src/RESTORE.sql
\i src/REVERT.sql
\i src/SCHEMA_MANAGEMENT.sql
\i src/CTL.sql

\echo
\echo 'Register all schemas with audited tables in audit_schema_log'
INSERT INTO pgmemento.audit_schema_log
  (log_id, schema_name, default_audit_id_column, trigger_create_table, txid_range)
SELECT
  nextval('pgmemento.schema_log_id_seq'),
  schema_name,
  'audit_id',
  :'trigger_create_table_enabled',
  schema_txid_range
FROM (
  SELECT
    schema_name,
    numrange(min(lower(txid_range)), NULL, '(]') AS schema_txid_range
  FROM
    pgmemento.audit_table_log
  GROUP BY
    schema_name
) s
ORDER BY
  lower(schema_txid_range);

\echo
\echo 'Recreate log triggers'
DO
$$
DECLARE
  rec RECORD;
  remove_all_tx_tg BOOLEAN := TRUE; 
BEGIN
  FOR rec IN
    SELECT schemaname, tablename
      FROM pgmemento.audit_tables_copy
     WHERE tg_is_active
     ORDER BY schemaname, tablename
  LOOP
    PERFORM pgmemento.create_table_log_trigger(rec.tablename, rec.schemaname, 'audit_id');
  END LOOP;
END;
$$
LANGUAGE plpgsql;

DROP TABLE pgmemento.audit_tables_copy;

\echo
\echo 'Recreate event triggers'
SELECT pgmemento.create_schema_event_trigger(:'trigger_create_table_enabled');

\echo
\echo 'pgMemento upgrade completed!'
