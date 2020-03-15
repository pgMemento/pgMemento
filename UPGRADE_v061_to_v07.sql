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
-- 0.1.0     2019-06-09   initial commit                                 FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'Updgrade pgMemento from v0.6.1 to v0.7.0 ...'

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

DROP FUNCTION IF EXISTS pgmemento.get_txid_bounds_to_table(table_oid OID, OUT txid_min INTEGER, OUT txid_max INTEGER);

DROP FUNCTION IF EXISTS pgmemento.log_table_event(event_txid BIGINT, table_oid OID, op_type TEXT);

DROP FUNCTION IF EXISTS pgememento.log_table_state(e_id INTEGER, columns TEXT[], table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.restore_record_definition(start_from_tid INTEGER, end_at_tid INTEGER, table_oid OID);

DROP FUNCTION IF EXISTS pgmemento.log_schema_baseline(schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.log_table_baseline(table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.create_schema_log_trigger(schema_name TEXT, except_tables);

DROP FUNCTION IF EXISTS pgmemento.create_table_log_trigger(table_name TEXT, schema_name TEXT);

DROP FUNCTION IF EXISTS pgmemento.create_schema_audit(schema_name TEXT, log_state BOOLEAN, except_tables TEXT[]);

DROP FUNCTION IF EXISTS pgmemento.create_table_audit(table_name TEXT, schema_name TEXT, log_state BOOLEAN);

DROP FUNCTION IF EXISTS pgmemento.create_schema_event_trigger(trigger_create_table BOOLEAN);

DROP AGGREGATE IF EXISTS pgmemento.jsonb_merge(jsonb);

\echo
\echo 'Alter tables and recreate functions'
\i ctl/UPGRADE.sql
\i src/SETUP.sql
\i src/LOG_UTIL.sql
\i src/DDL_LOG.sql
\i src/RESTORE.sql
\i src/REVERT.sql
\i src/SCHEMA_MANAGEMENT.sql
\i src/CTL.sql

\echo
\echo 'pgMemento upgrade completed!'
