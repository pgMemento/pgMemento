-- UPGRADE_v060_to_v061.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script upgrades a pgMemento extension of v0.5 to v0.6. All functions
-- will be replaced and tables will be altered (see changelog for more details)
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.1.0     2019-03-23   initial commit                                 FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'Updgrade pgMemento from v0.6.0 to v0.6.1 ...'

\echo
\echo 'Remove views'
DROP VIEW IF EXISTS pgmemento.audit_tables CASCADE;
DROP VIEW IF EXISTS pgmemento.audit_tables_dependency CASCADE;

\echo
\echo 'Drop functions'
DROP AGGREGATE IF EXISTS pgmemento.jsonb_merge(jsonb);
DROP FUNCTION IF EXISTS pgmemento.restore_record(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_name TEXT,
  schema_name TEXT,
  aid BIGINT,
  jsonb_output BOOLEAN);

\echo
\echo 'Alter tables and recreate functions'
\i src/SETUP.sql
\i src/LOG_UTIL.sql
\i src/DDL_LOG.sql
\i src/RESTORE.sql
\i src/REVERT.sql
\i src/SCHEMA_MANAGEMENT.sql

\echo
\echo 'Update ADD AUDIT_ID events'
UPDATE
  pgmemento.table_event_log
SET
  op_id = 21
WHERE
  table_operation = 'ADD AUDIT_ID';

\echo
\echo 'pgMemento upgrade completed!'
