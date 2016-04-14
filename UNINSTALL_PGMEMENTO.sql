-- UNINSTALL_PGMEMENTO.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script to remove pgMemento from a database
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.0     2015-06-20   initial commit                                   FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'Removing triggers and audit_id columns from audited tables ...'
SELECT pgmemento.drop_schema_event_trigger();
SELECT pgmemento.drop_table_audit(tablename, schemaname) FROM pgmemento.audit_tables;

\echo
\echo 'Removing pgmemento schema ...'
DROP SCHEMA pgmemento CASCADE;

\echo
\echo 'Updating search path ...'
SELECT replace(current_setting('search_path'), ', pgmemento', '') AS db_path \gset
ALTER DATABASE :"DBNAME" SET search_path TO :db_path;

\echo
\echo 'pgMemento completely removed!'