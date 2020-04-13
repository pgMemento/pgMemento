-- INIT.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script to start auditing for a given database schema
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.3.0     2020-03-30   simply call new init function                  FKun
-- 0.2.0     2020-02-29   add new option to log new data in row_log      FKun
-- 0.1.0     2016-03-09   initial commit                                 FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\prompt 'Please enter the name of the schema to be used along with pgMemento: ' schema_name
\prompt 'Store new data in audit logs, too? (y|N): ' log_new_data
\prompt 'Log existing data as inserted (baseline)? (y|N): ' log_baseline
\prompt 'Trigger CREATE TABLE statements? (y|N): ' trigger_create_table
\prompt 'Specify tables to be excluded from logging processes (separated by comma): ' except_tables

\echo
\echo 'Initializing pgMemento in ':schema_name' schema ...'

SELECT pgmemento.init(
  :'schema_name',
  'pgmemento_audit_id',
  TRUE,
  CASE WHEN lower(:'log_new_data') = 'y' OR lower(:'log_new_data') = 'yes' THEN TRUE ELSE FALSE END,
  CASE WHEN lower(:'log_baseline') = 'y' OR lower(:'log_baseline') = 'yes' THEN TRUE ELSE FALSE END,
  CASE WHEN lower(:'trigger_create_table') = 'y' OR lower(:'trigger_create_table') = 'yes' THEN TRUE ELSE FALSE END,
  string_to_array(:'except_tables',',')
);
