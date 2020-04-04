-- START_AUDITING.sql
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
-- 0.2.0     2020-02-29   add new option to log new data in row_log      FKun
-- 0.1.0     2016-03-09   initial commit                                 FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\prompt 'Please enter the name of the schema to be used along with pgMemento: ' schema_name
\prompt 'Store new data in audit logs, too? (y|N): ' log_new_data
\prompt 'Trigger CREATE TABLE statements? (y|N): ' trigger_create_table
\prompt 'Specify tables to be excluded from logging processes (separated by comma): ' except_tables

\i ctl/START.sql
