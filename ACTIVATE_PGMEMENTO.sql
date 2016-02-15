-- ACTIVATE_PGMEMENTO.sql
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
-- Version | Date       | Description                                    | Author
-- 0.3.0     2015-06-20   initial commit                                   FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\prompt 'Please enter the name of the schema to be used along with pgMemento: ' schema_name
\prompt 'Specify tables to be excluded from logging processes (seperated by comma): ' except_tables

\echo
\echo 'Creating triggers and audit_id columns for tables in ':schema_name' schema ...'
SELECT pgmemento.create_schema_audit(:'schema_name', string_to_array(:'except_tables',','));

\echo
\echo 'Log already existent content as part of an ''INSERT'' event ...'
SELECT pgmemento.log_schema_state(:'schema_name', string_to_array(:'except_tables',','));

\echo
\echo 'pgMemento is now running on ':schema_name' schema.'