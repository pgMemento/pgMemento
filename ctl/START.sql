-- START.sql
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
-- 0.1.0     2016-03-09   initial commit                                 FKun
--

\echo
\echo 'Creating triggers for tables in ':schema_name' schema ...'
SELECT pgmemento.create_schema_log_trigger(:'schema_name', string_to_array(:'except_tables',','));

\echo
\echo 'pgMemento is now started on ':schema_name' schema.'