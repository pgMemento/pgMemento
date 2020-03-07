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
-- 0.2.0     2020-02-29   add new option to log new data in row_log      FKun
-- 0.1.0     2016-03-09   initial commit                                 FKun
--

\echo
\echo 'Creating triggers for tables in ':schema_name' schema ...'
SELECT pgmemento.create_schema_log_trigger(
  :'schema_name',
  CASE WHEN lower(:'log_new_data') = 'y' OR lower(:'log_new_data') = 'yes' THEN TRUE ELSE FALSE END,
  string_to_array(:'except_tables',',')
);

\echo
\echo 'pgMemento is now started on ':schema_name' schema.'