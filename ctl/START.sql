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
-- 0.3.0     2020-03-30   simply call new start function                 FKun
-- 0.2.0     2020-02-29   add new option to log new data in row_log      FKun
-- 0.1.0     2016-03-09   initial commit                                 FKun
--

\echo
\echo 'Starting pgMemento for tables in ':schema_name' schema ...'

SELECT pgmemento.start(
  :'schema_name',
  'pgmemento_audit_id',
  TRUE,
  CASE WHEN lower(:'log_new_data') = 'y' OR lower(:'log_new_data') = 'yes' THEN TRUE ELSE FALSE END,
  CASE WHEN lower(:'trigger_create_table') = 'y' OR lower(:'trigger_create_table') = 'yes' THEN TRUE ELSE FALSE END,
  string_to_array(:'except_tables',',')
);
