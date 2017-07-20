-- RESTORE_TESTDB.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that restores a 3DCityDB instance and checks for correct number of tables
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2017-07-20   initial commit                                   FKun
--

\echo
\echo 'TEST 1: TestDB setup'

\echo
\echo 'Restore dumped 3DCityDB instance'
\i test/testdb/testdb_dump.sql

DO
$$
DECLARE
  n_tables INTEGER;
BEGIN
  SELECT
    count(*) INTO n_tables
  FROM
    pg_tables
  WHERE
    schemaname = 'citydb'
    OR schemaname = 'citydb_pkg'
    OR schemaname = 'public';

  ASSERT n_tables = 62, 'Error: Restored database is incomplete!';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST 1: TestDB setup correct'