-- TEST_RESTORE_TABLE_STATE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks if a previous tuple state can be restored from the logs
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2018-11-17   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento restore previous table states'

\echo
\echo 'TEST ':n'.1: Restore schema as VIEWs'
DO
$$
DECLARE
  tables TEXT[];
BEGIN
  -- restore states before txid 10 for tables 'object' and 'test'
  PERFORM pgmemento.restore_schema_state(1, 10, 'public', 'public_1_10', 'VIEW', FALSE);

  -- test if schema 'public_1_10' exists
  ASSERT (
    SELECT EXISTS(
      SELECT
        1
      FROM
        pg_namespace
      WHERE
        nspname = 'public_1_10'
    )
  ), 'Error: Did not find restore target ''public_1_10''!';

  -- check if tables got restored as views
  SELECT
    array_agg(relname::text ORDER BY relname)
  INTO
    tables
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON n.oid = c.relnamespace
  WHERE
    n.nspname = 'public_1_10'
    AND relkind = 'v';

  ASSERT tables[1] = 'object', 'Incorrect historic view for ''object'' table. Found %', tables[1];
  ASSERT tables[2] = 'tests', 'Incorrect historic view for ''tests'' table. Found %', tables[2];
END
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Restore schema as TABLEs'
DO
$$
DECLARE
  tables TEXT[];
BEGIN
  -- restore states before txid 10 for tables 'object' and 'test'
  PERFORM pgmemento.restore_schema_state(1, 10, 'public', 'public_1_10', 'TABLE', TRUE);

  -- check if tables got restored as tables
  SELECT
    array_agg(relname::text ORDER BY relname)
  INTO
    tables
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON n.oid = c.relnamespace
  WHERE
    n.nspname = 'public_1_10'
    AND relkind = 'r';

  ASSERT tables[1] = 'object', 'Incorrect historic table for ''object'' table. Found %', tables[1];
  ASSERT tables[2] = 'tests', 'Incorrect historic table for ''tests'' table. Found %', tables[2];

  -- drop the restored target schema
  DROP SCHEMA public_1_10 CASCADE;
END
$$
LANGUAGE plpgsql;
