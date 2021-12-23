-- TEST_CREATE_TABLE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an CREATE TABLE event happens
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.1     2021-12-23   session variables starting with letter           ol-teuto
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.1     2020-03-05   insert dummy tuple for subsequent tests          FKun
-- 0.2.0     2020-01-09   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-07-17   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit CREATE TABLE events'

\echo
\echo 'TEST ':n'.1: Log CREATE TABLE command'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_transaction INTEGER;
  test_event TEXT;
BEGIN
  -- create a new test table
  CREATE TABLE public.test (
    id SERIAL,
    test_column TEXT,
    test_geom_column public.geometry(PointZ,4326)
  );

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.t' || test_txid)::int;
  PERFORM set_config('pgmemento.create_table_test', test_transaction::text, FALSE);

  -- query for logged transaction
  ASSERT (
    SELECT EXISTS (
      SELECT
        txid
      FROM
        pgmemento.transaction_log
      WHERE
        txid = test_txid
    )
  ), 'Error: Did not find test entry in transaction_log table!';

  -- query for logged table event
  SELECT
    event_key
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction
    AND op_id = pgmemento.get_operation_id('CREATE TABLE');

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.2: Check entries audit_table_log'
DO
$$
DECLARE
  test_transaction INTEGER;
  tabid INTEGER;
  tabname TEXT;
  tid_range numrange;
BEGIN
  test_transaction := current_setting('pgmemento.create_table_test')::int;

  -- get parameters of created table
  SELECT
    id,
    table_name,
    txid_range
  INTO
    tabid,
    tabname,
    tid_range
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'test'
    AND schema_name = 'public'
    AND upper(txid_range) IS NULL;

  -- save table log id for next test
  PERFORM set_config('pgmemento.create_table_test2', tabid::text, FALSE);

  ASSERT tabname = 'test', 'Did not find table ''%'' in audit_table_log', tabname;
  ASSERT lower(tid_range) = test_transaction, 'Error: Starting transaction id % does not match the id % of CREATE TABLE event', lower(tid_range), test_transaction;
  ASSERT upper(tid_range) IS NULL, 'Error: Table should still exist and upper boundary of transaction range should be NULL, % instead', upper(tid_range);
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Check entries audit_column_log'
DO
$$
DECLARE
  test_transaction INTEGER;
  colnames TEXT[];
  datatypes TEXT[];
  defaults TEXT[];
  tid_ranges numrange[];
BEGIN
  test_transaction := current_setting('pgmemento.create_table_test')::int;

  -- get parameters of columns of created table
  SELECT
    array_agg(column_name),
    array_agg(data_type),
    array_agg(column_default),
    array_agg(txid_range)
  INTO
    colnames,
    datatypes,
    defaults,
    tid_ranges
  FROM
    pgmemento.audit_column_log
  WHERE
    audit_table_id = current_setting('pgmemento.create_table_test2')::int;

  ASSERT colnames[1] = 'id', 'Did not find column ''id'' in audit_column_log, but % instead', colnames[1];
  ASSERT datatypes[1] = 'integer', 'Incorrect datatype for integer-based ''id'' column in audit_column_log: %', datatypes[1];
  --ASSERT defaults[1] = E'nextval(\'public.test_id_seq\'::regclass)', 'Incorrect default value for serial column ''id'' in audit_column_log: %', defaults[1];
  ASSERT lower(tid_ranges[1]) = test_transaction, 'Error: Starting transaction id % for ''id'' column does not match the id % of CREATE TABLE event', lower(tid_ranges[1]), test_transaction;
  ASSERT upper(tid_ranges[1]) IS NULL, 'Error: Table should still exist and upper boundary of transaction range for ''id'' column should be NULL, but % instead', upper(tid_ranges[1]);
  ASSERT colnames[2] = 'test_column', 'Did not find column ''test_column'' in audit_column_log, but % instead', colnames[2];
  ASSERT datatypes[2] = 'text', 'Incorrect datatype for text-based ''test_column'' column in audit_column_log: %', datatypes[2];
  ASSERT lower(tid_ranges[2]) = test_transaction, 'Error: Starting transaction id % for ''test_column'' column does not match the id % of CREATE TABLE event', lower(tid_ranges[2]), test_transaction;
  ASSERT upper(tid_ranges[2]) IS NULL, 'Error: Table should still exist and upper boundary of transaction range for ''test_column'' column should be NULL, but % instead', upper(tid_ranges[2]);
  ASSERT colnames[3] = 'test_geom_column', 'Did not find column ''test_geom_column'' in audit_column_log, but % instead', colnames[3];
  ASSERT datatypes[3] = 'geometry(PointZ,4326)', 'Incorrect datatype for geometry-based ''test_geom_column'' column in audit_column_log: %', datatypes[3];
  ASSERT lower(tid_ranges[3]) = test_transaction, 'Error: Starting transaction id % for ''test_geom_column'' column does not match the id % of CREATE TABLE event', lower(tid_ranges[3]), test_transaction;
  ASSERT upper(tid_ranges[3]) IS NULL, 'Error: Table should still exist and upper boundary of transaction range for ''test_geom_column'' column should be NULL, but % instead', upper(tid_ranges[3]);
END;
$$
LANGUAGE plpgsql;

-- create one test row
INSERT INTO
  public.test (test_column)
VALUES
  ('test')
RETURNING
  pgmemento_audit_id AS ddl_audit_id
\gset

-- save table log id for next test
SELECT set_config('pgmemento.ddl_test_audit_id', :ddl_audit_id::text, FALSE);
