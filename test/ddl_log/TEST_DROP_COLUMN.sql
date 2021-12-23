-- TEST_DROP_COLUMN.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an DROP COLUMN event happens
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.2.1     2021-12-23   session variables starting with letter           ol-teuto
-- 0.2.0     2020-02-29   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-09-24   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit ALTER TABLE DROP COLUMN event'

\echo
\echo 'TEST ':n'.1: Log DROP COLUMN event'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_transaction INTEGER;
  test_event TEXT;
BEGIN
  -- drop two columns
  ALTER TABLE public.tests DROP test_tstzrange_column, DROP COLUMN test_column;

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.t' || test_txid)::int;
  PERFORM set_config('pgmemento.drop_column_test', test_transaction::text, FALSE);

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
    transaction_id = current_setting('pgmemento.t' || test_txid)::int
    AND op_id = pgmemento.get_operation_id('DROP COLUMN');

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- save event time for next test
  PERFORM set_config('pgmemento.drop_column_test_event', test_event, FALSE);
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.2: Check entries in audit_column_log table'
DO
$$
DECLARE
  test_transaction INTEGER;
  colnames TEXT[];
  datatypes TEXT[];
  tid_ranges numrange[];
BEGIN
  test_transaction := current_setting('pgmemento.drop_column_test')::int;

  -- get logs of dropped column
  SELECT
    array_agg(column_name ORDER BY id),
    array_agg(data_type ORDER BY id),
    array_agg(txid_range ORDER BY id)
  INTO
    colnames,
    datatypes,
    tid_ranges
  FROM
    pgmemento.audit_column_log
  WHERE
    upper(txid_range) = test_transaction;

  ASSERT colnames[1] = 'test_column', 'Expected test_column, but found ''%'' instead', colnames[1];
  ASSERT colnames[2] = 'test_tstzrange_column', 'Expected test_tstzrange_column, but found ''%'' instead', colnames[2];
  ASSERT datatypes[1] = 'text', 'Expected text data type, but found ''%'' instead', datatypes[1];
  ASSERT datatypes[2] = 'tstzrange', 'Expected tstzrange data type, but found ''%'' instead', datatypes[2];
  ASSERT upper(tid_ranges[1]) = test_transaction, 'Error: Upper transaction id % does not match the id % of DROP COLUMN event', upper(tid_ranges[1]), test_transaction;
  ASSERT upper(tid_ranges[2]) = test_transaction, 'Error: Upper transaction id % does not match the id % of DROP COLUMN event', upper(tid_ranges[2]), test_transaction;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Check row_log table for content of dropped column'
DO
$$
DECLARE
  test_event TEXT;
  jsonb_log JSONB;
BEGIN
  test_event := current_setting('pgmemento.drop_column_test_event');

  SELECT
    old_data
  INTO
    jsonb_log
  FROM
    pgmemento.row_log
  WHERE
    event_key = test_event;

  ASSERT jsonb_log->>'test_column' = 'test', 'Error: Wrong content in row_log table: %', jsonb_log;
  ASSERT jsonb_log->>'test_tstzrange_column' IS NOT NULL, 'Error: Wrong content in row_log table: %', jsonb_log;
END;
$$
LANGUAGE plpgsql;
