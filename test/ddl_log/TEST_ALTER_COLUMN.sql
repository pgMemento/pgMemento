-- TEST_ALTER_COLUMN.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an ALTER TABLE event happens
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2018-09-20   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit ALTER TABLE ALTER COLUMN events'

\echo
\echo 'TEST ':n'.1: Log ALTER COLUMN command'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_transaction INTEGER;
  test_event INTEGER;
  test_tsrange tsrange;
BEGIN
  -- make dummy insert to check if it's logged when column is altered
  INSERT INTO tests (test_tsrange_column) VALUES (tsrange(now()::timestamp, NULL, '(]'))
    RETURNING test_tsrange_column INTO test_tsrange;
  
  -- save inserted value for next test
  PERFORM set_config('pgmemento.alter_column_test_value', test_tsrange::text, FALSE);

  -- alter data type of one column
  ALTER TABLE citydb.tests ALTER test_tsrange_column TYPE tstzrange USING tstzrange(lower(test_tsrange_column), upper(test_tsrange_column), '(]');

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.' || test_txid)::int;
  PERFORM set_config('pgmemento.alter_column_test', test_transaction::text, FALSE);
  test_event := current_setting('pgmemento.' || test_txid || '_' || 'citydb.tests'::regclass::oid || '_5')::int;

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
    id
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction
    AND op_id = 5;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- save event_id for next test
  PERFORM set_config('pgmemento.alter_column_test_event', test_event::text, FALSE);
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
  test_transaction := current_setting('pgmemento.alter_column_test')::int;

  -- get logs of altered column
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
    upper(txid_range) = test_transaction
    OR lower(txid_range) = test_transaction;

  ASSERT colnames[1] = colnames[2], 'Error: Logged names of altered column in audit_column_log is not identical.';
  ASSERT datatypes[1] = 'tsrange', 'Expected tsrange data type, but found ''%'' instead', datatypes[1];
  ASSERT datatypes[2] = 'tstzrange', 'Expected tstzrange data type, but found ''%'' instead', datatypes[2];
  ASSERT upper(tid_ranges[1]) = test_transaction, 'Error: Upper transaction id % does not match the id % of ALTER COLUMN event', lower(tid_ranges[1]), test_transaction;
  ASSERT lower(tid_ranges[2]) = test_transaction, 'Error: Starting transaction id % does not match the id % of ALTER COLUMN event', lower(tid_ranges[2]), test_transaction;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Log RENAME COLUMN command'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_transaction INTEGER;
  test_event INTEGER;
BEGIN
  -- rename a column
  ALTER TABLE citydb.tests RENAME COLUMN test_tsrange_column TO test_tstzrange_column;

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.' || test_txid)::int;
  PERFORM set_config('pgmemento.rename_column_test', test_transaction::text, FALSE);

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
    id
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction
    AND op_id = 22;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.4: Check entries in audit_column_log table'
DO
$$
DECLARE
  test_transaction INTEGER;
  colnames TEXT[];
  datatypes TEXT[];
  tid_ranges numrange[];
BEGIN
  test_transaction := current_setting('pgmemento.rename_column_test')::int;

  -- get logs of altered column
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
    upper(txid_range) = test_transaction
    OR lower(txid_range) = test_transaction;

  ASSERT colnames[1] = 'test_tsrange_column', 'Did not find column ''%'' in audit_column_log', colnames[1];
  ASSERT colnames[2] = 'test_tstzrange_column', 'Did not find column ''%'' in audit_column_log', colnames[2];
  ASSERT datatypes[1] = datatypes[2], 'Error: Logged data types of renamed column in audit_column_log is not identical.';
  ASSERT upper(tid_ranges[1]) = test_transaction, 'Error: Upper transaction id % does not match the id % of ALTER COLUMN event', upper(tid_ranges[1]), test_transaction;
  ASSERT lower(tid_ranges[2]) = test_transaction, 'Error: Starting transaction id % does not match the id % of ALTER COLUMN event', lower(tid_ranges[2]), test_transaction;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.5: Check row_log table for content of altered column'
DO
$$
DECLARE
  test_event INTEGER;
  test_value TEXT;
  jsonb_log JSONB;
BEGIN
  test_event := current_setting('pgmemento.alter_column_test_event')::int;
  test_value := current_setting('pgmemento.alter_column_test_value');

  SELECT
    changes
  INTO
    jsonb_log
  FROM
    pgmemento.row_log
  WHERE
    event_id = test_event;

  ASSERT jsonb_log IS NOT NULL, 'Error: Wrong content in row_log table: %', jsonb_log;
END;
$$
LANGUAGE plpgsql;
