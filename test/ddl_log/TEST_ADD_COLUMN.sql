-- TEST_ADD_COLUMN.sql
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
\echo 'TEST ':n': pgMemento audit ALTER TABLE ADD COLUMN events'

\echo
\echo 'TEST ':n'.1: Log ADD COLUMN command'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_transaction INTEGER;
  test_event INTEGER;
BEGIN
  -- add two new columns to tests table
  ALTER TABLE citydb.tests
    ADD COLUMN test_json_column JSON,
    ADD COLUMN test_tsrange_column tsrange;

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.' || test_txid)::int;
  PERFORM set_config('pgmemento.add_column_test', test_transaction::text, FALSE);

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
    AND op_id = 2;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';
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
  test_transaction := current_setting('pgmemento.add_column_test')::int;

  -- get logs of added columns
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
    lower(txid_range) = test_transaction;

  ASSERT colnames[1] = 'test_json_column', 'Did not find column ''%'' in audit_column_log', colnames[1];
  ASSERT colnames[2] = 'test_tsrange_column', 'Did not find column ''%'' in audit_column_log', colnames[2];
  ASSERT datatypes[1] = 'json', 'Data type ''%'' not expected for ''test_json_column''', datatypes[1];
  ASSERT datatypes[2] = 'tsrange', 'Data type ''%'' not expected for ''test_tsrange_column''', datatypes[2];
  ASSERT lower(tid_ranges[1]) = test_transaction, 'Error: Starting transaction id % does not match the id % of ADD COLUMN event', lower(tid_ranges[1]), test_transaction;
  ASSERT lower(tid_ranges[2]) = test_transaction, 'Error: Starting transaction id % does not match the id % of ADD COLUMN event', lower(tid_ranges[2]), test_transaction;
END;
$$
LANGUAGE plpgsql;
