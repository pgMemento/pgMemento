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
-- Script that checks log tables when an ADD COLUMN event happens
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.1     2021-12-23   session variables starting with letter           ol-teuto
-- 0.3.0     2020-03-05   reflect new_data column in row_log               FKun
-- 0.2.0     2020-01-09   reflect changes on schema and triggers           FKun
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
  test_event TEXT;
  old_jsonb_log JSONB;
  new_jsonb_log JSONB;
BEGIN
  -- add two new columns to tests table
  ALTER TABLE public.tests ADD COLUMN test_json_column JSON DEFAULT '{"test": "value"}'::json, ADD COLUMN test_tsrange_column tsrange;

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.t' || test_txid)::int;
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
    event_key
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction
    AND op_id = pgmemento.get_operation_id('ADD COLUMN');

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- query for logged row
  SELECT
    old_data,
    new_data
  INTO
    old_jsonb_log,
    new_jsonb_log
  FROM
    pgmemento.row_log
  WHERE
    audit_id = current_setting('pgmemento.ddl_test_audit_id')::bigint
    AND event_key = test_event;

  ASSERT old_jsonb_log IS NULL, 'Error: Wrong old content in row_log table: %', old_jsonb_log;
  ASSERT new_jsonb_log = ('{"test_json_column": {"test": "value"}, "test_tsrange_column": null}')::jsonb, 'Error: Wrong new content in row_log table: %', new_jsonb_log;
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
  defaults TEXT[];
  tid_ranges numrange[];
BEGIN
  test_transaction := current_setting('pgmemento.add_column_test')::int;

  -- get logs of added columns
  SELECT
    array_agg(column_name ORDER BY id),
    array_agg(data_type ORDER BY id),
    array_agg(column_default ORDER BY id),
    array_agg(txid_range ORDER BY id)
  INTO
    colnames,
    datatypes,
    defaults,
    tid_ranges
  FROM
    pgmemento.audit_column_log
  WHERE
    lower(txid_range) = test_transaction;

  ASSERT colnames[1] = 'test_json_column', 'Did not find column ''%'' in audit_column_log', colnames[1];
  ASSERT colnames[2] = 'test_tsrange_column', 'Did not find column ''%'' in audit_column_log', colnames[2];
  ASSERT datatypes[1] = 'json', 'Data type ''%'' not expected for ''test_json_column''', datatypes[1];
  ASSERT datatypes[2] = 'tsrange', 'Data type ''%'' not expected for ''test_tsrange_column''', datatypes[2];
  ASSERT defaults[1] = '''{"test": "value"}''::json', 'Column default ''%'' not expected for ''test_json_column''', defaults[1];
  ASSERT defaults[2] IS NULL, 'Column default ''%'' not expected for ''test_tsrange_column''', defaults[2];
  ASSERT lower(tid_ranges[1]) = test_transaction, 'Error: Starting transaction id % does not match the id % of ADD COLUMN event', lower(tid_ranges[1]), test_transaction;
  ASSERT lower(tid_ranges[2]) = test_transaction, 'Error: Starting transaction id % does not match the id % of ADD COLUMN event', lower(tid_ranges[2]), test_transaction;
END;
$$
LANGUAGE plpgsql;
