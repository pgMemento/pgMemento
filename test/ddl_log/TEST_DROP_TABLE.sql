-- TEST_DROP_TABLE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an DROP TABLE event happens
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.1     2021-12-23   session variables starting with letter           ol-teuto
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.0     2020-02-29   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-09-25   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit DROP TABLE event'

\echo
\echo 'TEST ':n'.1: Log DROP TABLE event'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_transaction INTEGER;
  event_keys TEXT[];
BEGIN
  PERFORM set_config('pgmemento.session_info', '{"test":"drop table will first truncate table"}'::text, TRUE);

  -- drop table tests
  DROP TABLE public.tests;

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.t' || test_txid)::int;
  PERFORM set_config('pgmemento.drop_table_test', test_transaction::text, FALSE);

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
    array_agg(event_key ORDER BY id)
  INTO
    event_keys
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.t' || test_txid)::int
    AND (op_id = pgmemento.get_operation_id('TRUNCATE')
     OR op_id = pgmemento.get_operation_id('DROP TABLE'));

  ASSERT event_keys[1] IS NOT NULL, 'Error: Did not find test entry for TRUNCATE event in table_event_log table!';
  ASSERT event_keys[2] IS NOT NULL, 'Error: Did not find test entry for DROP TABLE event in table_event_log table!';

  -- save event time for next test
  PERFORM set_config('pgmemento.drop_table_test_event', event_keys[1], FALSE);
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
  tid_range numrange;
BEGIN
  test_transaction := current_setting('pgmemento.drop_table_test')::int;

  -- get parameters of dropped table
  SELECT
    id,
    txid_range
  INTO
    tabid,
    tid_range
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'tests'
    AND schema_name = 'public'
    AND upper(txid_range) IS NOT NULL;

  -- save table log id for next test
  PERFORM set_config('pgmemento.drop_table_test2', tabid::text, FALSE);

  ASSERT upper(tid_range) = test_transaction, 'Error: Upper transaction id % does not match the id % of DROP TABLE event', upper(tid_ranges[1]), test_transaction;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Check entries in audit_column_log table'
DO
$$
DECLARE
  test_transaction INTEGER;
  colnames TEXT[];
  datatypes TEXT[];
BEGIN
  test_transaction := current_setting('pgmemento.drop_table_test')::int;

  -- get logs of columns of dropped table
  SELECT
    array_agg(column_name ORDER BY id),
    array_agg(data_type ORDER BY id)
  INTO
    colnames,
    datatypes
  FROM
    pgmemento.audit_column_log
  WHERE
    audit_table_id = current_setting('pgmemento.drop_table_test2')::int
    AND upper(txid_range) = test_transaction;

  ASSERT colnames[1] = 'id', 'Expected id, but found ''%'' instead', colnames[1];
  ASSERT colnames[2] = 'test_geom_column', 'Expected test_geom_column, but found ''%'' instead', colnames[2];
  ASSERT colnames[3] = 'test_json_column', 'Expected test_json_column, but found ''%'' instead', colnames[3];
  ASSERT datatypes[1] = 'integer', 'Expected integer data type, but found ''%'' instead', datatypes[1];
  ASSERT datatypes[2] = 'geometry(PointZ,4326)', 'Expected geometry(PointZ,4326) data type, but found ''%'' instead', datatypes[2];
  ASSERT datatypes[3] = 'json', 'Expected json data type, but found ''%'' instead', datatypes[3];
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.4: Check row_log table for content of dropped table'
DO
$$
DECLARE
  test_event TEXT;
  log_audit_id BIGINT;
  jsonb_log JSONB;
BEGIN
  test_event := current_setting('pgmemento.drop_table_test_event');

  SELECT
    audit_id,
    old_data
  INTO
    log_audit_id,
    jsonb_log
  FROM
    pgmemento.row_log
  WHERE
    event_key = test_event;

  ASSERT (jsonb_log->>'id')::bigint = 1, 'Error: Wrong content in row_log table: %', jsonb_log->>'id';
  ASSERT jsonb_log->>'test_geom_column' IS NULL, 'Error: Wrong content in row_log table: %', jsonb_log->>'test_geom_column';
  ASSERT jsonb_log->>'test_json_column' = '{"test": "value"}', 'Error: Wrong content in row_log table: %', jsonb_log->>'test_json_column';
  ASSERT (jsonb_log->>'pgmemento_audit_id')::bigint = log_audit_id, 'Error: Audit_ids do not match: Expected %, found %', log_audit_id, jsonb_log->>'pgmemento_audit_id';
END;
$$
LANGUAGE plpgsql;
