-- TEST_REVERT_DROP_TABLE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an DROP TABLE event is reverted
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.2.0     2020-01-09   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-10-03   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento revert DROP TABLE event'

\echo
\echo 'TEST ':n'.1: Revert DROP TABLE event'
DO
$$
DECLARE
  tid INTEGER;
  tab_log_id INTEGER;
  test_transaction INTEGER;
  event_op_ids INTEGER[];
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Reverting drop table"}'::text, FALSE);

  -- get transaction_id of last drop table event
  SELECT
    transaction_id
  INTO
    tid
  FROM
    pgmemento.table_event_log
  WHERE
    op_id = pgmemento.get_operation_id('DROP TABLE');

  -- get log_id from audit_table_log and remember it for later tests
  SELECT
    log_id
  INTO
    tab_log_id
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'tests'
    AND schema_name = 'public'
    AND upper(txid_range) = tid;

  PERFORM set_config('pgmemento.revert_drop_table_test', tab_log_id::text, FALSE);

  -- now, revert transaction
  PERFORM pgmemento.revert_transaction(tid);

  -- query for logged transaction
  SELECT
    id
  INTO
    test_transaction
  FROM
    pgmemento.transaction_log
  WHERE
    session_info @> '{"message":"Reverting drop table"}'::jsonb;

  ASSERT test_transaction IS NOT NULL, 'Error: Did not find test entry in transaction_log table!';

  -- save transaction_id for next tests
  PERFORM set_config('pgmemento.revert_drop_table_test2', test_transaction::text, FALSE);

  -- query for logged table event
  SELECT
    array_agg(op_id ORDER BY id)
  INTO
    event_op_ids
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction;

  ASSERT event_op_ids[1] = 1, 'Error: Exepected id 1 for RECREATE TABLE, but found %!', event_op_ids[1];
  ASSERT event_op_ids[2] = 21, 'Error: Exepected id 21 for ADD AUDIT_ID, but found %!', event_op_ids[2];
  ASSERT event_op_ids[3] = 3, 'Error: Exepected id 3 for INSERT, but found %!', event_op_ids[3];
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.2: Check entries audit_table_log'
DO
$$
DECLARE
  test_log_id INTEGER;
  test_transaction INTEGER;
  tabid INTEGER;
  tab_log_id INTEGER;
  tid_range numrange;
BEGIN
  test_log_id := current_setting('pgmemento.revert_drop_table_test')::int;
  test_transaction := current_setting('pgmemento.revert_drop_table_test2')::int;

  -- get parameters of recreated table
  SELECT
    id,
    log_id,
    txid_range
  INTO
    tabid,
    tab_log_id,
    tid_range
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'tests'
    AND schema_name = 'public'
    AND upper(txid_range) IS NULL;

  -- save table log id for next test
  PERFORM set_config('pgmemento.revert_drop_table_test3', tabid::text, FALSE);

  ASSERT tab_log_id = test_log_id, 'Error: Expected log_id % got %', test_log_id, tab_log_id;
  ASSERT lower(tid_range) IS NOT NULL, 'Error: Lower transaction id % does not match the id % of DROP TABLE event', upper(tid_range[1]), test_transaction;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Check entries in audit_column_log table'
DO
$$
DECLARE
  test_transaction NUMERIC;
  colnames TEXT[];
  datatypes TEXT[];
  tid_ranges numrange[];
BEGIN
  test_transaction := current_setting('pgmemento.revert_drop_table_test2')::numeric;

  -- get logs of columns of dropped table
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
    audit_table_id = current_setting('pgmemento.revert_drop_table_test3')::int
    AND upper(txid_range) IS NULL;

  ASSERT colnames[1] = 'id', 'Expected id, but found ''%'' instead', colnames[1];
  ASSERT colnames[2] = 'test_geom_column', 'Expected test_geom_column, but found ''%'' instead', colnames[2];
  ASSERT colnames[3] = 'test_json_column', 'Expected test_json_column, but found ''%'' instead', colnames[3];
  ASSERT datatypes[1] = 'integer', 'Expected integer data type, but found ''%'' instead', datatypes[1];
  ASSERT datatypes[2] = 'geometry(PointZ,4326)', 'Expected geometry(PointZ,4326) data type, but found ''%'' instead', datatypes[2];
  ASSERT datatypes[3] = 'json', 'Expected json data type, but found ''%'' instead', datatypes[3];
  ASSERT lower(tid_ranges[1]) = test_transaction, 'Error: Lower transaction id % does not match the id % of RECREATE TABLE event', lower(tid_ranges[1]), test_transaction;
  ASSERT lower(tid_ranges[2]) = test_transaction, 'Error: Lower transaction id % does not match the id % of RECREATE TABLE event', lower(tid_ranges[2]), test_transaction;
  ASSERT lower(tid_ranges[3]) = test_transaction, 'Error: Lower transaction id % does not match the id % of RECREATE TABLE event', lower(tid_ranges[3]), test_transaction;
END;
$$
LANGUAGE plpgsql;
