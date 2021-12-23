-- TEST_ALTER_TABLE.sql
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
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.2.1     2021-12-23   session variables starting with letter           ol-teuto
-- 0.2.0     2020-01-09   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-08-14   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit ALTER TABLE events'

\echo
\echo 'TEST ':n'.1: Log RENAME TABLE command'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_transaction INTEGER;
  test_event TEXT;
BEGIN
  -- rename test table to tests
  ALTER TABLE public.test RENAME TO tests;

  -- save transaction_id for next tests
  test_transaction := current_setting('pgmemento.t' || test_txid)::int;
  PERFORM set_config('pgmemento.rename_table_test', test_transaction::text, FALSE);

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
    AND op_id = pgmemento.get_operation_id('RENAME TABLE');

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
  old_tab_log_id INTEGER;
  new_tab_log_id INTEGER;
  tid_range numrange;
BEGIN
  test_transaction := current_setting('pgmemento.rename_table_test')::int;

  -- get old parameters of renamed table
  SELECT
    id,
    table_name,
    log_id
  INTO
    tabid,
    tabname,
    old_tab_log_id
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'test'
    AND schema_name = 'public'
    AND upper(txid_range) = test_transaction;

  -- save table log id for next test
  PERFORM set_config('pgmemento.rename_table_test2', tabid::text, FALSE);

  ASSERT tabname = 'test', 'Did not find table ''%'' in audit_table_log', tabname;

  -- get new parameters of renamed table
  SELECT
    id,
    table_name,
    log_id,
    txid_range
  INTO
    tabid,
    tabname,
    new_tab_log_id,
    tid_range
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'tests'
    AND schema_name = 'public'
    AND lower(txid_range) = test_transaction;

  -- save table log id for next test
  PERFORM set_config('pgmemento.rename_table_test3', tabid::text, FALSE);

  ASSERT tabname = 'tests', 'Did not find table ''%'' in audit_table_log', tabname;
  ASSERT old_tab_log_id = new_tab_log_id, 'Error: audit_table_log.log_id mismatch: old % vs. new %', old_tab_log_id, new_tab_log_id;
  ASSERT upper(tid_range) IS NULL, 'Error: Renamed table should still exist and upper boundary of transaction range should be NULL, % instead', upper(tid_range);
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
  tid_ranges numrange[];
BEGIN
  test_transaction := current_setting('pgmemento.rename_table_test')::int;

  -- get column information of renamed table
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
    audit_table_id = current_setting('pgmemento.rename_table_test2')::int
    OR audit_table_id = current_setting('pgmemento.rename_table_test3')::int;

  ASSERT colnames[1] = colnames[4]
     AND colnames[2] = colnames[5]
     AND colnames[3] = colnames[6], 'Error: Column names of renamed table in audit_column_log are not identical.';
  ASSERT datatypes[1] = datatypes[4]
     AND datatypes[2] = datatypes[5]
     AND datatypes[3] = datatypes[6], 'Error: Data types of columns of renamed table in audit_column_log are not identical.';
  ASSERT upper(tid_ranges[1]) = test_transaction
     AND upper(tid_ranges[2]) = test_transaction
     AND upper(tid_ranges[3]) = test_transaction
     AND upper(tid_ranges[1]) = lower(tid_ranges[4])
     AND upper(tid_ranges[2]) = lower(tid_ranges[5])
     AND upper(tid_ranges[3]) = lower(tid_ranges[6]), 'Error: Start and end transaction ids for columns do not match the id % of ALTER TABLE RENAME TABLE event', test_transaction;
END;
$$
LANGUAGE plpgsql;
