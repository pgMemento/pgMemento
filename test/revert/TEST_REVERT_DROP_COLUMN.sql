-- TEST_REVERT_DROP_COLUMN.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an DROP COLUMN event is reverted
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
\echo 'TEST ':n': pgMemento revert DROP COLUMN event'

\echo
\echo 'TEST ':n'.1: Revert DROP COLUMN event'
DO
$$
DECLARE
  test_transaction INTEGER;
  event_op_ids INTEGER[];
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Reverting drop column"}'::text, FALSE);

  -- get transaction_id of last drop column event
  PERFORM
    pgmemento.revert_transaction(transaction_id)
  FROM
    pgmemento.table_event_log
  WHERE
    op_id = pgmemento.get_operation_id('DROP COLUMN');

  -- query for logged transaction
  SELECT
    id
  INTO
    test_transaction
  FROM
    pgmemento.transaction_log
  WHERE
    session_info @> '{"message":"Reverting drop column"}'::jsonb;

  ASSERT test_transaction IS NOT NULL, 'Error: Did not find test entry in transaction_log table!';

  -- save transaction_id for next tests
  PERFORM set_config('pgmemento.revert_drop_column_test', test_transaction::text, FALSE);

  -- query for logged table event
  SELECT
    array_agg(op_id ORDER BY id)
  INTO
    event_op_ids
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction;

  ASSERT event_op_ids[1] = 2, 'Error: Exepected id 2 for ADD COLUMN, but found %!', event_op_ids[1];
  ASSERT event_op_ids[2] = 4, 'Error: Exepected id 4 for UPDATE, but found %!', event_op_ids[2];
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
BEGIN
  test_transaction := current_setting('pgmemento.revert_drop_column_test')::int;

  -- get logs of readded columns
  SELECT
    array_agg(c.column_name),
    array_agg(c.data_type)
  INTO
    colnames,
    datatypes
  FROM
    pgmemento.audit_column_log c
  JOIN
    pgmemento.audit_table_log t
    ON t.id = c.audit_table_id
  WHERE
    t.table_name = 'tests'
    AND t.schema_name = 'public'
    AND lower(c.txid_range) = test_transaction;

  ASSERT colnames[1] = 'test_column', 'Expected test_column, but found ''%'' instead', colnames[1];
  ASSERT colnames[2] = 'test_tstzrange_column', 'Expected test_tstzrange_column, but found ''%'' instead', colnames[2];
  ASSERT datatypes[1] = 'text', 'Expected text data type, but found ''%'' instead', datatypes[1];
  ASSERT datatypes[2] = 'tstzrange', 'Expected tstzrange data type, but found ''%'' instead', datatypes[2];
END;
$$
LANGUAGE plpgsql;
