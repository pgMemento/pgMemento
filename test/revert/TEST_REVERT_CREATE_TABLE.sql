-- TEST_REVERT_CREATE_TABLE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an INSERT event happens
-- (also for logging initial state with pgmemento.log_table_state)
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2018-10-10   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento revert CREATE TABLE event'

\echo
\echo 'TEST ':n'.1: Revert CREATE TABLE event'
DO
$$
DECLARE
  test_transaction INTEGER;
  event_op_ids INTEGER[];
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Reverting create table"}'::text, FALSE);

  -- get transaction_id of first create table event
  PERFORM
    pgmemento.revert_transaction(transaction_id)
  FROM
    pgmemento.table_event_log
  WHERE
    op_id = 1
    AND transaction_id = 2;

  -- query for logged transaction
  SELECT
    id
  INTO
    test_transaction
  FROM
    pgmemento.transaction_log
  WHERE
    session_info @> '{"message":"Reverting create table"}'::jsonb;

  ASSERT test_transaction IS NOT NULL, 'Error: Did not find test entry in transaction_log table!';

  -- save transaction_id for next tests
  PERFORM set_config('pgmemento.revert_create_table_test', test_transaction::text, FALSE);

  -- query for logged table event
  SELECT
    array_agg(id ORDER BY id)
  INTO
    event_op_ids
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction
    AND (op_id = 8
     OR op_id = 9);

  ASSERT event_op_ids[1] IS NOT NULL, 'Error: Did not find test entry for TRUNCATE event in table_event_log table!';
  ASSERT event_op_ids[2] IS NOT NULL, 'Error: Did not find test entry for DROP TABLE event in table_event_log table!';
END;
$$
LANGUAGE plpgsql;