-- TEST_REVERT_INSERT.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an INSERT event is reverted
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.0     2020-02-29   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-10-20   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento revert INSERT event'

\echo
\echo 'TEST ':n'.1: Revert INSERT event'
DO
$$
DECLARE
  test_transaction INTEGER;
  test_event TEXT;
  delete_op_id SMALLINT := pgmemento.get_operation_id('DELETE');
  jsonb_log JSONB;
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Reverting insert"}'::text, FALSE);

  -- store current lineage value of row that will be changed during the revert process
  SELECT
    to_jsonb(object)
  INTO
    jsonb_log
  FROM
    public.object
  WHERE
    pgmemento_audit_id = 3;

  -- get transaction_id of insert event on test table
  PERFORM
    pgmemento.revert_transaction(id)
  FROM
    pgmemento.transaction_log
  WHERE
    session_info @> '{"message":"Live insert test"}'::jsonb;

  -- query for logged transaction
  SELECT
    id
  INTO
    test_transaction
  FROM
    pgmemento.transaction_log
  WHERE
    session_info @> '{"message":"Reverting insert"}'::jsonb;

  ASSERT test_transaction IS NOT NULL, 'Error: Did not find test entry in transaction_log table!';

  -- query for logged table event
  SELECT
    event_key
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_transaction
    AND op_id = delete_op_id;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- join table with entries from row_log table to check if deleted row has been logged
  ASSERT (
    SELECT EXISTS (
      SELECT
        id
      FROM
        pgmemento.row_log
      WHERE
        event_key = test_event
        AND old_data = jsonb_log
    )
  ),
  'Error: Entries of test table were not entirely logged in row_log table!';
END;
$$
LANGUAGE plpgsql;
