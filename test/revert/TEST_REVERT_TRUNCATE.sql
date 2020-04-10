-- TEST_REVERT_TRUNCATE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an TRUNCATE event is reverted
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.0     2020-02-29   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-10-18   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento revert TRUNCATE event'

\echo
\echo 'TEST ':n'.1: Revert TRUNCATE event'
DO
$$
DECLARE
  test_transaction INTEGER;
  test_event TEXT;
  insert_op_id SMALLINT := pgmemento.get_operation_id('INSERT');
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Reverting truncate"}'::text, FALSE);

  -- get transaction_id of truncate event on test table
  PERFORM
    pgmemento.revert_transaction(transaction_id)
  FROM
    pgmemento.table_event_log
  WHERE
    op_id = pgmemento.get_operation_id('TRUNCATE')
    AND table_name = 'object'
    AND schema_name = 'public';

  -- query for logged transaction
  SELECT
    id
  INTO
    test_transaction
  FROM
    pgmemento.transaction_log
  WHERE
    session_info @> '{"message":"Reverting truncate"}'::jsonb;

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
    AND op_id = insert_op_id;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- join table with entries from row_log table to check if every row has been logged
  ASSERT (
    SELECT EXISTS (
      SELECT
        t.pgmemento_audit_id
      FROM
        public.object t
      LEFT JOIN
        pgmemento.row_log r
        ON t.pgmemento_audit_id = r.audit_id
      WHERE
        r.event_key = test_event
        AND r.old_data IS NULL
    )
  ),
  'Error: Entries of test table were not entirely logged in row_log table!';
END;
$$
LANGUAGE plpgsql;
