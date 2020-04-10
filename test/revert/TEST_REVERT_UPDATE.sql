-- TEST_REVERT_UPDATE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an UPDATE event is reverted
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.0     2020-02-29   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-10-19   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento revert UPDATE event'

\echo
\echo 'TEST ':n'.1: Revert UPDATE events'
DO
$$
DECLARE
  test_transaction INTEGER;
  test_event TEXT;
  update_op_id SMALLINT := pgmemento.get_operation_id('UPDATE');
  jsonb_log JSONB;
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Reverting updates"}'::text, FALSE);

  -- store current lineage value of row that will be changed during the revert process
  SELECT
    jsonb_build_object('lineage', lineage)
  INTO
    jsonb_log
  FROM
    public.object
  WHERE
    pgmemento_audit_id = 3;

  -- get min and max transaction_ids of update events on test table
  PERFORM
    pgmemento.revert_distinct_transactions(min(transaction_id), max(transaction_id))
  FROM
    pgmemento.table_event_log
  WHERE
    op_id = update_op_id
    AND table_name = 'object'
    AND schema_name = 'public'
  GROUP BY
    op_id;

  -- query for logged transaction
  SELECT
    id
  INTO
    test_transaction
  FROM
    pgmemento.transaction_log
  WHERE
    session_info @> '{"message":"Reverting updates"}'::jsonb;

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
    AND op_id = update_op_id;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- join table with entries from row_log table to check if old value has been logged
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
        AND r.old_data = jsonb_log
    )
  ),
  'Error: Entries of test table were not logged correctly in row_log table!';
END;
$$
LANGUAGE plpgsql;
