-- TEST_DELETE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an DELETE event happens
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.4.1     2021-12-23   session variables starting with letter           ol-teuto
-- 0.4.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.3.0     2020-03-05   reflect new_data column in row_log               FKun
-- 0.2.0     2020-02-29   reflect changes on schema and triggers           FKun
-- 0.1.0     2017-11-19   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit DELETE events'

\echo
\echo 'TEST ':n'.1: Log DELETE command'
DO
$$
DECLARE
  delete_id INTEGER;
  delete_audit_id INTEGER;
  test_txid BIGINT := txid_current();
  test_event TEXT;
  delete_op_id SMALLINT := pgmemento.get_operation_id('DELETE');
  old_jsonb_log JSONB;
  new_jsonb_log JSONB;
BEGIN
  -- DELETE entry that has been inserted for other tests
  DELETE FROM public.object
    WHERE lineage = 'pgm_update_test'
    RETURNING id, pgmemento_audit_id
    INTO delete_id, delete_audit_id;

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
    transaction_id = current_setting('pgmemento.t' || test_txid)::int
    AND op_id = delete_op_id;

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
    audit_id = delete_audit_id
    AND event_key = test_event;

  ASSERT old_jsonb_log = ('{"id": '||delete_id||', "lineage": "pgm_update_test", "pgmemento_audit_id": '||delete_audit_id||'}')::jsonb, 'Error: Wrong old content in row_log table: %' old_jsonb_log;
  ASSERT new_jsonb_log IS NULL, 'Error: Wrong new content in row_log table: %' new_jsonb_log;
END;
$$
LANGUAGE plpgsql;
