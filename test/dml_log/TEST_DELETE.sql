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
-- 0.2.0     2020-01-09   reflect changes on schema and triggers           FKun
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
  jsonb_log JSONB;
BEGIN
  -- DELETE entry that has been inserted for other tests
  DELETE FROM public.object
    WHERE lineage = 'pgm_update_test'
    RETURNING id, audit_id
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
    transaction_id = current_setting('pgmemento.' || test_txid)::int
    AND op_id = delete_op_id;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- query for logged row
  SELECT
    changes
  INTO
    jsonb_log
  FROM
    pgmemento.row_log
  WHERE
    audit_id = delete_audit_id
    AND event_key = test_event;

  ASSERT jsonb_log = ('{"id": '||delete_id||', "lineage": "pgm_update_test", "audit_id": '||delete_audit_id||'}')::jsonb, 'Error: Wrong content in row_log table: %' jsonb_log;
END;
$$
LANGUAGE plpgsql;
