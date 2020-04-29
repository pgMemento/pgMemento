-- TEST_DELETE_LOGS.sql
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
-- 0.1.0     2020-04-29   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento delete from log tables'

-- create new table for tests
CREATE TABLE public.util_test AS
SELECT 'update_me' AS a, 'delete_me' AS b;

-- take baseline
SELECT pgmemento.log_table_baseline('util_test', 'public', 'pgmemento_audit_id', TRUE);

-- generate log entry for util_test table
UPDATE public.util_test SET a = 'ok', b = 'delete_me_next'
RETURNING pgmemento_audit_id \gset

SELECT set_config('pgmemento.delete_logs_test_aid', :pgmemento_audit_id::text, FALSE);

SELECT transaction_id, event_key
  FROM pgmemento.table_event_log
 WHERE table_name = 'util_test'
   AND schema_name = 'public'
   AND op_id = 4 \gset

SELECT set_config('pgmemento.delete_logs_test_transaction', :transaction_id::text, FALSE);
SELECT set_config('pgmemento.delete_logs_test_event', :'event_key', FALSE);

\echo
\echo 'TEST ':n'.1: Delete field in row_log'
DO
$$
DECLARE
  row_log_ids BIGINT[];
  old_jsonb_log JSONB[];
  new_jsonb_log JSONB[];
BEGIN
  SELECT
    array_agg(r.id)
  INTO
    row_log_ids
  FROM
    pgmemento.delete_key(current_setting('pgmemento.delete_logs_test_aid')::bigint, 'b', 'delete_me'::text) AS r(id);

  -- query row_log
  SELECT
    array_agg(r.old_data ORDER BY r.id),
    array_agg(r.new_data ORDER BY r.id)
  INTO
    old_jsonb_log,
    new_jsonb_log
  FROM
    pgmemento.row_log r
  JOIN
    unnest(row_log_ids) AS a(id)
    ON r.id = a.id;

  ASSERT old_jsonb_log[2] = ('{"a": "update_me"}')::jsonb, 'Error: Field not deleted from logs: %', old_jsonb_log[2];
  ASSERT new_jsonb_log[1] ->> 'b' = 'delete_me_next', 'Error: Field not updated in logs. Expected "delete_me_next", got %', new_jsonb_log[1] ->> 'b';
  ASSERT NOT(new_jsonb_log[2] ? 'b'), 'Error: Field not deleted from logs: %', new_jsonb_log[2];
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Update field in row_log'
DO
$$
DECLARE
  row_log_ids BIGINT[];
  old_jsonb_log JSONB[];
  new_jsonb_log JSONB[];
BEGIN
  SELECT
    array_agg(r.id)
  INTO
    row_log_ids
  FROM
    pgmemento.update_key(current_setting('pgmemento.delete_logs_test_aid')::bigint, '{a}', 'update_me'::text, 'update_me_next'::text) AS r(id);

  -- query row_log
  SELECT
    array_agg(r.old_data ORDER BY r.id),
    array_agg(r.new_data ORDER BY r.id)
  INTO
    old_jsonb_log,
    new_jsonb_log
  FROM
    pgmemento.row_log r
  JOIN
    unnest(row_log_ids) AS a(id)
    ON r.id = a.id;

  ASSERT old_jsonb_log[2] = ('{"a": "update_me_next"}')::jsonb, 'Error: Field not updated in logs: %', old_jsonb_log[2];
  ASSERT new_jsonb_log[1] ->> 'a' = 'update_me_next', 'Error: Field not updated in logs. Expected "update_me_next", got %', new_jsonb_log[1] ->> 'a';
  ASSERT new_jsonb_log[2] = ('{"a": "ok"}')::jsonb, 'Error: Field not updated in logs: %', new_jsonb_log[2];
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.3: Delete logged event'
DO
$$
DECLARE
  event_ids INTEGER[];
BEGIN
  SELECT
    array_agg(e_id)
  INTO
    event_ids
  FROM (
    SELECT
      pgmemento.delete_table_event_log(transaction_id, 'util_test', 'public') AS e_id
    FROM
      pgmemento.table_event_log
    WHERE
      event_key = current_setting('pgmemento.delete_logs_test_event')
  ) d;

  ASSERT array_length(event_ids, 1) = 1, 'Error: Expected id array with 1 entry, but has %', array_length(event_ids, 1);
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        1
      FROM
        pgmemento.row_log
      WHERE
        event_key = current_setting('pgmemento.delete_logs_test_event')
    )
  ), 'Error: Logs of deleted table event still exist in row_log table!';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.4: Delete all logs of audited table'
DO
$$
DECLARE
  audit_table_log_ids INTEGER[];
BEGIN
  -- first stop auditing for table to delete everything
  PERFORM pgmemento.drop_table_audit('util_test', 'public', 'pgmemento_audit_id', TRUE, FALSE);

  -- now call delete function
  SELECT
    array_agg(a_id)
  INTO
    audit_table_log_ids
  FROM
    pgmemento.delete_audit_table_log('util_test', 'public') AS a_id;

  ASSERT array_length(audit_table_log_ids, 1) = 1, 'Error: Expected id array with 1 entry, but has %', array_length(audit_table_log_ids, 1);
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        1
      FROM
        pgmemento.table_event_log
      WHERE
        table_name = 'util_test'
        AND schema_name = 'public'
    )
  ), 'Error: Logs for given table still exist in table_event_log table!';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.5: Delete logged transaction'
DO
$$
DECLARE
  transaction_id INTEGER;
BEGIN
  SELECT
    pgmemento.delete_txid_log(current_setting('pgmemento.delete_logs_test_transaction')::int)
  INTO
    transaction_id;

  ASSERT transaction_id = current_setting('pgmemento.delete_logs_test_transaction')::int, 'Error: Expected transaction id %, got %', current_setting('pgmemento.delete_logs_test_transaction')::int, transaction_id;
END;
$$
LANGUAGE plpgsql;
