-- TEST_INSERT.sql
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
-- (also for logging initial state with pgmemento.log_table_baseline)
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.2.0     2019-10-24   reflect changes on schema and triggers           FKun
-- 0.1.2     2018-11-10   reflect changes in SETUP                         FKun
-- 0.1.1     2017-11-20   added upsert case                                FKun
-- 0.1.0     2017-11-18   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit INSERT events'

\echo
\echo 'TEST ':n'.1: Log content as inserted'
DO
$$
DECLARE
  test_txid BIGINT := txid_current();
  test_event TIMESTAMP WITH TIME ZONE;
BEGIN
  -- create baseline for test table
  PERFORM pgmemento.log_table_baseline('object', 'public');

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
    stmt_time
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.' || test_txid)::int
    AND op_id = 3;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- join table with entries from row_log table to check if every row has been logged
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        o.id
      FROM
        public.object o
      LEFT JOIN
        pgmemento.row_log r
        ON o.audit_id = r.audit_id 
      WHERE
        r.audit_id IS NULL
    )
  ),
  'Error: Entries of test table were not entirely logged in row_log table!';
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.2: Log INSERT command'
DO
$$
DECLARE
  insert_audit_id INTEGER; 
  test_txid BIGINT := txid_current();
  test_event TIMESTAMP WITH TIME ZONE;
  insert_op_id SMALLINT := pgmemento.get_operation_id('INSERT');
  jsonb_log JSONB;
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Live insert test"}'::text, TRUE);

  -- INSERT new entry in table
  INSERT INTO
    public.object (id, lineage)
  VALUES
    (2, 'pgm_insert_test')
  RETURNING
    audit_id
  INTO
    insert_audit_id;

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
    stmt_time
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.' || test_txid)::int
    AND op_id = insert_op_id;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- query for logged row
  SELECT
    changes
  INTO
    jsonb_log
  FROM
    pgmemento.row_log
  WHERE
    audit_id = insert_audit_id
    AND stmt_time = test_event
    AND op_id = insert_op_id
    AND table_name = 'object'
    AND schema_name = 'public';

  ASSERT jsonb_log IS NULL, 'Error: Wrong content in row_log table: %', jsonb_log;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Log UPSERT command'
DO
$$
DECLARE
  insert_id INTEGER;
  upsert_audit_id INTEGER;
  test_txid BIGINT := txid_current();
  event_times TIMESTAMP WITH TIME ZONE[];
  event_op_ids INTEGER[];
  jsonb_log JSONB[];
BEGIN
  -- get audit_id of inserted row
  SELECT
    id
  INTO
    insert_id
  FROM
    public.object
  WHERE
    lineage = 'pgm_insert_test';

  -- INSERT new entry in table
  INSERT INTO
    public.object (id, lineage)
  VALUES
    (insert_id, 'pgm_insert_test')
  ON CONFLICT (id)
    DO UPDATE SET lineage = 'pgm_upsert_test'
  RETURNING 
    audit_id
  INTO
    upsert_audit_id;

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

  -- query for logged table events
  SELECT
    array_agg(stmt_time ORDER BY id),
    array_agg(op_id ORDER BY id)
  INTO
    event_times,
    event_op_ids
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.' || test_txid)::int
    AND (op_id = pgmemento.get_operation_id('INSERT') OR op_id = pgmemento.get_operation_id('UPDATE'));

  ASSERT array_length(event_op_ids, 1) = 2, 'Error: Did not find entries in table_event_log table!';

  -- query for logged row
  SELECT
    array_agg(r.changes ORDER BY r.id NULLS FIRST)
  INTO
    jsonb_log
  FROM
    unnest(event_op_ids) AS e(op_id)
  LEFT JOIN
    pgmemento.row_log r
    ON e.op_id = r.op_id
   AND r.stmt_time = event_times[2]
   AND r.table_name = 'object'
   AND r.schema_name = 'public';

  ASSERT jsonb_log[1] IS NULL, 'Error: INSERT event should not be logged: %', jsonb_log[1];
  ASSERT jsonb_log[2] = '{"lineage":"pgm_insert_test"}'::jsonb, 'Error: Wrong content in row_log table: %', jsonb_log[2];
END;
$$
LANGUAGE plpgsql;
