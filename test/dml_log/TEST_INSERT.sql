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
-- 0.4.1     2021-12-23   session variables starting with letter           ol-teuto
-- 0.4.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.3.0     2020-03-05   reflect new_data column in row_log               FKun
-- 0.2.0     2020-02-29   reflect changes on schema and triggers           FKun
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
  test_event TEXT;
BEGIN
  -- create baseline for test table
  PERFORM
    pgmemento.log_table_baseline(table_name, schema_name, audit_id_column, log_new_data)
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'object'
    AND schema_name = 'public';

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
        ON o.pgmemento_audit_id = r.audit_id
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
  insert_id INTEGER;
  insert_audit_id INTEGER;
  test_txid BIGINT := txid_current();
  test_event TEXT;
  insert_op_id SMALLINT := pgmemento.get_operation_id('INSERT');
  old_jsonb_log JSONB;
  new_jsonb_log JSONB;
BEGIN
  -- set session_info to query logged transaction later
  PERFORM set_config('pgmemento.session_info', '{"message":"Live insert test"}'::text, TRUE);

  -- INSERT new entry in table
  INSERT INTO
    public.object (id, lineage)
  VALUES
    (2, 'pgm_insert_test')
  RETURNING
    id, pgmemento_audit_id
  INTO
    insert_id, insert_audit_id;

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
    AND op_id = insert_op_id;

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
    audit_id = insert_audit_id
    AND event_key = test_event;

  ASSERT old_jsonb_log IS NULL, 'Error: Wrong old content in row_log table: %', old_jsonb_log;
  ASSERT new_jsonb_log = ('{"id": '||insert_id||', "lineage": "pgm_insert_test", "pgmemento_audit_id": '||insert_audit_id||'}')::jsonb, 'Error: Wrong new content in row_log table: %', new_jsonb_log;
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
  event_keys TEXT[];
  old_jsonb_log JSONB[];
  new_jsonb_log JSONB[];
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
    pgmemento_audit_id
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
    array_agg(event_key ORDER BY id)
  INTO
    event_keys
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.t' || test_txid)::int
    AND (op_id = pgmemento.get_operation_id('INSERT') OR op_id = pgmemento.get_operation_id('UPDATE'));

  ASSERT array_length(event_keys, 1) = 2, 'Error: Did not find entries in table_event_log table!';

  -- query for logged row
  SELECT
    array_agg(r.old_data ORDER BY r.id NULLS FIRST),
    array_agg(r.new_data ORDER BY r.id NULLS FIRST)
  INTO
    old_jsonb_log,
    new_jsonb_log
  FROM
    unnest(event_keys) AS e(key)
  LEFT JOIN
    pgmemento.row_log r
    ON e.key = r.event_key;

  ASSERT old_jsonb_log[1] IS NULL, 'Error: INSERT event should not be logged: %', old_jsonb_log[1];
  ASSERT old_jsonb_log[2] = '{"lineage":"pgm_insert_test"}'::jsonb, 'Error: Wrong old content in row_log table: %', old_jsonb_log[2];
  ASSERT new_jsonb_log[1] IS NULL, 'Error: INSERT event should not be logged: %', new_jsonb_log[1];
  ASSERT new_jsonb_log[2] = '{"lineage":"pgm_upsert_test"}'::jsonb, 'Error: Wrong new content in row_log table: %', new_jsonb_log[2];
END;
$$
LANGUAGE plpgsql;
