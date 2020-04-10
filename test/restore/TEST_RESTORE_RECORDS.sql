-- TEST_RESTORE_RECORDS.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks if a previous tuple states can be restored from the logs
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.0     2020-01-09   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-11-07   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento restore multiple previous row states at once'

\echo
\echo 'TEST ':n'.1: Get column definition list'
DO
$$
DECLARE
  column_list TEXT;
BEGIN
  SELECT
    pgmemento.restore_record_definition(1, 20, log_id)
  INTO
    column_list
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = 'object'
    AND schema_name = 'public';

  ASSERT column_list = 'AS (id integer, lineage text, pgmemento_audit_id bigint, stmt_time timestamp with time zone, table_operation text, transaction_id integer)', 'Incorrect column definition list: %', column_list;

  -- save column_list for next tests
  PERFORM set_config('pgmemento.column_list', column_list, FALSE);
END
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Restore multiple versions for single audit_id'
DO
$$
DECLARE
  query_sring TEXT := 'SELECT array_agg(lineage) FROM pgmemento.restore_records(1, 20, ''object'', ''public'', 3)';
  lineage_values TEXT[];
  jsonb_log JSONB;
BEGIN
  -- append saved column list to query string
  query_sring := query_sring || ' ' || current_setting('pgmemento.column_list');

  EXECUTE query_sring INTO lineage_values;

  ASSERT lineage_values[1] = 'pgm_insert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_insert_test'', but found %', lineage_values[1];
  ASSERT lineage_values[2] = 'pgm_upsert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_upsert_test'', but found %', lineage_values[2];
  ASSERT lineage_values[3] = 'pgm_update_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_update_test'', but found %', lineage_values[3];

  -- restore row as JSONB
  SELECT
    jsonb_agg(log)
  INTO
    jsonb_log
  FROM
    pgmemento.restore_records(1, 20, 'object', 'public', 3, TRUE)
    AS (log JSONB);

  ASSERT jsonb_log->0->>'lineage' = 'pgm_insert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_insert_test'', but found %', jsonb_log->0->>'lineage';
  ASSERT jsonb_log->0->>'table_operation' = 'INSERT' , 'Incorrect historic value for ''table_operation''. Expected ''INSERT'', but found %', jsonb_log->0->>'table_operation';
  ASSERT jsonb_log->0->>'transaction_id' = '15', 'Incorrect historic value for ''transaction_id'' column. Expected 15, but found %', jsonb_log->0->>'transaction_id';
  ASSERT jsonb_log->1->>'lineage' = 'pgm_upsert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_upsert_test'', but found %', jsonb_log->1->>'lineage';
  ASSERT jsonb_log->1->>'table_operation' = 'UPDATE' , 'Incorrect historic value for ''table_operation''. Expected ''UPDATE'', but found %', jsonb_log->1->>'table_operation';
  ASSERT jsonb_log->1->>'transaction_id' = '16', 'Incorrect historic value for ''transaction_id'' column. Expected 16, but found %', jsonb_log->1->>'transaction_id';
  ASSERT jsonb_log->2->>'lineage' = 'pgm_update_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_update_test'', but found %', jsonb_log->2->>'lineage';
  ASSERT jsonb_log->2->>'table_operation' = 'UPDATE' , 'Incorrect historic value for ''table_operation''. Expected ''UPDATE'', but found %', jsonb_log->2->>'table_operation';
  ASSERT jsonb_log->2->>'transaction_id' = '19', 'Incorrect historic value for ''transaction_id'' column. Expected 19, but found %', jsonb_log->2->>'transaction_id';
END;
$$
LANGUAGE plpgsql;
