-- TEST_RESTORE_RECORDSETS.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks if multiple previous table states can be restored at once
-- from the logs
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.4.0     2021-03-27   add NULL case                                    FKun
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.0     2020-01-09   reflect changes on schema and triggers           FKun
-- 0.1.0     2018-11-13   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento restore multiple previous table states at once'

\echo
\echo 'TEST ':n'.1: Restore multiple versions for given table in one result'
DO
$$
DECLARE
  audit_ids BIGINT[];
  operations TEXT[];
  txids INTEGER[];
  jsonb_log JSONB;
BEGIN
  SELECT
    array_agg(pgmemento_audit_id),
    array_agg(table_operation),
    array_agg(transaction_id)
  INTO
    audit_ids,
    operations,
    txids
  FROM
    pgmemento.restore_recordsets(1, 20, 'object', 'public')
    AS (id integer, lineage text, pgmemento_audit_id bigint, stmt_time timestamp with time zone, table_operation text, transaction_id integer);

  ASSERT audit_ids[1] = 1, 'Expected pgmemento_audit_id 1, but found %', audit_ids[1];
  ASSERT audit_ids[2] = 3, 'Expected pgmemento_audit_id 3, but found %', audit_ids[2];
  ASSERT audit_ids[2] = audit_ids[3] AND audit_ids[2] = audit_ids[4], 'Expected multiple versions for pgmemento_audit_ids 3, but found % and %', audit_ids[3], audit_ids[4];
  ASSERT operations[1] = 'INSERT', 'Expected ''INSERT'', but founds %', operations[1];
  ASSERT operations[2] = 'INSERT', 'Expected ''INSERT'', but founds %', operations[2];
  ASSERT operations[3] = 'UPDATE', 'Expected ''UPDATE'', but founds %', operations[3];
  ASSERT operations[4] = 'UPDATE', 'Expected ''UPDATE'', but founds %', operations[4];
  ASSERT txids[1] = 14, 'Expected transaction id 14, but found %', txids[1];
  ASSERT txids[2] = 15, 'Expected transaction id 15, but found %', txids[2];
  ASSERT txids[3] = 16, 'Expected transaction id 16, but found %', txids[3];
  ASSERT txids[4] = 19, 'Expected transaction id 19, but found %', txids[4];

  -- restore row as JSONB
  SELECT
    jsonb_agg(log)
  INTO
    jsonb_log
  FROM
    pgmemento.restore_recordsets(1, 20, 'object', 'public', TRUE)
    AS (log JSONB);

  ASSERT jsonb_log->0->>'id' = '1', 'Incorrect historic value for ''id'' column. Expected 1, but found %', jsonb_log->0->>'id';
  ASSERT jsonb_log->0->>'lineage' IS NULL, 'Incorrect historic value for ''lineage'' column. Expected NULL, but found %', jsonb_log->0->>'lineage';
  ASSERT jsonb_log->0->>'pgmemento_audit_id' = '1', 'Incorrect historic value for ''pgmemento_audit_id'' column. Expected 1, but found %', jsonb_log->0->>'id';
  ASSERT jsonb_log->0->>'table_operation' = 'INSERT' , 'Incorrect historic value for ''table_operation''. Expected ''INSERT'', but found %', jsonb_log->0->>'table_operation';
  ASSERT jsonb_log->0->>'transaction_id' = '14', 'Incorrect historic value for ''transaction_id'' column. Expected 14, but found %', jsonb_log->0->>'transaction_id';
  ASSERT jsonb_log->1->>'id' = '2', 'Incorrect historic value for ''id'' column. Expected 2, but found %', jsonb_log->1->>'id';
  ASSERT jsonb_log->1->>'lineage' = 'pgm_insert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_insert_test'', but found %', jsonb_log->1->>'lineage';
  ASSERT jsonb_log->1->>'pgmemento_audit_id' = '3', 'Incorrect historic value for ''pgmemento_audit_id'' column. Expected 3, but found %', jsonb_log->1->>'id';
  ASSERT jsonb_log->1->>'table_operation' = 'INSERT' , 'Incorrect historic value for ''table_operation''. Expected ''INSERT'', but found %', jsonb_log->1->>'table_operation';
  ASSERT jsonb_log->1->>'transaction_id' = '15', 'Incorrect historic value for ''transaction_id'' column. Expected 15, but found %', jsonb_log->1->>'transaction_id';
  ASSERT jsonb_log->2->>'lineage' = 'pgm_upsert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_upsert_test'', but found %', jsonb_log->2->>'lineage';
  ASSERT jsonb_log->2->>'table_operation' = 'UPDATE' , 'Incorrect historic value for ''table_operation''. Expected ''UPDATE'', but found %', jsonb_log->2->>'table_operation';
  ASSERT jsonb_log->2->>'transaction_id' = '16', 'Incorrect historic value for ''transaction_id'' column. Expected 16, but found %', jsonb_log->2->>'transaction_id';
  ASSERT jsonb_log->3->>'lineage' = 'pgm_update_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_update_test'', but found %', jsonb_log->3->>'lineage';
  ASSERT jsonb_log->3->>'table_operation' = 'UPDATE' , 'Incorrect historic value for ''table_operation''. Expected ''UPDATE'', but found %', jsonb_log->3->>'table_operation';
  ASSERT jsonb_log->3->>'transaction_id' = '19', 'Incorrect historic value for ''transaction_id'' column. Expected 19, but found %', jsonb_log->3->>'transaction_id';
END;
$$
LANGUAGE plpgsql;
