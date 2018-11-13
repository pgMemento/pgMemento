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
  txids INTEGER[];
  jsonb_log JSONB;
BEGIN
  SELECT
    array_agg(audit_id),
    array_agg(transaction_id)
  INTO
    audit_ids,
    txids
  FROM
    pgmemento.restore_recordsets(1, 18, 'object', 'public')
    AS (id integer, lineage text, audit_id bigint, event_id integer, transaction_id integer);

  ASSERT audit_ids[1] = 1, 'Expected audit_id 1, but found %', audit_ids[1];
  ASSERT audit_ids[2] = 3, 'Expected audit_id 3, but found %', audit_ids[2];
  ASSERT audit_ids[2] = audit_ids[3] AND audit_ids[2] = audit_ids[4], 'Expected multiple versions for audit_id 3, but found % and %', audit_ids[3], audit_ids[4];
  ASSERT txids[1] = 12, 'Expected transaction id 12, but found %', txids[1];
  ASSERT txids[2] = 13, 'Expected transaction id 13, but found %', txids[2];
  ASSERT txids[3] = 14, 'Expected transaction id 14, but found %', txids[3];
  ASSERT txids[4] = 17, 'Expected transaction id 17, but found %', txids[4];

  -- restore row as JSONB
  SELECT
    jsonb_agg(log)
  INTO
    jsonb_log
  FROM
    pgmemento.restore_recordsets(1, 18, 'object', 'public', TRUE)
    AS (log JSONB);

  ASSERT jsonb_log->0 = '{"id": 1, "lineage": "init", "audit_id": 1, "event_id": 12, "transaction_id": 12}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 1, "lineage": "init", "audit_id": 1, "event_id": 12, "transaction_id": 12}'', but found %', jsonb_log->0;
  ASSERT jsonb_log->1 = '{"id": 2, "lineage": "pgm_insert_test", "audit_id": 3, "event_id": 13, "transaction_id": 13}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 2, "lineage": "pgm_insert_test", "audit_id": 3, "event_id": 13, "transaction_id": 13}'', but found %', jsonb_log->1;
  ASSERT jsonb_log->2 = '{"id": 2, "lineage": "pgm_upsert_test", "audit_id": 3, "event_id": 15, "transaction_id": 14}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 2, "lineage": "pgm_upsert_test", "audit_id": 3, "event_id": 15, "transaction_id": 14}'', but found %', jsonb_log->2;
  ASSERT jsonb_log->3 = '{"id": 2, "lineage": "pgm_update_test", "audit_id": 3, "event_id": 17, "transaction_id": 17}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 2, "lineage": "pgm_update_test", "audit_id": 3, "event_id": 17, "transaction_id": 17}'', but found %', jsonb_log->3;
END;
$$
LANGUAGE plpgsql;