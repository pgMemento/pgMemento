-- TEST_RESTORE_RECORDSET.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks if a previous table state can be restored from the logs
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
\echo 'TEST ':n': pgMemento restore previous table state'

\echo
\echo 'TEST ':n'.1: Restore versions for single audit_id'
DO
$$
DECLARE
  lineage_values TEXT[];
  jsonb_log JSONB;
BEGIN
  SELECT
    array_agg(lineage)
  INTO
    lineage_values
  FROM
    pgmemento.restore_recordset(1, 18, 'object', 'public')
    AS (id integer, lineage text, audit_id bigint);

  ASSERT lineage_values[1] = 'init', 'Incorrect historic value for ''lineage'' column. Expected ''init'', but found %', lineage_values[1];
  ASSERT lineage_values[2] = 'pgm_update_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_update_test'', but found %', lineage_values[2];

  -- restore row as JSONB
  SELECT
    jsonb_agg(log)
  INTO
    jsonb_log
  FROM
    pgmemento.restore_recordset(1, 18, 'object', 'public', TRUE)
    AS (log JSONB);

  ASSERT jsonb_log->0 = '{"id": 1, "lineage": "init", "audit_id": 1}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 1, "lineage": "init", "audit_id": 1}'', but found %', jsonb_log->0;
  ASSERT jsonb_log->1 = '{"id": 2, "lineage": "pgm_update_test", "audit_id": 3}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 2, "lineage": "pgm_update_test", "audit_id": 3}'', but found %', jsonb_log->1;
END;
$$
LANGUAGE plpgsql;