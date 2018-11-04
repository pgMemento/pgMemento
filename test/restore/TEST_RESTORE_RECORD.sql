-- TEST_RESTORE_RECORD.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks if a previous table/schema state can be restored from
-- the logs
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2018-10-28   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento restore previous row states'

\echo
\echo 'TEST ':n'.1: Get column definition list'
DO
$$
DECLARE
  column_list TEXT;
BEGIN
  SELECT
    pgmemento.restore_record_definition(17, 'object', 'public')
  INTO
    column_list;

  ASSERT column_list = 'AS (id integer, lineage text, audit_id bigint)', 'Incorrect column definition list: %', column_list;
  
  -- save column_list for next tests
  PERFORM set_config('pgmemento.column_list', column_list, FALSE);
END
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Restore single row'
DO
$$
DECLARE
  rec RECORD;
  query_sring TEXT := 'SELECT * FROM pgmemento.restore_record(1, 17, ''object'', ''public'', 3)';
  jsonb_log JSONB;
BEGIN
  -- append saved column list to query string
  query_sring := query_sring || current_setting('pgmemento.column_list');
  
  EXECUTE query_sring INTO rec;

  ASSERT rec.id = 2, 'Incorrect historic value for ''id'' column. Expected 2, but found %', rec.id;
  ASSERT rec.lineage = 'pgm_upsert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_upsert_test'', but found %', rec.lineage;

  -- restore row as JSONB
  SELECT * INTO jsonb_log
    FROM pgmemento.restore_record(1, 17, 'object', 'public', 3, TRUE) AS (log JSONB);

  ASSERT jsonb_log = '{"id": 2, "lineage": "pgm_upsert_test", "audit_id": 3}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 2, "lineage": "pgm_upsert_test", "audit_id": 3}'', but found %', jsonb_log;
END;
$$
LANGUAGE plpgsql;