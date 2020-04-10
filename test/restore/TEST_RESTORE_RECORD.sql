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
-- Script that checks if a previous tuple state can be restored from the logs
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.3.0     2020-03-27   reflect new name of audit_id column              FKun
-- 0.2.0     2018-11-14   added test with restore_template                 FKun
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
    pgmemento.restore_record_definition(18, 'object', 'public')
  INTO
    column_list;

  ASSERT column_list = 'AS (id integer, lineage text, pgmemento_audit_id bigint)', 'Incorrect column definition list: %', column_list;

  -- save column_list for next test
  PERFORM set_config('pgmemento.column_list', column_list, FALSE);
END
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Restore single row'
DO
$$
DECLARE
  query_sring TEXT := 'SELECT * FROM pgmemento.restore_record(1, 18, ''object'', ''public'', 3)';
  rec RECORD;
BEGIN
  -- append saved column list to query string
  query_sring := query_sring || ' ' || current_setting('pgmemento.column_list');

  EXECUTE query_sring INTO rec;

  ASSERT rec.id = 2, 'Incorrect historic value for ''id'' column. Expected 2, but found %', rec.id;
  ASSERT rec.lineage = 'pgm_upsert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_upsert_test'', but found %', rec.lineage;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Restore single row as JSONB'
DO
$$
DECLARE
  jsonb_log JSONB;
BEGIN
  -- restore row as JSONB
  SELECT
    *
  INTO
    jsonb_log
  FROM
    pgmemento.restore_record(1, 17, 'object', 'public', 3, TRUE)
    AS (log JSONB);

  -- save jonsb log for next tests
  PERFORM set_config('pgmemento.restore_record_3_jsonb_log', jsonb_log::text, FALSE);

  ASSERT jsonb_log = '{"id": 2, "lineage": "pgm_upsert_test", "pgmemento_audit_id": 3}'::jsonb, 'Incorrect historic record. Expected JSON ''{"id": 2, "lineage": "pgm_upsert_test", "pgmemento_audit_id": 3}'', but found %', jsonb_log;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.4: Restore single row through JSONB and template'
DO
$$
DECLARE
  rec RECORD;
BEGIN
  -- create a template to be used for jsonb_populate_record
  PERFORM pgmemento.create_restore_template(17, 'object_tmp', 'object', 'public', FALSE);

  -- restore row as JSONB
  SELECT
    *
  INTO
    rec
  FROM
    jsonb_populate_record(null::object_tmp, current_setting('pgmemento.restore_record_3_jsonb_log')::jsonb);

  ASSERT rec.id = 2, 'Incorrect historic value for ''id'' column. Expected 2, but found %', rec.id;
  ASSERT rec.lineage = 'pgm_upsert_test', 'Incorrect historic value for ''lineage'' column. Expected ''pgm_upsert_test'', but found %', rec.lineage;
END;
$$
LANGUAGE plpgsql;
