-- TEST_UPDATE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an UPDATE event happens
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2017-11-19   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit UPDATE events'

\echo
\echo 'TEST ':n'.1: Log UPDATE command, that does not change anything'
DO
$$
DECLARE
  update_audit_id INTEGER; 
  test_txid BIGINT := txid_current();
  test_event INTEGER;
BEGIN
  -- UPDATE entry that has been inserted during INSERT test
  UPDATE citydb.cityobject SET lineage = 'pgm_upsert_test'
    WHERE lineage = 'pgm_upsert_test'
    RETURNING audit_id INTO update_audit_id;

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
    id
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_txid
    AND op_id = 4;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- there shall be not entry in row_log table
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        id
      FROM
        pgmemento.row_log
      WHERE
        audit_id = update_audit_id
        AND event_id = test_event
    )
  ), 'Error: Found entry in row_log table, even though UPDATE command did not change anything.';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.1: Log UPDATE command, that does not change anything - correct'

\echo
\echo 'TEST ':n'.2: Log UPDATE command'
DO
$$
DECLARE
  update_audit_id INTEGER; 
  test_txid BIGINT := txid_current();
  test_event INTEGER;
  jsonb_log JSONB;
BEGIN
  -- UPDATE entry that has been inserted during INSERT test
  UPDATE citydb.cityobject SET lineage = 'pgm_update_test'
    WHERE lineage = 'pgm_upsert_test'
    RETURNING audit_id INTO update_audit_id;

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
    id
  INTO
    test_event
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = test_txid
    AND op_id = 4;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- query for logged row
  SELECT
    changes
  INTO
    jsonb_log
  FROM
    pgmemento.row_log
  WHERE
    audit_id = update_audit_id
    AND event_id = test_event;

  ASSERT jsonb_log = '{"lineage":"pgm_upsert_test"}'::jsonb, 'Error: Wrong content in row_log table: %' jsonb_log;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Log UPDATE command - correct'

\echo
\echo 'TEST ':n': pgMemento audit UPDATE events - correct'