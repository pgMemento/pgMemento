-- TEST_TRUNCATE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks log tables when an TRUNCATE event happens
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2017-11-20   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento audit TRUNCATE event'

\echo
\echo 'TEST ':n'.1: Log TRUNCATE command'
DO
$$
DECLARE
  truncate_audit_ids INTEGER[];
  test_txid BIGINT := txid_current();
  test_event INTEGER;
BEGIN
  -- start logging for another table
  PERFORM pgmemento.create_table_audit('textureparam', 'citydb', 0);

  -- collect ids into array before doing a TRUNCATE
  SELECT
    array_agg(audit_id)
  INTO
    truncate_audit_ids
  FROM
    citydb.textureparam;

  -- TRUNCATE table
  TRUNCATE citydb.textureparam;

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
    AND op_id = 8;

  ASSERT test_event IS NOT NULL, 'Error: Did not find test entry in table_event_log table!';

  -- join table with entries from row_log table to check if every row has been logged
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        t.t_audit_id
      FROM
        (SELECT unnest(truncate_audit_ids) AS t_audit_id) t
      LEFT JOIN
        pgmemento.row_log r
        ON t.t_audit_id = r.audit_id 
      WHERE
        r.audit_id IS NULL
    )
  ),
  'Error: Entries of test table were not entirely logged in row_log table!';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.1: Log TRUNCATE command - correct'

\echo
\echo 'TEST ':n': pgMemento audit TRUNCATE event - correct'