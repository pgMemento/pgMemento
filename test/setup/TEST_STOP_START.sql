-- TEST_STOP_START.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that stops and starts auditing for given schema
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.1.1     2021-12-23   session variables starting with letter         ol-teuto
-- 0.1.0     2020-04-12   initial commit                                 FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento ctl functions stop and start'

\echo
\echo 'TEST ':n'.1: Stop pgMemento in public schema'
DO
$$
DECLARE
  tab_schema TEXT := 'public';
  tab TEXT := 'object';
BEGIN
  -- stop logging in public schema
  -- should drop log triggers
  PERFORM pgmemento.stop(tab_schema);

  -- query for logged transaction
  ASSERT (
    SELECT EXISTS (
      SELECT
        1
      FROM
        pgmemento.transaction_log
      WHERE
        id = current_setting('pgmemento.t' || txid_current())::numeric
        AND session_info ? 'pgmemento_stop'
    )
  ), 'Error: Could not find entry in transaction_log for stopping audit trail in schema %!', tab_schema;

  -- query for log trigger
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        1
      FROM (
        VALUES
          ('pgmemento_delete_trigger'),
          ('pgmemento_insert_trigger'),
          ('pgmemento_transaction_trigger'),
          ('pgmemento_truncate_trigger'),
          ('pgmemento_update_trigger')
        ) AS p (pgm_trigger)
      JOIN (
        SELECT
          tg.tgname
        FROM
          pg_trigger tg,
          pg_class c
        WHERE
          tg.tgrelid = c.oid
          AND c.relname = tab
        ) t
        ON t.tgname = p.pgm_trigger
    )
  ), 'Error: Some log trigger still exist for % table. Drop function did not work properly!', tab;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.2: Restart pgMemento in public schema'
DO
$$
DECLARE
  tab_schema TEXT := 'public';
  tab TEXT := 'object';
BEGIN
  -- restart logging in public schema
  -- but with default logging behavior which is different
  PERFORM pgmemento.start(tab_schema);

  -- query for log trigger
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        1
      FROM (
        VALUES
          ('pgmemento_delete_trigger'),
          ('pgmemento_insert_trigger'),
          ('pgmemento_transaction_trigger'),
          ('pgmemento_truncate_trigger'),
          ('pgmemento_update_trigger')
        ) AS p (pgm_trigger)
      LEFT JOIN (
        SELECT
          tg.tgname
        FROM
          pg_trigger tg,
          pg_class c
        WHERE
          tg.tgrelid = c.oid
          AND c.relname = tab
        ) t
        ON t.tgname = p.pgm_trigger
      WHERE
        t.tgname IS NULL
    )
  ), 'Error: Did not find all necessary trigger for % table!', tab;

  -- query for logged transaction
  ASSERT (
    SELECT EXISTS (
      SELECT
        1
      FROM
        pgmemento.transaction_log
      WHERE
        id = current_setting('pgmemento.t' || txid_current())::numeric
        AND session_info ? 'pgmemento_start'
    )
  ), 'Error: Could not find entry in transaction_log for stopping audit trail in schema %!', tab_schema;

  -- test if new entry was made in audit_schema_log table
  ASSERT (
    SELECT EXISTS(
      SELECT
        1
      FROM
        pgmemento.audit_schema_log
      WHERE
        schema_name = tab_schema
        AND lower(txid_range) = current_setting('pgmemento.t' || txid_current())::numeric
    )
  ), 'Error: Did not find entry for % schema in audit_schema_log!', tab_schema;
END;
$$
LANGUAGE plpgsql;
