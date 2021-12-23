-- TEST_UNINSTALL.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that drop essential logging parts from test table
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.2.1     2021-12-23   session variables starting with letter         ol-teuto
-- 0.2.0     2020-04-12   use new drop function                          FKun
-- 0.1.0     2017-09-08   initial commit                                 FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento drop log components'

\echo
\echo 'TEST ':n'.1: Drop pgMemento in public schema'
DO
$$
DECLARE
  tab_schema TEXT := 'public';
  tab TEXT := 'object';
BEGIN
  -- drop pgMemento from public schema which should drop audit_id column
  -- this should fire a DDL trigger to fill audit tables and update audit_column_log
  PERFORM pgmemento.drop(tab_schema, FALSE);

  -- query for logged transaction
  ASSERT (
    SELECT EXISTS (
      SELECT
        1
      FROM
        pgmemento.transaction_log
      WHERE
        id = current_setting('pgmemento.t' || txid_current())::numeric
        AND session_info ? 'pgmemento_drop'
    )
  ), 'Error: Could not find entry in transaction_log for stopping audit trail in schema %!', tab_schema;

  -- test if audit_id column has been dropped
  ASSERT (
    SELECT NOT EXISTS(
      SELECT
        1
      FROM
        pg_attribute
      WHERE
        attrelid = pgmemento.get_table_oid(tab, tab_schema)
        AND attnum > 0
        AND NOT attisdropped
        AND attname = 'pgmemento_audit_id'
    )
  ), 'Error: Audit_id column still exist in % table. Drop function did not work!', tab;

  -- test if range was closed for schema in audit_schema_log table
  ASSERT (
    SELECT EXISTS(
      SELECT
        1
      FROM
        pgmemento.audit_schema_log
      WHERE
        schema_name = tab_schema
        AND upper(txid_range) = current_setting('pgmemento.t' || txid_current())::numeric
    )
  ), 'Error: Did not find entry for % schema in audit_schema_log!', tab_schema;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Drop event trigger'
DO
$$
BEGIN
  -- drop the event triggers
  PERFORM pgmemento.drop_schema_event_trigger();

  -- query for event triggers
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        1
      FROM (
        VALUES
          ('pgmemento_schema_drop_pre_trigger'),
          ('pgmemento_table_alter_post_trigger'),
          ('pgmemento_table_alter_pre_trigger'),
          ('pgmemento_table_create_post_trigger'),
          ('pgmemento_table_drop_post_trigger'),
          ('pgmemento_table_drop_pre_trigger')
        ) AS p (pgm_event_trigger)
      JOIN (
        SELECT
          evtname
        FROM
          pg_event_trigger
        ) t
        ON t.evtname = p.pgm_event_trigger
    )
  ), 'Error: Some event trigger still exist. Drop function did not work properly!';
END;
$$
LANGUAGE plpgsql;
