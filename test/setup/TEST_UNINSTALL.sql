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
-- Version | Date       | Description                                    | Author
-- 0.1.0     2017-09-08   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento drop log components'

\echo
\echo 'TEST ':n'.1: Drop audit_id column'
DO
$$
DECLARE
  tab TEXT := 'object';
BEGIN
  -- drop audit_id column. It should fire a DDL trigger to fill audit tables and update audit_column_log
  PERFORM pgmemento.drop_table_audit_id(tab, 'public');

  -- test if audit_id column has been dropped
  ASSERT (
    SELECT NOT EXISTS(
      SELECT
        1
      FROM
        pg_attribute
      WHERE
        attrelid = ('public.' || tab)::regclass
        AND attnum > 0
        AND NOT attisdropped
        AND attname = 'audit_id'
    )
  ), 'Error: Audit_id column still exist in % table. Drop function did not work!', tab;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.2: Drop log trigger'
DO
$$
DECLARE
  tab TEXT := 'object';
BEGIN
  -- drop logging triggers
  PERFORM pgmemento.drop_table_log_trigger(tab, 'public');

  -- query for log trigger
  ASSERT (
    SELECT NOT EXISTS (
      SELECT
        1
      FROM (
        VALUES
          ('log_delete_trigger'),
          ('log_insert_trigger'),
          ('log_transaction_trigger'),
          ('log_truncate_trigger'),
          ('log_update_trigger')
        ) AS p (pgm_trigger)
      JOIN (
        SELECT
          tg.tgname
        FROM
          pg_trigger tg,
          pg_class c
        WHERE
          tg.tgrelid = c.oid
          AND c.relname = 'object'
        ) t
        ON t.tgname = p.pgm_trigger
    )
  ), 'Error: Some log trigger still exist for % table. Drop function did not work properly!', tab;
END;
$$
LANGUAGE plpgsql;


\echo
\echo 'TEST ':n'.3: Drop event trigger'
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
          ('schema_drop_pre_trigger'),
          ('table_alter_post_trigger'),
          ('table_alter_pre_trigger'),
          ('table_create_post_trigger'),
          ('table_drop_post_trigger'),
          ('table_drop_pre_trigger')
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
