-- TEST_INIT.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that initializes pgMemento for a single table in the test database
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.4.1     2021-12-23   session variables starting with letter         ol-teuto
-- 0.4.0     2020-04-12   call new init function to start auditing       FKun
-- 0.3.0     2020-03-05   reflect new_data column in row_log             FKun
-- 0.2.0     2017-09-08   moved drop parts to TEST_UNINSTALL.sql         FKun
-- 0.1.0     2017-07-20   initial commit                                 FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento initializaton'

\echo
\echo 'TEST ':n'.1: Create event trigger'
DO
$$
BEGIN
  -- create the event triggers
  PERFORM pgmemento.create_schema_event_trigger(TRUE);

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
          ('pgmemento_table_drop_post_trigger'),
          ('pgmemento_table_drop_pre_trigger')
        ) AS p (pgm_event_trigger)
      LEFT JOIN (
        SELECT
          evtname
        FROM
          pg_event_trigger
        ) t
        ON t.evtname = p.pgm_event_trigger
      WHERE
        t.evtname IS NULL
    )
  ), 'Error: Did not find all necessary event trigger!';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: Create table audit'
DO
$$
DECLARE
  tab_schema TEXT := 'public';
  tab TEXT := 'object';
BEGIN
  -- create table audit which creates triggers and adds the audit_it column
  -- as this is the first audited table call init to start auditing for the schema
  PERFORM pgmemento.init(tab_schema, 'pgmemento_audit_id', TRUE, TRUE, FALSE, TRUE, ARRAY['spatial_ref_sys']);

  -- query for logged transaction
  ASSERT (
    SELECT EXISTS (
      SELECT
        1
      FROM
        pgmemento.transaction_log
      WHERE
        id = current_setting('pgmemento.t' || txid_current())::numeric
        AND session_info ? 'pgmemento_init'
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

  -- test if pgmemento_audit_id column exists
  ASSERT (
    SELECT EXISTS(
      SELECT
        pgmemento_audit_id
      FROM
        public.object
    )
  ), 'Error: Did not find pgmemento_audit_id column in % table!', tab;

  -- test if entry was made in audit_schema_log table
  ASSERT (
    SELECT EXISTS(
      SELECT
        1
      FROM
        pgmemento.audit_schema_log
      WHERE
        schema_name = tab_schema
    )
  ), 'Error: Did not find entry for % schema in audit_schema_log!', tab_schema;

  -- test if entry was made in audit_table_log table
  ASSERT (
    SELECT EXISTS(
      SELECT
        1
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = tab
        AND schema_name = tab_schema
    )
  ), 'Error: Did not find entry for % table in audit_table_log!', tab;

  -- test if entry was made for each column of given table in audit_column_log table
  ASSERT (
    SELECT NOT EXISTS(
      SELECT
        a.attname
      FROM
        pg_attribute a
      LEFT JOIN
        pgmemento.audit_column_log c
        ON c.column_name = a.attname
      WHERE
        a.attrelid = pgmemento.get_table_oid(tab, tab_schema)
        AND a.attname <> 'pgmemento_audit_id'
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND c.column_name IS NULL
    )
  ), 'Error: Did not find entries for all columns of % table in audit_column_log!', tab;
END;
$$
LANGUAGE plpgsql;
