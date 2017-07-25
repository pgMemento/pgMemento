-- RESTORE_TESTDB.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that initializes pgMemento on test database and checks if 
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2017-07-20   initial commit                                   FKun
--

\echo
\echo 'TEST 3: pgMemento initializaton'
SELECT pgmemento.create_schema_event_trigger(1);

\echo
\echo 'TEST 3.1: Create log trigger'
DO
$$
DECLARE
  tab TEXT := 'cityobject';
BEGIN
  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger(tab, 'citydb');

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
      LEFT JOIN (
        SELECT
          tg.tgname
        FROM
          pg_trigger tg,
          pg_class c
        WHERE
          tg.tgrelid = c.oid
          AND c.relname = 'cityobject'
        ) t
        ON t.tgname = p.pgm_trigger
      WHERE
        t.tgname IS NULL
    )
  ), 'Error: Did not find all necessary trigger for % table!', tab;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST 3.1: Create log trigger correct'

\echo
\echo 'TEST 3.2: Create audit_id column'
DO
$$
DECLARE
  tab TEXT := 'cityobject';
BEGIN
  -- add audit_id column. It should fire a DDL trigger to fill audit_table_log and audit column_log table
  PERFORM pgmemento.create_table_audit_id(tab, 'citydb');

  -- test if audit_id column exists
  ASSERT (
    SELECT EXISTS(
      SELECT
        audit_id
      FROM
        citydb.cityobject
    )
  ), 'Error: Did not find audit_id column in % table!', tab;

  -- test if entry was made in audit_table_log table
  ASSERT (
    SELECT EXISTS(
      SELECT
        1
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = tab
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
        a.attrelid = ('citydb.' || tab)::regclass
        AND a.attname <> 'audit_id'
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND c.column_name IS NULL
    )
  ), 'Error: Did not find entries for all columns of % table in audit_column_log!', tab;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST 3.2: Create audit_id column correct'

\echo
\echo 'TEST 3.3: Drop audit_id column'
DO
$$
DECLARE
  tab TEXT := 'cityobject';
BEGIN
  -- drop audit_id column. It should fire a DDL trigger to fill audit tables and update audit_column_log
  PERFORM pgmemento.drop_table_audit_id(tab, 'citydb');

  -- test if audit_id column has been dropped
  ASSERT (
    SELECT NOT EXISTS(
      SELECT
        1
      FROM
        pg_attribute
      WHERE
        attrelid = ('citydb.' || tab)::regclass
        AND attnum > 0
        AND NOT attisdropped
        AND attname = 'audit_id'
    )
  ), 'Error: Audit_id column still exist in % table. Drop function did not work!', tab;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST 3.3: Drop audit_id column correct'

\echo
\echo 'TEST 3.4: Drop log trigger'
DO
$$
DECLARE
  tab TEXT := 'cityobject';
BEGIN
  -- drop logging triggers
  PERFORM pgmemento.drop_table_log_trigger(tab, 'citydb');

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
          AND c.relname = 'cityobject'
        ) t
        ON t.tgname = p.pgm_trigger
    )
  ), 'Error: Some log trigger still exist for % table. Drop function did not work properly!', tab;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST 3.4: Drop log trigger correct'

\echo
\echo 'TEST 3: pgMemento initializaton correct'