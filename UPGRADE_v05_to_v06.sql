-- UPGRADE_v05_to_v06.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script upgrades a pgMemento extension of v0.5 to v0.6. All functions
-- will be replaced and tables will be altered (see changelog for more details)
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.1.0     2018-07-23   initial commit                                 FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'Updgrade pgMemento from v0.5 to v0.6 ...'

\echo
\echo 'Remove views'
DROP VIEW IF EXISTS pgmemento.audit_tables CASCADE;
DROP VIEW IF EXISTS pgmemento.audit_tables_dependency CASCADE;

\echo
\echo 'Remove all functions'
DO
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
      format('%I.%I(%s)', ns.nspname, p.proname, oidvectortypes(p.proargtypes)) AS fspec,
      proisagg
    FROM
      pg_proc p
    JOIN
      pg_namespace ns
      ON (p.pronamespace = ns.oid)
    WHERE
      ns.nspname = 'pgmemento'
  LOOP
    IF rec.proisagg THEN
      EXECUTE 'DROP AGGREGATE ' || rec.fspec || ' CASCADE';
    ELSE
      EXECUTE 'DROP FUNCTION ' || rec.fspec || ' CASCADE';
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'Alter tables and recreate functions'
\i ctl/UPGRADE.sql
\i src/SETUP.sql
\i src/LOG_UTIL.sql
\i src/DDL_LOG.sql
\i src/VERSIONING.sql
\i src/REVERT.sql
\i src/SCHEMA_MANAGEMENT.sql

\echo
\echo 'pgMemento setup completed!'