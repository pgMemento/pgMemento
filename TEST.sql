-- TEST.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Central script to start all tests
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2017-07-20   initial commit                                   FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

-- prepare test database
\echo
\echo 'Install pgMemento'
\i INSTALL_PGMEMENTO.sql

-- add PostGIS extension
CREATE EXTENSION postgis;

-- set test counter
CREATE SEQUENCE pgmemento.test_seq;

-- create table for testing
CREATE TABLE public.object (
  id INTEGER PRIMARY KEY,
  lineage TEXT
);

INSERT INTO
  public.object(id, lineage)
VALUES
  (1, 'init');

-- test schema
\i test/setup/TEST_INSTALL.sql

-- test setup functions
\i test/setup/TEST_INIT.sql

-- test DDL logging
\i test/ddl_log/TEST_CREATE_TABLE.sql
\i test/ddl_log/TEST_ALTER_TABLE.sql
\i test/ddl_log/TEST_ADD_COLUMN.sql
\i test/ddl_log/TEST_ALTER_COLUMN.sql
\i test/ddl_log/TEST_DROP_COLUMN.sql
\i test/ddl_log/TEST_DROP_TABLE.sql

-- test DML logging
\i test/dml_log/TEST_INSERT.sql
\i test/dml_log/TEST_UPDATE.sql
\i test/dml_log/TEST_DELETE.sql
\i test/dml_log/TEST_TRUNCATE.sql

-- test reverts
\i test/revert/TEST_REVERT_DROP_TABLE.sql;
\i test/revert/TEST_REVERT_DROP_COLUMN.sql;
\i test/revert/TEST_REVERT_ALTER_COLUMN.sql;
/*\i test/revert/TEST_REVERT_ADD_COLUMN.sql;
\i test/revert/TEST_REVERT_ALTER_TABLE.sql;
\i test/revert/TEST_REVERT_CREATE_TABLE.sql;

\i test/revert/TEST_REVERT_TRUNCATE.sql;
\i test/revert/TEST_REVERT_DELETE.sql;
\i test/revert/TEST_REVERT_UPDATE.sql;
\i test/revert/TEST_REVERT_INSERT.sql;*/

-- test uninstalling everything
\i test/setup/TEST_UNINSTALL.sql

\echo
\echo 'Uninstall pgMemento'
DROP SCHEMA pgmemento CASCADE;

\echo
\echo 'Uninstall test tables'
DROP TABLE object;
DROP TABLE tests;
SET search_path TO pg_catalog,public;

DROP EXTENSION postgis CASCADE;

\echo
\echo 'pgMemento test completed!'
