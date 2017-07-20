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
\echo 'Add test schema'
\i test/testdb/RESTORE_TESTDB.sql

\echo
\echo 'Install pgMemento'
\i INSTALL_PGMEMENTO.sql

\echo
\echo 'Check for pgMemento elements'
\i test/setup/TEST_INSTALL.sql

\echo
\echo 'Test if pgMemento has been initialized correctly'
\i test/setup/TEST_INIT.sql

\echo
\echo 'Uninstall pgMemento'
\i UNINSTALL_PGMEMENTO.sql

\echo
\echo 'Uninstall test database'
DROP SCHEMA CITYDB_PKG CASCADE;
DROP SCHEMA CITYDB CASCADE;
SET search_path TO pg_catalog,public;

\echo
\echo 'pgMemento test completed!'