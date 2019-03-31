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
-- 0.3.3     2018-11-13   added tests to restore previous tuple states     FKun
-- 0.3.2     2018-10-20   added tests for reverting DML changes            FKun
-- 0.3.1     2018-10-10   added tests for reverting DDL changes            FKun
-- 0.3.0     2018-09-27   removed test dump to only work with dummy data   FKun
-- 0.2.2     2018-09-25   added tests for DDL changes                      FKun
-- 0.2.1     2017-11-20   added tests for DML changes                      FKun
-- 0.2.0     2017-09-08   added event trigger and deinstallation tests     FKun
--                        using a sequence for enumerating tests
-- 0.1.0     2017-07-20   initial commit                                   FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

-- prepare test database
\echo
\echo 'Install pgMemento'
CREATE EXTENSION if not exists pgmemento;

-- add PostGIS extension
CREATE EXTENSION if not exists postgis;

\i test/SUITE.sql

DROP EXTENSION postgis CASCADE;

\echo
\echo 'Uninstall pgMemento'
DROP EXTENSION pgmemento CASCADE;

\echo
\! psql --version
\echo 'pgMemento test completed!'
\echo
