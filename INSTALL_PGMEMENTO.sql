-- INSTALL_PGMEMENTO.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script to setup pgMemento
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.6.1     2018-07-23   schema parts of SETUP.sql moved to SCHEMA.sql    FKun
-- 0.3.0     2015-06-20   initial commit                                   FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'Creating pgMemento schema, tables and functions ...'
\i src/SCHEMA.sql
\i src/SETUP.sql
\i src/LOG_UTIL.sql
\i src/DDL_LOG.sql
\i src/RESTORE.sql
\i src/REVERT.sql
\i src/SCHEMA_MANAGEMENT.sql
\i src/CTL.sql

\echo
\echo 'Introducing pgMemento to search path ...'
SELECT current_setting('search_path') AS db_path \gset
ALTER DATABASE :"DBNAME" SET search_path TO :db_path, pgmemento;

\echo
\echo 'pgMemento setup completed!'
