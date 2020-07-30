-- UPGRADE_v07_to_v071.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script upgrades a pgMemento extension of v0.7.0 to v0.7.1 which
-- replaces some functions (see changelog for more details)
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.1.0     2020-07-30   initial commit                                 FKun
--

\pset footer off
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP ON

\echo
\echo 'Updgrade pgMemento from v0.7.0 to v0.7.1 ...'

DROP AGGREGATE IF EXISTS pgmemento.jsonb_merge(jsonb);

COMMENT ON COLUMN pgmemento.transaction_log.user_name IS 'Stores the result of session_user function';

\echo
\echo 'Recreate functions'
\i src/SETUP.sql
\i src/LOG_UTIL.sql
\i src/DDL_LOG.sql
\i src/RESTORE.sql
\i src/CTL.sql

\echo
\echo 'pgMemento upgrade completed!'
