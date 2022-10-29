-- TEST_COLUMN_ARRAY_TO_COLUMN_LIST.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that checks whether the json composing util works correctly
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2022-09-20   initial commit                                   ekeuus
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento test util functions'

\echo
\echo 'TEST ':n'.1: pgmemento.column_array_to_column_list should work with 1 column'
DO
$$
DECLARE
  result TEXT;
BEGIN
  SELECT
    pgmemento.column_array_to_column_list(ARRAY['col'])
  INTO
    result;

  ASSERT result = 'SELECT d FROM (SELECT col) d', 'Error: Columns not concatenated correctly';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.2: pgmemento.column_array_to_column_list should work with 2 columns'
DO
$$
DECLARE
  result TEXT;
BEGIN
  SELECT
    pgmemento.column_array_to_column_list(ARRAY['col', 'other_column'])
  INTO
    result;

  ASSERT result = 'SELECT d FROM (SELECT col, other_column) d', 'Error: Columns not concatenated correctly';
END;
$$
LANGUAGE plpgsql;
