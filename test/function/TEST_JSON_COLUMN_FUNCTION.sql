-- TEST_DELETE_LOGS.sql
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
-- 0.7.3     2022-09-12   initial commit                                   FKun
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

  ASSERT result = 'jsonb_build_object(''col'', col)', 'Error: Columns not concatenated correctly';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.1: pgmemento.column_array_to_column_list should work with 2 columns'
DO
$$
DECLARE
  result TEXT;
BEGIN
  SELECT
    pgmemento.column_array_to_column_list(ARRAY['col', 'other_column'])
  INTO
    result;

  ASSERT result = 'jsonb_build_object(''col'', col, ''other_column'', other_column)', 'Error: Columns not concatenated correctly';
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n'.1: pgmemento.column_array_to_column_list should work with 100 columns'
DO
$$
DECLARE
  result TEXT;
BEGIN
  SELECT
    pgmemento.column_array_to_column_list(ARRAY[
      'col1',
      'col2',
      'col3',
      'col4',
      'col5',
      'col6',
      'col7',
      'col8',
      'col9',
      'col10',
      'col11',
      'col12',
      'col13',
      'col14',
      'col15',
      'col16',
      'col17',
      'col18',
      'col19',
      'col20',
      'col21',
      'col22',
      'col23',
      'col24',
      'col25',
      'col26',
      'col27',
      'col28',
      'col29',
      'col30',
      'col31',
      'col32',
      'col33',
      'col34',
      'col35',
      'col36',
      'col37',
      'col38',
      'col39',
      'col40',
      'col41',
      'col42',
      'col43',
      'col44',
      'col45',
      'col46',
      'col47',
      'col48',
      'col49',
      'col50',
      'col51',
      'col52',
      'col53',
      'col54',
      'col55',
      'col56',
      'col57',
      'col58',
      'col59',
      'col60',
      'col61',
      'col62',
      'col63',
      'col64',
      'col65',
      'col66',
      'col67',
      'col68',
      'col69',
      'col70',
      'col71',
      'col72',
      'col73',
      'col74',
      'col75',
      'col76',
      'col77',
      'col78',
      'col79',
      'col80',
      'col81',
      'col82',
      'col83',
      'col84',
      'col85',
      'col86',
      'col87',
      'col88',
      'col89',
      'col90',
      'col91',
      'col92',
      'col93',
      'col94',
      'col95',
      'col96',
      'col97',
      'col98',
      'col99',
      'col100'
    ]
  )
  INTO
    result;

  ASSERT result = 'jsonb_build_object(''col1'', col1, ''col2'', col2, ''col3'', col3, ''col4'', col4, ''col5'', col5, ''col6'', col6, ''col7'', col7, ''col8'', col8, ''col9'', col9, ''col10'', col10, ''col11'', col11, ''col12'', col12, ''col13'', col13, ''col14'', col14, ''col15'', col15, ''col16'', col16, ''col17'', col17, ''col18'', col18, ''col19'', col19, ''col20'', col20, ''col21'', col21, ''col22'', col22, ''col23'', col23, ''col24'', col24, ''col25'', col25, ''col26'', col26, ''col27'', col27, ''col28'', col28, ''col29'', col29, ''col30'', col30, ''col31'', col31, ''col32'', col32, ''col33'', col33, ''col34'', col34, ''col35'', col35, ''col36'', col36, ''col37'', col37, ''col38'', col38, ''col39'', col39, ''col40'', col40, ''col41'', col41, ''col42'', col42, ''col43'', col43, ''col44'', col44, ''col45'', col45, ''col46'', col46, ''col47'', col47, ''col48'', col48, ''col49'', col49, ''col50'', col50) || jsonb_build_object(''col51'', col51, ''col52'', col52, ''col53'', col53, ''col54'', col54, ''col55'', col55, ''col56'', col56, ''col57'', col57, ''col58'', col58, ''col59'', col59, ''col60'', col60, ''col61'', col61, ''col62'', col62, ''col63'', col63, ''col64'', col64, ''col65'', col65, ''col66'', col66, ''col67'', col67, ''col68'', col68, ''col69'', col69, ''col70'', col70, ''col71'', col71, ''col72'', col72, ''col73'', col73, ''col74'', col74, ''col75'', col75, ''col76'', col76, ''col77'', col77, ''col78'', col78, ''col79'', col79, ''col80'', col80, ''col81'', col81, ''col82'', col82, ''col83'', col83, ''col84'', col84, ''col85'', col85, ''col86'', col86, ''col87'', col87, ''col88'', col88, ''col89'', col89, ''col90'', col90, ''col91'', col91, ''col92'', col92, ''col93'', col93, ''col94'', col94, ''col95'', col95, ''col96'', col96, ''col97'', col97, ''col98'', col98, ''col99'', col99, ''col100'', col100)', 'Error: Columns not concatenated correctly';
END;
$$
LANGUAGE plpgsql;
