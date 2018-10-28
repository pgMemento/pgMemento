-- RESTORE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This skript is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides functions to enable versioning of PostgreSQL databases
-- by using logged content from the audit tables.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                       | Author
-- 0.6.5     2018-10-28   renamed file to RESTORE.sql                         FKun
-- 0.6.4     2018-10-25   renamed generate functions to restore_record/set    FKun
--                        which do not return JSONB anymore
--                        new template helper restore_record_definition
--                        use BOOLEAN type instead of INTEGER (0,1)
-- 0.6.3     2018-10-24   restoring tables now works without templates        FKun
--                        moved audit_table_check to LOG_UTIL
-- 0.6.2     2018-10-23   rewritten restore_query to return relational        FKun
--                        instead of JSONB
-- 0.6.1     2018-09-22   new functions to retrieve the value of a single     FKun
--                        columns from the logs
-- 0.6.0     2018-07-16   reflect changes in transaction_id handling          FKun
-- 0.5.1     2017-07-26   reflect changes of updated logging behaviour        FKun
-- 0.5.0     2017-07-12   reflect changes to audit_column_log table           FKun
-- 0.4.4     2017-04-07   split up restore code to different functions        FKun
-- 0.4.3     2017-04-05   greatly improved performance for restoring          FKun
--                        using window functions with a FILTER
-- 0.4.2     2017-03-28   better logic to query tables if nothing found       FKun
--                        in logs (considers also rename events)
-- 0.4.1     2017-03-15   reflecting new DDL log table schema                 FKun
-- 0.4.0     2017-03-05   updated JSONB functions                             FKun
-- 0.3.0     2016-04-14   a new template mechanism for restoring              FKun
-- 0.2.2     2016-03-08   minor change to generate_log_entry function         FKun
-- 0.2.1     2016-02-14   removed unnecessary plpgsql and dynamic sql code    FKun
-- 0.2.0     2015-05-26   more efficient queries                              FKun
-- 0.1.0     2014-11-26   initial commit                                      FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   create_restore_template(until_tid INTEGER, template_name TEXT, table_name TEXT, schema_name TEXT,
*     preserve_template BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   jsonb_populate_value(jsonb_log JSONB, column_name TEXT, INOUT template anyelement) RETURNS anyelement
*   restore_change(during_tid INTEGER, aid BIGINT, column_name TEXT, INOUT restored_value anyelement) RETURNS anyelement
*   restore_query(start_from_tid INTEGER, end_at_tid INTEGER, table_name TEXT, schema_name TEXT, aid BIGINT DEFAULT NULL,
*     all_versions BOOLEAN DEFAULT FALSE) RETURNS TEXT
*   restore_record(start_from_tid INTEGER, end_at_tid INTEGER, table_name TEXT, schema_name TEXT, aid BIGINT,
*     all_versions BOOLEAN DEFAULT FALSE, jsonb_output BOOLEAN DEFAULT FALSE) RETURNS RECORD
*   restore_record_definition(until_tid INTEGER,table_name TEXT, schema_name TEXT, include_events BOOLEAN DEFAULT FALSE) RETURNS TEXT
*   restore_recordset(start_from_tid INTEGER, end_at_tid INTEGER, table_name TEXT, schema_name TEXT,
*     all_versions BOOLEAN DEFAULT FALSE, jsonb_output BOOLEAN DEFAULT FALSE) RETURNS SETOF RECORD
*   restore_schema_state(start_from_tid INTEGER, end_at_tid INTEGER, original_schema_name TEXT, target_schema_name TEXT, 
*     target_table_type TEXT DEFAULT 'VIEW', update_state BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   restore_table_state(start_from_tid INTEGER, end_at_tid INTEGER, original_table_name TEXT, original_schema_name TEXT, 
*     target_schema_name TEXT, target_table_type TEXT DEFAULT 'VIEW', update_state BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   restore_value(until_tid INTEGER, aid BIGINT, column_name TEXT, INOUT restored_value anyelement) RETURNS anyelement
***********************************************************/


/**********************************************************
* RESTORE VALUE
*
* Returns the historic value before a given transaction_id
* and given audit_id with the correct data type.
* - jsonb_populate_value is used for casting
* - restore_value returns the historic column value <= tid
* - restore_change returns the historic column value in case
*   it was changed during given tid (NULL otherwise)
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.jsonb_populate_value(
  jsonb_log JSONB,
  column_name TEXT,
  INOUT template anyelement
  ) RETURNS anyelement AS
$$
BEGIN
  IF $1 IS NOT NULL THEN
    EXECUTE format('SELECT ($1->>$2)::%s', pg_typeof($3))
      INTO template USING $1, $2;
  ELSE
    EXECUTE format('SELECT NULL::%s', pg_typeof($3))
      INTO template;
  END IF;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pgmemento.restore_value(
  until_tid INTEGER,
  aid BIGINT,
  column_name TEXT,
  INOUT restored_value anyelement
  ) RETURNS anyelement AS
$$
SELECT
  pgmemento.jsonb_populate_value(r.changes, $3, $4) AS restored_value
FROM
  pgmemento.row_log r
JOIN
  pgmemento.table_event_log e
  ON r.event_id = e.id
WHERE
  r.audit_id = $2
  AND r.changes ? $3
  AND e.transaction_id <= $1
ORDER BY
  e.id DESC
LIMIT 1;
$$
LANGUAGE sql;


CREATE OR REPLACE FUNCTION pgmemento.restore_change(
  during_tid INTEGER,
  aid BIGINT,
  column_name TEXT,
  INOUT restored_value anyelement
  ) RETURNS anyelement AS
$$
SELECT
  pgmemento.jsonb_populate_value(r.changes, $3, $4) AS restored_value
FROM
  pgmemento.row_log r
JOIN
  pgmemento.table_event_log e
  ON r.event_id = e.id
WHERE
  r.audit_id = $2
  AND e.transaction_id = $1
ORDER BY
  e.id DESC
LIMIT 1;
$$
LANGUAGE sql;


/**********************************************************
* RESTORE QUERY
*
* Helper function to produce query string for restore
* single or multiple log entries (depends if aid is given)
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.restore_query(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_name TEXT,
  schema_name TEXT,
  aid BIGINT DEFAULT NULL,
  all_versions BOOLEAN DEFAULT FALSE
  ) RETURNS TEXT AS
$$
DECLARE
  tab_oid OID;
  tab_id INTEGER;
  tab_name TEXT;
  tab_schema TEXT;
  new_tab_id INTEGER;
  new_tab_name TEXT;
  new_tab_schema TEXT;
  query_text TEXT := '';
  find_logs TEXT := E'SELECT\n';
  join_recent_state BOOLEAN := FALSE;
BEGIN
  -- set variables
  SELECT
    log_tab_oid,
    log_tab_name,
    log_tab_schema,
    log_tab_id,
    recent_tab_name,
    recent_tab_schema,
    recent_tab_id
  INTO
    tab_oid,
    tab_name,
    tab_schema,
    tab_id,
    new_tab_name,
    new_tab_schema,
    new_tab_id
  FROM
    pgmemento.audit_table_check($2,$3,$4);

  IF new_tab_name IS NULL THEN
    RETURN NULL;
  END IF;

  -- loop over all columns and query the historic value for each column separately
  SELECT
    string_agg(
      format('  COALESCE((first_value(a.changes ->> %L) OVER ', c_old.column_name)
      || '(PARTITION BY '
      || CASE WHEN $6 THEN 'f.event_id, ' ELSE '' END
      || format('a.audit_id ORDER BY a.changes ->> %L IS NULL, a.id))::%s, ', c_old.column_name, c_old.data_type)
      || CASE WHEN c_new.column_name IS NOT NULL THEN format('x.%I', c_new.column_name) ELSE '' END
      || format(', NULL::%s) AS %s', c_old.data_type, c_old.column_name)
      , E',\n' ORDER BY c_old.ordinal_position)
  INTO
    find_logs
  FROM
    pgmemento.audit_column_log c_old
  LEFT JOIN
    pgmemento.audit_column_log c_new
    ON c_old.ordinal_position = c_new.ordinal_position
    AND c_old.data_type = c_new.data_type
    AND c_new.audit_table_id = new_tab_id
  WHERE
    c_old.audit_table_id = tab_id
    AND c_old.txid_range @> $2::numeric
    AND upper(c_new.txid_range) IS NULL;

  -- check if it is necessary to join with recent state of the table
  IF find_logs LIKE '%, x.%' THEN
    join_recent_state := TRUE;
  END IF;

  -- finish restore query
  query_text := query_text
    -- use DISTINCT ON to get only one row
    || 'SELECT DISTINCT ON ('
    || CASE WHEN $6 THEN 'f.event_id, ' ELSE '' END
    || 'a.audit_id'
    || CASE WHEN join_recent_state THEN ', x.audit_id' ELSE '' END
    || E')\n'
    -- add column selection that has been set up above 
    || find_logs
    || E',\n  f.audit_id'
    || CASE WHEN $6 THEN E',\n  f.event_id,\n  f.transaction_id\n' ELSE E'\n' END
    -- add subquery f to get last event for given audit_id before given transaction
    || E'FROM (\n'
    || '  SELECT '
    || CASE WHEN $6 THEN E'\n' ELSE E'DISTINCT ON (r.audit_id)\n' END
    || E'    r.audit_id, r.event_id, e.op_id, e.transaction_id\n'
    || E'  FROM\n'
    || E'    pgmemento.row_log r\n'
    || E'  JOIN\n'
    || E'    pgmemento.table_event_log e ON e.id = r.event_id\n'
    || format(E'  WHERE e.transaction_id >= %L AND e.transaction_id < %L\n', $1, $2)
    || CASE WHEN $5 IS NULL THEN
         format(E'    AND e.table_relid = %L\n', tab_oid)
       ELSE
         format(E'    AND r.audit_id = %L\n', $5)
       END
    || E'  ORDER BY\n'
    || E'    r.audit_id, e.id DESC\n'
    || E') f\n'
    -- left join on row_log table and consider only events younger than the one extracted in subquery f
    || E'LEFT JOIN\n'
    || E'  pgmemento.row_log a ON a.audit_id = f.audit_id AND a.event_id > f.event_id\n'
    -- left join on actual table to get the recent value for a field if nothing is found in the logs
    || CASE WHEN join_recent_state THEN
         E'LEFT JOIN\n'
         || format(E'  %I.%I x ON x.audit_id = f.audit_id\n', new_tab_schema, new_tab_name)
       ELSE
         ''
       END
    -- do not produce a result if row with audit_id did not exist before given transaction
    -- could be if filtered event has been either DELETE, TRUNCATE or DROP TABLE
    || E'WHERE\n'
    || E'  f.op_id < 7\n'
    -- order by oldest log entry for given audit_id
    || E'ORDER BY\n'
    || CASE WHEN $6 THEN '  f.event_id, ' ELSE '  ' END
    || 'a.audit_id'
    || CASE WHEN join_recent_state THEN ', x.audit_id' ELSE '' END;

  RETURN query_text;
END;
$$
LANGUAGE plpgsql STABLE;


/**********************************************************
* RESTORE RECORD/SET
*
* Functions to reproduce historic tuples for a given
* transaction range. To see all different versions of the
* tuples and not just the version at 'end_at_tid' set
* the all_versions flag to TRUE.
* Retrieving the correct result requires you to provide a
* column definition list. If you prefer to retrieve the
* logs as JSONB, set the last flag to TRUE. Then the column
* definition list requires just one JSONB column which is
* easier to write.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.restore_record(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_name TEXT,
  schema_name TEXT,
  aid BIGINT,
  all_versions BOOLEAN DEFAULT FALSE,
  jsonb_output BOOLEAN DEFAULT FALSE
  ) RETURNS RECORD AS
$$
DECLARE
  -- init query string
  restore_query_text TEXT := pgmemento.restore_query($1, $2, $3, $4, $5, $6);
  restore_result RECORD;
BEGIN
  IF $7 IS TRUE THEN
    restore_query_text := E'SELECT to_jsonb(t) FROM (\n' || restore_query_text || E'\n) t';
  END IF;

  -- execute the SQL command
  EXECUTE restore_query_text INTO restore_result;
  RETURN restore_result;
END;
$$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.restore_recordset(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_name TEXT,
  schema_name TEXT,
  all_versions BOOLEAN DEFAULT FALSE,
  jsonb_output BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF RECORD AS
$$
DECLARE
  -- init query string
  restore_query_text TEXT := pgmemento.restore_query($1, $2, $3, $4, NULL, $5);
BEGIN
  IF $6 IS TRUE THEN
    restore_query_text := E'SELECT to_jsonb(t) FROM (\n' || restore_query_text || E'\n) t';
  END IF;

  -- execute the SQL command
  RETURN QUERY EXECUTE restore_query_text;
END;
$$
LANGUAGE plpgsql STRICT;


/**********************************************************
* RESTORE RECORD DEFINITION
*
* Function that returns a column definition list for
* retrieving historic tuples with functions restor_record
* and restore_recordset. Simply attach the output to your
* restore query. When restoring mutliple versions of one
* row that set the flag include events to TRUE
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.restore_record_definition(
  until_tid INTEGER,
  table_name TEXT,
  schema_name TEXT,
  include_events BOOLEAN DEFAULT FALSE
  ) RETURNS TEXT AS
$$
SELECT
  'AS (' || list || ', audit_id bigint'
  || CASE WHEN $4 THEN ', event_id, transaction_id' ELSE '' END
  || ')' AS record_definition
FROM (
  SELECT
    string_agg(
      c.column_name || ' ' || c.data_type,
      ', ' ORDER BY c.ordinal_position
    ) AS list
  FROM
    pgmemento.audit_column_log c
  JOIN
    pgmemento.audit_table_log t
    ON t.id = c.audit_table_id
  WHERE
    t.table_name = $2
    AND t.schema_name = $3
    AND t.txid_range @> $1::numeric
    AND c.txid_range @> $1::numeric
) l;
$$
LANGUAGE sql;


/**********************************************************
* CREATE RESTORE TEMPLATE
*
* Function to create a temporary table to be used as a
* historically correct template for restoring data with
* jsonb_populate_record function 
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.create_restore_template(
  until_tid INTEGER,
  template_name TEXT,
  table_name TEXT,
  schema_name TEXT,
  preserve_template BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  stmt TEXT;
BEGIN
  -- get columns that exist before transaction with id end_at_tid
  SELECT
    string_agg(
      c.column_name
      || ' '
      || c.data_type
      || CASE WHEN c.column_default IS NOT NULL AND c.column_default NOT LIKE '%::regclass%'
         THEN ' DEFAULT ' || c.column_default ELSE '' END
      || CASE WHEN c.not_null THEN ' NOT NULL' ELSE '' END,
      ', ' ORDER BY c.ordinal_position
    ) INTO stmt
  FROM
    pgmemento.audit_column_log c
  JOIN
    pgmemento.audit_table_log t
    ON t.id = c.audit_table_id
  WHERE
    t.table_name = $3
    AND t.schema_name = $4
    AND t.txid_range @> $1::numeric
    AND c.txid_range @> $1::numeric;

  -- create temp table
  IF stmt IS NOT NULL THEN
    EXECUTE format(
      'CREATE TEMPORARY TABLE IF NOT EXISTS %I ('
         || stmt
         || ', audit_id bigint DEFAULT nextval(''pgmemento.audit_id_seq''::regclass) unique not null'
         || ') '
         || CASE WHEN $5 THEN 'ON COMMIT PRESERVE ROWS' ELSE 'ON COMMIT DROP' END,
       $2);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;


/**********************************************************
* RESTORE TABLE STATE
*
* See what the table looked like at a given date.
* The table state will be restored in a separate schema.
* The user can choose if it will appear as a TABLE or VIEW.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.restore_table_state(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  original_table_name TEXT,
  original_schema_name TEXT,
  target_schema_name TEXT,
  target_table_type TEXT DEFAULT 'VIEW',
  update_state BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  replace_view TEXT := '';
  restore_query TEXT;
BEGIN
  -- test if target schema already exist
  IF NOT EXISTS (
    SELECT
      1
    FROM
      pg_namespace
    WHERE
      nspname = $5
  ) THEN
    EXECUTE format('CREATE SCHEMA %I', $5);
  END IF;

  -- test if table or view already exist in target schema
  IF EXISTS (
    SELECT
      1
    FROM
      pg_class c,
      pg_namespace n
    WHERE
      c.relnamespace = n.oid
      AND c.relname = $3
      AND n.nspname = $5
      AND (
        c.relkind = 'r'
        OR c.relkind = 'v'
      )
  ) THEN
    IF $7 THEN
      IF $6 = 'TABLE' THEN
        -- drop the table state
        PERFORM pgmemento.drop_table_state($3, $5);
      ELSE
        replace_view := 'OR REPLACE ';
      END IF;
    ELSE
      RAISE EXCEPTION '% ''%'' in schema ''%'' does already exist. Either delete the % or choose another name or target schema.',
                         $6, $3, $5, $6;
    END IF;
  END IF;

  -- let's go back in time - restore a table state for given transaction interval
  IF upper($6) = 'VIEW' OR upper($6) = 'TABLE' THEN
    restore_query := 'CREATE ' 
      || replace_view || $6 
      || format(E' %I.%I AS\n', $5, $3)
      || pgmemento.restore_query($1, $2, $3, $4);

      -- finally execute query string
      EXECUTE restore_query;
  ELSE
    RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', $6;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

-- perform restore_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.restore_schema_state(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  original_schema_name TEXT,
  target_schema_name TEXT, 
  target_table_type TEXT DEFAULT 'VIEW',
  update_state BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
SELECT
  pgmemento.restore_table_state($1,$2,table_name,schema_name,$4,$5,$6)
FROM
  pgmemento.audit_table_log 
WHERE
  schema_name = $3
  AND txid_range @> $2::numeric;
$$
LANGUAGE sql STRICT;
