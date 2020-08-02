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

\echo
\echo 'Updgrade pgMemento from v0.7.0 to v0.7.1 ...'

DROP AGGREGATE IF EXISTS pgmemento.jsonb_merge(jsonb);

COMMENT ON COLUMN pgmemento.transaction_log.user_name IS 'Stores the result of session_user function';

\echo
\echo 'Recreate functions'
CREATE OR REPLACE FUNCTION pgmemento.version(
  OUT full_version TEXT,
  OUT major_version INTEGER,
  OUT minor_version INTEGER,
  OUT revision INTEGER,
  OUT build_id TEXT
  ) RETURNS RECORD AS
$$
SELECT 'pgMemento 0.7.1'::text AS full_version, 0 AS major_version, 7 AS minor_version, 1 AS revision, '72'::text AS build_id;
$$
LANGUAGE sql;


CREATE OR REPLACE FUNCTION pgmemento.create_table_audit(
  tablename TEXT,
  schemaname TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE,
  log_state BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  except_tables TEXT[] DEFAULT '{}';
BEGIN
  -- check if pgMemento is already initialized for schema
  IF NOT EXISTS (
    SELECT 1
      FROM pgmemento.audit_schema_log
     WHERE schema_name = $2
       AND upper(txid_range) IS NULL
  ) THEN
    SELECT
      array_agg(c.relname)
    INTO
      except_tables
    FROM
      pg_class c
    JOIN
      pg_namespace n
      ON c.relnamespace = n.oid
    WHERE
      n.nspname = $2
      AND c.relname <> $1
      AND c.relkind = 'r';

    PERFORM pgmemento.create_schema_audit($2, $3, $4, $5, $6, FALSE, except_tables);
    RETURN;
  END IF;

  -- remember audit_id_column when registering table in audit_table_log later
  PERFORM set_config('pgmemento.' || $2 || '.' || $1 || '.audit_id.' || txid_current(), $3, TRUE);

  -- remember logging behavior when registering table in audit_table_log later
  PERFORM set_config('pgmemento.' || $2 || '.' || $1 || '.log_data.' || txid_current(),
    CASE WHEN log_old_data THEN 'old=true,' ELSE 'old=false,' END ||
    CASE WHEN log_new_data THEN 'new=true' ELSE 'new=false' END, TRUE);

  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger($1, $2, $3, $4, $5);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id($1, $2, $3);

  -- log existing table content as inserted
  IF $6 THEN
    PERFORM pgmemento.log_table_baseline($1, $2, $3, $5);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.log_transaction(current_txid BIGINT) RETURNS INTEGER AS
$$
DECLARE
  session_info_text TEXT;
  session_info_obj JSONB;
  transaction_log_id INTEGER;
BEGIN
  -- retrieve session_info set by client
  BEGIN
    session_info_text := current_setting('pgmemento.session_info');

    IF session_info_text IS NULL OR session_info_text = '' THEN
      session_info_obj := NULL;
    ELSE
      session_info_obj := session_info_text::jsonb;
    END IF;

    EXCEPTION
      WHEN undefined_object THEN
        session_info_obj := NULL;
      WHEN invalid_text_representation THEN
        BEGIN
          session_info_obj := to_jsonb(current_setting('pgmemento.session_info'));
        END;
      WHEN others THEN
        RAISE NOTICE 'Unable to parse session info: %', session_info_text;
        session_info_obj := NULL;
  END;

  -- try to log corresponding transaction
  INSERT INTO pgmemento.transaction_log
    (txid, txid_time, process_id, user_name, client_name, client_port, application_name, session_info)
  VALUES
    ($1, transaction_timestamp(), pg_backend_pid(), session_user, inet_client_addr(), inet_client_port(),
     current_setting('application_name'), session_info_obj
    )
  ON CONFLICT (txid_time, txid)
    DO NOTHING
  RETURNING id
  INTO transaction_log_id;

  IF transaction_log_id IS NOT NULL THEN
    PERFORM set_config('pgmemento.' || $1, transaction_log_id::text, TRUE);
  ELSE
    transaction_log_id := current_setting('pgmemento.' || $1)::int;
  END IF;

  RETURN transaction_log_id;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.table_create_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj record;
  tablename TEXT;
  schemaname TEXT;
  current_default_column TEXT;
  current_log_old_data BOOLEAN;
  current_log_new_data BOOLEAN;
BEGIN
  FOR obj IN
    SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    IF obj.command_tag NOT IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO') OR obj.object_type != 'table' THEN
      CONTINUE;
    END IF;

    -- remove quotes if exists
    tablename := pgmemento.trim_outer_quotes(split_part(obj.object_identity, '.' ,2));
    schemaname := pgmemento.trim_outer_quotes(split_part(obj.object_identity, '.' ,1));

    -- check if auditing is active for schema
    SELECT
      default_audit_id_column,
      default_log_old_data,
      default_log_new_data
    INTO
      current_default_column,
      current_log_old_data,
      current_log_new_data
    FROM
      pgmemento.audit_schema_log
    WHERE
      schema_name = schemaname
      AND upper(txid_range) IS NULL;

    IF current_default_column IS NOT NULL THEN
      -- log as 'create table' event
      PERFORM pgmemento.log_table_event(
        tablename,
        schemaname,
        'CREATE TABLE'
      );

      -- start auditing for new table
      PERFORM pgmemento.create_table_audit(
        tablename,
        schemaname,
        current_default_column,
        current_log_old_data,
        current_log_new_data,
        FALSE
      );
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.get_column_list(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_log_id INTEGER,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  all_versions BOOLEAN DEFAULT FALSE,
  OUT column_name TEXT,
  OUT column_count INTEGER,
  OUT data_type TEXT,
  OUT ordinal_position INTEGER,
  OUT txid_range numrange
  ) RETURNS SETOF RECORD AS
$$
BEGIN
  IF $6 THEN
    RETURN QUERY
      SELECT t.column_name, t.column_count, t.data_type, t.ordinal_position, t.txid_range
        FROM pgmemento.get_column_list_by_txid_range($1, $2, $3) t;
  ELSE
    RETURN QUERY
      SELECT t.column_name, NULL::int, t.data_type, t.ordinal_position, NULL::numrange
        FROM pgmemento.get_column_list_by_txid($2, $4, $5) t;
  END IF;
END;
$$
LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION pgmemento.restore_query(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  aid BIGINT DEFAULT NULL,
  all_versions BOOLEAN DEFAULT FALSE
  ) RETURNS TEXT AS
$$
DECLARE
  log_id INTEGER;
  tab_name TEXT;
  tab_schema TEXT;
  tab_audit_id_column TEXT;
  tab_id INTEGER;
  new_tab_name TEXT;
  new_tab_schema TEXT;
  new_audit_id_column TEXT;
  new_tab_id INTEGER;
  join_recent_state BOOLEAN := FALSE;
  extract_logs TEXT;
  query_text TEXT;
BEGIN
  -- first check if table can be restored
  SELECT
    table_log_id,
    log_tab_name,
    log_tab_schema,
    log_audit_id_column,
    log_tab_id,
    recent_tab_name,
    recent_tab_schema,
    recent_audit_id_column,
    recent_tab_id
  INTO
    log_id,
    tab_name,
    tab_schema,
    tab_audit_id_column,
    tab_id,
    new_tab_name,
    new_tab_schema,
    new_audit_id_column,
    new_tab_id
  FROM
    pgmemento.audit_table_check($2, $3, $4);

  IF tab_id IS NULL THEN
    RAISE EXCEPTION 'Can not restore table ''%'' because it did not exist before requested transaction %', $3, $2;
  END IF;

  -- check if recent state can be queried
  IF new_tab_id IS NULL THEN
    new_tab_id := tab_id;
  ELSE
    join_recent_state := TRUE;
  END IF;

  -- loop over all columns and query the historic value for each column separately
  SELECT
    string_agg(
      CASE WHEN $6
      THEN format(E'  COALESCE(\n    CASE WHEN transaction_id >= %L AND transaction_id < %L\n    THEN ',
        CASE WHEN lower(c_old.txid_range) IS NOT NULL
        THEN lower(c_old.txid_range)
        ELSE $1 END,
        CASE WHEN upper(c_old.txid_range) IS NOT NULL
        THEN upper(c_old.txid_range)
        ELSE $2 END)
      ELSE '  ' END
    || CASE WHEN join_recent_state AND c_new.column_name IS NOT NULL
       THEN 'COALESCE('
       ELSE '' END
    || CASE WHEN right(c_old.data_type, 2) = '[]'
       THEN 'translate('
       ELSE '' END
    || format('first_value(a.old_data ->> %L) OVER ', c_old.column_name)
    || format('(PARTITION BY f.event_key, a.audit_id ORDER BY a.old_data -> %L IS NULL, a.id)', c_old.column_name)
    || CASE WHEN right(c_old.data_type, 2) = '[]'
       THEN ',''[]'',''{}'')'
       ELSE '' END
    || format('::%s', c_old.data_type)
    || CASE WHEN join_recent_state AND c_new.column_name IS NOT NULL
       THEN format(', x.%I)', c_new.column_name)
       ELSE '' END
    || CASE WHEN $6
       THEN format(E'\n    ELSE NULL END,\n    NULL::%s)', c_old.data_type)
       ELSE '' END
    || format(' AS %s',
         quote_ident(c_old.column_name || CASE WHEN c_old.column_count > 1
                                          THEN '_' || c_old.column_count
                                          ELSE '' END))
      , E',\n' ORDER BY c_old.ordinal_position, c_old.column_count)
  INTO
    extract_logs
  FROM
    pgmemento.get_column_list($1, $2, log_id, tab_name, tab_schema, $6) c_old
  LEFT JOIN
    pgmemento.audit_column_log c_new
    ON c_old.ordinal_position = c_new.ordinal_position
   AND c_old.data_type = c_new.data_type
   AND c_new.audit_table_id = new_tab_id
   AND upper(c_new.txid_range) IS NULL;

  -- finish restore query
  -- use DISTINCT ON to get only one row
  query_text := 'SELECT DISTINCT ON ('
    || CASE WHEN $6 THEN 'f.event_key, ' ELSE '' END
    || 'a.audit_id'
    || CASE WHEN join_recent_state THEN ', x.' || new_audit_id_column ELSE '' END
    || E')\n'
    -- add column selection that has been set up above
    || extract_logs
    || format(E',\n  f.audit_id AS %s', quote_ident(tab_audit_id_column))
    || CASE WHEN $6 THEN E',\n  f.stmt_time,\n  f.table_operation,\n  f.transaction_id\n' ELSE E'\n' END
    -- add subquery f to get last event for given audit_id before given transaction
    || E'FROM (\n'
    || '  SELECT '
    || CASE WHEN $6 THEN E'\n' ELSE E'DISTINCT ON (r.audit_id)\n' END
    || E'    r.audit_id, e.event_key, e.stmt_time, e.op_id, e.table_operation, e.transaction_id\n'
    || E'  FROM\n'
    || E'    pgmemento.row_log r\n'
    || E'  JOIN\n'
    || E'    pgmemento.table_event_log e ON r.event_key = e.event_key\n'
    || format(E'  WHERE e.transaction_id >= %L AND e.transaction_id < %L\n', $1, $2)
    || CASE WHEN $5 IS NULL THEN
         format(E'    AND e.table_name = %L AND e.schema_name = %L\n', tab_name, tab_schema)
       ELSE
         format(E'    AND r.audit_id = %L\n', $5)
       END
    || E'  ORDER BY\n'
    || E'    r.audit_id, e.id DESC\n'
    || E') f\n'
    -- left join on row_log table and consider only events younger than the one extracted in subquery f
    || E'LEFT JOIN\n'
    || E'  pgmemento.row_log a ON a.audit_id = f.audit_id AND (a.event_key > f.event_key)\n'
    -- left join on actual table to get the recent value for a field if nothing is found in the logs
    || CASE WHEN join_recent_state THEN
         E'LEFT JOIN\n'
         || format(E'  %I.%I x ON x.' || new_audit_id_column || E' = f.audit_id\n', new_tab_schema, new_tab_name)
       ELSE
         ''
       END
    -- if 'all_versions' flag is FALSE do not produce a result if row did not exist before second transaction ID
    -- therefore, filter out DELETE, TRUNCATE or DROP TABLE events
    || CASE WHEN $6 THEN '' ELSE E'WHERE\n  f.op_id < 7\n' END
    -- order by oldest log entry for given audit_id
    || E'ORDER BY\n'
    || CASE WHEN $6 THEN '  f.event_key, ' ELSE '  ' END
    || 'a.audit_id'
    || CASE WHEN join_recent_state THEN ', x.' || new_audit_id_column ELSE '' END;

  RETURN query_text;
END;
$$
LANGUAGE plpgsql STABLE;

\echo
\echo 'pgMemento upgrade completed!'
