-- UPGRADE_v07_to_v072.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script upgrades a pgMemento extension of v0.7.0 to v0.7.2 which
-- replaces some functions (see changelog for more details)
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.2.0     2021-03-21   reflect fixes for v0.7.2                       FKun
-- 0.1.0     2020-07-30   initial commit                                 FKun
--

\echo
\echo 'Updgrade pgMemento from v0.7.0 to v0.7.2 ...'

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
SELECT 'pgMemento 0.7.2'::text AS full_version, 0 AS major_version, 7 AS minor_version, 2 AS revision, '87'::text AS build_id;
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


CREATE OR REPLACE FUNCTION pgmemento.jsonb_populate_value(
  jsonb_log JSONB,
  column_name TEXT,
  INOUT template anyelement
  ) RETURNS anyelement AS
$$
BEGIN
  IF $1 IS NOT NULL THEN
    IF right(pg_typeof($3)::text, 2) = '[]' THEN
      EXECUTE format('SELECT translate($1->>$2, ''[]'', ''{}'')::%s', pg_typeof($3))
        INTO template USING $1, $2;
    ELSE
      EXECUTE format('SELECT ($1->>$2)::%s', pg_typeof($3))
        INTO template USING $1, $2;
    END IF;
  ELSE
    EXECUTE format('SELECT NULL::%s', pg_typeof($3))
      INTO template;
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
  find_logs TEXT;
  query_text TEXT := E'SELECT\n';
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
       format(E'  CASE WHEN jsonb_typeof(g.log_%s) = ''null'' THEN NULL::%s\n', c_old.column_name, c_old.data_type)
    || CASE WHEN join_recent_state AND c_new.column_name IS NOT NULL
       THEN format(E'       WHEN g.log_%s IS NULL THEN x.%I\n', c_old.column_name, c_new.column_name) ELSE '' END
    || '       ELSE '
    || CASE WHEN right(c_old.data_type, 2) = '[]'
       THEN 'translate(' ELSE '' END
    || format('(jsonb_build_object(%L, g.log_%s) ->> %L)', c_old.column_name,
       quote_ident(c_old.column_name || CASE WHEN c_old.column_count > 1 THEN '_' || c_old.column_count ELSE '' END), c_old.column_name)
    || CASE WHEN right(c_old.data_type, 2) = '[]'
       THEN ',''[]'',''{}'')' ELSE '' END
    || format(E'::%s\n', c_old.data_type)
    || format('  END AS %s', quote_ident(c_old.column_name || CASE WHEN c_old.column_count > 1 THEN '_' || c_old.column_count ELSE '' END))
      , E',\n' ORDER BY c_old.ordinal_position, c_old.column_count
    ),
    string_agg(
      CASE WHEN $6
      THEN format(E'    CASE WHEN transaction_id >= %L AND transaction_id < %L\n    THEN ',
        CASE WHEN lower(c_old.txid_range) IS NOT NULL
        THEN lower(c_old.txid_range)
        ELSE $1 END,
        CASE WHEN upper(c_old.txid_range) IS NOT NULL
        THEN upper(c_old.txid_range)
        ELSE $2 END)
      ELSE '    ' END
    || format('first_value(a.old_data -> %L) OVER ', c_old.column_name, c_old.column_name)
    || format('(PARTITION BY f.event_key, a.audit_id ORDER BY a.old_data -> %L IS NULL, a.id)', c_old.column_name)
    || CASE WHEN $6
       THEN format(E'\n    ELSE jsonb_build_object(%L, NULL) -> %L END', c_old.column_name, c_old.column_name)
       ELSE '' END
    || format(' AS log_%s', quote_ident(c_old.column_name || CASE WHEN c_old.column_count > 1 THEN '_' || c_old.column_count ELSE '' END))
        , E',\n' ORDER BY c_old.ordinal_position, c_old.column_count
    )
  INTO
    extract_logs,
    find_logs
  FROM
    pgmemento.get_column_list($1, $2, log_id, tab_name, tab_schema, $6) c_old
  LEFT JOIN
    pgmemento.audit_column_log c_new
    ON c_old.ordinal_position = c_new.ordinal_position
   AND c_new.audit_table_id = new_tab_id
   AND upper(c_new.txid_range) IS NULL
   AND lower(c_new.txid_range) IS NOT NULL;

  -- finish restore query
  query_text := query_text
    -- add part to extract values from logs or get recent state
    || extract_logs
    || format(E',\n  g.audit_id AS %s', quote_ident(tab_audit_id_column))
    || CASE WHEN $6 THEN E',\n  g.stmt_time,\n  g.table_operation,\n  g.transaction_id\n' ELSE E'\n' END
    -- use DISTINCT ON to get only one row
    || E'FROM (\n  SELECT DISTINCT ON ('
    || CASE WHEN $6 THEN 'f.event_key, ' ELSE '' END
    || E'f.audit_id)\n'
    -- add subquery g that finds the right JSONB log snippets for each column
    || find_logs
    || format(E',\n    f.audit_id')
    || CASE WHEN $6 THEN E',\n    f.stmt_time,\n    f.table_operation,\n    f.transaction_id\n' ELSE E'\n' END
    -- add subquery f to get last event for given audit_id before given transaction
    || E'  FROM (\n'
    || '    SELECT '
    || CASE WHEN $6 THEN E'\n' ELSE E'DISTINCT ON (r.audit_id)\n' END
    || '      r.audit_id, e.event_key, e.op_id'
    || CASE WHEN $6 THEN E', e.stmt_time, e.table_operation, e.transaction_id\n' ELSE E'\n' END
    || E'    FROM\n'
    || E'      pgmemento.row_log r\n'
    || E'    JOIN\n'
    || E'      pgmemento.table_event_log e ON r.event_key = e.event_key\n'
    || format(E'    WHERE e.transaction_id >= %L AND e.transaction_id < %L\n', $1, $2)
    || CASE WHEN $5 IS NULL THEN
         format(E'      AND e.table_name = %L AND e.schema_name = %L\n', tab_name, tab_schema)
       ELSE
         format(E'      AND r.audit_id = %L\n', $5)
       END
    || E'    ORDER BY\n'
    || E'      r.audit_id, e.id DESC\n'
    || E'  ) f\n'
    -- left join on row_log table and consider only events younger than the one extracted in subquery f
    || E'  LEFT JOIN\n'
    || E'    pgmemento.row_log a ON a.audit_id = f.audit_id AND a.event_key > f.event_key\n'
    -- if 'all_versions' flag is FALSE do not produce a result if row did not exist before second transaction ID
    -- therefore, filter out DELETE, TRUNCATE or DROP TABLE events
    || CASE WHEN $6 THEN '' ELSE E'  WHERE\n    f.op_id < 7\n' END
    -- order by oldest log entry for given audit_id
    || E'  ORDER BY\n'
    || CASE WHEN $6 THEN '    f.event_key, ' ELSE '    ' END
    || 'f.audit_id'
    || E'\n) g\n'
    -- left join on actual table to get the recent value for a field if nothing is found in the logs
    || CASE WHEN join_recent_state THEN
         E'LEFT JOIN\n'
         || format(E'  %I.%I x ON x.' || new_audit_id_column || E' = g.audit_id\n', new_tab_schema, new_tab_name)
       ELSE
         ''
       END;

  RETURN query_text;
END;
$$
LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION pgmemento.jsonb_unroll_for_update(
  path TEXT,
  nested_value JSONB,
  complex_typname TEXT
  ) RETURNS TEXT AS
$$
SELECT
  string_agg(set_columns,', ')
FROM (
  SELECT
    CASE WHEN jsonb_typeof(j.value) = 'object' AND p.typname IS NOT NULL THEN
      pgmemento.jsonb_unroll_for_update($1 || '.' || quote_ident(j.key), j.value, p.typname)
    ELSE
      $1 || '.' || quote_ident(j.key) || '=' ||
      CASE WHEN jsonb_typeof(j.value) = 'array' THEN
        quote_nullable(translate($2 ->> j.key, '[]', '{}'))
      ELSE
        quote_nullable($2 ->> j.key)
      END
    END AS set_columns
  FROM
    jsonb_each($2) j
  LEFT JOIN
    pg_attribute a
    ON a.attname = j.key
   AND jsonb_typeof(j.value) = 'object'
  LEFT JOIN
    pg_class c
    ON c.oid = a.attrelid
  LEFT JOIN
    pg_type t
    ON t.typrelid = c.oid
   AND t.typname = $3
  LEFT JOIN
    pg_type p
    ON p.typname = format_type(a.atttypid, a.atttypmod)
   AND p.typcategory = 'C'
) u
$$
LANGUAGE sql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.recover_audit_version(
  tid INTEGER,
  aid BIGINT,
  changes JSONB,
  table_op INTEGER,
  tab_name TEXT,
  tab_schema TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
DECLARE
  except_tables TEXT[] DEFAULT '{}';
  stmt TEXT;
  table_log_id INTEGER;
  current_transaction INTEGER;
BEGIN
  CASE
  -- CREATE TABLE case
  WHEN $4 = 1 THEN
    -- try to drop table
    BEGIN
      EXECUTE format('DROP TABLE %I.%I', $6, $5);

      EXCEPTION
        WHEN undefined_table THEN
          RAISE NOTICE 'Could not revert CREATE TABLE event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- REINIT TABLE case
  WHEN $4 = 11 THEN
    BEGIN
      -- reinit only given table and exclude all others
      SELECT
        array_agg(table_name)
      INTO
        except_tables
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name <> $5
        AND schema_name = $6
        AND upper(txid_range) = $1;

      PERFORM
        pgmemento.reinit($6, audit_id_column, log_old_data, log_new_data, FALSE, except_tables)
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = $5
        AND schema_name = $6
        AND upper(txid_range) = $1;

      -- if auditing was stopped within the same transaction (e.g. reverted ADD AUDIT_ID event)
      -- the REINIT TABLE event will not be logged by reinit function
      -- therefore, we have to make the insert here
      IF NOT EXISTS (
        SELECT
          1
        FROM
          pgmemento.table_event_log
        WHERE
          transaction_id = current_setting('pgmemento.' || txid_current())::int
          AND table_name = $5
          AND schema_name = $6
          AND op_id = 11  -- REINIT TABLE event
      ) THEN
        PERFORM pgmemento.log_table_event($5, $6, 'REINIT TABLE');
      END IF;

      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert REINIT TABLE event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- RENAME TABLE case
  WHEN $4 = 12 THEN
    BEGIN
      -- collect information of renamed table
      SELECT
        format('%I.%I',
          t_old.schema_name,
          t_old.table_name
        )
      INTO
        stmt
      FROM
        pgmemento.audit_table_log t_old,
        pgmemento.audit_table_log t_new
      WHERE
        t_old.log_id = t_new.log_id
        AND t_new.table_name = $5
        AND t_new.schema_name = $6
        AND upper(t_new.txid_range) = $1
        AND lower(t_old.txid_range) = $1;

      -- try to re-rename table
      IF stmt IS NOT NULL THEN
        EXECUTE 'ALTER TABLE ' || stmt || format(' RENAME TO %I', $5);
      END IF;

      EXCEPTION
        WHEN undefined_table THEN
          RAISE NOTICE 'Could not revert RENAME TABLE event for table %: %', stmt, SQLERRM;
    END;

  -- ADD COLUMN case
  WHEN $4 = 2 THEN
    BEGIN
      -- collect added columns
      SELECT
        string_agg(
          'DROP COLUMN '
          || quote_ident(c.column_name),
          ', ' ORDER BY c.id DESC
        ) INTO stmt
      FROM
        pgmemento.audit_column_log c
      JOIN
        pgmemento.audit_table_log t
        ON c.audit_table_id = t.id
      WHERE
        lower(c.txid_range) = $1
        AND t.table_name = $5
        AND t.schema_name = $6;

      -- try to execute ALTER TABLE command
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert ADD COLUMN event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- ADD AUDIT_ID case
  WHEN $4 = 21 THEN
    PERFORM pgmemento.drop_table_audit($5, $6, $7, TRUE, FALSE);

  -- RENAME COLUMN case
  WHEN $4 = 22 THEN
    BEGIN
      -- collect information of renamed table
      SELECT
        'RENAME COLUMN ' || quote_ident(c_old.column_name) ||
        ' TO ' || quote_ident(c_new.column_name)
      INTO
        stmt
      FROM
        pgmemento.audit_table_log t,
        pgmemento.audit_column_log c_old,
        pgmemento.audit_column_log c_new
      WHERE
        c_old.audit_table_id = t.id
        AND c_new.audit_table_id = t.id
        AND t.table_name = $5
        AND t.schema_name = $6
        AND t.txid_range @> $1::numeric
        AND c_old.ordinal_position = c_new.ordinal_position
        AND upper(c_new.txid_range) = $1
        AND lower(c_old.txid_range) = $1;

      -- try to re-rename table
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt, $6, $5);
      END IF;

      EXCEPTION
        WHEN undefined_table THEN
          RAISE NOTICE 'Could not revert RENAME COLUMN event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- INSERT case
  WHEN $4 = 3 THEN
    -- aid can be null in case of conflicts during insert
    IF $2 IS NOT NULL THEN
      -- delete inserted row
      BEGIN
        EXECUTE format(
          'DELETE FROM %I.%I WHERE %I = $1',
          $6, $5, $7)
          USING $2;

        -- row is already deleted
        EXCEPTION
          WHEN no_data_found THEN
            NULL;
      END;
    END IF;

  -- UPDATE case
  WHEN $4 = 4 THEN
    -- update the row with values from changes
    IF $2 IS NOT NULL AND $3 <> '{}'::jsonb THEN
      BEGIN
        -- create SET part
        SELECT
          string_agg(set_columns,', ')
        INTO
          stmt
        FROM (
          SELECT
            CASE WHEN jsonb_typeof(j.value) = 'object' AND p.typname IS NOT NULL THEN
              pgmemento.jsonb_unroll_for_update(j.key, j.value, p.typname)
            ELSE
              quote_ident(j.key) || '=' ||
              CASE WHEN jsonb_typeof(j.value) = 'array' THEN
                quote_nullable(translate($3 ->> j.key, '[]', '{}'))
              ELSE
                quote_nullable($3 ->> j.key)
              END
            END AS set_columns
          FROM
            jsonb_each($3) j
          LEFT JOIN
            pgmemento.audit_column_log c
            ON c.column_name = j.key
           AND jsonb_typeof(j.value) = 'object'
           AND upper(c.txid_range) IS NULL
           AND lower(c.txid_range) IS NOT NULL
          LEFT JOIN
            pgmemento.audit_table_log t
            ON t.id = c.audit_table_id
           AND t.table_name = $5
           AND t.schema_name = $6
          LEFT JOIN
            pg_type p
            ON p.typname = c.data_type
           AND p.typcategory = 'C'
        ) u;

        -- try to execute UPDATE command
        EXECUTE format(
          'UPDATE %I.%I t SET ' || stmt || ' WHERE t.%I = $1',
          $6, $5, $7)
          USING $2;

        -- row is already deleted
        EXCEPTION
          WHEN others THEN
            RAISE NOTICE 'Could not revert UPDATE event for table %.%: %', $6, $5, SQLERRM;
      END;
    END IF;

  -- ALTER COLUMN case
  WHEN $4 = 5 THEN
    BEGIN
      -- collect information of altered columns
      SELECT
        string_agg(
          format('ALTER COLUMN %I SET DATA TYPE %s USING pgmemento.restore_change(%L, %I, %L, NULL::%s)',
            c_new.column_name, c_old.data_type, $1, $7, quote_ident(c_old.column_name), c_old.data_type),
          ', ' ORDER BY c_new.id
        ) INTO stmt
      FROM
        pgmemento.audit_table_log t,
        pgmemento.audit_column_log c_old,
        pgmemento.audit_column_log c_new
      WHERE
        c_old.audit_table_id = t.id
        AND c_new.audit_table_id = t.id
        AND t.table_name = $5
        AND t.schema_name = $6
        AND t.txid_range @> $1::numeric
        AND upper(c_old.txid_range) = $1
        AND lower(c_new.txid_range) = $1
        AND c_old.ordinal_position = c_new.ordinal_position
        AND c_old.data_type <> c_new.data_type;

      -- alter table if it has not been done, yet
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      -- it did not work for some reason
      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert ALTER COLUMN event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- DROP COLUMN case
  WHEN $4 = 6 THEN
    BEGIN
      -- collect information of dropped columns
      SELECT
        string_agg(
          'ADD COLUMN '
          || quote_ident(c_old.column_name)
          || ' '
          || CASE WHEN c_old.column_default LIKE 'nextval(%'
                   AND pgmemento.trim_outer_quotes(c_old.column_default) LIKE E'%_seq\'::regclass)' THEN
               CASE WHEN c_old.data_type = 'smallint' THEN 'smallserial'
                    WHEN c_old.data_type = 'integer' THEN 'serial'
                    WHEN c_old.data_type = 'bigint' THEN 'bigserial'
                    ELSE c_old.data_type END
             ELSE
               c_old.data_type
               || CASE WHEN c_old.column_default IS NOT NULL
                  THEN ' DEFAULT ' || c_old.column_default ELSE '' END
             END
          || CASE WHEN c_old.not_null THEN ' NOT NULL' ELSE '' END,
          ', ' ORDER BY c_old.id
        ) INTO stmt
      FROM
        pgmemento.audit_table_log t
      JOIN
        pgmemento.audit_column_log c_old
        ON c_old.audit_table_id = t.id
      LEFT JOIN LATERAL (
        SELECT
          c.column_name
        FROM
          pgmemento.audit_table_log atl
        JOIN
          pgmemento.audit_column_log c
          ON c.audit_table_id = atl.id
        WHERE
          atl.table_name = t.table_name
          AND atl.schema_name = t.schema_name
          AND upper(c.txid_range) IS NULL
          AND lower(c.txid_range) IS NOT NULL
        ) c_new
        ON c_old.column_name = c_new.column_name
      WHERE
        upper(c_old.txid_range) = $1
        AND c_new.column_name IS NULL
        AND t.table_name = $5
        AND t.schema_name = $6;

      -- try to execute ALTER TABLE command
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      -- fill in data with an UPDATE statement if audit_id is set
      IF $2 IS NOT NULL THEN
        PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6, $7);
      END IF;

      EXCEPTION
        WHEN duplicate_column THEN
          -- if column already exists just do an UPDATE
          PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6, $7);
	END;

  -- DELETE or TRUNCATE case
  WHEN $4 = 7 OR $4 = 8 THEN
    IF $2 IS NOT NULL THEN
      BEGIN
        EXECUTE format(
          'INSERT INTO %I.%I SELECT * FROM jsonb_populate_record(null::%I.%I, $1)',
          $6, $5, $6, $5)
          USING $3;

        -- row has already been re-inserted, so update it based on the values of this deleted version
        EXCEPTION
          WHEN unique_violation THEN
            -- merge changes with recent version of table record and update row
            PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6, $7);
      END;
    END IF;

  -- DROP AUDIT_ID case
  WHEN $4 = 81 THEN
    -- first check if a preceding CREATE TABLE event already recreated the audit_id
    BEGIN
      current_transaction := current_setting('pgmemento.' || txid_current())::int;

      EXCEPTION
        WHEN undefined_object THEN
          NULL;
    END;

    BEGIN
      IF current_transaction IS NULL OR NOT EXISTS (
        SELECT
          1
        FROM
          pgmemento.table_event_log
        WHERE
          transaction_id = current_transaction
          AND table_name = $5
          AND schema_name = $6
          AND op_id = 1  -- RE/CREATE TABLE event
      ) THEN
        -- try to restart auditing for table
        PERFORM
          pgmemento.create_table_audit(table_name, schema_name, audit_id_column, log_old_data, log_new_data, FALSE)
        FROM
          pgmemento.audit_table_log
        WHERE
          table_name = $5
          AND schema_name = $6
          AND upper(txid_range) = $1;
      END IF;
      
      -- audit_id already exists
      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert DROP AUDIT_ID event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- DROP TABLE case
  WHEN $4 = 9 THEN
    -- collect information of columns of dropped table
    SELECT
      t.log_id,
      string_agg(
        quote_ident(c_old.column_name)
        || ' '
        || CASE WHEN c_old.column_default LIKE 'nextval(%'
                 AND pgmemento.trim_outer_quotes(c_old.column_default) LIKE E'%_seq\'::regclass)' THEN
             CASE WHEN c_old.data_type = 'smallint' THEN 'smallserial'
                  WHEN c_old.data_type = 'integer' THEN 'serial'
                  WHEN c_old.data_type = 'bigint' THEN 'bigserial'
                  ELSE c_old.data_type END
           ELSE
             c_old.data_type
             || CASE WHEN c_old.column_default IS NOT NULL
                THEN ' DEFAULT ' || c_old.column_default ELSE '' END
           END
        || CASE WHEN c_old.not_null THEN ' NOT NULL' ELSE '' END,
        ', ' ORDER BY c_old.ordinal_position
      )
    INTO
      table_log_id,
      stmt
    FROM
      pgmemento.audit_table_log t
    JOIN
      pgmemento.audit_column_log c_old
      ON c_old.audit_table_id = t.id
    LEFT JOIN LATERAL (
      SELECT
        atl.table_name
      FROM
        pgmemento.audit_table_log atl
      WHERE
        atl.table_name = t.table_name
        AND atl.schema_name = t.schema_name
        AND upper(atl.txid_range) IS NULL
        AND lower(atl.txid_range) IS NOT NULL
      ) t_new
      ON t.table_name = t_new.table_name
    WHERE
      upper(c_old.txid_range) = $1
      AND c_old.column_name <> $7
      AND t_new.table_name IS NULL
      AND t.table_name = $5
      AND t.schema_name = $6
    GROUP BY
      t.log_id;

    -- try to create table
    IF stmt IS NOT NULL THEN
      PERFORM pgmemento.log_table_event($5, $6, 'RECREATE TABLE');
      PERFORM set_config('pgmemento.' || $6 || '.' || $5, table_log_id::text, TRUE);
      EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (' || stmt || ')', $6, $5);
    END IF;

    -- fill in truncated data with an INSERT statement if audit_id is set
    IF $2 IS NOT NULL THEN
      PERFORM pgmemento.recover_audit_version($1, $2, $3, 8, $5, $6, $7);
    END IF;

  END CASE;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE VIEW pgmemento.audit_tables AS
  SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    atl.audit_id_column,
    atl.log_old_data,
    atl.log_new_data,
    bounds.txid_min,
    bounds.txid_max,
    CASE WHEN tg.tgenabled IS NOT NULL AND tg.tgenabled <> 'D' THEN
      TRUE
    ELSE
      FALSE
    END AS tg_is_active
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON c.relnamespace = n.oid
  JOIN
    pgmemento.audit_schema_log asl
    ON asl.schema_name = n.nspname
   AND upper(asl.txid_range) IS NULL
   AND lower(asl.txid_range) IS NOT NULL
  JOIN (
    SELECT DISTINCT ON (log_id)
      log_id,
      table_name,
      schema_name,
      audit_id_column,
      log_old_data,
      log_new_data
    FROM
      pgmemento.audit_table_log
    WHERE
      upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL
    ORDER BY
      log_id, id
    ) atl
    ON atl.table_name = c.relname
   AND atl.schema_name = n.nspname
  JOIN
    pg_attribute a
    ON a.attrelid = c.oid
   AND a.attname = atl.audit_id_column
  JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(atl.log_id)
    ) bounds ON (true)
  LEFT JOIN (
    SELECT
      tgrelid,
      tgenabled
    FROM
      pg_trigger
    WHERE
      tgname = 'pgmemento_transaction_trigger'::name
    ) AS tg
    ON c.oid = tg.tgrelid
  WHERE
    c.relkind = 'r'
  ORDER BY
    schemaname,
    tablename;


\echo
\echo 'pgMemento upgrade completed!'
