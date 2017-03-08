-- VERSIONING.sql
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
*   generate_log_entry(tid BIGINT, aid BIGINT, original_table_name TEXT, original_schema_name TEXT DEFAULT 'public') RETURNS jsonb
*   log_schema_state(schemaname TEXT DEFAULT 'public') RETURNS SETOF VOID
*   log_table_state(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   restore_schema_state(start_from_tid BIGINT, end_at_tid BIGINT, original_schema_name TEXT, target_schema_name TEXT, 
*     target_table_type TEXT DEFAULT 'VIEW', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   restore_table_state(start_from_tid BIGINT, end_at_tid BIGINT, original_table_name TEXT, original_schema_name TEXT, 
*     target_schema_name TEXT, target_table_type TEXT DEFAULT 'VIEW') RETURNS SETOF VOID
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.generate_log_entry(
  tid BIGINT,
  aid BIGINT,
  original_table_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS jsonb AS
$$
DECLARE
  table_oid OID;
  restore_query TEXT;
  v_column TEXT;
  v_columns TEXT := '';
  v_columns_count NUMERIC := 0;
  delimiter VARCHAR(1) := '';
  for_each_valid_id TEXT := '';
  jsonb_result JSONB := '{}'::jsonb;
BEGIN
  -- check if the table existed when tid happened
  SELECT relid INTO table_oid FROM pgmemento.audit_table_log 
    WHERE schema_name = original_schema_name AND table_name = original_table_name
      AND txid_range @> tid::numeric;

  IF NOT FOUND THEN
    RAISE NOTICE 'Did not found entries in log table for table ''%''.', original_table_name;
  ELSE
    -- start building the SQL command
    restore_query := 'SELECT jsonb_build_object(';

    -- get the content of each column that happened to be in the table when the transaction was executed
    FOR v_column IN 
      SELECT column_name FROM pgmemento.audit_column_log 
        WHERE table_relid = table_oid
          AND txid_range @> tid::numeric
    LOOP
      v_columns_count := v_columns_count + 1;
      v_columns := v_columns || delimiter || 'q' || v_columns_count || '.key, ' || 'q' || v_columns_count || '.value';

      -- first: try to find the value within logged changes in row_log (first occurrence greater than txid will be the correct value)
      -- second: if not found, query the recent state for information
      for_each_valid_id := for_each_valid_id || delimiter || format(
        '(SELECT * FROM (
            SELECT %L AS key, COALESCE(
              (SELECT (r.changes -> %L) 
                 FROM pgmemento.row_log r
                   JOIN pgmemento.table_event_log e ON r.event_id = e.id
                   JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                   WHERE t.txid >= %L
                     AND r.audit_id = %L
                     AND (r.changes ? %L)
                     ORDER BY r.id LIMIT 1
              ),
              (SELECT COALESCE(to_json(%I), NULL)::jsonb 
                 FROM %I.%I
                 WHERE audit_id = %L
              )
            ) AS value) q
          ) q',
          v_column, v_column, tid, aid, v_column,
          v_column, original_schema_name, original_table_name, aid) || v_columns_count;

      delimiter := ',';
    END LOOP;

    -- complete the SQL command
    restore_query := restore_query || v_columns || ') AS log_entry FROM ' || for_each_valid_id;

    -- execute the SQL command
    EXECUTE restore_query INTO jsonb_result;
  END IF;

  RETURN jsonb_result;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* RESTORE TABLE STATE
*
* See what the table looked like at a given date.
* The table state will be restored in a separate schema.
* The user can choose if it will appear as a TABLE or VIEW.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.restore_table_state(
  start_from_tid BIGINT,
  end_at_tid BIGINT,
  original_table_name TEXT,
  original_schema_name TEXT,
  target_schema_name TEXT,
  target_table_type TEXT DEFAULT 'VIEW',
  update_state INTEGER DEFAULT '0'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  replace_view TEXT := '';
  table_oid OID;
  template_name TEXT;
  ddl_command TEXT := 'CREATE TEMPORARY TABLE ';
  log_column RECORD;
  delimiter VARCHAR(1) := '';
  restore_query TEXT;
  v_column TEXT;
  v_columns TEXT := '';
  v_columns_count NUMERIC := 0;
  for_each_valid_id TEXT := '';
BEGIN
  -- test if target schema already exist
  PERFORM 1 FROM information_schema.schemata 
      WHERE schema_name = target_schema_name;

  IF NOT FOUND THEN
    EXECUTE format('CREATE SCHEMA %I', target_schema_name);
  END IF;

  -- test if table or view already exist in target schema
  PERFORM 1 FROM information_schema.tables
    WHERE table_name = original_table_name
      AND table_schema = target_schema_name
      AND (table_type = 'BASE TABLE' OR table_type = 'VIEW');

  IF FOUND THEN
    IF update_state = 1 THEN
      IF target_table_type = 'TABLE' THEN
        RAISE EXCEPTION 'Only VIEWs are updatable.' USING HINT = 'Create another target schema when using TABLE as target table type.'; 
      ELSE
        replace_view := 'OR REPLACE ';
      END IF;
    ELSE
      RAISE EXCEPTION 'Entity ''%'' in schema ''%'' does already exist. Either delete the table or choose another name or target schema.',
                         original_table_name, target_schema_name;
    END IF;
  END IF;

  -- check if the table existed when end_at_tid happened
  SELECT relid INTO table_oid FROM pgmemento.audit_table_log 
    WHERE schema_name = original_schema_name AND table_name = original_table_name
      AND txid_range @> end_at_tid::numeric;

  IF FOUND THEN
    -- disable schema_create_trigger when creating temporary tables
    PERFORM 1 FROM pg_event_trigger
      WHERE evtname = 'schema_create_trigger';

    IF FOUND THEN
      ALTER EVENT TRIGGER schema_create_trigger DISABLE;
    END IF;

    -- create a temporary table used as template for jsonb_populate_record
    template_name := original_table_name || '_tmp' || trunc(random() * 99999 + 1);
    ddl_command := ddl_command || template_name || '(';

    -- get columns that exist at transaction with id end_at_tid
    FOR log_column IN SELECT column_name, column_default, is_nullable, data_type_name, char_max_length, 
                             numeric_precision, numeric_scale, datetime_precision, interval_type
                        FROM pgmemento.audit_column_log
                          WHERE table_relid = table_oid
                            AND txid_range @> end_at_tid::numeric
                        ORDER BY ordinal_position
    LOOP
      ddl_command := ddl_command || delimiter || log_column.column_name || ' ' || log_column.data_type_name ||
      CASE WHEN log_column.char_max_length IS NOT NULL
        THEN '('||log_column.char_max_length||')' ELSE '' END ||
      CASE WHEN log_column.numeric_precision IS NOT NULL 
        AND log_column.data_type_name = 'numeric' 
        THEN '('||log_column.numeric_precision||','||log_column.numeric_scale||')' ELSE '' END ||
      CASE WHEN log_column.datetime_precision IS NOT NULL 
        AND NOT log_column.data_type_name = 'date' 
        AND log_column.interval_type IS NULL 
        THEN '('||log_column.datetime_precision||')' ELSE '' END ||
      CASE WHEN log_column.interval_type IS NOT NULL 
        THEN ' ' || log_column.interval_type ELSE '' END ||
      CASE WHEN log_column.is_nullable IS NOT NULL 
        THEN ' NOT NULL' ELSE '' END ||
      CASE WHEN log_column.column_default IS NOT NULL 
        THEN ' DEFAULT ' || log_column.column_default ELSE '' END;
      delimiter := ',';
    END LOOP;

    -- create temp table
    IF delimiter = ',' THEN
      ddl_command := ddl_command || ') ' ||
      CASE WHEN target_table_type = 'TABLE' THEN 'ON COMMIT DROP' ELSE 'ON COMMIT PRESERVE ROWS' END;
      EXECUTE ddl_command;
    ELSE
      RETURN;
    END IF;

    delimiter := '';

    -- check if logging entries exist in the audit_log table
    PERFORM 1 FROM pgmemento.table_event_log 
	  WHERE table_relid = table_oid;

    IF FOUND THEN
      -- let's go back in time - restore a table state at a given date
      IF upper(target_table_type) = 'VIEW' OR upper(target_table_type) = 'TABLE' THEN
        restore_query := format('CREATE ' || replace_view || target_table_type || ' %I.%I AS
          WITH restore AS (
            WITH fetch_audit_ids AS (
              SELECT DISTINCT ON (r.audit_id) r.audit_id, e.op_id
                FROM pgmemento.row_log r
                JOIN pgmemento.table_event_log e ON r.event_id = e.id
                JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                WHERE t.txid >= %L AND t.txid < %L
                  AND e.table_relid = %L
                  ORDER BY r.audit_id, e.id DESC
            )
            SELECT v.log_entry FROM fetch_audit_ids f
            JOIN LATERAL (
              SELECT jsonb_build_object(',
              target_schema_name, original_table_name,
              start_from_tid, end_at_tid, table_oid,
              start_from_tid, end_at_tid, table_oid);

        -- get the content of each column that happened to be in the table when the transaction was executed
        FOR v_column IN 
          SELECT column_name FROM pgmemento.audit_column_log 
            WHERE table_relid = table_oid
              AND txid_range @> end_at_tid::numeric 
        LOOP
          v_columns_count := v_columns_count + 1;
          v_columns := v_columns || delimiter || 'q' || v_columns_count || '.key, ' || 'q' || v_columns_count || '.value';

          -- first: try to find the value within logged changes in row_log (first occurrence greater than txid will be the correct value)
          -- second: if not found, query the recent state for information
          for_each_valid_id := for_each_valid_id || delimiter || format(
            '(SELECT * FROM (
                SELECT %L AS key, COALESCE(
                  (SELECT (r.changes -> %L) 
                     FROM pgmemento.row_log r
                     JOIN pgmemento.table_event_log e ON r.event_id = e.id
                     JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                     WHERE t.txid >= %L
                       AND r.audit_id = f.audit_id
                       AND (r.changes ? %L)
                       ORDER BY r.id LIMIT 1
                  ),
                  (SELECT COALESCE(to_json(%I), NULL)::jsonb 
                     FROM %I.%I
                     WHERE audit_id = f.audit_id
                  )
                ) AS value) q
              ) q',
              v_column, v_column, end_at_tid, v_column,
              v_column, original_schema_name, original_table_name) || v_columns_count;

          delimiter := ',';
        END LOOP;

        restore_query := restore_query || v_columns || ')::jsonb AS log_entry FROM ' || for_each_valid_id || ') v ON (true) '
                         || ' WHERE f.op_id < 3 ORDER BY f.audit_id)' ||
          format('SELECT p.* FROM restore rq
                    JOIN LATERAL (
                      SELECT * FROM jsonb_populate_record(null::%I, rq.log_entry)
                    ) p ON (true)',
                    template_name);

        EXECUTE restore_query;
      ELSE
        RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', target_table_type;
      END IF;
    ELSE
      -- no entries found in log table - table is regarded empty
      RAISE NOTICE 'Did not found entries in log table for table ''%''.', original_table_name;
      IF upper(target_table_type) = 'TABLE' THEN
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I)', target_schema_name, original_table_name, template_name);
      ELSIF upper(target_table_type) = 'VIEW' THEN
        EXECUTE format('CREATE ' || replace_view || 'VIEW %I.%I AS SELECT * FROM %I.%I LIMIT 0', target_schema_name, original_table_name, original_schema_name, original_table_name);        
      ELSE
        RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', target_table_type;
      END IF;
    END IF;

    -- enable schema_create_trigger
    PERFORM 1 FROM pg_event_trigger
      WHERE evtname = 'schema_create_trigger';

    IF FOUND THEN
      ALTER EVENT TRIGGER schema_create_trigger ENABLE;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- perform restore_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.restore_schema_state(
  start_from_tid BIGINT,
  end_at_tid BIGINT,
  original_schema_name TEXT,
  target_schema_name TEXT, 
  target_table_type TEXT DEFAULT 'VIEW',
  update_state INTEGER DEFAULT '0'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.restore_table_state(
    start_from_tid, end_at_tid, table_name, schema_name, target_schema_name, target_table_type, update_state
  ) FROM pgmemento.audit_table_log 
    WHERE schema_name = original_schema_name
      AND txid_range @> end_at_tid::numeric;
$$
LANGUAGE sql;


/**********************************************************
* LOG TABLE STATE
*
* Log table content in the audit_log table (as inserted values)
* to have a baseline for table versioning.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_table_state(
  original_table_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  is_empty INTEGER := 0;
  e_id INTEGER;
  pkey_columns TEXT := '';
BEGIN
  -- first, check if table is not empty
  EXECUTE format(
    'SELECT 1 FROM %I.%I LIMIT 1',
    original_schema_name, original_table_name)
    INTO is_empty;

  IF is_empty <> 0 THEN
    -- fill transaction_log table 
    INSERT INTO pgmemento.transaction_log
      (txid, stmt_date, user_name, client_name)
    VALUES 
      (txid_current(), statement_timestamp(), current_user, inet_client_addr())
    ON CONFLICT (txid)
      DO NOTHING;

    -- fill table_event_log table  
    INSERT INTO pgmemento.table_event_log
      (transaction_id, op_id, table_operation, table_relid) 
    VALUES
      (txid_current(), 1, 'INSERT', (original_schema_name || '.' || original_table_name)::regclass::oid)
    ON CONFLICT (transaction_id, table_relid, op_id)
      DO NOTHING
      RETURNING id INTO e_id;

    -- fill row_log table
    IF e_id IS NOT NULL THEN
      -- get the primary key columns
      SELECT array_to_string(array_agg(pga.attname),',') INTO pkey_columns
        FROM pg_index pgi, pg_class pgc, pg_attribute pga 
          WHERE pgc.oid = (original_schema_name || '.' || original_table_name)::regclass::oid
            AND pgi.indrelid = pgc.oid 
            AND pga.attrelid = pgc.oid 
            AND pga.attnum = ANY(pgi.indkey) AND pgi.indisprimary;

      IF pkey_columns IS NOT NULL THEN
        pkey_columns := ' ORDER BY ' || pkey_columns;
      END IF;

      EXECUTE format(
        'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
           SELECT $1, audit_id, NULL::jsonb AS changes FROM %I.%I' || pkey_columns,
           original_schema_name, original_table_name) USING e_id;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- perform log_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.log_schema_state(
  schemaname TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.log_table_state(table_name, schema_name) FROM pgmemento.audit_table_log 
    WHERE schema_name = schemaname AND upper(txid_range) IS NULL;
$$
LANGUAGE sql;