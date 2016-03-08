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
*   restore_schema_state(start_from_tid BIGINT, end_at_tid BIGINT, original_schema_name TEXT, target_schema_name TEXT, 
*     target_table_type TEXT DEFAULT 'VIEW', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   restore_table_state(start_from_tid BIGINT, end_at_tid BIGINT, original_table_name TEXT, original_schema_name TEXT, 
*     target_schema_name TEXT, target_table_type TEXT DEFAULT 'VIEW') RETURNS SETOF VOID
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.generate_log_entry(
  tid BIGINT,
  aid BIGINT,
  original_table_name TEXT,
  original_schema_name TEXT DEFAULT 'citydb'
  ) RETURNS jsonb AS
$$
DECLARE
  logged INTEGER := 0;
  template_schema TEXT;
  template_table TEXT;
  restore_query TEXT;
  column_name TEXT;
  v_columns TEXT := '';
  v_columns_count NUMERIC := 0;
  for_each_valid_id TEXT := '';
  delimiter VARCHAR(1) := '';
  json_result JSONB := '{}'::jsonb;
BEGIN
  -- check if logging entries exist in the audit_log table
  SELECT 1 INTO logged FROM pgmemento.table_event_log 
	WHERE schema_name = original_schema_name AND table_name = original_table_name LIMIT 1;

  IF logged IS NOT NULL THEN
    -- if the table structure has changed over time we need to use a template table
    -- that we hopefully created with 'pgmemento.create_table_template' before altering the table
    SELECT name INTO template_table FROM pgmemento.table_templates
      WHERE original_schema = original_schema_name AND original_table = original_table_name
        AND creation_date >= (SELECT stmt_date FROM pgmemento.transaction_log WHERE txid = tid)
        ORDER BY creation_date DESC LIMIT 1;

    IF template_table IS NULL THEN
      template_schema := original_schema_name;
      template_table := original_table_name;
    ELSE
      template_schema := 'pgmemento';
    END IF;
	
    -- start building the SQL command
    restore_query := 'SELECT json_build_object(';

    -- get the content of each column that happened to be in the table when the transaction was executed
    FOR column_name IN 
      SELECT attname FROM pg_attribute 
        WHERE attrelid = (template_schema || '.' || template_table)::regclass 
          AND attstattarget != 0 ORDER BY attnum
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
          column_name, column_name, tid, aid, column_name,
          column_name, original_schema_name, original_table_name, aid) || v_columns_count;

      delimiter := ',';
    END LOOP;

    -- complete the SQL command
    restore_query := restore_query || v_columns || ')::jsonb AS log_entry FROM ' || for_each_valid_id;

    -- execute the SQL command
    EXECUTE restore_query INTO json_result;
  ELSE
    RAISE NOTICE 'Did not found entries in log table for table ''%''.', original_table_name;
  END IF;

  RETURN json_result;
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
  is_set_schema INTEGER := 0;
  is_set_table INTEGER := 0;
  logged INTEGER := 0;
  template_schema TEXT;
  template_table TEXT;
  replace_view TEXT := '';
  restore_query TEXT;
  column_name TEXT;
  v_columns TEXT := '';
  v_columns_count NUMERIC := 0;
  for_each_valid_id TEXT := '';
  delimiter VARCHAR(1) := '';
BEGIN
  -- test if target schema already exist
  SELECT 1 INTO is_set_schema 
    FROM information_schema.schemata 
      WHERE schema_name = target_schema_name;

  IF is_set_schema IS NULL THEN
    EXECUTE format('CREATE SCHEMA %I', target_schema_name);
  END IF;

  -- test if table or view already exist in target schema
  SELECT 1 INTO is_set_table FROM information_schema.tables 
    WHERE table_name = original_table_name 
      AND table_schema = target_schema_name 
      AND (table_type = 'BASE TABLE' OR table_type = 'VIEW');

  IF is_set_table IS NOT NULL THEN
    IF update_state = 1 THEN
      IF target_table_type = 'TABLE' THEN
        RAISE EXCEPTION 'Only VIEWs are updatable.' USING HINT = 'Create another target schema when using TABLE as target table type.'; 
      ELSE
        replace_view := 'OR REPLACE ';
      END IF;
    ELSE
      RAISE NOTICE 'Entity ''%'' in schema ''%'' does already exist. Either delete the table or choose another name or target schema.',
                      original_table_name, target_schema_name;
    END IF;
  ELSE
    -- check if logging entries exist in the audit_log table
    SELECT 1 INTO logged FROM pgmemento.table_event_log 
	  WHERE schema_name = original_schema_name AND table_name = original_table_name LIMIT 1;

    IF logged IS NOT NULL THEN
      -- if the table structure has changed over time we need to use a template table
      -- that we hopefully created with 'pgmemento.create_table_template' before altering the table
      SELECT name INTO template_table FROM pgmemento.table_templates
        WHERE original_schema = original_schema_name AND original_table = original_table_name
          AND creation_date >= (SELECT stmt_date FROM pgmemento.transaction_log WHERE txid = start_from_tid)
          AND creation_date <= (SELECT stmt_date FROM pgmemento.transaction_log WHERE txid = end_at_tid)
          ORDER BY creation_date DESC LIMIT 1;

      IF template_table IS NULL THEN
        template_schema := original_schema_name;
        template_table := original_table_name;
      ELSE
        template_schema := 'pgmemento';
      END IF;

      -- let's go back in time - restore a table state at a given date
      IF upper(target_table_type) = 'VIEW' OR upper(target_table_type) = 'TABLE' THEN
        restore_query := format('CREATE ' || replace_view || target_table_type || ' %I.%I AS
          WITH restore AS (
            WITH excluded_ids AS (
              SELECT DISTINCT r.audit_id
                FROM pgmemento.row_log r
                JOIN pgmemento.table_event_log e ON r.event_id = e.id
                JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                WHERE t.txid >= %L AND t.txid < %L
                  AND e.table_relid = %L::regclass::oid 
                  AND e.op_id > 2), 
            valid_ids AS (
              SELECT DISTINCT y.audit_id
                FROM pgmemento.row_log y
                JOIN pgmemento.table_event_log e ON y.event_id = e.id
                JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                LEFT OUTER JOIN excluded_ids n ON n.audit_id = y.audit_id
                WHERE t.txid >= %L AND t.txid < %L
                  AND e.table_relid = %L::regclass::oid 
                  AND (n.audit_id IS NULL OR y.audit_id != n.audit_id)
            )
            SELECT v.log_entry FROM valid_ids f
            JOIN LATERAL (
              SELECT json_build_object(',
              target_schema_name, original_table_name,
              start_from_tid, end_at_tid, original_schema_name || '.' || original_table_name,
              start_from_tid, end_at_tid, original_schema_name || '.' || original_table_name);

        -- get the content of each column that happened to be in the table when the transaction was executed
        FOR column_name IN 
          EXECUTE 'SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass and attstattarget != 0 ORDER BY attnum' 
                     USING template_schema || '.' || template_table LOOP

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
              column_name, column_name, end_at_tid, column_name,
              column_name, original_schema_name, original_table_name) || v_columns_count;

          delimiter := ',';
        END LOOP;

        restore_query := restore_query || v_columns || ')::jsonb AS log_entry FROM ' || for_each_valid_id || ') v ON (true) ORDER BY f.audit_id)' ||
          format('SELECT p.* FROM restore rq
                    JOIN LATERAL (
                      SELECT * FROM jsonb_populate_record(null::%I.%I, rq.log_entry)
                    ) p ON (true)',
                    template_schema, template_table);

        EXECUTE restore_query;
      ELSE
        RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', target_table_type;
      END IF;
    ELSE
      -- no entries found in log table - recent state of table will be transferred to the requested state
      RAISE NOTICE 'Did not found entries in log table for table ''%''.', original_table_name;
      IF upper(target_table_type) = 'VIEW' OR upper(target_table_type) = 'TABLE' THEN
        EXECUTE format('CREATE ' || target_table_type || ' %I.%I AS SELECT * FROM %I.%I', target_schema_name, original_table_name, original_schema_name, original_table_name);
      ELSE
        RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', target_table_type;
      END IF;
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
  except_tables TEXT[] DEFAULT '{}',
  update_state INTEGER DEFAULT '0'
  ) RETURNS SETOF VOID AS
$$
  SELECT pgmemento.restore_table_state(
    start_from_tid, end_at_tid, tablename, schemaname, target_schema_name, target_table_type, update_state
  ) FROM pg_tables 
    WHERE schemaname = original_schema_name AND tablename <> ALL (except_tables);
$$
LANGUAGE sql;