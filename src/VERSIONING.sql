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
  tab_oid OID;
  tab_id INTEGER;
  tab_name TEXT;
  tab_schema TEXT;
  tab_upper_txid NUMERIC;
  restore_query TEXT;
  log_column RECORD;
  v_columns TEXT := '';
  v_columns_count NUMERIC := 0;
  delimiter VARCHAR(1) := '';
  new_tab_id INTEGER;
  new_tab_name TEXT;
  new_tab_schema TEXT;
  new_tab_upper_txid NUMERIC;
  new_column_name TEXT;
  for_each_valid_id TEXT := '';
  jsonb_result JSONB := '{}'::jsonb;
BEGIN
  -- check if the table exists in audit_table_log
  SELECT relid INTO tab_oid
    FROM pgmemento.audit_table_log
      WHERE schema_name = $4
        AND table_name = $3
        LIMIT 1;

  IF tab_oid IS NULL THEN
    RAISE NOTICE 'Table ''%'' is not audited.', $3;
    RETURN NULL;
  END IF;

  -- check if the table existed when tid happened
  -- save schema and name in case it was renamed
  SELECT id, schema_name, table_name, upper(txid_range)
    INTO tab_id, tab_schema, tab_name, tab_upper_txid 
    FROM pgmemento.audit_table_log 
      WHERE relid = tab_oid
        AND txid_range @> $1::numeric;

  IF NOT FOUND THEN
    RAISE NOTICE 'Table ''%'' did not exist for requested txid range.', $3;
    RETURN NULL;
  END IF;

  -- take into account that the table might not exist anymore or it has been renamed
  -- try to find out if there is an active table with the same oid
  IF tab_upper_txid IS NOT NULL THEN
    SELECT id, schema_name, table_name, upper(txid_range)
      INTO new_tab_id, new_tab_schema, new_tab_name, new_tab_upper_txid
      FROM pgmemento.audit_table_log 
      WHERE relid = tab_oid
        AND upper(txid_range) IS NULL;
  END IF;

  -- if not, set new_tab_* attributes, as we need them later
  IF new_tab_id IS NULL THEN
    new_tab_id := tab_id;
    new_tab_schema := tab_schema;
    new_tab_name := tab_name;
    new_tab_upper_txid := tab_upper_txid;
  END IF;

  -- start building the SQL command
  restore_query := 'SELECT jsonb_build_object(';

  -- get the content of each column that happened to be in the table when the transaction was executed
  FOR log_column IN 
    SELECT column_name, ordinal_position, data_type, data_type_name
      FROM pgmemento.audit_column_log 
        WHERE audit_table_id = tab_id
          AND txid_range @> $1::numeric
  LOOP
    new_column_name := NULL;
    v_columns_count := v_columns_count + 1;
    v_columns := v_columns || delimiter || 'q' || v_columns_count || '.key, ' || 'q' || v_columns_count || '.value';

    -- first: try to find the value within logged changes in row_log
    -- (first occurrence greater than txid will be the correct value)
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
              ), ',
       log_column.column_name, log_column.column_name, $1, $2, log_column.column_name);

    -- second: if not found, query the recent state for information if table exists
    -- additionally, check if column still exists
    SELECT column_name INTO new_column_name
      FROM pgmemento.audit_column_log
        WHERE audit_table_id = new_tab_id
          AND ordinal_position = log_column.ordinal_position
          AND data_type = log_column.data_type
          AND data_type_name = log_column.data_type_name
          AND upper(txid_range) IS NULL;

    IF new_tab_upper_txid IS NOT NULL OR new_column_name IS NULL THEN
      -- there is either no existing table or column, so value will be NULL
      IF tab_name <> new_tab_name THEN
        RAISE NOTICE 'No matching field found for column ''%'' in active table ''%'' (formerly known as ''%'').',
                        log_column.column_name, new_tab_name, tab_name;
      ELSE
        RAISE NOTICE 'No matching field found for column ''%'' in active table ''%''.',
                        log_column.column_name, new_tab_name;
      END IF;
      for_each_valid_id := for_each_valid_id || 'NULL';
    ELSE
      -- take current value from matching column (and hope that the data is really fitting)
      for_each_valid_id := for_each_valid_id || format(
        '(SELECT COALESCE(to_json(%I), NULL)::jsonb FROM %I.%I WHERE audit_id = %L)',
      new_column_name, new_tab_schema, new_tab_name, $2);
    END IF;

    -- complete this part of the query
    for_each_valid_id := for_each_valid_id || ') AS value) q) q' || v_columns_count;
    delimiter := ',';
  END LOOP;

  -- complete the SQL command
  restore_query := restore_query || v_columns || ') AS log_entry FROM ' || for_each_valid_id;

  -- execute the SQL command
  EXECUTE restore_query INTO jsonb_result;

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
  tab_oid OID;
  tab_id INTEGER;
  tab_name TEXT;
  tab_schema TEXT;
  tab_upper_txid NUMERIC;
  template_name TEXT;
  ddl_command TEXT := 'CREATE TEMPORARY TABLE ';
  log_column RECORD;
  delimiter VARCHAR(1) := '';
  restore_query TEXT;
  v_columns TEXT := '';
  v_columns_count NUMERIC := 0;
  new_tab_id INTEGER;
  new_tab_name TEXT;
  new_tab_schema TEXT;
  new_tab_upper_txid NUMERIC;
  new_column_name TEXT;
  for_each_valid_id TEXT := '';
BEGIN
  -- test if target schema already exist
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.schemata 
      WHERE schema_name = $5
  ) THEN
    EXECUTE format('CREATE SCHEMA %I', $5);
  END IF;

  -- test if table or view already exist in target schema
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
      WHERE table_name = $3
        AND table_schema = $5
        AND (table_type = 'BASE TABLE' OR table_type = 'VIEW')
  ) THEN
    IF $7 = 1 THEN
      IF $6 = 'TABLE' THEN
        RAISE EXCEPTION 'Only VIEWs are updatable.'
          USING HINT = 'Create another target schema when using TABLE as target table type.'; 
      ELSE
        replace_view := 'OR REPLACE ';
      END IF;
    ELSE
      RAISE EXCEPTION 'Entity ''%'' in schema ''%'' does already exist. Either delete the table or choose another name or target schema.',
                         $3, $5;
    END IF;
  END IF;

  -- check if the table exists in audit_table_log
  SELECT relid INTO tab_oid
    FROM pgmemento.audit_table_log
      WHERE schema_name = $4
        AND table_name = $3
        LIMIT 1;

  IF tab_oid IS NULL THEN
    RAISE NOTICE 'Table ''%'' is not audited.', $3;
    RETURN;
  END IF;

  -- check if the table existed when tid happened
  -- save schema and name in case it was renamed
  SELECT id, schema_name, table_name, upper(txid_range)
    INTO tab_id, tab_schema, tab_name, tab_upper_txid 
    FROM pgmemento.audit_table_log 
      WHERE relid = tab_oid
        AND txid_range @> $2::numeric;

  IF NOT FOUND THEN
    RAISE NOTICE 'Table ''%'' did not exist for requested txid range.', $3;
    RETURN;
  END IF;

  -- take into account that the table might not exist anymore or it has been renamed
  -- try to find out if there is an active table with the same oid
  IF tab_upper_txid IS NOT NULL THEN
    SELECT id, schema_name, table_name, upper(txid_range)
      INTO new_tab_id, new_tab_schema, new_tab_name, new_tab_upper_txid
      FROM pgmemento.audit_table_log 
      WHERE relid = tab_oid
        AND upper(txid_range) IS NULL;
  END IF;

  -- if not, set new_tab_* attributes, as we need them later
  IF new_tab_id IS NULL THEN
    new_tab_id := tab_id;
    new_tab_schema := tab_schema;
    new_tab_name := tab_name;
    new_tab_upper_txid := tab_upper_txid;
  END IF;

  -- create a temporary table used as template for jsonb_populate_record
  template_name := new_tab_name || '_tmp' || trunc(random() * 99999 + 1);
  ddl_command := ddl_command || template_name || '(';

  -- get columns that exist at transaction with id end_at_tid
  FOR log_column IN
    SELECT column_name, column_default, is_nullable, data_type_name, char_max_length, 
           numeric_precision, numeric_scale, datetime_precision, interval_type
      FROM pgmemento.audit_column_log
        WHERE audit_table_id = tab_id
          AND txid_range @> $2::numeric
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
      CASE WHEN $6 = 'TABLE' THEN 'ON COMMIT DROP' ELSE 'ON COMMIT PRESERVE ROWS' END;
    EXECUTE ddl_command;
  ELSE
    RETURN;
  END IF;

  -- reset delimiter for later loop
  delimiter := '';

  -- check if logging entries exist in the audit_log table
  IF EXISTS (
    SELECT 1 FROM pgmemento.table_event_log 
      WHERE table_relid = tab_oid
  ) THEN
    -- let's go back in time - restore a table state at a given date
    -- first: fetch audit_ids valid at given txid window
    IF upper($6) = 'VIEW' OR upper($6) = 'TABLE' THEN
      restore_query := format('CREATE ' || replace_view || $6 || ' %I.%I AS
        WITH restore AS (
          WITH fetch_audit_ids AS (
            SELECT DISTINCT ON (r.audit_id) r.audit_id, e.op_id
              FROM pgmemento.row_log r
              JOIN pgmemento.table_event_log e ON r.event_id = e.id
              JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                WHERE t.txid >= %L AND t.txid < %L
                  AND e.table_relid = %L
                  ORDER BY r.audit_id, e.id DESC
          )',
          $5, tab_name,
          $1, $2, tab_oid
        );

      -- second: get the content of each column that happened to be in the table when the second transaction was executed
      -- recreate JSONB objects representing the past tuples
      restore_query := restore_query ||
        ' SELECT v.log_entry FROM fetch_audit_ids f
            JOIN LATERAL (
              SELECT jsonb_build_object(';

      -- third: loop over all columns and query the historic value for each column separately
      FOR log_column IN 
        SELECT column_name, ordinal_position, data_type, data_type_name
          FROM pgmemento.audit_column_log 
            WHERE audit_table_id = tab_id
              AND txid_range @> $2::numeric
      LOOP
        new_column_name := NULL;
        v_columns_count := v_columns_count + 1;
        v_columns := v_columns || delimiter || 'q' || v_columns_count || '.key, ' || 'q' || v_columns_count || '.value';

        -- try to find the value within logged changes in row_log
        -- (first occurrence greater than txid will be the correct value)
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
                 ), ',
          log_column.column_name, log_column.column_name, $2, log_column.column_name);

        -- if not found, query the recent state for information if table exists
        -- additionally, check if column still exists
        SELECT column_name INTO new_column_name
          FROM pgmemento.audit_column_log
            WHERE audit_table_id = new_tab_id
              AND ordinal_position = log_column.ordinal_position
              AND data_type = log_column.data_type
              AND data_type_name = log_column.data_type_name
              AND upper(txid_range) IS NULL;

        IF new_tab_upper_txid IS NOT NULL OR new_column_name IS NULL THEN
          -- there is either no existing table or column, so value will be NULL
          IF tab_name <> new_tab_name THEN
            RAISE NOTICE 'No matching field found for column ''%'' in active table ''%'' (formerly known as ''%'').',
                            log_column.column_name, new_tab_name, tab_name;
          ELSE
            RAISE NOTICE 'No matching field found for column ''%'' in active table ''%''.',
                            log_column.column_name, new_tab_name;
          END IF;
          for_each_valid_id := for_each_valid_id || 'NULL';
        ELSE
          -- take current value from matching column (and hope that the data is really fitting)
          for_each_valid_id := for_each_valid_id || format(
            '(SELECT COALESCE(to_json(%I), NULL)::jsonb FROM %I.%I WHERE audit_id = f.audit_id)',
          new_column_name, new_tab_schema, new_tab_name);
        END IF;

        -- complete this part of the query
        for_each_valid_id := for_each_valid_id || ') AS value) q) q' || v_columns_count;
        delimiter := ',';
      END LOOP;

      -- fourth: closing the jsonb_build_object function call, opened in second step
      restore_query := restore_query || v_columns || 
        ')::jsonb AS log_entry'
        -- fifth: closing the 'restore' WITH block
        || ' FROM ' || for_each_valid_id || ') v ON (true) '
        || ' WHERE f.op_id < 3 ORDER BY f.audit_id)'
        -- sixth: coming to the final SELECT which converts the JSONB objects back to relational record sets
        || format(
          'SELECT p.* FROM restore rq
             JOIN LATERAL (
               SELECT * FROM jsonb_populate_record(null::%I, rq.log_entry)
             ) p ON (true)',
             template_name
          );

      EXECUTE restore_query;
    ELSE
      RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', $6;
    END IF;
  ELSE
    -- no entries found in log table - table is regarded empty
    IF tab_name <> new_tab_name THEN
      RAISE NOTICE 'Did not found entries in log table for table ''%'' (formerly known as ''%'').', new_tab_name, tab_name;
    ELSE
      RAISE NOTICE 'Did not found entries in log table for table ''%''.', new_tab_name;
    END IF;
    IF upper($6) = 'TABLE' THEN
      EXECUTE format('CREATE TABLE %I.%I (LIKE %I)', $5, tab_name, template_name);
    ELSIF upper($6) = 'VIEW' THEN
      EXECUTE format('CREATE ' || replace_view || 'VIEW %I.%I AS SELECT * FROM %I.%I LIMIT 0', $5, tab_name, $4, new_tab_name);        
    ELSE
      RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', $6;
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
  $1, $2, table_name, schema_name, $4, $5, $6
) FROM pgmemento.audit_table_log 
  WHERE schema_name = $3
    AND txid_range @> $2::numeric;
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
    $2, $1)
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
      (txid_current(), 1, 'INSERT', ($2 || '.' || $1)::regclass::oid)
    ON CONFLICT (transaction_id, table_relid, op_id)
      DO NOTHING
      RETURNING id INTO e_id;

    -- fill row_log table
    IF e_id IS NOT NULL THEN
      -- get the primary key columns
      SELECT array_to_string(array_agg(pga.attname),',') INTO pkey_columns
        FROM pg_index pgi, pg_class pgc, pg_attribute pga 
          WHERE pgc.oid = ($2 || '.' || $1)::regclass::oid
            AND pgi.indrelid = pgc.oid 
            AND pga.attrelid = pgc.oid 
            AND pga.attnum = ANY(pgi.indkey) AND pgi.indisprimary;

      IF pkey_columns IS NOT NULL THEN
        pkey_columns := ' ORDER BY ' || pkey_columns;
      END IF;

      EXECUTE format(
        'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
           SELECT $1, audit_id, NULL::jsonb AS changes FROM %I.%I' || pkey_columns,
           $2, $1) USING e_id;
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
SELECT pgmemento.log_table_state(a.table_name, a.schema_name)
  FROM pgmemento.audit_table_log a, pgmemento.audit_tables_dependency d
    WHERE a.schema_name = d.schemaname
      AND a.table_name = d.tablename
      AND a.schema_name = $1
      AND d.schemaname = $1
      AND upper(txid_range) IS NULL
      ORDER BY d.depth;
$$
LANGUAGE sql;