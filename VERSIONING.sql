-- VERSIONING.sql
--
-- Author:      Felix Kunde <fkunde@virtualcitysystems.de>
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
-- Version | Date       | Description                       | Author
-- 0.2.0     2014-05-26   some intermediate version           FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   build_json(json_keys ANYARRAY, json_values ANYARRAY) RETURNS JSON AS
*   fetch_audit_ids(queried_date TIMESTAMP, search_date TIMESTAMP, original_table_name TEXT, original_schema_name TEXT)
*     RETURNS SETOF INTEGER
*   generate_log_entry(queried_date TIMESTAMP, audit_log_id INTEGER, original_table_name TEXT, 
*     original_schema_name TEXT DEFAULT 'public') RETURNS JSON
*   merge_json(json_log JSON, json_diff JSON) RETURNS JSON AS
*   restore_schema_state(queried_date TIMESTAMP, original_schema_name TEXT, target_schema_name TEXT, 
*     target_table_type TEXT DEFAULT 'VIEW', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   restore_table_state(queried_date TIMESTAMP, original_table_name TEXT, original_schema_name TEXT, 
*     target_schema_name TEXT, target_table_type TEXT DEFAULT 'VIEW') RETURNS SETOF VOID
***********************************************************/

/**********************************************************
* HELPER METHODS
*
* These methods are necessary to realize the restoring of
* past states of a table.
***********************************************************/
-- builds a JSON object from two arrays where the first array
-- includes the keys and the second includes the values 
CREATE OR REPLACE FUNCTION pgmemento.build_json(
  json_keys ANYARRAY, 
  json_values ANYARRAY
  ) RETURNS JSON AS
$$
DECLARE
  json_string TEXT := '{';
  delimeter TEXT := '';
  json_result JSON;
BEGIN
  FOR i IN array_lower(json_keys, 1)..array_upper(json_keys, 1) LOOP
    json_string := json_string || delimeter || json_keys[i] || ':' || json_values[i];
    delimeter := ',';
  END LOOP;

  json_string := json_string || '}';

  EXECUTE format('SELECT %L::json', json_string) INTO json_result;
  RETURN json_result;
END
$$
LANGUAGE plpgsql;

-- merges one JSON object (like a diff log) into another one (the complete row)
CREATE OR REPLACE FUNCTION pgmemento.merge_json(
  json_log JSON,
  json_diff JSON
  ) RETURNS JSON AS
$$
DECLARE
  json_result JSON;
BEGIN
  IF json_log IS NULL THEN
    RETURN json_diff;
  END IF;

  EXECUTE 'SELECT pgmemento.build_json(
             array_agg(to_json(CASE WHEN old.key IS NULL THEN new.key ELSE old.key END)), 
             array_agg(CASE WHEN old.key IS NULL THEN new.value ELSE old.value END))
           FROM json_each($1) old
           FULL OUTER JOIN json_each($2) new
           ON old.key = new.key'
           INTO json_result USING json_log, json_diff;

  RETURN json_result;
END;
$$
LANGUAGE plpgsql;

-- reproduces a row that has existed at a given date
CREATE OR REPLACE FUNCTION pgmemento.generate_log_entry(
  queried_date TIMESTAMP,
  audit_log_id INTEGER,
  original_table_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS JSON AS
$$
DECLARE
  json_log JSON;
  json_diff JSON;
  json_result JSON;
BEGIN
  -- merge every change recorded in audit_log from the queried date till now into json_result
  -- json_result is null during the first loop
  FOR json_diff IN EXECUTE 'SELECT table_content FROM pgmemento.audit_log
                             WHERE table_relid = $1::regclass::oid
                               AND audit_id = $2
                               AND stmt_date > $3
                             ORDER BY stmt_date ASC'
                             USING original_schema_name || '.' || original_table_name, audit_log_id, queried_date LOOP
	json_result := pgmemento.merge_json(json_result, json_diff);
  END LOOP;

  -- only if a row has been deleted json_result would be complete by now
  -- thus check if a row still exists in the table and transform it into JSON to have the last json_diff
  EXECUTE format('SELECT pgmemento.merge_json(%L,row_to_json(%I)) FROM %I.%I WHERE audit_id = %L',
                    json_result, original_table_name, original_schema_name, original_table_name, audit_log_id)
                    INTO json_log;

  IF json_log IS NULL THEN
    RETURN json_result;
  ELSE
    RETURN json_log;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- collects all entries that have existed at a given date
-- which are used 
CREATE OR REPLACE FUNCTION pgmemento.fetch_audit_ids(
  queried_date TIMESTAMP,
  search_date TIMESTAMP,
  searching_backwards NUMERIC,
  exclude_deleted_values NUMERIC,
  original_table_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF INTEGER AS
$$
DECLARE
  search_direction_a TEXT;
  search_direction_b TEXT; 
  operation_condition TEXT := '';
BEGIN
  IF searching_backwards = 1 THEN
    search_direction_a := '>';
    search_direction_b := '<=';
  ELSE
    search_direction_a := '<';
    search_direction_b := '>=';
  END IF;

  IF exclude_deleted_values = 1 THEN
    operation_condition := 'AND (table_operation = ''INSERT'' OR table_operation = ''UPDATE'')';
  END IF;

  RETURN QUERY EXECUTE 'SELECT collect.trace FROM (
                          SELECT clog.audit_id AS trace FROM pgmemento.audit_log clog
                            JOIN pgmemento.transaction_log ctlog 
                              ON clog.internal_transaction_id = ctlog.internal_transaction_id
                             AND clog.table_relid = ctlog.table_relid
                             AND clog.stmt_date = ctlog.stmt_date
                             WHERE ctlog.schema_name = $1 AND ctlog.table_name = $2
                               AND ctlog.stmt_date = $3 ' || operation_condition || 
                          ') collect
                        LEFT OUTER JOIN (
                          SELECT elog.audit_id AS trace FROM pgmemento.audit_log elog
                            JOIN pgmemento.transaction_log etlog
                              ON elog.internal_transaction_id = etlog.internal_transaction_id
                             AND elog.table_relid = etlog.table_relid
                             AND elog.stmt_date = etlog.stmt_date
                            WHERE etlog.schema_name = $1 AND etlog.table_name = $2
                            AND (etlog.stmt_date ' || search_direction_a || ' $3 AND etlog.stmt_date ' || search_direction_b || ' $4)
                          ) exclude
                        ON collect.trace = exclude.trace
                          WHERE exclude.trace IS NULL'
                        USING original_schema_name, original_table_name, search_date, queried_date;
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
  queried_date TIMESTAMP,
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
BEGIN
  -- test if target schema already exist
  EXECUTE 'SELECT 1 FROM information_schema.schemata WHERE schema_name = $1' INTO is_set_schema USING target_schema_name;

  IF is_set_schema IS NULL THEN
    EXECUTE format('CREATE SCHEMA %I', target_schema_name);
  END IF;

  -- test if table or view already exist in target schema
  EXECUTE 'SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2 
             AND (table_type = ''BASE TABLE'' OR table_type = ''VIEW'')' 
             INTO is_set_table USING original_table_name, target_schema_name;

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
    EXECUTE 'SELECT 1 FROM pgmemento.transaction_log WHERE schema_name = $1 AND table_name = $2 LIMIT 1'
               INTO logged USING original_schema_name, original_table_name;

    IF logged IS NOT NULL THEN
      -- if the table structure has changed over time we need to use a template table
      -- that we hopefully created with 'pgmemento.create_table_template' before altering the table
      EXECUTE 'SELECT name FROM pgmemento.table_templates
                 WHERE original_schema = $1 AND original_table = $2 AND creation_date <= $3 
                 ORDER BY creation_date DESC LIMIT 1'
                 INTO template_table USING original_schema_name, original_table_name, queried_date;

      IF template_table IS NULL THEN
        template_schema := original_schema_name;
        template_table := original_table_name;
      ELSE
        template_schema := 'pgmemento';
      END IF;

      -- let's go back in time - restore a table state at a given date
      IF upper(target_table_type) = 'VIEW' OR upper(target_table_type) = 'TABLE' THEN
        EXECUTE format('CREATE ' || replace_view || target_table_type || ' %I.%I AS 
                          SELECT * FROM json_populate_recordset(null::%I.%I,
                            (SELECT json_agg(pgmemento.generate_log_entry(%L, f.log_id, %L, %L)) FROM
                              (SELECT pgmemento.fetch_audit_ids(%L, stmt_date, 1, 1, %L, %L) AS log_id
                                 FROM pgmemento.transaction_log 
                                   WHERE stmt_date <= %L 
                                     AND schema_name = %L AND table_name = %L
                                 GROUP BY stmt_date ORDER BY stmt_date DESC
                              ) f
                            )
                          )',
                          target_schema_name, original_table_name, template_schema, template_table,
                          queried_date, original_table_name, original_schema_name,
				          queried_date, original_table_name, original_schema_name,
                          queried_date, original_schema_name, original_table_name);
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
  queried_date TIMESTAMP, 
  original_schema_name TEXT,
  target_schema_name TEXT, 
  target_table_type TEXT DEFAULT 'VIEW',
  except_tables TEXT[] DEFAULT '{}',
  update_state INTEGER DEFAULT '0'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.restore_table_state($1::timestamp, tablename, schemaname, $2, $3, $4) FROM pg_tables 
             WHERE schemaname = $5 AND tablename <> ALL ($6)'
             USING queried_date, target_schema_name, target_table_type, update_state, original_schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;