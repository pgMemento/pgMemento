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
-- Version | Date       | Description                                   | Author
-- 0.2.0     2015-02-21   new queries, JSON concat using PL/V8            FKun
-- 0.1.0     2014-11-26   initial commit                                  FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   concat_json(obj1 jsonb, obj2 jsonb) RETURNS json
*   generate_log_entry(tid INTEGER, schema_name TEXT, table_name TEXT, aid BIGINT) RETURNS jsonb
*   restore_schema_state(tid INTEGER, original_schema_name TEXT, target_schema_name TEXT, 
*     target_table_type TEXT DEFAULT 'VIEW', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   restore_table_state(tid INTEGER, original_table_name TEXT, original_schema_name TEXT, 
*     target_schema_name TEXT, target_table_type TEXT DEFAULT 'VIEW') RETURNS SETOF VOID
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.concat_json(obj1 jsonb, obj2 jsonb) RETURNS json AS
$$
  var a = JSON.parse(obj1);
  var b = JSON.parse(obj2);

  if (typeof(a)!=='object' || a == null) 
    a={};
  if (typeof(b)!=='object' || b == null) 
    b={};
    
  for (var key in b)
    if (b.hasOwnProperty(key))
      a[key] = b[key];

 return a;
$$
LANGUAGE plv8;


CREATE OR REPLACE FUNCTION pgmemento.generate_log_entry(
  tid INTEGER,
  schema_name TEXT,
  table_name TEXT,
  template_schema TEXT,
  template_table TEXT,
  aid BIGINT
  ) RETURNS jsonb AS
$$
DECLARE
  column_name TEXT;
  json_object JSONB;
  json_result JSONB;
BEGIN
  -- get the content of each column that happened to be in the table when the transaction was executed
  FOR column_name IN 
    EXECUTE 'SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass and attstattarget != 0 ORDER BY attnum' 
               USING template_schema || '.' || template_table LOOP
    -- first: try to find the value within logged changes in row_log (first occurrence greater than txid will be the correct value)
    -- second: if not found, query the recent state for information	
    EXECUTE format('SELECT json_object_agg(%L,
      COALESCE(
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
           FROM %I.%I a 
           WHERE a.audit_id = %L
        )
      ))::jsonb',
      column_name, column_name, tid, aid, column_name,
      column_name, schema_name, table_name, aid)
      INTO json_object;

    IF json_object IS NOT NULL THEN
      json_result := pgmemento.concat_json(json_result, json_object)::jsonb;
    END IF;
  END LOOP;

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
  tid INTEGER,
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
    EXECUTE 'SELECT 1 FROM pgmemento.table_event_log WHERE schema_name = $1 AND table_name = $2 LIMIT 1'
               INTO logged USING original_schema_name, original_table_name;

    IF logged IS NOT NULL THEN
      -- if the table structure has changed over time we need to use a template table
      -- that we hopefully created with 'pgmemento.create_table_template' before altering the table
      EXECUTE 'SELECT name FROM pgmemento.table_templates
                 WHERE original_schema = $1 AND original_table = $2 AND creation_date <= 
                   (SELECT stmt_date FROM pgmemento.transaction_log WHERE txid = $3)
                 ORDER BY creation_date DESC LIMIT 1'
                 INTO template_table USING original_schema_name, original_table_name, tid;

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
                            (WITH excluded_ids AS (
                               SELECT DISTINCT r.audit_id
                               FROM pgmemento.row_log r
                               JOIN pgmemento.table_event_log e ON r.event_id = e.id
                               JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                                 WHERE t.txid < %L 
                                   AND e.table_relid = %L::regclass::oid 
	                               AND e.op_id > 2), 
                             valid_ids AS (
                               SELECT DISTINCT y.audit_id
                               FROM pgmemento.row_log y
                               JOIN pgmemento.table_event_log e ON y.event_id = e.id
                               JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
                               LEFT OUTER JOIN excluded_ids n ON n.audit_id = y.audit_id
                                 WHERE t.txid < %L
                                   AND e.table_relid = %L::regclass::oid 
                                   AND (n.audit_id IS NULL OR y.audit_id != n.audit_id)
                            )
                            SELECT json_agg(pgmemento.generate_log_entry(%L, %L, %L, %L, %L, audit_id)) 
                            FROM valid_ids ORDER BY audit_id
                            )
                          )',
                          target_schema_name, original_table_name, template_schema, template_table,
                          tid, original_schema_name || '.' || original_table_name,
				          tid, original_schema_name, original_table_name, template_schema, template_table);
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
  tid INTEGER,
  original_schema_name TEXT,
  target_schema_name TEXT, 
  target_table_type TEXT DEFAULT 'VIEW',
  except_tables TEXT[] DEFAULT '{}',
  update_state INTEGER DEFAULT '0'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.restore_table_state($1, tablename, schemaname, $2, $3, $4) FROM pg_tables 
             WHERE schemaname = $5 AND tablename <> ALL ($6)'
             USING tid, target_schema_name, target_table_type, update_state, original_schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;