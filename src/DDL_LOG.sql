-- DDL_LOG.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides functions to track table changes in all database 
-- schemas using event triggers.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                 | Author
-- 0.3.0     2017-03-15   data logging before DDL drop events           FKun
-- 0.2.0     2017-03-11   update to Pg9.5 and adding more trigger       FKun
-- 0.1.0     2016-04-14   initial commit                                FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   create_schema_event_trigger(trigger_create_table INTEGER DEFAULT 0) RETURNS SETOF VOID
*   drop_schema_event_trigger() RETURNS SETOF VOID
*   modify_ddl_log_tables(tablename TEXT, schemaname TEXT, ddl_action TEXT) RETURNS SETOF VOID
*
* TRIGGER FUNCTIONS:
*   schema_drop_pre_trigger() RETURNS event_trigger
*   table_alter_post_trigger() RETURNS event_trigger
*   table_alter_pre_trigger() RETURNS event_trigger
*   table_create_post_trigger() RETURNS event_trigger
*   table_drop_post_trigger() RETURNS event_trigger
*   table_drop_pre_trigger() RETURNS event_trigger
*
***********************************************************/

/**********************************************************
* MODIFY DDL LOGS
*
* Helper function to update tables audit_table_log and 
* audit_column_log
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.modify_ddl_log_tables(
  tablename TEXT,
  schemaname TEXT,
  ddl_action TEXT
  ) RETURNS SETOF VOID AS
$$
DECLARE
  tab_id INTEGER;
BEGIN
  IF $3 = 'CREATE' THEN
    INSERT INTO pgmemento.audit_table_log
      (relid, schema_name, table_name, txid_range)
    VALUES 
      (($2 || '.' || $1)::regclass::oid, $2, $1, numrange(txid_current(), NULL, '[)'))
    RETURNING id INTO tab_id;

    -- insert columns of new audited table into 'audit_column_log'
    INSERT INTO pgmemento.audit_column_log 
      (id, audit_table_id, column_name, ordinal_position, column_default, is_nullable, 
       data_type, data_type_name, char_max_length, numeric_precision, numeric_precision_radix, numeric_scale, 
       datetime_precision, interval_type, txid_range)
    (
      SELECT 
        nextval('pgmemento.audit_column_log_id_seq') AS id,
        tab_id AS audit_table_id, column_name, ordinal_position, column_default, is_nullable,
        data_type, udt_name, character_maximum_length, numeric_precision, numeric_precision_radix, numeric_scale,
        datetime_precision, interval_type, numrange(txid_current(), NULL, '[)') AS txid_range
      FROM information_schema.columns
        WHERE table_name = $1
          AND table_schema = $2
    );
  ELSIF $3 = 'ALTER' THEN
    -- get id from audit_table_log
    SELECT id INTO tab_id
      FROM pgmemento.audit_table_log
        WHERE table_name = $1
          AND schema_name = $2
          AND upper(txid_range) IS NULL;

    -- EVENT: New column created
    -- insert columns that do not exist in audit_column_log table
    INSERT INTO pgmemento.audit_column_log
      (id, audit_table_id, column_name, ordinal_position, column_default, is_nullable, 
       data_type, data_type_name, char_max_length, numeric_precision, numeric_precision_radix, numeric_scale, 
       datetime_precision, interval_type, txid_range)
    (
      SELECT 
        nextval('pgmemento.audit_column_log_id_seq') AS id, 
        tab_id AS audit_table_id, col.column_name, col.ordinal_position, col.column_default, col.is_nullable,
        col.data_type, col.udt_name, col.character_maximum_length, col.numeric_precision, col.numeric_precision_radix, col.numeric_scale,
        col.datetime_precision, col.interval_type, numrange(txid_current(), NULL, '[)') AS txid_range
      FROM information_schema.columns col 
      LEFT JOIN (
        SELECT a.table_name, c.column_name, a.schema_name
          FROM pgmemento.audit_column_log c
          JOIN pgmemento.audit_table_log a
            ON a.id = c.audit_table_id
            WHERE a.table_name = $1
              AND a.schema_name = $2
              AND upper(a.txid_range) IS NULL
              AND upper(c.txid_range) IS NULL
      ) acl
        ON acl.table_name = col.table_name
       AND acl.column_name = col.column_name
       AND acl.schema_name = col.table_schema
        WHERE col.table_name = $1
          AND col.table_schema = $2
          AND acl.column_name IS NULL
    );

    -- EVENT: Column dropped
    -- update txid_range for removed columns in audit_column_log table
    WITH dropped_columns AS (
      SELECT c.id
        FROM pgmemento.audit_table_log a
        JOIN pgmemento.audit_column_log c
          ON c.audit_table_id = a.id
        LEFT JOIN (
          SELECT column_name, table_name, table_schema
            FROM information_schema.columns
              WHERE table_name = $1
                AND table_schema = $2
        ) col
          ON col.column_name = c.column_name
         AND col.table_name = a.table_name
         AND col.table_schema = a.schema_name
          WHERE a.table_name = $1
            AND a.schema_name = $2
            AND col.column_name IS NULL
            AND upper(a.txid_range) IS NULL
            AND upper(c.txid_range) IS NULL
    )
    UPDATE pgmemento.audit_column_log acl
      SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
      FROM dropped_columns dc
        WHERE acl.id = dc.id;

    -- EVENT: Column altered
    -- update txid_range for updated columns and insert new versions into audit_column_log table
    WITH updated_columns AS (
      SELECT acl.id, acl.audit_table_id, col.column_name, col.ordinal_position, col.column_default, col.is_nullable,
             col.data_type, col.udt_name, col.character_maximum_length, col.numeric_precision, col.numeric_precision_radix, col.numeric_scale,
             col.datetime_precision, col.interval_type
        FROM information_schema.columns col
        JOIN (
          SELECT c.*, a.table_name, a.schema_name
            FROM pgmemento.audit_column_log c
            JOIN pgmemento.audit_table_log a
              ON a.id = c.audit_table_id
              WHERE a.table_name = $1
                AND a.schema_name = $2
                AND upper(a.txid_range) IS NULL
                AND upper(c.txid_range) IS NULL
        ) acl
          ON col.column_name = acl.column_name
         AND col.table_name = acl.table_name
         AND col.table_schema = acl.schema_name
          WHERE (
               col.column_default <> acl.column_default
            OR col.is_nullable <> acl.is_nullable
            OR col.data_type <> acl.data_type
            OR col.udt_name <> acl.data_type_name
            OR col.character_maximum_length <> acl.char_max_length
            OR col.numeric_precision <> acl.numeric_precision
            OR col.numeric_precision_radix <> acl.numeric_precision_radix
            OR col.numeric_scale <> acl.numeric_scale
            OR col.datetime_precision <> acl.datetime_precision
            OR col.interval_type <> acl.interval_type
          )
      ), insert_new_versions AS (
        INSERT INTO pgmemento.audit_column_log           
          (id, audit_table_id, column_name, ordinal_position, column_default, is_nullable, 
           data_type, data_type_name, char_max_length, numeric_precision, numeric_precision_radix, numeric_scale, 
           datetime_precision, interval_type, txid_range)
        (
          SELECT
            nextval('pgmemento.audit_column_log_id_seq') AS id, 
            audit_table_id, column_name, ordinal_position, column_default, is_nullable,
            data_type, udt_name, character_maximum_length, numeric_precision, numeric_precision_radix, numeric_scale,
            datetime_precision, interval_type, numrange(txid_current(), NULL, '[)') AS txid_range
          FROM updated_columns
        )
      )
      UPDATE pgmemento.audit_column_log acl
        SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
        FROM updated_columns uc
          WHERE uc.id = acl.id;
  -- DROP scenario
  ELSE
    -- update txid_range for removed table in audit_table_log table
    UPDATE pgmemento.audit_table_log
      SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
      WHERE table_name = $1
        AND schema_name = $2
        AND upper(txid_range) IS NULL
        RETURNING id INTO tab_id;

    IF tab_id IS NOT NULL THEN
      -- update txid_range for removed columns in audit_column_log table
      UPDATE pgmemento.audit_column_log
        SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
        WHERE audit_table_id = tab_id
          AND upper(txid_range) IS NULL;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE schema_drop_pre_trigger
*
* Procedure that is called BEFORE schema will be dropped.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.schema_drop_pre_trigger() RETURNS event_trigger AS
$$
DECLARE
  ddl_text TEXT := current_query();
  cascading BOOLEAN := FALSE;
  schema_name TEXT;
  rec RECORD;
BEGIN
  -- has CASCADE been used?
  cascading := lower(ddl_text) LIKE '%cascade%';

  -- extracting the schema name from the DDL command
  schema_name := replace(lower(ddl_text), 'drop schema ', '');
  schema_name := replace(schema_name, 'if exists ', '');
  schema_name := replace(schema_name, ' cascade', '');
  schema_name := replace(schema_name, ' restrict', '');
  schema_name := replace(schema_name, ';', '');

  -- truncate tables to log the data
  FOR rec IN 
    SELECT p.schemaname, p.tablename 
      FROM pg_tables p
      JOIN pgmemento.audit_tables_dependency d
        ON p.schemaname = d.schemaname
        AND p.tablename = d.tablename
        WHERE p.schemaname = schema_name
        ORDER BY p.schemaname, d.depth
  LOOP
    IF cascading THEN
      EXECUTE format('TRUNCATE %I.%I CASCADE', rec.schemaname, rec.tablename);
    ELSE
      EXECUTE format('TRUNCATE %I.%I', rec.schemaname, rec.tablename);
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_alter_post_trigger
*
* Procedure that is called AFTER tables have been altered
* e.g. to add, alter or drop columns
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_alter_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj RECORD;
  tab_id INTEGER;
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    -- first check if table is audited
    IF NOT EXISTS (
      SELECT 1 FROM pgmemento.audit_tables
        WHERE schemaname = obj.schema_name
          AND tablename = split_part(obj.object_identity, '.' ,2)
    ) THEN
      RETURN;
    ELSE
      -- check if affected table exists in 'audit_table_log' (with open range)
      SELECT id INTO tab_id
        FROM pgmemento.audit_table_log 
          WHERE schema_name = obj.schema_name
            AND table_name = split_part(obj.object_identity, '.' ,2)
            AND upper(txid_range) IS NULL;

      IF tab_id IS NULL THEN
        -- EVENT: if table got renamed "close" the old version
        PERFORM pgmemento.modify_ddl_log_tables(split_part(obj.object_identity, '.' ,2), obj.schema_name, 'DROP');

        -- EVENT: Activating auditing adds the audit_id column which fires the event trigger
        -- EVENT: Renaming a table will also produce a new version
        PERFORM pgmemento.modify_ddl_log_tables(split_part(obj.object_identity, '.' ,2), obj.schema_name, 'CREATE');
      ELSE
        -- EVENT: Table schema altered
        PERFORM pgmemento.modify_ddl_log_tables(split_part(obj.object_identity, '.' ,2), obj.schema_name, 'ALTER');
      END IF;
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_alter_pre_trigger
*
* Procedure that is called BEFORE tables will be altered
* e.g. to log data following an old schema
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_alter_pre_trigger() RETURNS event_trigger AS
$$
DECLARE
  ddl_text TEXT := current_query();
  dropping BOOLEAN := FALSE;
  alter_text TEXT;
  build_name BOOLEAN := TRUE;
  table_ident TEXT := '';
  t_name TEXT;
  s_name TEXT;
  ntables INTEGER;
  objs TEXT[];
  columnname TEXT;
  drop_next BOOLEAN := FALSE;
BEGIN
  -- are columns renamed, altered or dropped
  dropping := lower(ddl_text) LIKE '%drop column%' OR lower(ddl_text) LIKE '%drop%';

  IF dropping THEN
    -- extracting the table identifier from the DDL command
    alter_text := replace(lower(ddl_text), 'alter table ', '');
    alter_text := replace(alter_text, 'if exists ', '');
    alter_text := replace(alter_text, ' cascade', '');
    alter_text := replace(alter_text, ' restrict', '');
    alter_text := replace(alter_text, ';', '');

    FOR i IN 1..length(alter_text) LOOP
      EXIT WHEN build_name = FALSE;
      IF substr(alter_text,i,1) <> ' ' THEN
        table_ident := table_ident || substr(alter_text,i,1);
      ELSE
        IF length(table_ident) > 0 THEN
          build_name := FALSE;
        END IF;
      END IF;
    END LOOP;

    -- get table and schema name
    IF split_part(table_ident, '.', 2) IS NULL THEN
      t_name := split_part(table_ident, '.', 1);

      -- check if table is audited and not ambiguous
      SELECT count(*) INTO ntables
        FROM pgmemento.audit_tables
          WHERE tablename = t_name
            AND tg_is_active = TRUE;		
    ELSE
      s_name := split_part(table_ident, '.', 1);
      t_name := split_part(table_ident, '.', 2);

      -- check if table is audited and not ambiguous
      SELECT count(*) INTO ntables
        FROM pgmemento.audit_tables
          WHERE tablename = t_name
            AND schemaname = s_name
            AND tg_is_active = TRUE;
    END IF;

    -- table not found in audit_table_log, so it can be altered without logging
    IF ntables IS NULL THEN
      RETURN;
    END IF;

    IF ntables > 1 THEN
      -- table name is found more than once in audit_table_log
      RAISE EXCEPTION 'Please specify the schema name in the ALTER TABLE command.';
    ELSE
      alter_text := replace(alter_text, s_name || '.', '');
      alter_text := replace(alter_text, t_name, '');
      objs := regexp_split_to_array(alter_text, E'\\s+');

      FOREACH columnname IN ARRAY objs LOOP
        columnname := translate(columnname, ',', '');
        IF drop_next THEN
          IF columnname <> 'column'
          OR columnname <> 'if'
          OR columnname <> 'exists' THEN
            IF EXISTS (
              SELECT 1 
                FROM pgmemento.audit_column_log c, 
                     pgmemento.audit_table_log a
                  WHERE c.audit_table_id = a.id
                    AND c.column_name = columnname
                    AND a.table_name = t_name
                    AND a.schema_name = s_name
                    AND upper(c.txid_range) IS NULL
            ) THEN
              -- update entire column to log its content
              EXECUTE format(
                'UPDATE %I.%I SET %I = NULL',
                s_name, t_name, columnname);
              drop_next := FALSE;
            END IF;
          END IF;			
        ELSE
          IF columnname = 'drop' THEN
            drop_next = TRUE;
          END IF;
        END IF;
      END LOOP;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_create_post_trigger
*
* Procedure that is called AFTER new tables have been created
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_create_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj record;
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    IF obj.object_type = 'table' AND obj.schema_name <> 'pg_temp' THEN
      PERFORM pgmemento.create_table_audit(split_part(obj.object_identity, '.' ,2), obj.schema_name);
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_drop_post_trigger
*
* Procedure that is called AFTER tables have been dropped
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_drop_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj RECORD;
  tab_id INTEGER;
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_dropped_objects()
  LOOP
    IF obj.object_type = 'table' AND NOT obj.is_temporary THEN
      -- update txid_range for removed table in audit_table_log table
      PERFORM pgmemento.modify_ddl_log_tables(obj.object_name, obj.schema_name, 'DROP');
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_drop_pre_trigger
*
* Procedure that is called BEFORE tables will be dropped.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_drop_pre_trigger() RETURNS event_trigger AS
$$
DECLARE
  ddl_text TEXT := current_query();
  table_ident TEXT;
  cascading BOOLEAN := FALSE;
  table_name TEXT;
  schema_name TEXT;
  ntables INTEGER;
BEGIN
  -- has CASCADE been used?
  cascading := lower(ddl_text) LIKE '%cascade%';

  -- extracting the table identifier from the DDL command
  table_ident := replace(lower(ddl_text), 'drop table ', '');
  table_ident := replace(table_ident, 'if exists ', '');
  table_ident := replace(table_ident, ' cascade', '');
  table_ident := replace(table_ident, ' restrict', '');
  table_ident := replace(table_ident, ';', '');

  -- get table and schema name
  IF split_part(table_ident, '.', 2) IS NULL THEN
    table_name := split_part(table_ident, '.', 1);

    -- check if table is audited and not ambiguous
    SELECT count(*) INTO ntables
      FROM pgmemento.audit_tables
        WHERE tablename = tablename
          AND tg_is_active = TRUE;		
  ELSE
    schema_name := split_part(table_ident, '.', 1);
    table_name := split_part(table_ident, '.', 2);

    -- check if table is audited and not ambiguous
    SELECT count(*) INTO ntables
      FROM pgmemento.audit_tables
        WHERE tablename = table_name
          AND schemaname = schema_name
          AND tg_is_active = TRUE;
  END IF;

  -- table not found in audit_table_log, so it can be dropped
  IF ntables IS NULL OR ntables = 0 THEN
    RETURN;
  END IF;

  IF ntables > 1 THEN
    -- table name is found more than once in audit_table_log
    RAISE EXCEPTION 'Please specify the schema name in the DROP TABLE command.';
  ELSE
    -- truncate table to log the data
    IF cascading THEN
      EXECUTE format('TRUNCATE '
        || CASE WHEN schema_name IS NULL THEN '' ELSE (schema_name || '.') END
        || '%I CASCADE', table_name);
    ELSE
      EXECUTE format('TRUNCATE '
        || CASE WHEN schema_name IS NULL THEN '' ELSE (schema_name || '.') END
        || '%I', table_name);
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER
*
* Global event triggers that are fired when tables are
* created, altered or dropped
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.create_schema_event_trigger(
  trigger_create_table INTEGER DEFAULT 0
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- Create event trigger for DROP SCHEMA events to log data
  -- before it is lost
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'schema_drop_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER schema_drop_pre_trigger ON ddl_command_start
      WHEN TAG IN ('DROP SCHEMA')
        EXECUTE PROCEDURE pgmemento.schema_drop_pre_trigger();
  END IF;

  -- Create event trigger for ALTER TABLE events to update 'audit_column_log' table
  -- after table is altered
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'table_alter_post_trigger'
  ) THEN
    CREATE EVENT TRIGGER table_alter_post_trigger ON ddl_command_end
      WHEN TAG IN ('ALTER TABLE')
        EXECUTE PROCEDURE pgmemento.table_alter_post_trigger();
  END IF;

  -- Create event trigger for ALTER TABLE events to log data
  -- before table is altered
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'table_alter_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER table_alter_pre_trigger ON ddl_command_start
      WHEN TAG IN ('ALTER TABLE')
        EXECUTE PROCEDURE pgmemento.table_alter_pre_trigger();
  END IF;

  -- Create event trigger for CREATE TABLE events to automatically start auditing on new tables
  -- The user can decide if he wants this behaviour during initializing pgMemento.
  IF trigger_create_table <> 0 THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_event_trigger
        WHERE evtname = 'table_create_post_trigger'
    ) THEN
      CREATE EVENT TRIGGER table_create_post_trigger ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE')
          EXECUTE PROCEDURE pgmemento.table_create_post_trigger();
    END IF;
  END IF;

  -- Create event trigger for DROP TABLE events to update tables 'audit_table_log' and 'audit_column_log'
  -- after table is dropped
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'table_drop_post_trigger'
  ) THEN
    CREATE EVENT TRIGGER table_drop_post_trigger ON sql_drop
      WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE pgmemento.table_drop_post_trigger();
  END IF;

  -- Create event trigger for DROP TABLE events to log data
  -- before it is lost
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'table_drop_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER table_drop_pre_trigger ON ddl_command_start
      WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE pgmemento.table_drop_pre_trigger();
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.drop_schema_event_trigger() RETURNS SETOF VOID AS
$$
  DROP EVENT TRIGGER IF EXISTS schema_drop_pre_trigger;
  DROP EVENT TRIGGER IF EXISTS table_alter_post_trigger;
  DROP EVENT TRIGGER IF EXISTS table_alter_pre_trigger;
  DROP EVENT TRIGGER IF EXISTS table_create_post_trigger;
  DROP EVENT TRIGGER IF EXISTS table_drop_post_trigger;
  DROP EVENT TRIGGER IF EXISTS table_drop_pre_trigger;
$$
LANGUAGE sql;