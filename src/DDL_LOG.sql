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
-- Version | Date       | Description                                    | Author
-- 0.5.1     2017-08-08   DROP TABLE/SCHEMA events log data as truncated   FKun
-- 0.5.0     2017-07-25   improved processing of DDL events                FKun
-- 0.4.1     2017-07-18   now using register functions from SETUP          FKun
-- 0.4.0     2017-07-12   reflect changes to audit_column_log table        FKun
-- 0.3.2     2017-04-10   log also CREATE/DROP TABLE and ADD COLUMN        FKun
--                        event in log tables (no data logging)
-- 0.3.1     2017-03-31   data logging before ALTER COLUMN events          FKun
-- 0.3.0     2017-03-15   data logging before DDL drop events              FKun
-- 0.2.0     2017-03-11   update to Pg9.5 and adding more trigger          FKun
-- 0.1.0     2016-04-14   initial commit                                   FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   create_schema_event_trigger(trigger_create_table INTEGER DEFAULT 0) RETURNS SETOF VOID
*   drop_schema_event_trigger() RETURNS SETOF VOID
*   get_ddl_from_context(stack TEXT) RETURNS TEXT
*   log_ddl_event(table_name TEXT, schema_name TEXT, op_type INTEGER, op_text TEXT) RETURNS INTEGER
*   modify_ddl_log_tables(tablename TEXT, schemaname TEXT) RETURNS SETOF VOID
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
* GET DDL FROM CONTEXT
*
* Helper function to parse DDL statement from PG_CONTEXT
* of GET DIAGNOSTICS command
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_ddl_from_context(stack TEXT) RETURNS TEXT AS
$$
DECLARE
  ddl_text TEXT;
  objs TEXT[] := '{}';
  do_next BOOLEAN := TRUE;
  ddl_pos INTEGER;
BEGIN
  -- split context by lines
  objs := regexp_split_to_array($1, E'\\n+');

  -- if context is greater than 1 line, trigger was fired from inside a function
  IF array_length(objs,1) > 1 THEN
    FOR i IN 2..array_length(objs,1) LOOP
      EXIT WHEN do_next = FALSE;
      -- try to find starting position of DDL command
      ddl_pos := GREATEST(
                   position('ALTER TABLE' IN objs[i]),
                   position('DROP TABLE' IN objs[i]),
                   position('DROP SCHEMA' IN objs[i])
                 );
      IF ddl_pos > 0 THEN
        ddl_text := substr(objs[2], ddl_pos, length(objs[2]) - ddl_pos);
        do_next := FALSE;
      END IF;
    END LOOP;
  END IF;

  RETURN ddl_text;
END;
$$
LANGUAGE plpgsql IMMUTABLE STRICT;


/**********************************************************
* LOG DDL EVENT
*
* Function that write information of ddl events into
* transaction_log and table_event_log and returns the
* ID of the latter table
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_ddl_event(
  table_name TEXT,
  schema_name TEXT,
  op_type INTEGER,
  op_text TEXT
  ) RETURNS INTEGER AS
$$
DECLARE
  e_id INTEGER;
  table_oid OID;
BEGIN
  -- log transaction of ddl event
  -- on conflict do nothing
  INSERT INTO pgmemento.transaction_log 
    (txid, stmt_date, user_name, client_name)
  VALUES
    (txid_current(), statement_timestamp(), current_user, inet_client_addr())
  ON CONFLICT (txid)
    DO NOTHING;

  IF table_name IS NOT NULL AND schema_name IS NOT NULL THEN
    table_oid := ($2 || '.' || $1)::regclass::oid;
  END IF;

  -- try to log corresponding table event
  -- on conflict do dummy update to get event_id
  INSERT INTO pgmemento.table_event_log 
    (transaction_id, op_id, table_operation, table_relid) 
  VALUES
    (txid_current(), $3, $4, table_oid)
  ON CONFLICT (transaction_id, table_relid, op_id)
    DO UPDATE SET op_id = $3 RETURNING id INTO e_id;

  RETURN e_id;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* MODIFY DDL LOGS
*
* Helper function to update tables audit_table_log and 
* audit_column_log
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.modify_ddl_log_tables(
  tablename TEXT,
  schemaname TEXT
  ) RETURNS SETOF VOID AS
$$
DECLARE
  tab_id INTEGER;
  column_ids int[] := '{}';
BEGIN
  -- get id from audit_table_log for given table
  tab_id := pgmemento.register_audit_table($1,$2);

  IF tab_id IS NOT NULL THEN
    -- EVENT: New column created
    -- insert columns that do not exist in audit_column_log table
    WITH added_columns AS (
      INSERT INTO pgmemento.audit_column_log
        (id, audit_table_id, column_name, ordinal_position, data_type, column_default, not_null, txid_range)
      (
        SELECT 
          nextval('pgmemento.audit_column_log_id_seq') AS id,
          tab_id AS audit_table_id,
          a.attname AS column_name,
          a.attnum AS ordinal_position,
          substr(
            format_type(a.atttypid, a.atttypmod),
            position('.' IN format_type(a.atttypid, a.atttypmod))+1,
            length(format_type(a.atttypid, a.atttypmod))
          ) AS data_type,
          d.adsrc AS column_default,
          a.attnotnull AS not_null,
          numrange(txid_current(), NULL, '[)') AS txid_range
        FROM
          pg_attribute a
        LEFT JOIN
          pg_attrdef d
          ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
        LEFT JOIN (
          SELECT
            a.table_name,
            c.column_name,
            a.schema_name
          FROM
            pgmemento.audit_column_log c
          JOIN
            pgmemento.audit_table_log a ON a.id = c.audit_table_id
          WHERE
            a.id = tab_id
            AND upper(a.txid_range) IS NULL
            AND lower(a.txid_range) IS NOT NULL
            AND upper(c.txid_range) IS NULL
            AND lower(c.txid_range) IS NOT NULL
          ) acl
          ON acl.column_name = a.attname
        WHERE
          a.attrelid = ($2 || '.' || $1)::regclass
          AND a.attname <> 'audit_id'
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND acl.column_name IS NULL
          ORDER BY a.attnum
      )
      RETURNING id
    )
	SELECT array_agg(id) INTO column_ids FROM added_columns;

    -- log add column event
    IF column_ids IS NOT NULL AND array_length(column_ids, 1) > 0 THEN
      PERFORM pgmemento.log_ddl_event(tablename, schemaname, 2, 'ADD COLUMN');
    END IF;

    -- EVENT: Column dropped
    -- update txid_range for removed columns in audit_column_log table
    WITH dropped_columns AS (
      SELECT
        c.id
      FROM
        pgmemento.audit_table_log a
      JOIN
        pgmemento.audit_column_log c
        ON c.audit_table_id = a.id
      LEFT JOIN (
        SELECT
          attname AS column_name,
          $1 AS table_name,
          $2 AS schema_name
        FROM
          pg_attribute
        WHERE
          attrelid = ($2 || '.' || $1)::regclass
        ) col
        ON col.column_name = c.column_name
        AND col.table_name = a.table_name
        AND col.schema_name = a.schema_name
      WHERE
        a.id = tab_id
        AND col.column_name IS NULL
        AND upper(a.txid_range) IS NULL
        AND lower(a.txid_range) IS NOT NULL
        AND upper(c.txid_range) IS NULL
        AND lower(c.txid_range) IS NOT NULL
    )
    UPDATE
      pgmemento.audit_column_log acl
    SET
      txid_range = numrange(lower(acl.txid_range), txid_current(), '[)') 
    FROM
      dropped_columns dc
    WHERE
      acl.id = dc.id;

    -- EVENT: Column altered
    -- update txid_range for updated columns and insert new versions into audit_column_log table
    WITH updated_columns AS (
      SELECT
        acl.id, acl.audit_table_id, col.column_name,
        col.ordinal_position, col.data_type, col.column_default, col.not_null
      FROM (
        SELECT
          a.attname AS column_name,
          a.attnum AS ordinal_position,
          substr(
            format_type(a.atttypid, a.atttypmod),
            position('.' IN format_type(a.atttypid, a.atttypmod))+1,
            length(format_type(a.atttypid, a.atttypmod))
          ) AS data_type,
          d.adsrc AS column_default,
          a.attnotnull AS not_null,
          $1 AS table_name,
          $2 AS schema_name
        FROM
          pg_attribute a
        LEFT JOIN
          pg_attrdef d
          ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
        WHERE
          a.attrelid = ($2 || '.' || $1)::regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
      ) col
      JOIN (
        SELECT
          c.*,
          a.table_name,
          a.schema_name
        FROM
          pgmemento.audit_column_log c
        JOIN
          pgmemento.audit_table_log a
          ON a.id = c.audit_table_id
        WHERE
          a.id = tab_id
          AND upper(a.txid_range) IS NULL
          AND lower(a.txid_range) IS NOT NULL
          AND upper(c.txid_range) IS NULL
          AND lower(c.txid_range) IS NOT NULL
        ) acl
        ON col.column_name = acl.column_name
        AND col.table_name = acl.table_name
        AND col.schema_name = acl.schema_name
      WHERE (
        col.column_default <> acl.column_default
        OR col.not_null <> acl.not_null
        OR col.data_type <> acl.data_type
      )
    ), insert_new_versions AS (
      INSERT INTO pgmemento.audit_column_log
        (id, audit_table_id, column_name, ordinal_position, data_type, column_default, not_null, txid_range)
      (
        SELECT
          nextval('pgmemento.audit_column_log_id_seq') AS id,
          audit_table_id,
          column_name, 
          ordinal_position,
          data_type,
          column_default,
          not_null,
          numrange(txid_current(), NULL, '[)') AS txid_range
        FROM
          updated_columns
      )
    )
    UPDATE
      pgmemento.audit_column_log acl
    SET
      txid_range = numrange(lower(acl.txid_range), txid_current(), '[)') 
    FROM
      updated_columns uc
    WHERE
      uc.id = acl.id;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;


/**********************************************************
* EVENT TRIGGER PROCEDURE schema_drop_pre_trigger
*
* Procedure that is called BEFORE schema will be dropped.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.schema_drop_pre_trigger() RETURNS event_trigger AS
$$
DECLARE
  ddl_text TEXT := current_query();
  stack TEXT;
  schema_name TEXT;
  rec RECORD;
  e_id INTEGER;
BEGIN
  -- get context in which trigger has been fired
  GET DIAGNOSTICS stack = PG_CONTEXT;
  stack := pgmemento.get_ddl_from_context(stack);

  -- if DDL command was found in context, trigger was fired from inside a function
  IF stack IS NOT NULL THEN
    ddl_text := stack;
  END IF;

  -- lowercase everything
  ddl_text := lower(ddl_text);

  -- check if input string contains comments
  IF ddl_text LIKE '%--%'
  OR ddl_text LIKE '%/*%'
  OR ddl_text LIKE '%*/%' THEN
    RAISE EXCEPTION 'Query contains comments. Unable to log event properly. Please, remove them. Query: %', ddl_text;
  END IF;

  -- extracting the schema name from the DDL command
  -- remove irrelevant parts and line breaks from the DDL string
  schema_name := replace(lower(ddl_text), 'drop schema ', '');
  schema_name := replace(schema_name, 'if exists ', '');
  schema_name := replace(schema_name, ' cascade', '');
  schema_name := replace(schema_name, ' restrict', '');
  schema_name := replace(schema_name, ';', '');
  schema_name := regexp_replace(schema_name, '[\r\n]+', ' ', 'g');
  schema_name := substring(schema_name, '\S(?:.*\S)*');

  -- truncate tables to log the data
  FOR rec IN 
    SELECT
      n.nspname AS schemaname,
      c.relname AS tablename 
    FROM
      pg_class c
    JOIN
      pg_namespace n
      ON n.oid = c.relnamespace
    JOIN
      pgmemento.audit_tables_dependency d
      ON d.schemaname = n.nspname
      AND d.tablename = c.relname
    WHERE
      n.nspname = schema_name
    ORDER BY
      n.oid,
      d.depth DESC
  LOOP
    -- log the whole content of the dropped table as truncated
    e_id :=  pgmemento.log_ddl_event(rec.tablename, rec.schemaname, 8, 'TRUNCATE');

    EXECUTE format(
      'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
         SELECT $1, audit_id, to_jsonb(%I) AS content FROM %I.%I ORDER BY audit_id',
         rec.tablename, rec.schemaname, rec.tablename) USING e_id;

    -- now log drop table event
    PERFORM pgmemento.log_ddl_event(rec.tablename, rec.schemaname, 9, 'DROP TABLE');

    -- unregister table from log tables
    PERFORM pgmemento.unregister_audit_table(rec.tablename, rec.schemaname);
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
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    PERFORM pgmemento.modify_ddl_log_tables(split_part(obj.object_identity, '.' ,2), obj.schema_name);
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
  stack TEXT;
  altering BOOLEAN := FALSE;
  dropping BOOLEAN := FALSE;
  do_next BOOLEAN := TRUE;
  table_ident TEXT := '';
  schemaname TEXT;
  tablename TEXT;
  ntables INTEGER := 0;
  objs TEXT[];
  columnname TEXT;
  e_id INTEGER;
BEGIN
  -- get context in which trigger has been fired
  GET DIAGNOSTICS stack = PG_CONTEXT;
  stack := pgmemento.get_ddl_from_context(stack);

  -- if DDL command was found in context, trigger was fired from inside a function
  IF stack IS NOT NULL THEN
    ddl_text := stack;
  END IF;

  -- lowercase everything
  ddl_text := lower(ddl_text);

  -- are columns renamed, altered or dropped
  altering := ddl_text LIKE '%using%' AND NOT ddl_text LIKE '%using index%';
  dropping := ddl_text LIKE '%drop column%' OR ddl_text LIKE '%drop %';

  IF altering OR dropping THEN
    -- check if input string contains comments
    IF ddl_text LIKE '%--%'
    OR ddl_text LIKE '%/*%'
    OR ddl_text LIKE '%*/%' THEN
      RAISE EXCEPTION 'Query contains comments. Unable to log event properly. Please, remove them. Query: %', ddl_text;
    END IF;

    -- extracting the table identifier from the DDL command
    -- remove irrelevant parts and line breaks from the DDL string
    ddl_text := replace(ddl_text, 'alter table ', '');
    ddl_text := replace(ddl_text, 'if exists ', '');
    ddl_text := replace(ddl_text, ' cascade', '');
    ddl_text := replace(ddl_text, ' restrict', '');
    ddl_text := replace(ddl_text, ';', '');
    ddl_text := regexp_replace(ddl_text, '[\r\n]+', ' ', 'g');

    FOR i IN 1..length(ddl_text) LOOP
      EXIT WHEN do_next = FALSE;
      IF substr(ddl_text,i,1) <> ' ' OR position('"' IN table_ident) = 1 THEN
        table_ident := table_ident || substr(ddl_text,i,1);
      ELSE
        IF length(table_ident) > 0 THEN
          do_next := FALSE;
        END IF;
      END IF;
    END LOOP;
	
    -- get table and schema name
    IF table_ident LIKE '%.%' THEN
      -- check if table is audited
      SELECT
        table_name,
        schema_name
      INTO
        tablename,
        schemaname
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = split_part(table_ident, '.', 2)
        AND schema_name = split_part(table_ident, '.', 1)
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL;

      IF schemaname IS NOT NULL AND tablename IS NOT NULL THEN
        ntables := 1;
      END IF;
    ELSE
      tablename := table_ident;

      -- check if table is audited and not ambiguous
      FOR schemaname IN
        SELECT
          schema_name
        FROM
          pgmemento.audit_table_log
        WHERE
          table_name = tablename
          AND upper(txid_range) IS NULL
          AND lower(txid_range) IS NOT NULL
      LOOP
        ntables := ntables + 1;
      END LOOP;
    END IF;

    -- table not found in audit_table_log, so it can be altered without logging
    IF ntables IS NULL OR ntables = 0 THEN
      RETURN;
    END IF;

    IF ntables > 1 THEN
      -- table name is found more than once in audit_table_log
      RAISE EXCEPTION 'Please specify the schema name in the ALTER TABLE command.';
    END IF;

    -- remove schema and table name from DDL string and try to process columns
    ddl_text := replace(ddl_text, schemaname || '.', '');
    ddl_text := replace(ddl_text, tablename, '');
    objs := regexp_split_to_array(ddl_text, E'\\s+');

    -- set 'do_next' to FALSE because first element in objs will not be the column name
    do_next := FALSE;

    FOREACH columnname IN ARRAY objs LOOP
      columnname := replace(columnname, ',', '');
      columnname := substring(columnname, '\S(?:.*\S)*');
      IF do_next THEN
        IF columnname <> 'column' THEN
          IF EXISTS (
            SELECT
              1
            FROM
              pgmemento.audit_column_log c,
              pgmemento.audit_table_log a
            WHERE
              c.audit_table_id = a.id
              AND c.column_name = columnname
              AND a.table_name = tablename
              AND a.schema_name = schemaname
              AND upper(c.txid_range) IS NULL
              AND lower(c.txid_range) IS NOT NULL
          ) THEN
            -- try to log corresponding table event
            IF altering THEN
              e_id := pgmemento.log_ddl_event(tablename, schemaname, 5, 'ALTER COLUMN');
            ELSE
              e_id := pgmemento.log_ddl_event(tablename, schemaname, 6, 'DROP COLUMN');
            END IF;

            -- log data of entire column
            EXECUTE format(
              'INSERT INTO pgmemento.row_log(event_id, audit_id, changes)
                 SELECT $1, t.audit_id, jsonb_build_object($2,t.%I) AS content FROM %I.%I t',
                 columnname, schemaname, tablename) USING e_id, columnname;
          END IF;

          -- done with the column, but there might be more to come
          do_next := FALSE;
        END IF;
      END IF;

      -- is the column name next?
      IF columnname = 'alter'
      OR columnname = 'drop' THEN
        do_next := TRUE;
      ELSIF columnname = 'set' 
      OR columnname = 'collate'
      OR columnname = 'using' THEN
        do_next := FALSE;
      END IF;
    END LOOP;
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
    IF obj.object_type = 'table' AND obj.schema_name NOT LIKE 'pg_temp%' THEN
      -- log create table event
      PERFORM pgmemento.log_ddl_event(split_part(obj.object_identity, '.' ,2), obj.schema_name, 1, 'CREATE TABLE');

      -- start auditing for new table
      PERFORM pgmemento.create_table_audit(split_part(obj.object_identity, '.' ,2), obj.schema_name, 0);
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
      PERFORM pgmemento.unregister_audit_table(obj.object_name, obj.schema_name);
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
  stack TEXT;
  schemaname TEXT;
  tablename TEXT;
  ntables INTEGER := 0;
  e_id INTEGER;
BEGIN
  -- get context in which trigger has been fired
  GET DIAGNOSTICS stack = PG_CONTEXT;
  stack := pgmemento.get_ddl_from_context(stack);

  -- if DDL command was found in context, trigger was fired from inside a function
  IF stack IS NOT NULL THEN
    ddl_text := stack;
  END IF;

  -- lowercase everything
  ddl_text := lower(ddl_text);

  -- check if input string contains comments
  IF ddl_text LIKE '%--%'
  OR ddl_text LIKE '%/*%'
  OR ddl_text LIKE '%*/%' THEN
    RAISE EXCEPTION 'Query contains comments. Unable to log event properly. Please, remove them. Query: %', ddl_text;
  END IF;

  -- extracting the table identifier from the DDL command
  -- remove irrelevant parts and line breaks from the DDL string
  ddl_text := replace(ddl_text, 'drop table ', '');
  ddl_text := replace(ddl_text, 'if exists ', '');
  ddl_text := replace(ddl_text, ' cascade', '');
  ddl_text := replace(ddl_text, ' restrict', '');
  ddl_text := replace(ddl_text, ';', '');
  ddl_text := regexp_replace(ddl_text, '[\r\n]+', ' ', 'g');
  ddl_text := substring(ddl_text, '\S(?:.*\S)*');

  -- get table and schema name
  IF ddl_text LIKE '%.%' THEN
    -- check if table is audited
    SELECT
      table_name,
      schema_name
    INTO
      tablename,
      schemaname
    FROM
      pgmemento.audit_table_log
    WHERE
      table_name = split_part(ddl_text, '.', 2)
      AND schema_name = split_part(ddl_text, '.', 1)
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;

    IF schemaname IS NOT NULL AND tablename IS NOT NULL THEN
      ntables := 1;
    END IF;
  ELSE
    tablename := ddl_text;

    -- check if table is audited and not ambiguous
    FOR schemaname IN
      SELECT
        schema_name
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = tablename
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL
    LOOP
      ntables := ntables + 1;
    END LOOP;
  END IF;

  -- table not found in audit_table_log, so it can be dropped
  IF ntables IS NULL OR ntables = 0 THEN
    RETURN;
  END IF;

  IF ntables > 1 THEN
    -- table name is found more than once in audit_table_log
    RAISE EXCEPTION 'Please specify the schema name in the DROP TABLE command.';
  ELSE
    -- log the whole content of the dropped table as truncated
    e_id :=  pgmemento.log_ddl_event(tablename, schemaname, 8, 'TRUNCATE');

    EXECUTE format(
      'INSERT INTO pgmemento.row_log (event_id, audit_id, changes)
         SELECT $1, audit_id, to_jsonb(%I) AS content FROM %I.%I ORDER BY audit_id',
         tablename, schemaname, tablename) USING e_id;

    -- now log drop table event
    PERFORM pgmemento.log_ddl_event(tablename, schemaname, 9, 'DROP TABLE');
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
LANGUAGE plpgsql STRICT;

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