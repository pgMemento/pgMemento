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
-- 0.6.5     2018-11-10   better treatment of dropping audit_id column     FKun
-- 0.6.4     2018-11-01   reflect range bounds change in audit tables      FKun
-- 0.6.3     2018-10-25   bool argument in create_schema_event_trigger     FKun
-- 0.6.2     2018-09-24   altering or dropping multiple columns at once    FKun
--                        produces only one JSONB log
-- 0.6.1     2018-07-24   RENAME events now appear in table_event_log      FKun
-- 0.6.0     2018-07-16   now calling log_table_event for ddl events       FKun
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
*   create_schema_event_trigger(trigger_create_table BOOLEAN DEFAULT FALSE) RETURNS SETOF VOID
*   drop_schema_event_trigger() RETURNS SETOF VOID
*   get_ddl_from_context(stack TEXT) RETURNS TEXT
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
BEGIN
  -- get id from audit_table_log for given table
  tab_id := pgmemento.register_audit_table($1, $2);

  IF tab_id IS NOT NULL THEN
    IF EXISTS (
      SELECT
        1
      FROM
        pg_attribute a
      LEFT JOIN (
        SELECT
          c.ordinal_position
        FROM
          pgmemento.audit_table_log a
        JOIN
          pgmemento.audit_column_log c
          ON c.audit_table_id = a.id
        WHERE
          a.id = tab_id
          AND upper(a.txid_range) IS NULL
          AND lower(a.txid_range) IS NOT NULL
          AND upper(c.txid_range) IS NULL
          AND lower(c.txid_range) IS NOT NULL
        ) acl
        ON acl.ordinal_position = a.attnum
      WHERE
        a.attrelid = ($2 || '.' || $1)::regclass::oid
        AND a.attname <> 'audit_id'
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND acl.ordinal_position IS NULL
      LIMIT 1
    ) THEN
      -- EVENT: New column created
      PERFORM pgmemento.log_table_event(txid_current(), (schemaname || '.' || tablename)::regclass::oid, 'ADD COLUMN');
    END IF;

    -- insert columns that do not exist in audit_column_log table
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
        numrange(current_setting('pgmemento.' || txid_current())::numeric, NULL, '(]') AS txid_range
      FROM
        pg_attribute a
      LEFT JOIN
        pg_attrdef d
        ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
      LEFT JOIN (
        SELECT
          c.ordinal_position,
          c.column_name
        FROM
          pgmemento.audit_table_log a
        JOIN
          pgmemento.audit_column_log c
          ON c.audit_table_id = a.id
        WHERE
          a.id = tab_id
          AND upper(a.txid_range) IS NULL
          AND lower(a.txid_range) IS NOT NULL
          AND upper(c.txid_range) IS NULL
          AND lower(c.txid_range) IS NOT NULL
        ) acl
      ON acl.ordinal_position = a.attnum
      WHERE
        a.attrelid = ($2 || '.' || $1)::regclass::oid
        AND a.attname <> 'audit_id'
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND (acl.ordinal_position IS NULL
         OR acl.column_name <> a.attname)
      ORDER BY
        a.attnum
    );

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
          attrelid = ($2 || '.' || $1)::regclass::oid
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
      txid_range = numrange(lower(acl.txid_range), current_setting('pgmemento.' || txid_current())::numeric, '(]') 
    FROM
      dropped_columns dc
    WHERE
      acl.id = dc.id;

    -- EVENT: Column altered
    -- update txid_range for updated columns and insert new versions into audit_column_log table
    WITH updated_columns AS (
      SELECT
        acl.id,
        acl.audit_table_id,
        col.column_name,
        col.ordinal_position,
        col.data_type,
        col.column_default,
        col.not_null
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
          a.attrelid = ($2 || '.' || $1)::regclass::oid
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
          numrange(current_setting('pgmemento.' || txid_current())::numeric, NULL, '(]') AS txid_range
        FROM
          updated_columns
      )
    )
    UPDATE
      pgmemento.audit_column_log acl
    SET
      txid_range = numrange(lower(acl.txid_range), current_setting('pgmemento.' || txid_current())::numeric, '(]') 
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
    -- check if context starts with DROP command
    IF lower(stack) NOT LIKE 'drop%' THEN
      RAISE EXCEPTION 'Could not parse DROP SCHEMA event! SQL context is: %', stack;
    END IF;
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
    e_id := pgmemento.log_table_event(txid_current(), (rec.schemaname || '.' || rec.tablename)::regclass::oid, 'TRUNCATE');
    PERFORM pgmemento.log_table_state(e_id, '{}'::text[], rec.tablename, rec.schemaname);

    -- now log drop table event
    PERFORM pgmemento.log_table_event(txid_current(), (rec.schemaname || '.' || rec.tablename)::regclass::oid, 'DROP TABLE');

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
  do_next BOOLEAN := TRUE;
  table_ident TEXT := '';
  schemaname TEXT;
  tablename TEXT;
  ntables INTEGER := 0;
  objs TEXT[];
  columnname TEXT;
  event_type TEXT;
  altered_columns TEXT[] := '{}'::text[];
  dropped_columns TEXT[] := '{}'::text[];
  e_id INTEGER;
BEGIN
  -- get context in which trigger has been fired
  GET DIAGNOSTICS stack = PG_CONTEXT;
  stack := pgmemento.get_ddl_from_context(stack);

  -- if DDL command was found in context, trigger was fired from inside a function
  IF stack IS NOT NULL THEN
    -- check if context starts with ALTER command
    IF lower(stack) NOT LIKE 'alter%' THEN
      RAISE EXCEPTION 'Could not parse ALTER TABLE event! SQL context is: %', stack;
    END IF;
    ddl_text := stack;
  END IF;

  -- lowercase everything
  ddl_text := lower(ddl_text);

  -- are columns renamed, altered or dropped
  IF (ddl_text LIKE '%using%' AND NOT ddl_text LIKE '%using index%') OR
     (ddl_text LIKE '%drop column%' OR ddl_text LIKE '%drop %' OR ddl_text LIKE '%rename %') THEN
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

    -- check if table got renamed and log event if yes
    IF ddl_text LIKE '%rename to%' THEN
      PERFORM pgmemento.log_table_event(txid_current(), table_ident::regclass::oid, 'RENAME TABLE');
      RETURN;
    END IF;

    -- remove schema and table name from DDL string and try to process columns
    ddl_text := replace(ddl_text, schemaname || '.', '');
    ddl_text := replace(ddl_text, tablename, '');
    objs := regexp_split_to_array(ddl_text, E'\\s+');

    FOREACH columnname IN ARRAY objs LOOP
      columnname := replace(columnname, ',', '');
      columnname := substring(columnname, '\S(?:.*\S)*');
      -- if keyword 'column' is found, do not reset event type
      IF columnname <> 'column' THEN
        IF event_type IS NOT NULL THEN
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
            CASE event_type
              WHEN 'RENAME' THEN
                -- log event as only one RENAME COLUMN action is possible per table per transaction
                PERFORM pgmemento.log_table_event(txid_current(), (schemaname || '.' || tablename)::regclass::oid, 'RENAME COLUMN');
              WHEN 'ALTER' THEN
                altered_columns := array_append(altered_columns, columnname);
              WHEN 'DROP' THEN
                dropped_columns := array_append(dropped_columns, columnname);
              ELSE
                RAISE NOTICE 'Event type % unknown', event_type;
            END CASE;
          END IF;
        END IF;
        
        -- when event is found column name might be next
        CASE columnname
          WHEN 'rename' THEN event_type := 'RENAME';
          WHEN 'alter' THEN event_type := 'ALTER';
          WHEN 'drop' THEN event_type := 'DROP';
          ELSE event_type := NULL;
        END CASE;
      END IF;
    END LOOP;

    IF array_length(altered_columns, 1) > 0 THEN
      -- log ALTER COLUMN table event
      e_id := pgmemento.log_table_event(txid_current(), (schemaname || '.' || tablename)::regclass::oid, 'ALTER COLUMN');

      -- log data of entire column(s)
      PERFORM pgmemento.log_table_state(e_id, altered_columns, tablename, schemaname);
    END IF;

    IF array_length(dropped_columns, 1) > 0 THEN
      IF 'audit_id' <> ANY(dropped_columns) THEN
        -- log DROP COLUMN table event
        e_id := pgmemento.log_table_event(txid_current(), (schemaname || '.' || tablename)::regclass::oid, 'DROP COLUMN');

        -- log data of entire column(s)
        PERFORM pgmemento.log_table_state(e_id, dropped_columns, tablename, schemaname);
      END IF;
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
    IF obj.object_type = 'table' AND obj.schema_name NOT LIKE 'pg_temp%' THEN
      -- log as 'create table' event
      PERFORM pgmemento.log_table_event(txid_current(),(obj.schema_name || '.' || split_part(obj.object_identity, '.' ,2))::regclass::oid, 'CREATE TABLE');

      -- start auditing for new table
      PERFORM pgmemento.create_table_audit(split_part(obj.object_identity, '.' ,2), obj.schema_name, FALSE);
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
    -- check if context starts with DROP command
    IF lower(stack) NOT LIKE 'drop%' THEN
      RAISE EXCEPTION 'Could not parse DROP TABLE event! SQL context is: %', stack;
    END IF;
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
    e_id :=  pgmemento.log_table_event(txid_current(), (schemaname || '.' || tablename)::regclass::oid, 'TRUNCATE');
    PERFORM pgmemento.log_table_state(e_id, '{}'::text[], tablename, schemaname);

    -- now log drop table event
    PERFORM pgmemento.log_table_event(txid_current(), (schemaname || '.' || tablename)::regclass::oid, 'DROP TABLE');
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
  trigger_create_table BOOLEAN DEFAULT FALSE
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
  IF $1 THEN
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
