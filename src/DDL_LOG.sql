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
-- 0.7.9     2021-12-23   session variables starting with letter           ol-teuto
-- 0.7.8     2020-04-13   remove txid from log_table_event                 FKun
-- 0.7.7     2020-04-05   add tags CREATE TABLE AS and SELECT INTO         FKun
-- 0.7.6     2020-03-29   reflect that logging old data is configurable    FKun
-- 0.7.5     2020-03-23   use audit_schema_log to check audit config       FKun
-- 0.7.4     2020-03-07   set SECURITY DEFINER where log tables are used   FKun
-- 0.7.3     2020-02-29   add triggers to log new data in row_log          FKun
-- 0.7.2     2020-02-09   reflect changes on schema and triggers           FKun
-- 0.7.1     2019-02-08   refactoring with new split_table_from_query      FKun
-- 0.7.0     2019-04-14   reflect schema changes in UDFs and VIEWs         FKun
-- 0.6.9     2019-02-24   new function flatten_ddl to remove comments      FKun
-- 0.6.8     2019-02-14   permit drop audit_id in pre alter trigger        FKun
-- 0.6.7     2019-02-09   fetch_ident: improved parsing of DDL context     FKun
-- 0.6.6     2018-11-19   log ADD COLUMN events in pre alter trigger       FKun
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
*   fetch_ident(context TEXT, fetch_count INTEGER DEFAULT 1) RETURNS TEXT
*   flatten_ddl(ddl_command TEXT) RETURNS TEXT
*   get_ddl_from_context(stack TEXT) RETURNS TEXT
*   modify_ddl_log_tables(tablename TEXT, schemaname TEXT, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text) RETURNS SETOF VOID
*   split_table_from_query(INOUT query TEXT, OUT audit_table_name TEXT, OUT audit_schema_name TEXT,
*     OUT audit_table_log_id INTEGER, OUT audit_id_column_name TEXT, OUT audit_old_data BOOLEAN) RETURNS RECORD AS
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
  objs := regexp_split_to_array($1, E'\\r?\\n+');

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
* flatten_ddl
*
* Helper function for to remove comments and line breaks
* from parsed DDL command
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.flatten_ddl(ddl_command TEXT) RETURNS TEXT AS
$$
SELECT
  string_agg(
    CASE WHEN position('--' in ddl_part) > 0 THEN
      left(ddl_part, position('--' in ddl_part) - 1)
    ELSE
      ddl_part
    END,
    ' '
  )
FROM
  unnest(regexp_split_to_array(
    regexp_replace($1, '/\*(.*?)\*/', '', 'g'),
    E'\\r?\\n'
  )) AS s (ddl_part);
$$
LANGUAGE sql STRICT;


/**********************************************************
* fetch_ident
*
* Helper function to return first word from DDL context
* which could be a schema, table or column name
* (incl. quotes, commas and other special characters)
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.fetch_ident(
  context TEXT,
  fetch_count INTEGER DEFAULT 1
  ) RETURNS TEXT AS
$$
DECLARE
  do_next BOOLEAN := TRUE;
  sql_ident TEXT := '';
  quote_pos INTEGER := 1;
  quote_count INTEGER := 0;
  obj_count INTEGER := 0;
  fetch_result TEXT;
BEGIN
  IF $2 <= 0 THEN
    RAISE EXCEPTION 'Second input must be greather than 0!';
  END IF;

  FOR i IN 1..length($1) LOOP
    EXIT WHEN do_next = FALSE;
    -- parse as long there is no space or within quotes
    IF (substr($1,i,1) <> ' ' AND substr($1,i,1) <> ',' AND substr($1,i,1) <> ';')
       OR (substr(sql_ident,quote_pos,1) = '"' AND (
       (right(sql_ident, 1) = '"') = (quote_pos = length(sql_ident))
      ))
    THEN
      sql_ident := sql_ident || substr($1,i,1);
      IF substr($1,i,1) = '"' THEN
        quote_count := quote_count + 1;
        IF quote_count > 2 THEN
          quote_pos := length(sql_ident);
          quote_count := 1;
        ELSE
          quote_pos := position('"' in sql_ident);
        END IF;
      END IF;
    ELSE
      IF length(sql_ident) > 0 THEN
        obj_count := obj_count + 1;
        IF fetch_result IS NULL THEN
          fetch_result := sql_ident;
        ELSE
          fetch_result := fetch_result || ' ' || sql_ident;
        END IF;
        IF obj_count = $2 THEN
          do_next := FALSE;
        END IF;
        sql_ident := '';
        quote_pos := 1;
        quote_count := 0;
      END IF;
    END IF;
  END LOOP;

  IF length(sql_ident) > 0 THEN
    IF fetch_result IS NULL THEN
      fetch_result := sql_ident;
    ELSE
      fetch_result := fetch_result || ' ' || sql_ident;
    END IF;
  END IF;
  RETURN fetch_result;
END;
$$
LANGUAGE plpgsql STRICT;


/**********************************************************
* GET_TABLE_FROM_QUERY
*
* Helper function to retrieve single schema and table name
* as well as matching log_id from audit_table_log
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.split_table_from_query(
  INOUT query TEXT,
  OUT audit_table_name TEXT,
  OUT audit_schema_name TEXT,
  OUT audit_table_log_id INTEGER,
  OUT audit_id_column_name TEXT,
  OUT audit_old_data BOOLEAN
  ) RETURNS RECORD AS
$$
DECLARE
  fetch_next BOOLEAN := TRUE;
  table_ident TEXT := '';
  ntables INTEGER := 0;
  rec RECORD;
BEGIN
  -- remove comments and line breaks from the DDL string
  query := pgmemento.flatten_ddl(query);

  WHILE fetch_next LOOP
    -- extracting the table identifier from the DDL command
    table_ident := pgmemento.fetch_ident(query);

    -- exit loop when nothing has been fetched
    IF table_ident IS NULL OR length(table_ident) = 0 THEN
      EXIT;
    END IF;

    -- shrink ddl_text by table_ident
    query := substr(query, position(table_ident in query) + length(table_ident), length(query));

    IF position('"' IN table_ident) > 0 OR (
         position('"' IN table_ident) = 0 AND (
           lower(table_ident) NOT IN ('drop', 'table', 'if', 'exists')
         )
       )
    THEN
      BEGIN
        -- if table exists, this should work
        PERFORM table_ident::regclass;
        fetch_next := FALSE;

        EXCEPTION
          WHEN undefined_table THEN
            fetch_next := TRUE;
          WHEN invalid_name THEN
            fetch_next := FALSE;
      END;
    END IF;
  END LOOP;

  -- get table and schema name
  IF table_ident LIKE '%.%' THEN
    -- check if table is audited
    SELECT
      table_name,
      schema_name,
      log_id,
      audit_id_column,
      log_old_data
    INTO
      audit_table_name,
      audit_schema_name,
      audit_table_log_id,
      audit_id_column_name,
      audit_old_data
    FROM
      pgmemento.audit_table_log
    WHERE
      table_name = pgmemento.trim_outer_quotes(split_part(table_ident, '.', 2))
      AND schema_name = pgmemento.trim_outer_quotes(split_part(table_ident, '.', 1))
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;

    IF audit_schema_name IS NOT NULL AND audit_table_name IS NOT NULL THEN
      ntables := 1;
    END IF;
  ELSE
    audit_table_name := pgmemento.trim_outer_quotes(table_ident);

    -- check if table is audited and not ambiguous
    FOR rec IN
      SELECT
        schema_name AS schema_name,
        log_id,
        audit_id_column,
        log_old_data
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = audit_table_name
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL
    LOOP
      ntables := ntables + 1;
      IF ntables > 1 THEN
        -- table name is found more than once in audit_table_log
        RAISE EXCEPTION 'Please specify the schema name in the DDL command.';
      END IF;
      audit_schema_name := rec.schema_name;
      audit_table_log_id := rec.log_id;
      audit_id_column_name := rec.audit_id_column;
      audit_old_data := rec.log_old_data;
    END LOOP;
  END IF;

  -- table not found in audit_table_log, so it can be changed without logging
  IF ntables IS NULL OR ntables = 0 THEN
    query := NULL;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;


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
        pg_get_expr(d.adbin, d.adrelid, TRUE) AS column_default,
        a.attnotnull AS not_null,
        numrange(current_setting('pgmemento.t' || txid_current())::numeric, NULL, '(]') AS txid_range
      FROM
        pg_attribute a
      LEFT JOIN
        pg_attrdef d
        ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
      LEFT JOIN (
        SELECT
          a.audit_id_column,
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
      OR acl.audit_id_column = a.attname
      WHERE
        a.attrelid = pgmemento.get_table_oid($1, $2)
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND (acl.ordinal_position IS NULL
         OR (acl.column_name <> a.attname
        AND acl.audit_id_column <> a.attname))
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
          attrelid = pgmemento.get_table_oid($1, $2)
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
      txid_range = numrange(lower(acl.txid_range), current_setting('pgmemento.t' || txid_current())::numeric, '(]')
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
          pg_get_expr(d.adbin, d.adrelid, TRUE) AS column_default,
          a.attnotnull AS not_null,
          $1 AS table_name,
          $2 AS schema_name
        FROM
          pg_attribute a
        LEFT JOIN
          pg_attrdef d
          ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
        WHERE
          a.attrelid = pgmemento.get_table_oid($1, $2)
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
      WHERE
        col.column_default IS DISTINCT FROM acl.column_default
        OR col.not_null IS DISTINCT FROM acl.not_null
        OR col.data_type IS DISTINCT FROM acl.data_type
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
          numrange(current_setting('pgmemento.t' || txid_current())::numeric, NULL, '(]') AS txid_range
        FROM
          updated_columns
      )
    )
    UPDATE
      pgmemento.audit_column_log acl
    SET
      txid_range = numrange(lower(acl.txid_range), current_setting('pgmemento.t' || txid_current())::numeric, '(]')
    FROM
      updated_columns uc
    WHERE
      uc.id = acl.id;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;


/**********************************************************
* MODIFY ROW LOG
*
* Helper function to update row log table for ADD COLUMN
* or ALTER COLUMN events
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.modify_row_log(
  tablename TEXT,
  schemaname TEXT,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
DECLARE
  added_columns TEXT[] := '{}'::text[];
  altered_columns TEXT[] := '{}'::text[];
BEGIN
  SELECT
    array_agg(c_new.column_name) FILTER (WHERE c_old.column_name IS NULL),
    array_agg(c_new.column_name) FILTER (WHERE c_old.column_name IS NOT NULL)
  INTO
    added_columns,
    altered_columns
  FROM
    pgmemento.audit_column_log c_new
  JOIN
    pgmemento.audit_table_log a
    ON a.id = c_new.audit_table_id
   AND a.table_name = $1
   AND a.schema_name = $2
  LEFT JOIN
    pgmemento.audit_column_log c_old
    ON c_old.column_name = c_new.column_name
   AND c_old.ordinal_position = c_new.ordinal_position
   AND c_old.audit_table_id = a.id
   AND upper(c_old.txid_range) = current_setting('pgmemento.t' || txid_current())::numeric
  WHERE
    lower(c_new.txid_range) = current_setting('pgmemento.t' || txid_current())::numeric
    AND upper(c_new.txid_range) IS NULL;

  IF added_columns IS NOT NULL OR array_length(added_columns, 1) > 0 THEN
    PERFORM pgmemento.log_new_table_state(added_columns, $1, $2,
      concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id('ADD COLUMN'), $1, $2),
      $3
    );
  END IF;

  IF altered_columns IS NOT NULL OR array_length(altered_columns, 1) > 0 THEN
    PERFORM pgmemento.log_new_table_state(altered_columns, $1, $2,
      concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id('ALTER COLUMN'), $1, $2),
      $3
    );
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;


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
  fetch_next BOOLEAN := TRUE;
  schema_ident TEXT;
  rec RECORD;
  table_event_key TEXT;
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

  -- remove comments and line breaks from the DDL string
  ddl_text := pgmemento.flatten_ddl(ddl_text);

  WHILE fetch_next LOOP
    -- extracting the schema identifier from the DDL command
    schema_ident := pgmemento.fetch_ident(ddl_text);

    -- exit loop when nothing has been fetched
    IF schema_ident IS NULL OR length(schema_ident) = 0 THEN
      EXIT;
    END IF;

    -- shrink ddl_text by schema_ident
    ddl_text := substr(ddl_text, position(schema_ident in ddl_text) + length(schema_ident), length(ddl_text));

    IF position('"' IN schema_ident) > 0 OR (
         position('"' IN schema_ident) = 0 AND (
           lower(schema_ident) NOT IN ('drop', 'schema', 'if', 'exists')
         )
       )
    THEN
      SELECT NOT EXISTS (
        SELECT
          1
        FROM
          pg_namespace
        WHERE
          nspname = pgmemento.trim_outer_quotes(schema_ident)
      )
      INTO
        fetch_next;
    END IF;
  END LOOP;

  IF EXISTS (
    SELECT 1 FROM pgmemento.audit_schema_log
     WHERE schema_name = schema_ident
       AND upper(txid_range) IS NULL
  ) THEN
    -- truncate tables to log the data
    FOR rec IN
      SELECT
        n.nspname AS schemaname,
        c.relname AS tablename,
        a.audit_id_column,
        a.log_old_data
      FROM
        pg_class c
      JOIN
        pg_namespace n
        ON n.oid = c.relnamespace
      JOIN
        pgmemento.audit_table_log a
        ON a.table_name = c.relname
       AND a.schema_name = n.nspname
       AND upper(a.txid_range) IS NULL
       AND lower(a.txid_range) IS NOT NULL
      JOIN
        pgmemento.audit_tables_dependency d
        ON d.schemaname = a.table_name
       AND d.tablename = a.schema_name
      WHERE
        n.nspname = pgmemento.trim_outer_quotes(schema_ident)
      ORDER BY
        n.oid,
        d.depth DESC
    LOOP
      -- log the whole content of the dropped table as truncated
      table_event_key := pgmemento.log_table_event(rec.tablename, rec.schemaname, 'TRUNCATE');
      IF rec.log_old_data THEN
        PERFORM pgmemento.log_old_table_state('{}'::text[], rec.tablename, rec.schemaname, table_event_key, rec.audit_id_column);
      END IF;

      -- now log drop table event
      PERFORM pgmemento.log_table_event(rec.tablename, rec.schemaname, 'DROP TABLE');

      -- unregister table from log tables
      PERFORM pgmemento.unregister_audit_table(rec.tablename, rec.schemaname);
    END LOOP;
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
  tid INTEGER;
  table_log_id INTEGER;
  tg_tablename TEXT;
  tg_schemaname TEXT;
  current_table_name TEXT;
  current_schema_name TEXT;
  current_audit_id_column TEXT;
  current_log_old_data BOOLEAN;
  current_log_new_data BOOLEAN;
  event_op_id SMALLINT;
BEGIN
  tid := current_setting('pgmemento.t' || txid_current())::int;

  FOR obj IN
    SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    -- get table from trigger variable - remove quotes if exists
    tg_tablename := pgmemento.trim_outer_quotes(split_part(obj.object_identity, '.' ,2));
    tg_schemaname := pgmemento.trim_outer_quotes(split_part(obj.object_identity, '.' ,1));

    BEGIN
      -- check if event required to remember log_id from audit_table_log (e.g. RENAME)
      table_log_id := current_setting('pgmemento.' || obj.object_identity)::int;

      -- get old table and schema name for this log_id
      SELECT
        table_name,
        schema_name,
        audit_id_column,
        log_old_data,
        log_new_data
      INTO
        current_table_name,
        current_schema_name,
        current_audit_id_column,
        current_log_old_data,
        current_log_new_data
      FROM
        pgmemento.audit_table_log
      WHERE
        log_id = table_log_id
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL;

      EXCEPTION
        WHEN others THEN
          NULL; -- no log id set or no open txid_range. Use names from obj.
    END;

    IF current_table_name IS NULL THEN
      current_table_name := tg_tablename;
      current_schema_name := tg_schemaname;

      -- get current settings for audit table
      SELECT
        audit_id_column,
        log_old_data,
        log_new_data
      INTO
        current_audit_id_column,
        current_log_old_data,
        current_log_new_data
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = current_table_name
        AND schema_name = current_schema_name
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL;
    ELSE
      -- table got renamed and so remember audit_id_column and logging behavior to register renamed version
      PERFORM set_config('pgmemento.' || tg_schemaname || '.' || tg_tablename || '.audit_id.t' || txid_current(), current_audit_id_column, TRUE);
      PERFORM set_config('pgmemento.' || tg_schemaname || '.' || tg_tablename || '.log_data.t' || txid_current(),
        CASE WHEN current_log_old_data THEN 'old=true,' ELSE 'old=false,' END ||
        CASE WHEN current_log_new_data THEN 'new=true' ELSE 'new=false' END, TRUE);
    END IF;

    -- modify audit_table_log and audit_column_log if DDL events happened
    SELECT
      op_id
    INTO
      event_op_id
    FROM
      pgmemento.table_event_log
    WHERE
      transaction_id = tid
      AND table_name = current_table_name
      AND schema_name = current_schema_name
      AND op_id IN (12, 2, 21, 22, 5, 6);

    IF event_op_id IS NOT NULL THEN
      PERFORM pgmemento.modify_ddl_log_tables(tg_tablename, tg_schemaname);
    END IF;

    -- update row_log to with new log data
    IF current_log_new_data AND (event_op_id = 2 OR event_op_id = 5) THEN
      PERFORM pgmemento.modify_row_log(tg_tablename, tg_schemaname, current_audit_id_column);
    END IF;
  END LOOP;

  EXCEPTION
    WHEN undefined_object THEN
      RETURN; -- no event has been logged, yet
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
  fetch_next BOOLEAN := TRUE;
  table_ident TEXT := '';
  rec RECORD;
  tablename TEXT;
  schemaname TEXT;
  table_log_id INTEGER;
  ntables INTEGER := 0;
  audit_id_columnname TEXT;
  log_old_data BOOLEAN;
  column_candidate TEXT;
  columnname TEXT;
  event_type TEXT;
  column_type TEXT;
  added_columns BOOLEAN := FALSE;
  dropped_columns TEXT[] := '{}'::text[];
  altered_columns TEXT[] := '{}'::text[];
  altered_columns_log TEXT[] := '{}'::text[];
  table_event_key TEXT;
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

  -- are columns renamed, altered or dropped
  IF lower(ddl_text) LIKE '% type %' OR
     lower(ddl_text) LIKE '% using %' OR
     lower(ddl_text) LIKE '% not null%' OR
     lower(ddl_text) LIKE '%default%' OR
     lower(ddl_text) LIKE '%add column%' OR
     lower(ddl_text) LIKE '%add %' OR
     lower(ddl_text) LIKE '%drop column%' OR
     lower(ddl_text) LIKE '%drop %' OR
     lower(ddl_text) LIKE '%rename %'
  THEN
    -- remove table name from ddl_text
    SELECT
      query,
      audit_table_name,
      audit_schema_name,
      audit_table_log_id,
      audit_id_column_name,
      audit_old_data
    INTO
      ddl_text,
      tablename,
      schemaname,
      table_log_id,
      audit_id_columnname,
      log_old_data
    FROM
      pgmemento.split_table_from_query(ddl_text);

    -- if table is not audited ddl_text will be NULL
    IF ddl_text IS NULL THEN
      RETURN;
    END IF;

    -- check if table got renamed and log event if yes
    IF lower(ddl_text) LIKE ' rename to%' THEN
      PERFORM pgmemento.log_table_event(tablename, schemaname, 'RENAME TABLE');
      -- make sure to quote ident as variable will later be read
      -- from obj trigger variable which can come with quotes
      PERFORM set_config(
        'pgmemento.' || quote_ident(schemaname) || '.' ||
        pgmemento.fetch_ident(substr(ddl_text,11,length(ddl_text))),
        table_log_id::text,
        TRUE
      );
      RETURN;
    END IF;

    -- start parsing columns
    WHILE length(ddl_text) > 0 LOOP
      -- process each single following word in DDL string
      -- hope to find event types, column names and data types
      column_candidate := pgmemento.fetch_ident(ddl_text);

      -- exit loop when nothing has been fetched
      IF column_candidate IS NULL OR length(column_candidate) = 0 THEN
        EXIT;
      END IF;

      -- shrink ddl_text by column_candidate
      ddl_text := substr(ddl_text, position(column_candidate in ddl_text) + length(column_candidate), length(ddl_text));

      -- if keyword 'column' is found, do not reset event type
      IF lower(column_candidate) <> 'column' THEN
        IF event_type IS NOT NULL THEN
          IF event_type = 'ADD' THEN
            -- after ADD we might find a column name
            -- if next word is a data type it must be an ADD COLUMN event
            -- otherwise it could also be an ADD constraint event, which is not audited
            column_type := pgmemento.fetch_ident(ddl_text);
            ddl_text := substr(ddl_text, position(column_type in ddl_text) + length(column_type), length(ddl_text));

            FOR i IN 0..length(ddl_text) LOOP
              EXIT WHEN added_columns = TRUE;
              BEGIN
                IF current_setting('server_version_num')::int < 90600 THEN
                  IF to_regtype((column_type || substr(ddl_text, 1, i))::cstring) IS NOT NULL THEN
                    added_columns := TRUE;
                  END IF;
                ELSE
                  IF to_regtype(column_type || substr(ddl_text, 1, i)) IS NOT NULL THEN
                    added_columns := TRUE;
                  END IF;
                END IF;

                EXCEPTION
                  WHEN syntax_error THEN
                    CONTINUE;
              END;
            END LOOP;
          ELSE
            IF column_candidate = audit_id_columnname THEN
              columnname := column_candidate;
            ELSE
              SELECT
                c.column_name
              INTO
                columnname
              FROM
                pgmemento.audit_column_log c,
                pgmemento.audit_table_log a
              WHERE
                c.audit_table_id = a.id
                AND c.column_name = pgmemento.trim_outer_quotes(column_candidate)
                AND a.table_name = tablename
                AND a.schema_name = schemaname
                AND upper(c.txid_range) IS NULL
                AND lower(c.txid_range) IS NOT NULL;
            END IF;

            IF columnname IS NOT NULL THEN
              CASE event_type
                WHEN 'RENAME' THEN
                  IF column_candidate = audit_id_columnname THEN
                    RAISE EXCEPTION 'Renaming the % column is not possible!', audit_id_columnname;
                  END IF;
                  -- log event as only one RENAME COLUMN action is possible per table per transaction
                  PERFORM pgmemento.log_table_event(tablename, schemaname, 'RENAME COLUMN');
                WHEN 'DROP' THEN
                  dropped_columns := array_append(dropped_columns, columnname);
                WHEN 'ALTER' THEN
                  altered_columns := array_append(altered_columns, columnname);

                  -- check if logging column content is really required
                  column_type := pgmemento.fetch_ident(ddl_text, 6);
                  IF lower(column_type) LIKE '% collate %' OR lower(column_type) LIKE '% using %' THEN
                    altered_columns_log := array_append(altered_columns_log, columnname);
                  END IF;
                ELSE
                  RAISE NOTICE 'Event type % unknown', event_type;
              END CASE;
            END IF;
          END IF;
        END IF;

        -- when event is found column name might be next
        CASE lower(column_candidate)
          WHEN 'add' THEN
            event_type := 'ADD';
          WHEN 'rename' THEN
            event_type := 'RENAME';
          WHEN 'alter' THEN
            event_type := 'ALTER';
          WHEN 'drop' THEN
            event_type := 'DROP';
          ELSE
            event_type := NULL;
        END CASE;
      END IF;
    END LOOP;

    IF added_columns THEN
      -- log ADD COLUMN table event
      table_event_key := pgmemento.log_table_event(tablename, schemaname, 'ADD COLUMN');
    END IF;

    IF array_length(altered_columns, 1) > 0 THEN
      -- log ALTER COLUMN table event
      table_event_key := pgmemento.log_table_event(tablename, schemaname, 'ALTER COLUMN');

      -- log data of entire column(s)
      IF array_length(altered_columns_log, 1) > 0 AND log_old_data THEN
        PERFORM pgmemento.log_old_table_state(altered_columns_log, tablename, schemaname, table_event_key, audit_id_columnname);
      END IF;
    END IF;

    IF array_length(dropped_columns, 1) > 0 THEN
      IF NOT (audit_id_columnname = ANY(dropped_columns)) THEN
        -- log DROP COLUMN table event
        table_event_key := pgmemento.log_table_event(tablename, schemaname, 'DROP COLUMN');

        -- log data of entire column(s)
        IF log_old_data THEN
          PERFORM pgmemento.log_old_table_state(dropped_columns, tablename, schemaname, table_event_key, audit_id_columnname);
        END IF;
      ELSE
        RAISE EXCEPTION 'To remove the % column, please use pgmemento.drop_table_audit!', audit_id_columnname;
      END IF;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_create_post_trigger
*
* Procedure that is called AFTER new tables have been created
**********************************************************/
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


/**********************************************************
* EVENT TRIGGER PROCEDURE table_drop_post_trigger
*
* Procedure that is called AFTER tables have been dropped
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_drop_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj RECORD;
  tid INTEGER;
  tablename TEXT;
  schemaname TEXT;
BEGIN
  FOR obj IN
    SELECT * FROM pg_event_trigger_dropped_objects()
  LOOP
    IF obj.object_type = 'table' AND NOT obj.is_temporary THEN
      BEGIN
        tid := current_setting('pgmemento.t' || txid_current())::int;

        -- remove quotes if exists
        tablename := pgmemento.trim_outer_quotes(split_part(obj.object_identity, '.' ,2));
        schemaname := pgmemento.trim_outer_quotes(split_part(obj.object_identity, '.' ,1));

        -- if DROP AUDIT_ID event exists for table in the current transaction
        -- only create a DROP TABLE event, because auditing has already stopped
        IF EXISTS (
          SELECT
            1
          FROM
            pgmemento.table_event_log
          WHERE
            transaction_id = tid
            AND table_name = tablename
            AND schema_name = schemaname
            AND op_id = 81  -- DROP AUDIT_ID event
        ) THEN
          PERFORM pgmemento.log_table_event(
            tablename,
            schemaname,
            'DROP TABLE'
          );
        ELSE
          -- update txid_range for removed table in audit_table_log table
          PERFORM pgmemento.unregister_audit_table(
            tablename,
            schemaname
          );
        END IF;

        EXCEPTION
          WHEN undefined_object THEN
            RETURN; -- no event has been logged, yet. Thus, table was not audited.
      END;
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
  audit_id_columnname TEXT;
  log_old_data BOOLEAN;
  table_event_key TEXT;
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

  -- remove table name from ddl_text
  SELECT
    query,
    audit_table_name,
    audit_schema_name,
    audit_id_column_name,
    audit_old_data
  INTO
    ddl_text,
    tablename,
    schemaname,
    audit_id_columnname,
    log_old_data
  FROM
    pgmemento.split_table_from_query(ddl_text);

  -- if table is not audited ddl_text will be NULL
  IF ddl_text IS NULL THEN
    RETURN;
  END IF;

  -- log the whole content of the dropped table as truncated
  table_event_key :=  pgmemento.log_table_event(tablename, schemaname, 'TRUNCATE');
  IF log_old_data THEN
    PERFORM pgmemento.log_old_table_state('{}'::text[], tablename, schemaname, table_event_key, audit_id_columnname);
  END IF;

  -- now log drop table event
  PERFORM pgmemento.log_table_event(tablename, schemaname, 'DROP TABLE');
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


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
      WHERE evtname = 'pgmemento_schema_drop_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER pgmemento_schema_drop_pre_trigger ON ddl_command_start
      WHEN TAG IN ('DROP SCHEMA')
        EXECUTE PROCEDURE pgmemento.schema_drop_pre_trigger();
  END IF;

  -- Create event trigger for ALTER TABLE events to update 'audit_column_log' table
  -- after table is altered
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'pgmemento_table_alter_post_trigger'
  ) THEN
    CREATE EVENT TRIGGER pgmemento_table_alter_post_trigger ON ddl_command_end
      WHEN TAG IN ('ALTER TABLE')
        EXECUTE PROCEDURE pgmemento.table_alter_post_trigger();
  END IF;

  -- Create event trigger for ALTER TABLE events to log data
  -- before table is altered
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'pgmemento_table_alter_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER pgmemento_table_alter_pre_trigger ON ddl_command_start
      WHEN TAG IN ('ALTER TABLE')
        EXECUTE PROCEDURE pgmemento.table_alter_pre_trigger();
  END IF;

  -- Create event trigger for CREATE TABLE events to automatically start auditing on new tables
  -- The user can decide if he wants this behaviour during initializing pgMemento.
  IF $1 THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_event_trigger
        WHERE evtname = 'pgmemento_table_create_post_trigger'
    ) THEN
      CREATE EVENT TRIGGER pgmemento_table_create_post_trigger ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
          EXECUTE PROCEDURE pgmemento.table_create_post_trigger();
    END IF;
  END IF;

  -- Create event trigger for DROP TABLE events to update tables 'audit_table_log' and 'audit_column_log'
  -- after table is dropped
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'pgmemento_table_drop_post_trigger'
  ) THEN
    CREATE EVENT TRIGGER pgmemento_table_drop_post_trigger ON sql_drop
      WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE pgmemento.table_drop_post_trigger();
  END IF;

  -- Create event trigger for DROP TABLE events to log data
  -- before it is lost
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'pgmemento_table_drop_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER pgmemento_table_drop_pre_trigger ON ddl_command_start
      WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE pgmemento.table_drop_pre_trigger();
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.drop_schema_event_trigger() RETURNS SETOF VOID AS
$$
  DROP EVENT TRIGGER IF EXISTS pgmemento_schema_drop_pre_trigger;
  DROP EVENT TRIGGER IF EXISTS pgmemento_table_alter_post_trigger;
  DROP EVENT TRIGGER IF EXISTS pgmemento_table_alter_pre_trigger;
  DROP EVENT TRIGGER IF EXISTS pgmemento_table_create_post_trigger;
  DROP EVENT TRIGGER IF EXISTS pgmemento_table_drop_post_trigger;
  DROP EVENT TRIGGER IF EXISTS pgmemento_table_drop_pre_trigger;
$$
LANGUAGE sql;
