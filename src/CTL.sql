-- CTL.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script to start auditing for a given database schema
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.3.2     2020-04-16   better support for quoted schemas              FKun
-- 0.3.1     2020-04-11   add drop endpoint                              FKun
-- 0.3.0     2020-03-29   make logging of old data configurable, too     FKun
-- 0.2.0     2020-03-21   write changes to audit_schema_log              FKun
-- 0.1.0     2020-03-15   initial commit                                 FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   drop(schemaname TEXT DEFAULT 'public'::text, keep_log BOOLEAN DEFAULT TRUE, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   init(schemaname TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_old_data BOOLEAN DEFAULT TRUE, log_new_data BOOLEAN DEFAULT FALSE, log_state BOOLEAN DEFAULT FALSE,
*     trigger_create_table BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   start(schemaname TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_old_data BOOLEAN DEFAULT TRUE, log_new_data BOOLEAN DEFAULT FALSE, trigger_create_table BOOLEAN DEFAULT FALSE,
*     except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   stop(schemaname TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   version(OUT full_version TEXT, OUT major_version INTEGER, OUT minor_version INTEGER, OUT build_id TEXT) RETURNS RECORD
*
***********************************************************/

CREATE OR REPLACE FUNCTION pgmemento.init(
  schemaname TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE,
  log_state BOOLEAN DEFAULT FALSE,
  trigger_create_table BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS TEXT AS
$$
DECLARE
  schema_quoted TEXT;
  txid_log_id INTEGER;
BEGIN
  -- make sure schema is quoted no matter how it is passed to init
  schema_quoted := quote_ident(pgmemento.trim_outer_quotes($1));

  -- check if schema is already logged
  IF EXISTS (
    SELECT
      1
    FROM
      pgmemento.audit_schema_log
    WHERE
      schema_name = pgmemento.trim_outer_quotes($1)
      AND upper(txid_range) IS NULL
    ORDER BY
      id DESC
    LIMIT 1
  ) THEN
    RETURN format('pgMemento is already intialized for %s schema.', schema_quoted);
  END IF;

  -- log transaction that initializes pgMemento for a schema
  -- and store configuration in session_info object
  PERFORM set_config(
    'pgmemento.session_info',
    format('{"pgmemento_init": {"schema_name": %L, "default_audit_id_column": %L, "default_log_old_data": %L, "default_log_new_data": %L, "log_state": %L, "trigger_create_table": %L, "except_tables": %L}}',
    to_jsonb($1), to_jsonb($2), to_jsonb($3), to_jsonb($4), to_jsonb($5), to_jsonb($6), to_jsonb($7))::text,
    TRUE
  );
  txid_log_id := pgmemento.log_transaction(txid_current());

  -- insert new entry in audit_schema_log
  INSERT INTO pgmemento.audit_schema_log
    (log_id, schema_name, default_audit_id_column, default_log_old_data, default_log_new_data, trigger_create_table, txid_range)
  VALUES
    (nextval('pgmemento.schema_log_id_seq'), $1, $2, $3, $4, $6,
     numrange(txid_log_id, NULL, '(]'));

  -- create event trigger to log schema changes
  PERFORM pgmemento.create_schema_event_trigger($6);

  -- start auditing for tables in given schema'
  PERFORM pgmemento.create_schema_audit(pgmemento.trim_outer_quotes($1), $2, $3, $4, $5, $6, $7);

  RETURN format('pgMemento is initialized for %s schema.', schema_quoted);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.start(
  schemaname TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE,
  trigger_create_table BOOLEAN DEFAULT FALSE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS TEXT AS
$$
DECLARE
  schema_quoted TEXT;
  current_audit_schema_log pgmemento.audit_schema_log%ROWTYPE;
  txid_log_id INTEGER;
BEGIN
  -- make sure schema is quoted no matter how it is passed to start
  schema_quoted := quote_ident(pgmemento.trim_outer_quotes($1));

  -- check if schema is already logged
  SELECT
    *
  INTO
    current_audit_schema_log
  FROM
    pgmemento.audit_schema_log
  WHERE
    schema_name = pgmemento.trim_outer_quotes($1)
    AND upper(txid_range) IS NULL;

  IF current_audit_schema_log.id IS NULL THEN
    RETURN format('pgMemento is not yet intialized for %s schema. Run init first.', schema_quoted);
  END IF;

  -- log transaction that starts pgMemento for a schema
  -- and store configuration in session_info object
  PERFORM set_config(
    'pgmemento.session_info',
     format('{"pgmemento_start": {"schema_name": %L, "default_audit_id_column": %L, "default_log_old_data": %L, "default_log_new_data": %L, "trigger_create_table": %L, "except_tables": %L}}',
       to_jsonb($1), to_jsonb($2), to_jsonb($3), to_jsonb($4), to_jsonb($5), to_jsonb($6))::text,
     TRUE
  );
  txid_log_id := pgmemento.log_transaction(txid_current());

  -- configuration differs, so close txid_range for audit_schema_log entry
  IF current_audit_schema_log.default_log_old_data != $3
     OR current_audit_schema_log.default_log_new_data != $4
     OR current_audit_schema_log.trigger_create_table != $5
  THEN
    UPDATE pgmemento.audit_schema_log
       SET txid_range = numrange(lower(txid_range), txid_log_id::numeric, '(]')
     WHERE id = current_audit_schema_log.id;

    -- create new entry in audit_schema_log
    INSERT INTO pgmemento.audit_schema_log
      (log_id, schema_name, default_audit_id_column, default_log_old_data, default_log_new_data, trigger_create_table, txid_range)
    VALUES
      (current_audit_schema_log.log_id, $1, $2, $3, $4, $5,
       numrange(txid_log_id, NULL, '(]'));

    -- drop active triggers as they need to be replaced
    PERFORM
      pgmemento.drop_table_log_trigger(c.relname, $1)
    FROM
      pg_class c
    JOIN
      pg_namespace n
      ON c.relnamespace = n.oid
    JOIN pgmemento.audit_tables at
      ON at.tablename = c.relname
     AND at.schemaname = n.nspname
     AND tg_is_active
    WHERE
      n.nspname = pgmemento.trim_outer_quotes($1)
      AND c.relkind = 'r'
      AND c.relname <> ALL (COALESCE($6,'{}'));
  END IF;

  -- enable triggers where they are not active
  PERFORM
    pgmemento.create_table_log_trigger(c.relname, $1, at.audit_id_column, $3)
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON c.relnamespace = n.oid
  JOIN pgmemento.audit_tables at
    ON at.tablename = c.relname
   AND at.schemaname = n.nspname
   AND NOT tg_is_active
  WHERE
    n.nspname = pgmemento.trim_outer_quotes($1)
    AND c.relkind = 'r'
    AND c.relname <> ALL (COALESCE($6,'{}'));

  --create event triggers if they were not enabled for schema
  IF $3 AND NOT current_audit_schema_log.trigger_create_table THEN
    PERFORM pgmemento.create_schema_event_trigger($5);
  END IF;

  RETURN format('pgMemento is started for %s schema.', schema_quoted);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.stop(
  schemaname TEXT DEFAULT 'public'::text,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS TEXT AS
$$
DECLARE
  schema_quoted TEXT;
BEGIN
  -- make sure schema is quoted no matter how it is passed to stop
  schema_quoted := quote_ident(pgmemento.trim_outer_quotes($1));

  -- check if schema is already logged
  IF NOT EXISTS (
    SELECT
      1
    FROM
      pgmemento.audit_schema_log
    WHERE
      schema_name = pgmemento.trim_outer_quotes($1)
      AND upper(txid_range) IS NULL
  ) THEN
    RETURN format('pgMemento is not intialized for %s schema. Nothing to stop.', schema_quoted);
  END IF;

  -- log transaction that stops pgMemento for a schema
  -- and store configuration in session_info object
  PERFORM set_config(
    'pgmemento.session_info',
     format('{"pgmemento_stop": {"schema_name": %L, "except_tables": %L}}',
       to_jsonb($1), to_jsonb($2))::text,
     TRUE
  );
  PERFORM pgmemento.log_transaction(txid_current());

  -- drop log triggers for all tables except those from passed array
  PERFORM pgmemento.drop_schema_log_trigger(pgmemento.trim_outer_quotes($1), $2);

  IF $2 IS NOT NULL AND array_length($2, 1) > 0 THEN
    -- check if excluded tables are still audited
    IF EXISTS (
      SELECT 1
        FROM pgmemento.audited_tables at
        JOIN unnest($2) AS t(audit_table)
          ON t.audit_table = at.tablename
         AND at.schemaname = pgmemento.trim_outer_quotes($1)
       WHERE tg_is_active
    ) THEN
      RETURN format('pgMemento is partly stopped for %s schema.', schema_quoted);
    END IF;
  END IF;

  RETURN format('pgMemento is stopped for %s schema.', schema_quoted);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.drop(
  schemaname TEXT DEFAULT 'public'::text,
  keep_log BOOLEAN DEFAULT TRUE,
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS TEXT AS
$$
DECLARE
  schema_quoted TEXT;
  current_schema_log_id INTEGER;
  current_schema_log_range numrange;
  txid_log_id INTEGER;
BEGIN
  -- make sure schema is quoted no matter how it is passed to drop
  schema_quoted := quote_ident(pgmemento.trim_outer_quotes($1));

  -- check if schema is already logged
  SELECT
    id,
    txid_range
  INTO
    current_schema_log_id,
    current_schema_log_range
  FROM
    pgmemento.audit_schema_log
  WHERE
    schema_name = pgmemento.trim_outer_quotes($1)
  ORDER BY
    id DESC
  LIMIT 1;

  IF current_schema_log_id IS NULL THEN
    RETURN format('pgMemento is not intialized for %s schema. Nothing to drop.', schema_quoted);
  END IF;

  IF upper(current_schema_log_range) IS NOT NULL THEN
    RETURN format('pgMemento is already dropped from %s schema.', schema_quoted);
  END IF;

  -- log transaction that drops pgMemento from a schema
  -- and store configuration in session_info object
  PERFORM set_config(
    'pgmemento.session_info',
     format('{"pgmemento_drop": {"schema_name": %L, "keep_log": %L ,"except_tables": %L}}',
       to_jsonb($1), to_jsonb($2), to_jsonb($3))::text,
     TRUE
  );
  txid_log_id := pgmemento.log_transaction(txid_current());

  -- drop auditing for all tables except those from passed array
  PERFORM pgmemento.drop_schema_audit(pgmemento.trim_outer_quotes($1), $2, $3);

  IF $3 IS NOT NULL AND array_length($3, 1) > 0 THEN
    -- check if excluded tables are still audited
    IF EXISTS (
      SELECT 1
        FROM pgmemento.audited_tables at
        JOIN unnest($3) AS t(audit_table)
          ON t.audit_table = at.tablename
         AND at.schemaname = pgmemento.trim_outer_quotes($1)
       WHERE tg_is_active
    ) THEN
      RETURN format('pgMemento is partly dropped from %s schema.', schema_quoted);
    END IF;
  END IF;

  IF upper(current_schema_log_range) IS NULL THEN
    -- close txid_range for audit_schema_log entry
    UPDATE pgmemento.audit_schema_log
       SET txid_range = numrange(lower(txid_range), txid_log_id::numeric, '(]')
     WHERE id = current_schema_log_id;
  END IF;

  RETURN format('pgMemento is dropped from %s schema.', schema_quoted);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.version(
  OUT full_version TEXT,
  OUT major_version INTEGER,
  OUT minor_version INTEGER,
  OUT build_id TEXT
  ) RETURNS RECORD AS
$$
SELECT 'pgMemento 0.7'::text AS full_version, 0 AS major_version, 7 AS minor_version, '54'::text AS build_id;
$$
LANGUAGE sql;