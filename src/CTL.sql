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
-- 0.5.2     2021-12-28   start will call reinit if log params differ    FKun
-- 0.5.1     2021-01-02   fix session_info entries                       FKun
-- 0.5.0     2020-05-04   add revision to version endpoint               FKun
-- 0.4.0     2020-04-19   add reinit endpoint                            FKun
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
*   drop(schemaname TEXT DEFAULT 'public'::text, log_state BOOLEAN DEFAULT TRUE, drop_log BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   init(schemaname TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_old_data BOOLEAN DEFAULT TRUE, log_new_data BOOLEAN DEFAULT FALSE, log_state BOOLEAN DEFAULT FALSE,
*     trigger_create_table BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   reinit(schemaname TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_old_data BOOLEAN DEFAULT TRUE, log_new_data BOOLEAN DEFAULT FALSE, trigger_create_table BOOLEAN DEFAULT FALSE, except_tables TEXT[] DEFAULT '{}'
*   start(schemaname TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
*     log_old_data BOOLEAN DEFAULT TRUE, log_new_data BOOLEAN DEFAULT FALSE, trigger_create_table BOOLEAN DEFAULT FALSE,
*     except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   stop(schemaname TEXT DEFAULT 'public'::text, except_tables TEXT[] DEFAULT '{}') RETURNS TEXT
*   version(OUT full_version TEXT, OUT major_version INTEGER, OUT minor_version INTEGER, OUT revision INTEGER,
*     OUT build_id TEXT) RETURNS RECORD
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
  ) THEN
    RETURN format('pgMemento is already intialized for %s schema.', schema_quoted);
  END IF;

  -- log transaction that initializes pgMemento for a schema
  -- and store configuration in session_info object
  PERFORM set_config(
    'pgmemento.session_info', '{"pgmemento_init": ' ||
    jsonb_build_object(
      'schema_name', $1,
      'default_audit_id_column', $2,
      'default_log_old_data', $3,
      'default_log_new_data', $4,
      'log_state', $5,
      'trigger_create_table', $6,
      'except_tables', $7)::text
      || '}',
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

CREATE OR REPLACE FUNCTION pgmemento.reinit(
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
  rec RECORD;
BEGIN
  -- make sure schema is quoted no matter how it is passed to reinit
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
  ORDER BY
    id DESC
  LIMIT 1;

  IF current_audit_schema_log.id IS NULL THEN
    RETURN format('pgMemento has never been intialized for %s schema. Run init instread.', schema_quoted);
  END IF;

  IF upper(current_audit_schema_log.txid_range) IS NOT NULL THEN
    RETURN format('pgMemento is already dropped from %s schema. Run init instead.', schema_quoted);
  END IF;

  -- log transaction that reinitializes pgMemento for a schema
  -- and store configuration in session_info object
  PERFORM set_config(
    'pgmemento.session_info', '{"pgmemento_reinit": ' ||
    jsonb_build_object(
      'schema_name', $1,
      'default_audit_id_column', $2,
      'default_log_old_data', $3,
      'default_log_new_data', $4,
      'trigger_create_table', $5,
      'except_tables', $6)::text
    || '}', 
    TRUE
  );
  txid_log_id := pgmemento.log_transaction(txid_current());

  -- configuration differs, so reinitialize
  IF current_audit_schema_log.default_audit_id_column != $2
     OR current_audit_schema_log.default_log_old_data != $3
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
  END IF;

  -- recreate auditing if parameters differ
  FOR rec IN
    SELECT
      c.relname AS table_name,
      n.nspname AS schema_name,
      at.audit_id_column
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
      AND c.relname <> ALL (COALESCE($6,'{}'::text[]))
      AND (at.audit_id_column IS DISTINCT FROM $2
       OR at.log_old_data IS DISTINCT FROM $3
       OR at.log_new_data IS DISTINCT FROM $4)
  LOOP
    -- drop auditing from table but do not log or drop anything
    PERFORM pgmemento.drop_table_audit(rec.table_name, rec.schema_name, rec.audit_id_column, FALSE, FALSE);

    -- log reinit event to keep log_id in audit_table_log
    PERFORM pgmemento.log_table_event(rec.table_name, rec.schema_name, 'REINIT TABLE');

    -- recreate auditing
    PERFORM pgmemento.create_table_audit(rec.table_name, rec.schema_name, $2, $3, $4, FALSE);
  END LOOP;

  -- update event triggers
  IF $5 != current_audit_schema_log.trigger_create_table THEN
    PERFORM pgmemento.create_schema_event_trigger($5);
  END IF;

  RETURN format('pgMemento is reinitialized for %s schema.', schema_quoted);
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
  reinit_test TEXT := '';
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
    'pgmemento.session_info', '{"pgmemento_start": ' ||
    jsonb_build_object(
      'schema_name', $1,
      'default_audit_id_column', $2,
      'default_log_old_data', $3,
      'default_log_new_data', $4,
      'trigger_create_table', $5,
      'except_tables', $6)::text
    || '}',
    TRUE
  );
  txid_log_id := pgmemento.log_transaction(txid_current());

  -- enable triggers where they are not active
  PERFORM
    pgmemento.create_table_log_trigger(c.relname, $1, at.audit_id_column, asl.default_log_old_data, asl.default_log_new_data)
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON c.relnamespace = n.oid
  JOIN
    pgmemento.audit_schema_log asl
    ON asl.schema_name = n.nspname
   AND lower(asl.txid_range) IS NOT NULL
   AND upper(asl.txid_range) IS NULL
  JOIN pgmemento.audit_tables at
    ON at.tablename = c.relname
   AND at.schemaname = n.nspname
   AND NOT tg_is_active
  WHERE
    n.nspname = pgmemento.trim_outer_quotes($1)
    AND c.relkind = 'r'
    AND c.relname <> ALL (COALESCE($6,'{}'::text[]));

  -- configuration differs, perform reinit
  IF current_audit_schema_log.default_log_old_data != $3
     OR current_audit_schema_log.default_log_new_data != $4
     OR current_audit_schema_log.trigger_create_table != $5
  THEN
    PERFORM pgmemento.reinit($1, $2, $3, $4, $5, $6);
    reinit_test := ' and reinitialized';
  END IF;

  RETURN format('pgMemento is started%s for %s schema.', reinit_test, schema_quoted);
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
  PERFORM set_config('pgmemento.session_info', '{"pgmemento_stop": ' ||
    jsonb_build_object(
      'schema_name', $1,
      'except_tables', $2)::text
    || '}',
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
  log_state BOOLEAN DEFAULT TRUE,
  drop_log BOOLEAN DEFAULT FALSE,
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
    'pgmemento.session_info', '{"pgmemento_drop": ' ||
    jsonb_build_object(
      'schema_name', $1,
      'log_state', $2,
      'drop_log', $3,
      'except_tables', $4)::text
    || '}',
    TRUE
  );
  txid_log_id := pgmemento.log_transaction(txid_current());

  -- drop auditing for all tables except those from passed array
  PERFORM pgmemento.drop_schema_audit(pgmemento.trim_outer_quotes($1), $2, $3, $4);

  IF $4 IS NOT NULL AND array_length($4, 1) > 0 THEN
    -- check if excluded tables are still audited
    IF EXISTS (
      SELECT 1
        FROM pgmemento.audited_tables at
        JOIN unnest($4) AS t(audit_table)
          ON t.audit_table = at.tablename
         AND at.schemaname = pgmemento.trim_outer_quotes($1)
       WHERE tg_is_active
    ) THEN
      RETURN format('pgMemento is partly dropped from %s schema.', schema_quoted);
    END IF;
  END IF;

  -- close txid_range for audit_schema_log entry
  UPDATE pgmemento.audit_schema_log
     SET txid_range = numrange(lower(txid_range), txid_log_id::numeric, '(]')
   WHERE id = current_schema_log_id;

  RETURN format('pgMemento is dropped from %s schema.', schema_quoted);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.version(
  OUT full_version TEXT,
  OUT major_version INTEGER,
  OUT minor_version INTEGER,
  OUT revision INTEGER,
  OUT build_id TEXT
  ) RETURNS RECORD AS
$$
SELECT 'pgMemento 0.7.4'::text AS full_version, 0 AS major_version, 7 AS minor_version, 4 AS revision, '100'::text AS build_id;
$$
LANGUAGE sql;
