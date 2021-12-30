-- UPGRADE_v07_to_v073.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script upgrades a pgMemento extension of v0.7.0 to v0.7.3 which
-- replaces some functions (see changelog for more details)
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                  | Author
-- 0.3.0     2021-12-27   bump to 7.3                                    FKun
-- 0.2.0     2021-03-21   reflect fixes for v0.7.2                       FKun
-- 0.1.0     2020-07-30   initial commit                                 FKun
--

\echo
\echo 'Updgrade pgMemento from v0.7.0 to v0.7.3 ...'

COMMENT ON COLUMN pgmemento.transaction_log.user_name IS 'Stores the result of session_user function';

\echo
\echo 'Recreate functions'
CREATE OR REPLACE FUNCTION pgmemento.version(
  OUT full_version TEXT,
  OUT major_version INTEGER,
  OUT minor_version INTEGER,
  OUT revision INTEGER,
  OUT build_id TEXT
  ) RETURNS RECORD AS
$$
SELECT 'pgMemento 0.7.3'::text AS full_version, 0 AS major_version, 7 AS minor_version, 3 AS revision, '93'::text AS build_id;
$$
LANGUAGE sql;

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

CREATE OR REPLACE FUNCTION pgmemento.unregister_audit_table(
  audit_table_name TEXT,
  audit_schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
DECLARE
  tab_id INTEGER;
BEGIN
  -- update txid_range for removed table in audit_table_log table
  UPDATE
    pgmemento.audit_table_log
  SET
    txid_range = numrange(lower(txid_range), current_setting('pgmemento.t' || txid_current())::numeric, '(]')
  WHERE
    table_name = $1
    AND schema_name = $2
    AND upper(txid_range) IS NULL
    AND lower(txid_range) IS NOT NULL
  RETURNING
    id INTO tab_id;

  IF tab_id IS NOT NULL THEN
    -- update txid_range for removed columns in audit_column_log table
    UPDATE
      pgmemento.audit_column_log
    SET
      txid_range = numrange(lower(txid_range), current_setting('pgmemento.t' || txid_current())::numeric, '(]')
    WHERE
      audit_table_id = tab_id
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pgmemento.register_audit_table(
  audit_table_name TEXT,
  audit_schema_name TEXT DEFAULT 'public'::text
  ) RETURNS INTEGER AS
$$
DECLARE
  tab_id INTEGER;
  table_log_id INTEGER;
  old_table_name TEXT;
  old_schema_name TEXT;
  audit_id_column_name TEXT;
  log_data_settings TEXT;
BEGIN
  -- check if affected table exists in 'audit_table_log' (with open range)
  SELECT
    id INTO tab_id
  FROM
    pgmemento.audit_table_log
  WHERE
    table_name = $1
    AND schema_name = $2
    AND upper(txid_range) IS NULL
    AND lower(txid_range) IS NOT NULL;

  IF tab_id IS NOT NULL THEN
    RETURN tab_id;
  END IF;

  BEGIN
    -- check if table exists in 'audit_table_log' with another name (and open range)
    table_log_id := current_setting('pgmemento.' || quote_ident($2) || '.' || quote_ident($1))::int;

    IF NOT EXISTS (
      SELECT
        1
      FROM
        pgmemento.table_event_log
      WHERE
        transaction_id = current_setting('pgmemento.t' || txid_current())::int
        AND table_name = $1
        AND schema_name = $2
        AND ((op_id = 1 AND table_operation = 'RECREATE TABLE')
         OR op_id = 11)  -- REINIT TABLE event
    ) THEN
      SELECT
        table_name,
        schema_name
      INTO
        old_table_name,
        old_schema_name
      FROM
        pgmemento.audit_table_log
      WHERE
        log_id = table_log_id
        AND upper(txid_range) IS NULL
        AND lower(txid_range) IS NOT NULL;
    END IF;

    EXCEPTION
      WHEN others THEN
        table_log_id := nextval('pgmemento.table_log_id_seq');
  END;

  -- if so, unregister first before making new inserts
  IF old_table_name IS NOT NULL AND old_schema_name IS NOT NULL THEN
    PERFORM pgmemento.unregister_audit_table(old_table_name, old_schema_name);
  END IF;

  -- get audit_id_column name which was set in create_table_audit_id or in event trigger when renaming the table
  audit_id_column_name := current_setting('pgmemento.' || $2 || '.' || $1 || '.audit_id.t' || txid_current());

  -- get logging behavior which was set in create_table_audit_id or in event trigger when renaming the table
  log_data_settings := current_setting('pgmemento.' || $2 || '.' || $1 || '.log_data.t' || txid_current());

  -- now register table and corresponding columns in audit tables
  INSERT INTO pgmemento.audit_table_log
    (log_id, relid, schema_name, table_name, audit_id_column, log_old_data, log_new_data, txid_range)
  VALUES
    (table_log_id, pgmemento.get_table_oid($1, $2), $2, $1, audit_id_column_name,
     CASE WHEN split_part(log_data_settings, ',' ,1) = 'old=true' THEN TRUE ELSE FALSE END,
     CASE WHEN split_part(log_data_settings, ',' ,2) = 'new=true' THEN TRUE ELSE FALSE END,
     numrange(current_setting('pgmemento.t' || txid_current())::numeric, NULL, '(]'))
  RETURNING id INTO tab_id;

  -- insert columns of new audited table into 'audit_column_log'
  INSERT INTO pgmemento.audit_column_log
    (id, audit_table_id, column_name, ordinal_position, column_default, not_null, data_type, txid_range)
  (
    SELECT
      nextval('pgmemento.audit_column_log_id_seq') AS id,
      tab_id AS audit_table_id,
      a.attname AS column_name,
      a.attnum AS ordinal_position,
      pg_get_expr(d.adbin, d.adrelid, TRUE) AS column_default,
      a.attnotnull AS not_null,
      substr(
        format_type(a.atttypid, a.atttypmod),
        position('.' IN format_type(a.atttypid, a.atttypmod))+1,
        length(format_type(a.atttypid, a.atttypmod))
      ) AS data_type,
      numrange(current_setting('pgmemento.t' || txid_current())::numeric, NULL, '(]') AS txid_range
    FROM
      pg_attribute a
    LEFT JOIN
      pg_attrdef d
      ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
    WHERE
      a.attrelid = pgmemento.get_table_oid($1, $2)
      AND a.attname <> audit_id_column_name
      AND a.attnum > 0
      AND NOT a.attisdropped
      ORDER BY a.attnum
  );

  -- rename unique constraint for audit_id column
  IF old_table_name IS NOT NULL AND old_schema_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
      $2, $1, old_table_name || '_' || audit_id_column_name || '_key', $1 || '_' || audit_id_column_name || '_key');
  END IF;

  RETURN tab_id;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.log_old_table_state(
  columns TEXT[],
  tablename TEXT,
  schemaname TEXT,
  table_event_key TEXT,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $1 IS NOT NULL AND array_length($1, 1) IS NOT NULL THEN
    -- log content of given columns
    EXECUTE format(
      'INSERT INTO pgmemento.row_log AS r (audit_id, event_key, old_data)
         SELECT %I, $1, jsonb_build_object('||pgmemento.column_array_to_column_list($1)||') AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET
         old_data = COALESCE(excluded.old_data, ''{}''::jsonb) || COALESCE(r.old_data, ''{}''::jsonb)',
       $5, $3, $2, $5) USING $4;
  ELSE
    -- log content of entire table
    EXECUTE format(
      'INSERT INTO pgmemento.row_log (audit_id, event_key, old_data)
         SELECT %I, $1, to_jsonb(%I) AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO NOTHING',
       $5, $2, $3, $2, $5) USING $4;
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pgmemento.log_new_table_state(
  columns TEXT[],
  tablename TEXT,
  schemaname TEXT,
  table_event_key TEXT,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  IF $1 IS NOT NULL AND array_length($1, 1) IS NOT NULL THEN
    -- log content of given columns
    EXECUTE format(
      'INSERT INTO pgmemento.row_log AS r (audit_id, event_key, new_data)
         SELECT %I, $1, jsonb_build_object('||pgmemento.column_array_to_column_list($1)||') AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET new_data = COALESCE(r.new_data, ''{}''::jsonb) || COALESCE(excluded.new_data, ''{}''::jsonb)',
       $5, $3, $2, $5) USING $4;
  ELSE
    -- log content of entire table
    EXECUTE format(
      'INSERT INTO pgmemento.row_log r (audit_id, event_key, new_data)
         SELECT %I, $1, to_jsonb(%I) AS content
           FROM %I.%I ORDER BY %I
       ON CONFLICT (audit_id, event_key)
       DO UPDATE SET COALESCE(r.new_data, ''{}''::jsonb) || COALESCE(excluded.new_data, ''{}''::jsonb)',
       $5, $2, $3, $2, $5) USING $4;
  END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.log_transaction(current_txid BIGINT) RETURNS INTEGER AS
$$
DECLARE
  session_info_text TEXT;
  session_info_obj JSONB;
  transaction_log_id INTEGER;
BEGIN
  -- retrieve session_info set by client
  BEGIN
    session_info_text := current_setting('pgmemento.session_info');

    IF session_info_text IS NULL OR session_info_text = '' THEN
      session_info_obj := NULL;
    ELSE
      session_info_obj := session_info_text::jsonb;
    END IF;

    EXCEPTION
      WHEN undefined_object THEN
        session_info_obj := NULL;
      WHEN invalid_text_representation THEN
        BEGIN
          session_info_obj := to_jsonb(current_setting('pgmemento.session_info'));
        END;
      WHEN others THEN
        RAISE NOTICE 'Unable to parse session info: %', session_info_text;
        session_info_obj := NULL;
  END;

  -- try to log corresponding transaction
  INSERT INTO pgmemento.transaction_log
    (txid, txid_time, process_id, user_name, client_name, client_port, application_name, session_info)
  VALUES
    ($1, transaction_timestamp(), pg_backend_pid(), session_user, inet_client_addr(), inet_client_port(),
     current_setting('application_name'), session_info_obj
    )
  ON CONFLICT (txid_time, txid)
    DO NOTHING
  RETURNING id
  INTO transaction_log_id;

  IF transaction_log_id IS NOT NULL THEN
    PERFORM set_config('pgmemento.t' || $1, transaction_log_id::text, TRUE);
  ELSE
    transaction_log_id := current_setting('pgmemento.t' || $1)::int;
  END IF;

  RETURN transaction_log_id;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.log_update() RETURNS trigger AS
$$
DECLARE
  new_audit_id BIGINT;
  jsonb_diff_old JSONB;
  jsonb_diff_new JSONB;
BEGIN
  EXECUTE 'SELECT $1.' || TG_ARGV[0] USING NEW INTO new_audit_id;

  -- log values of updated columns for the processed row
  -- therefore, a diff between OLD and NEW is necessary
  IF TG_ARGV[1] = 'true' THEN
    SELECT COALESCE(
      (SELECT
         ('{' || string_agg(to_json(key) || ':' || value, ',') || '}')
       FROM
         jsonb_each(to_jsonb(OLD))
       WHERE
         to_jsonb(NEW) ->> key IS DISTINCT FROM to_jsonb(OLD) ->> key
      ),
      '{}')::jsonb INTO jsonb_diff_old;
  END IF;

  IF TG_ARGV[2] = 'true' THEN
    -- switch the diff to only get the new values
    SELECT COALESCE(
      (SELECT
         ('{' || string_agg(to_json(key) || ':' || value, ',') || '}')
       FROM
         jsonb_each(to_jsonb(NEW))
       WHERE
         to_jsonb(OLD) ->> key IS DISTINCT FROM to_jsonb(NEW) ->> key
      ),
      '{}')::jsonb INTO jsonb_diff_new;
  END IF;

  IF jsonb_diff_old <> '{}'::jsonb OR jsonb_diff_new <> '{}'::jsonb THEN
    -- log delta, on conflict concat logs, for old_data oldest should overwrite, for new_data vice versa
    INSERT INTO pgmemento.row_log AS r
      (audit_id, event_key, old_data, new_data)
    VALUES
      (new_audit_id,
       concat_ws(';', extract(epoch from transaction_timestamp()), extract(epoch from statement_timestamp()), txid_current(), pgmemento.get_operation_id(TG_OP), TG_TABLE_NAME, TG_TABLE_SCHEMA),
       jsonb_diff_old, jsonb_diff_new)
    ON CONFLICT (audit_id, event_key)
    DO UPDATE SET
      old_data = COALESCE(excluded.old_data, '{}'::jsonb) || COALESCE(r.old_data, '{}'::jsonb),
      new_data = COALESCE(r.new_data, '{}'::jsonb) || COALESCE(excluded.new_data, '{}'::jsonb);
  END IF;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.log_truncate() RETURNS trigger AS
$$
BEGIN
  -- log the whole content of the truncated table in the row_log table
  PERFORM
    pgmemento.log_old_table_state('{}'::text[], TG_TABLE_NAME, TG_TABLE_SCHEMA, event_key, TG_ARGV[0])
  FROM
    pgmemento.table_event_log
  WHERE
    transaction_id = current_setting('pgmemento.t' || txid_current())::int
    AND table_name = TG_TABLE_NAME
    AND schema_name = TG_TABLE_SCHEMA
    AND op_id = 8;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER;


CREATE OR REPLACE FUNCTION pgmemento.create_table_audit(
  tablename TEXT,
  schemaname TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text,
  log_old_data BOOLEAN DEFAULT TRUE,
  log_new_data BOOLEAN DEFAULT FALSE,
  log_state BOOLEAN DEFAULT FALSE
  ) RETURNS SETOF VOID AS
$$
DECLARE
  except_tables TEXT[] DEFAULT '{}';
BEGIN
  -- check if pgMemento is already initialized for schema
  IF NOT EXISTS (
    SELECT 1
      FROM pgmemento.audit_schema_log
     WHERE schema_name = $2
       AND upper(txid_range) IS NULL
  ) THEN
    SELECT
      array_agg(c.relname)
    INTO
      except_tables
    FROM
      pg_class c
    JOIN
      pg_namespace n
      ON c.relnamespace = n.oid
    WHERE
      n.nspname = $2
      AND c.relname <> $1
      AND c.relkind = 'r';

    PERFORM pgmemento.create_schema_audit($2, $3, $4, $5, $6, FALSE, except_tables);
    RETURN;
  END IF;

  -- remember audit_id_column when registering table in audit_table_log later
  PERFORM set_config('pgmemento.' || $2 || '.' || $1 || '.audit_id.t' || txid_current(), $3, TRUE);

  -- remember logging behavior when registering table in audit_table_log later
  PERFORM set_config('pgmemento.' || $2 || '.' || $1 || '.log_data.t' || txid_current(),
    CASE WHEN log_old_data THEN 'old=true,' ELSE 'old=false,' END ||
    CASE WHEN log_new_data THEN 'new=true' ELSE 'new=false' END, TRUE);

  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger($1, $2, $3, $4, $5);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id($1, $2, $3);

  -- log existing table content as inserted
  IF $6 THEN
    PERFORM pgmemento.log_table_baseline($1, $2, $3, $5);
  END IF;
END;
$$
LANGUAGE plpgsql STRICT
SECURITY DEFINER;


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


CREATE OR REPLACE FUNCTION pgmemento.get_column_list(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_log_id INTEGER,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  all_versions BOOLEAN DEFAULT FALSE,
  OUT column_name TEXT,
  OUT column_count INTEGER,
  OUT data_type TEXT,
  OUT ordinal_position INTEGER,
  OUT txid_range numrange
  ) RETURNS SETOF RECORD AS
$$
BEGIN
  IF $6 THEN
    RETURN QUERY
      SELECT t.column_name, t.column_count, t.data_type, t.ordinal_position, t.txid_range
        FROM pgmemento.get_column_list_by_txid_range($1, $2, $3) t;
  ELSE
    RETURN QUERY
      SELECT t.column_name, NULL::int, t.data_type, t.ordinal_position, NULL::numrange
        FROM pgmemento.get_column_list_by_txid($2, $4, $5) t;
  END IF;
END;
$$
LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION pgmemento.jsonb_populate_value(
  jsonb_log JSONB,
  column_name TEXT,
  INOUT template anyelement
  ) RETURNS anyelement AS
$$
BEGIN
  IF $1 IS NOT NULL THEN
    IF right(pg_typeof($3)::text, 2) = '[]' THEN
      EXECUTE format('SELECT translate($1->>$2, ''[]'', ''{}'')::%s', pg_typeof($3))
        INTO template USING $1, $2;
    ELSE
      EXECUTE format('SELECT ($1->>$2)::%s', pg_typeof($3))
        INTO template USING $1, $2;
    END IF;
  ELSE
    EXECUTE format('SELECT NULL::%s', pg_typeof($3))
      INTO template;
  END IF;
END;
$$
LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION pgmemento.restore_query(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  aid BIGINT DEFAULT NULL,
  all_versions BOOLEAN DEFAULT FALSE
  ) RETURNS TEXT AS
$$
DECLARE
  log_id INTEGER;
  tab_name TEXT;
  tab_schema TEXT;
  tab_audit_id_column TEXT;
  tab_id INTEGER;
  new_tab_name TEXT;
  new_tab_schema TEXT;
  new_audit_id_column TEXT;
  new_tab_id INTEGER;
  join_recent_state BOOLEAN := FALSE;
  extract_logs TEXT;
  find_logs TEXT;
  query_text TEXT := E'SELECT\n';
BEGIN
  -- first check if table can be restored
  SELECT
    table_log_id,
    log_tab_name,
    log_tab_schema,
    log_audit_id_column,
    log_tab_id,
    recent_tab_name,
    recent_tab_schema,
    recent_audit_id_column,
    recent_tab_id
  INTO
    log_id,
    tab_name,
    tab_schema,
    tab_audit_id_column,
    tab_id,
    new_tab_name,
    new_tab_schema,
    new_audit_id_column,
    new_tab_id
  FROM
    pgmemento.audit_table_check($2, $3, $4);

  IF tab_id IS NULL THEN
    RAISE EXCEPTION 'Can not restore table ''%'' because it did not exist before requested transaction %', $3, $2;
  END IF;

  -- check if recent state can be queried
  IF new_tab_id IS NULL THEN
    new_tab_id := tab_id;
  ELSE
    join_recent_state := TRUE;
  END IF;

  -- loop over all columns and query the historic value for each column separately
  SELECT
    string_agg(
       format(E'  CASE WHEN jsonb_typeof(g.log_%s) = ''null'' THEN NULL::%s\n', c_old.column_name, c_old.data_type)
    || CASE WHEN join_recent_state AND c_new.column_name IS NOT NULL
       THEN format(E'       WHEN g.log_%s IS NULL THEN x.%I\n', c_old.column_name, c_new.column_name) ELSE '' END
    || '       ELSE '
    || CASE WHEN right(c_old.data_type, 2) = '[]'
       THEN 'translate(' ELSE '' END
    || format('(jsonb_build_object(%L, g.log_%s) ->> %L)', c_old.column_name,
       quote_ident(c_old.column_name || CASE WHEN c_old.column_count > 1 THEN '_' || c_old.column_count ELSE '' END), c_old.column_name)
    || CASE WHEN right(c_old.data_type, 2) = '[]'
       THEN ',''[]'',''{}'')' ELSE '' END
    || format(E'::%s\n', c_old.data_type)
    || format('  END AS %s', quote_ident(c_old.column_name || CASE WHEN c_old.column_count > 1 THEN '_' || c_old.column_count ELSE '' END))
      , E',\n' ORDER BY c_old.ordinal_position, c_old.column_count
    ),
    string_agg(
      CASE WHEN $6
      THEN format(E'    CASE WHEN transaction_id >= %L AND transaction_id < %L\n    THEN ',
        CASE WHEN lower(c_old.txid_range) IS NOT NULL
        THEN lower(c_old.txid_range)
        ELSE $1 END,
        CASE WHEN upper(c_old.txid_range) IS NOT NULL
        THEN upper(c_old.txid_range)
        ELSE $2 END)
      ELSE '    ' END
    || format('first_value(a.old_data -> %L) OVER ', c_old.column_name, c_old.column_name)
    || format('(PARTITION BY f.event_key, a.audit_id ORDER BY a.old_data -> %L IS NULL, a.id)', c_old.column_name)
    || CASE WHEN $6
       THEN format(E'\n    ELSE jsonb_build_object(%L, NULL) -> %L END', c_old.column_name, c_old.column_name)
       ELSE '' END
    || format(' AS log_%s', quote_ident(c_old.column_name || CASE WHEN c_old.column_count > 1 THEN '_' || c_old.column_count ELSE '' END))
        , E',\n' ORDER BY c_old.ordinal_position, c_old.column_count
    )
  INTO
    extract_logs,
    find_logs
  FROM
    pgmemento.get_column_list($1, $2, log_id, tab_name, tab_schema, $6) c_old
  LEFT JOIN
    pgmemento.audit_column_log c_new
    ON c_old.ordinal_position = c_new.ordinal_position
   AND c_new.audit_table_id = new_tab_id
   AND upper(c_new.txid_range) IS NULL
   AND lower(c_new.txid_range) IS NOT NULL;

  -- finish restore query
  query_text := query_text
    -- add part to extract values from logs or get recent state
    || extract_logs
    || format(E',\n  g.audit_id AS %s', quote_ident(tab_audit_id_column))
    || CASE WHEN $6 THEN E',\n  g.stmt_time,\n  g.table_operation,\n  g.transaction_id\n' ELSE E'\n' END
    -- use DISTINCT ON to get only one row
    || E'FROM (\n  SELECT DISTINCT ON ('
    || CASE WHEN $6 THEN 'f.event_key, ' ELSE '' END
    || E'f.audit_id)\n'
    -- add subquery g that finds the right JSONB log snippets for each column
    || find_logs
    || format(E',\n    f.audit_id')
    || CASE WHEN $6 THEN E',\n    f.stmt_time,\n    f.table_operation,\n    f.transaction_id\n' ELSE E'\n' END
    -- add subquery f to get last event for given audit_id before given transaction
    || E'  FROM (\n'
    || '    SELECT '
    || CASE WHEN $6 THEN E'\n' ELSE E'DISTINCT ON (r.audit_id)\n' END
    || '      r.audit_id, e.event_key, e.op_id'
    || CASE WHEN $6 THEN E', e.stmt_time, e.table_operation, e.transaction_id\n' ELSE E'\n' END
    || E'    FROM\n'
    || E'      pgmemento.row_log r\n'
    || E'    JOIN\n'
    || E'      pgmemento.table_event_log e ON r.event_key = e.event_key\n'
    || format(E'    WHERE e.transaction_id >= %L AND e.transaction_id < %L\n', $1, $2)
    || CASE WHEN $5 IS NULL THEN
         format(E'      AND e.table_name = %L AND e.schema_name = %L\n', tab_name, tab_schema)
       ELSE
         format(E'      AND r.audit_id = %L\n', $5)
       END
    || E'    ORDER BY\n'
    || E'      r.audit_id, e.id DESC\n'
    || E'  ) f\n'
    -- left join on row_log table and consider only events younger than the one extracted in subquery f
    || E'  LEFT JOIN\n'
    || E'    pgmemento.row_log a ON a.audit_id = f.audit_id AND a.event_key > f.event_key\n'
    -- if 'all_versions' flag is FALSE do not produce a result if row did not exist before second transaction ID
    -- therefore, filter out DELETE, TRUNCATE or DROP TABLE events
    || CASE WHEN $6 THEN '' ELSE E'  WHERE\n    f.op_id < 7\n' END
    -- order by oldest log entry for given audit_id
    || E'  ORDER BY\n'
    || CASE WHEN $6 THEN '    f.event_key, ' ELSE '    ' END
    || 'f.audit_id'
    || E'\n) g\n'
    -- left join on actual table to get the recent value for a field if nothing is found in the logs
    || CASE WHEN join_recent_state THEN
         E'LEFT JOIN\n'
         || format(E'  %I.%I x ON x.' || new_audit_id_column || E' = g.audit_id\n', new_tab_schema, new_tab_name)
       ELSE
         ''
       END;

  RETURN query_text;
END;
$$
LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION pgmemento.jsonb_unroll_for_update(
  path TEXT,
  nested_value JSONB,
  complex_typname TEXT
  ) RETURNS TEXT AS
$$
SELECT
  string_agg(set_columns,', ')
FROM (
  SELECT
    CASE WHEN jsonb_typeof(j.value) = 'object' AND p.typname IS NOT NULL THEN
      pgmemento.jsonb_unroll_for_update($1 || '.' || quote_ident(j.key), j.value, p.typname)
    ELSE
      $1 || '.' || quote_ident(j.key) || '=' ||
      CASE WHEN jsonb_typeof(j.value) = 'array' THEN
        quote_nullable(translate($2 ->> j.key, '[]', '{}'))
      ELSE
        quote_nullable($2 ->> j.key)
      END
    END AS set_columns
  FROM
    jsonb_each($2) j
  LEFT JOIN
    pg_attribute a
    ON a.attname = j.key
   AND jsonb_typeof(j.value) = 'object'
  LEFT JOIN
    pg_class c
    ON c.oid = a.attrelid
  LEFT JOIN
    pg_type t
    ON t.typrelid = c.oid
   AND t.typname = $3
  LEFT JOIN
    pg_type p
    ON p.typname = format_type(a.atttypid, a.atttypmod)
   AND p.typcategory = 'C'
) u
$$
LANGUAGE sql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.recover_audit_version(
  tid INTEGER,
  aid BIGINT,
  changes JSONB,
  table_op INTEGER,
  tab_name TEXT,
  tab_schema TEXT DEFAULT 'public'::text,
  audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text
  ) RETURNS SETOF VOID AS
$$
DECLARE
  except_tables TEXT[] DEFAULT '{}';
  stmt TEXT;
  table_log_id INTEGER;
  current_transaction INTEGER;
BEGIN
  CASE
  -- CREATE TABLE case
  WHEN $4 = 1 THEN
    -- try to drop table
    BEGIN
      EXECUTE format('DROP TABLE %I.%I', $6, $5);

      EXCEPTION
        WHEN undefined_table THEN
          RAISE NOTICE 'Could not revert CREATE TABLE event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- REINIT TABLE case
  WHEN $4 = 11 THEN
    BEGIN
      -- reinit only given table and exclude all others
      SELECT
        array_agg(table_name)
      INTO
        except_tables
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name <> $5
        AND schema_name = $6
        AND upper(txid_range) = $1;

      PERFORM
        pgmemento.reinit($6, audit_id_column, log_old_data, log_new_data, FALSE, except_tables)
      FROM
        pgmemento.audit_table_log
      WHERE
        table_name = $5
        AND schema_name = $6
        AND upper(txid_range) = $1;

      -- if auditing was stopped within the same transaction (e.g. reverted ADD AUDIT_ID event)
      -- the REINIT TABLE event will not be logged by reinit function
      -- therefore, we have to make the insert here
      IF NOT EXISTS (
        SELECT
          1
        FROM
          pgmemento.table_event_log
        WHERE
          transaction_id = current_setting('pgmemento.t' || txid_current())::int
          AND table_name = $5
          AND schema_name = $6
          AND op_id = 11  -- REINIT TABLE event
      ) THEN
        PERFORM pgmemento.log_table_event($5, $6, 'REINIT TABLE');
      END IF;

      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert REINIT TABLE event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- RENAME TABLE case
  WHEN $4 = 12 THEN
    BEGIN
      -- collect information of renamed table
      SELECT
        format('%I.%I',
          t_old.schema_name,
          t_old.table_name
        )
      INTO
        stmt
      FROM
        pgmemento.audit_table_log t_old,
        pgmemento.audit_table_log t_new
      WHERE
        t_old.log_id = t_new.log_id
        AND t_new.table_name = $5
        AND t_new.schema_name = $6
        AND upper(t_new.txid_range) = $1
        AND lower(t_old.txid_range) = $1;

      -- try to re-rename table
      IF stmt IS NOT NULL THEN
        EXECUTE 'ALTER TABLE ' || stmt || format(' RENAME TO %I', $5);
      END IF;

      EXCEPTION
        WHEN undefined_table THEN
          RAISE NOTICE 'Could not revert RENAME TABLE event for table %: %', stmt, SQLERRM;
    END;

  -- ADD COLUMN case
  WHEN $4 = 2 THEN
    BEGIN
      -- collect added columns
      SELECT
        string_agg(
          'DROP COLUMN '
          || quote_ident(c.column_name),
          ', ' ORDER BY c.id DESC
        ) INTO stmt
      FROM
        pgmemento.audit_column_log c
      JOIN
        pgmemento.audit_table_log t
        ON c.audit_table_id = t.id
      WHERE
        lower(c.txid_range) = $1
        AND t.table_name = $5
        AND t.schema_name = $6;

      -- try to execute ALTER TABLE command
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert ADD COLUMN event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- ADD AUDIT_ID case
  WHEN $4 = 21 THEN
    PERFORM pgmemento.drop_table_audit($5, $6, $7, TRUE, FALSE);

  -- RENAME COLUMN case
  WHEN $4 = 22 THEN
    BEGIN
      -- collect information of renamed table
      SELECT
        'RENAME COLUMN ' || quote_ident(c_old.column_name) ||
        ' TO ' || quote_ident(c_new.column_name)
      INTO
        stmt
      FROM
        pgmemento.audit_table_log t,
        pgmemento.audit_column_log c_old,
        pgmemento.audit_column_log c_new
      WHERE
        c_old.audit_table_id = t.id
        AND c_new.audit_table_id = t.id
        AND t.table_name = $5
        AND t.schema_name = $6
        AND t.txid_range @> $1::numeric
        AND c_old.ordinal_position = c_new.ordinal_position
        AND upper(c_new.txid_range) = $1
        AND lower(c_old.txid_range) = $1;

      -- try to re-rename table
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt, $6, $5);
      END IF;

      EXCEPTION
        WHEN undefined_table THEN
          RAISE NOTICE 'Could not revert RENAME COLUMN event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- INSERT case
  WHEN $4 = 3 THEN
    -- aid can be null in case of conflicts during insert
    IF $2 IS NOT NULL THEN
      -- delete inserted row
      BEGIN
        EXECUTE format(
          'DELETE FROM %I.%I WHERE %I = $1',
          $6, $5, $7)
          USING $2;

        -- row is already deleted
        EXCEPTION
          WHEN no_data_found THEN
            NULL;
      END;
    END IF;

  -- UPDATE case
  WHEN $4 = 4 THEN
    -- update the row with values from changes
    IF $2 IS NOT NULL AND $3 <> '{}'::jsonb THEN
      BEGIN
        -- create SET part
        SELECT
          string_agg(set_columns,', ')
        INTO
          stmt
        FROM (
          SELECT
            CASE WHEN jsonb_typeof(j.value) = 'object' AND p.typname IS NOT NULL THEN
              pgmemento.jsonb_unroll_for_update(j.key, j.value, p.typname)
            ELSE
              quote_ident(j.key) || '=' ||
              CASE WHEN jsonb_typeof(j.value) = 'array' THEN
                quote_nullable(translate($3 ->> j.key, '[]', '{}'))
              ELSE
                quote_nullable($3 ->> j.key)
              END
            END AS set_columns
          FROM
            jsonb_each($3) j
          LEFT JOIN
            pgmemento.audit_column_log c
            ON c.column_name = j.key
           AND jsonb_typeof(j.value) = 'object'
           AND upper(c.txid_range) IS NULL
           AND lower(c.txid_range) IS NOT NULL
          LEFT JOIN
            pgmemento.audit_table_log t
            ON t.id = c.audit_table_id
           AND t.table_name = $5
           AND t.schema_name = $6
          LEFT JOIN
            pg_type p
            ON p.typname = c.data_type
           AND p.typcategory = 'C'
        ) u;

        -- try to execute UPDATE command
        EXECUTE format(
          'UPDATE %I.%I t SET ' || stmt || ' WHERE t.%I = $1',
          $6, $5, $7)
          USING $2;

        -- row is already deleted
        EXCEPTION
          WHEN others THEN
            RAISE NOTICE 'Could not revert UPDATE event for table %.%: %', $6, $5, SQLERRM;
      END;
    END IF;

  -- ALTER COLUMN case
  WHEN $4 = 5 THEN
    BEGIN
      -- collect information of altered columns
      SELECT
        string_agg(
          format('ALTER COLUMN %I SET DATA TYPE %s USING pgmemento.restore_change(%L, %I, %L, NULL::%s)',
            c_new.column_name, c_old.data_type, $1, $7, quote_ident(c_old.column_name), c_old.data_type),
          ', ' ORDER BY c_new.id
        ) INTO stmt
      FROM
        pgmemento.audit_table_log t,
        pgmemento.audit_column_log c_old,
        pgmemento.audit_column_log c_new
      WHERE
        c_old.audit_table_id = t.id
        AND c_new.audit_table_id = t.id
        AND t.table_name = $5
        AND t.schema_name = $6
        AND t.txid_range @> $1::numeric
        AND upper(c_old.txid_range) = $1
        AND lower(c_new.txid_range) = $1
        AND c_old.ordinal_position = c_new.ordinal_position
        AND c_old.data_type <> c_new.data_type;

      -- alter table if it has not been done, yet
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      -- it did not work for some reason
      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert ALTER COLUMN event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- DROP COLUMN case
  WHEN $4 = 6 THEN
    BEGIN
      -- collect information of dropped columns
      SELECT
        string_agg(
          'ADD COLUMN '
          || quote_ident(c_old.column_name)
          || ' '
          || CASE WHEN c_old.column_default LIKE 'nextval(%'
                   AND pgmemento.trim_outer_quotes(c_old.column_default) LIKE E'%_seq\'::regclass)' THEN
               CASE WHEN c_old.data_type = 'smallint' THEN 'smallserial'
                    WHEN c_old.data_type = 'integer' THEN 'serial'
                    WHEN c_old.data_type = 'bigint' THEN 'bigserial'
                    ELSE c_old.data_type END
             ELSE
               c_old.data_type
               || CASE WHEN c_old.column_default IS NOT NULL
                  THEN ' DEFAULT ' || c_old.column_default ELSE '' END
             END
          || CASE WHEN c_old.not_null THEN ' NOT NULL' ELSE '' END,
          ', ' ORDER BY c_old.id
        ) INTO stmt
      FROM
        pgmemento.audit_table_log t
      JOIN
        pgmemento.audit_column_log c_old
        ON c_old.audit_table_id = t.id
      LEFT JOIN LATERAL (
        SELECT
          c.column_name
        FROM
          pgmemento.audit_table_log atl
        JOIN
          pgmemento.audit_column_log c
          ON c.audit_table_id = atl.id
        WHERE
          atl.table_name = t.table_name
          AND atl.schema_name = t.schema_name
          AND upper(c.txid_range) IS NULL
          AND lower(c.txid_range) IS NOT NULL
        ) c_new
        ON c_old.column_name = c_new.column_name
      WHERE
        upper(c_old.txid_range) = $1
        AND c_new.column_name IS NULL
        AND t.table_name = $5
        AND t.schema_name = $6;

      -- try to execute ALTER TABLE command
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      -- fill in data with an UPDATE statement if audit_id is set
      IF $2 IS NOT NULL THEN
        PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6, $7);
      END IF;

      EXCEPTION
        WHEN duplicate_column THEN
          -- if column already exists just do an UPDATE
          PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6, $7);
	END;

  -- DELETE or TRUNCATE case
  WHEN $4 = 7 OR $4 = 8 THEN
    IF $2 IS NOT NULL THEN
      BEGIN
        EXECUTE format(
          'INSERT INTO %I.%I SELECT * FROM jsonb_populate_record(null::%I.%I, $1)',
          $6, $5, $6, $5)
          USING $3;

        -- row has already been re-inserted, so update it based on the values of this deleted version
        EXCEPTION
          WHEN unique_violation THEN
            -- merge changes with recent version of table record and update row
            PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6, $7);
      END;
    END IF;

  -- DROP AUDIT_ID case
  WHEN $4 = 81 THEN
    -- first check if a preceding CREATE TABLE event already recreated the audit_id
    BEGIN
      current_transaction := current_setting('pgmemento.t' || txid_current())::int;

      EXCEPTION
        WHEN undefined_object THEN
          NULL;
    END;

    BEGIN
      IF current_transaction IS NULL OR NOT EXISTS (
        SELECT
          1
        FROM
          pgmemento.table_event_log
        WHERE
          transaction_id = current_transaction
          AND table_name = $5
          AND schema_name = $6
          AND op_id = 1  -- RE/CREATE TABLE event
      ) THEN
        -- try to restart auditing for table
        PERFORM
          pgmemento.create_table_audit(table_name, schema_name, audit_id_column, log_old_data, log_new_data, FALSE)
        FROM
          pgmemento.audit_table_log
        WHERE
          table_name = $5
          AND schema_name = $6
          AND upper(txid_range) = $1;
      END IF;
      
      -- audit_id already exists
      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert DROP AUDIT_ID event for table %.%: %', $6, $5, SQLERRM;
    END;

  -- DROP TABLE case
  WHEN $4 = 9 THEN
    -- collect information of columns of dropped table
    SELECT
      t.log_id,
      string_agg(
        quote_ident(c_old.column_name)
        || ' '
        || CASE WHEN c_old.column_default LIKE 'nextval(%'
                 AND pgmemento.trim_outer_quotes(c_old.column_default) LIKE E'%_seq\'::regclass)' THEN
             CASE WHEN c_old.data_type = 'smallint' THEN 'smallserial'
                  WHEN c_old.data_type = 'integer' THEN 'serial'
                  WHEN c_old.data_type = 'bigint' THEN 'bigserial'
                  ELSE c_old.data_type END
           ELSE
             c_old.data_type
             || CASE WHEN c_old.column_default IS NOT NULL
                THEN ' DEFAULT ' || c_old.column_default ELSE '' END
           END
        || CASE WHEN c_old.not_null THEN ' NOT NULL' ELSE '' END,
        ', ' ORDER BY c_old.ordinal_position
      )
    INTO
      table_log_id,
      stmt
    FROM
      pgmemento.audit_table_log t
    JOIN
      pgmemento.audit_column_log c_old
      ON c_old.audit_table_id = t.id
    LEFT JOIN LATERAL (
      SELECT
        atl.table_name
      FROM
        pgmemento.audit_table_log atl
      WHERE
        atl.table_name = t.table_name
        AND atl.schema_name = t.schema_name
        AND upper(atl.txid_range) IS NULL
        AND lower(atl.txid_range) IS NOT NULL
      ) t_new
      ON t.table_name = t_new.table_name
    WHERE
      upper(c_old.txid_range) = $1
      AND c_old.column_name <> $7
      AND t_new.table_name IS NULL
      AND t.table_name = $5
      AND t.schema_name = $6
    GROUP BY
      t.log_id;

    -- try to create table
    IF stmt IS NOT NULL THEN
      PERFORM pgmemento.log_table_event($5, $6, 'RECREATE TABLE');
      PERFORM set_config('pgmemento.' || $6 || '.' || $5, table_log_id::text, TRUE);
      EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (' || stmt || ')', $6, $5);
    END IF;

    -- fill in truncated data with an INSERT statement if audit_id is set
    IF $2 IS NOT NULL THEN
      PERFORM pgmemento.recover_audit_version($1, $2, $3, 8, $5, $6, $7);
    END IF;

  END CASE;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE VIEW pgmemento.audit_tables AS
  SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    atl.audit_id_column,
    atl.log_old_data,
    atl.log_new_data,
    bounds.txid_min,
    bounds.txid_max,
    CASE WHEN tg.tgenabled IS NOT NULL AND tg.tgenabled <> 'D' THEN
      TRUE
    ELSE
      FALSE
    END AS tg_is_active
  FROM
    pg_class c
  JOIN
    pg_namespace n
    ON c.relnamespace = n.oid
  JOIN
    pgmemento.audit_schema_log asl
    ON asl.schema_name = n.nspname
   AND upper(asl.txid_range) IS NULL
   AND lower(asl.txid_range) IS NOT NULL
  JOIN (
    SELECT DISTINCT ON (log_id)
      log_id,
      table_name,
      schema_name,
      audit_id_column,
      log_old_data,
      log_new_data
    FROM
      pgmemento.audit_table_log
    WHERE
      upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL
    ORDER BY
      log_id, id
    ) atl
    ON atl.table_name = c.relname
   AND atl.schema_name = n.nspname
  JOIN
    pg_attribute a
    ON a.attrelid = c.oid
   AND a.attname = atl.audit_id_column
  JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(atl.log_id)
    ) bounds ON (true)
  LEFT JOIN (
    SELECT
      tgrelid,
      tgenabled
    FROM
      pg_trigger
    WHERE
      tgname = 'pgmemento_transaction_trigger'::name
    ) AS tg
    ON c.oid = tg.tgrelid
  WHERE
    c.relkind = 'r'
  ORDER BY
    schemaname,
    tablename;


\echo
\echo 'pgMemento upgrade completed!'
