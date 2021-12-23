-- REVERT.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This skript is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides functions to revert single transactions and entire database
-- states.
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                   | Author
-- 0.7.9     2021-12-23   session variables starting with letter          ol-teuto
-- 0.7.8     2021-03-21   fix revert for array columns                    FKun
-- 0.7.7     2020-04-20   add revert for DROP AUDIT_ID event              FKun
-- 0.7.6     2020-04-19   add revert for REINIT TABLE event               FKun 
-- 0.7.5     2020-04-13   remove txid from log_table_event                FKun
-- 0.7.4     2020-03-23   reflect configurable audit_id column            FKun
-- 0.7.3     2020-02-29   reflect new schema of row_log table             FKun
-- 0.7.2     2020-01-09   reflect changes on schema and triggers          FKun
-- 0.7.1     2019-04-21   reuse log_id when reverting DROP TABLE events   FKun
-- 0.7.0     2019-03-23   reflect schema changes in UDFs                  FKun
-- 0.6.4     2019-02-14   Changed revert ADD AUDIT_ID events              FKun
-- 0.6.3     2018-11-20   revert updates with composite data types        FKun
-- 0.6.2     2018-09-24   improved reverts when column type is altered    FKun
-- 0.6.1     2018-07-24   support for RENAME events & improved queries    FKun
-- 0.6.0     2018-07-16   reflect changes in transaction_id handling      FKun
-- 0.5.1     2017-08-08   sort reverts by row_log ID and not audit_id     FKun
--                        improved revert_distinct_transaction(s)
-- 0.5.0     2017-07-25   add revert support for DDL events               FKun
-- 0.4.1     2017-04-11   improved revert_distinct_transaction(s)         FKun
-- 0.4.0     2017-03-08   integrated table dependencies                   FKun
--                        recover_audit_version takes txid as first arg
-- 0.3.0     2016-04-29   splitting up the functions to match the new     FKun
--                        logging behavior for table events
-- 0.2.2     2016-03-08   added another revert procedure                  FKun
-- 0.2.1     2016-02-14   removed dynamic sql code                        FKun
-- 0.2.0     2015-02-26   added revert_transaction procedure              FKun
-- 0.1.0     2014-11-26   initial commit                                  FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   recover_audit_version(tid INTEGER, aid BIGINT, changes JSONB, table_op INTEGER,
*     table_name TEXT, schema_name TEXT DEFAULT 'public'::text, audit_id_column_name TEXT DEFAULT 'pgmemento_audit_id'::text) RETURNS SETOF VOID
*   revert_distinct_transaction(tid INTEGER) RETURNS SETOF VOID
*   revert_distinct_transactions(start_from_tid INTEGER, end_at_tid INTEGER) RETURNS SETOF VOID
*   revert_transaction(tid INTEGER) RETURNS SETOF VOID
*   revert_transactions(start_from_tid INTEGER, end_at_tid INTEGER) RETURNS SETOF VOID
***********************************************************/

/**********************************************************
* RECOVER
*
* Procedure to apply DML operations recovered from the logs
***********************************************************/
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


/**********************************************************
* REVERT TRANSACTION
*
* Procedures to revert a single transaction or a range of
* transactions. All table operations are processed in
* order of table dependencies so no foreign keys should be
* violated.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.revert_transaction(tid INTEGER) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
      t.id,
      r.audit_id,
      r.old_data,
      e.op_id,
      a.table_name,
      a.schema_name,
      a.audit_id_column,
      rank() OVER (PARTITION BY r.event_key ORDER BY r.id DESC) AS audit_order,
      CASE WHEN e.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM
      pgmemento.transaction_log t
    JOIN
      pgmemento.table_event_log e
      ON e.transaction_id = t.id
    JOIN
      pgmemento.audit_table_log a
      ON a.table_name = e.table_name
     AND a.schema_name = e.schema_name
     AND ((a.txid_range @> t.id::numeric AND NOT e.op_id IN (1, 11, 21))
      OR (lower(a.txid_range) = t.id::numeric AND NOT e.op_id IN (81, 9)))
    LEFT JOIN
      pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    LEFT JOIN
      pgmemento.row_log r
      ON r.event_key = e.event_key
     AND e.op_id <> 5
    WHERE
      t.id = $1
    ORDER BY
      dependency_order,
      e.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.id, rec.audit_id, rec.old_data, rec.op_id, rec.table_name, rec.schema_name, rec.audit_id_column);
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.revert_transactions(
  start_from_tid INTEGER,
  end_at_tid INTEGER
  ) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
      t.id,
      r.audit_id,
      r.old_data,
      e.op_id,
      a.table_name,
      a.schema_name,
      a.audit_id_column,
      rank() OVER (PARTITION BY t.id, r.event_key ORDER BY r.id DESC) AS audit_order,
      CASE WHEN e.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM
      pgmemento.transaction_log t
    JOIN
      pgmemento.table_event_log e
      ON e.transaction_id = t.id
    JOIN
      pgmemento.audit_table_log a
      ON a.table_name = e.table_name
     AND a.schema_name = e.schema_name
     AND ((a.txid_range @> t.id::numeric AND NOT e.op_id IN (1, 11, 21))
      OR (lower(a.txid_range) = t.id::numeric AND NOT e.op_id IN (81, 9)))
    LEFT JOIN
      pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    LEFT JOIN
      pgmemento.row_log r
      ON r.event_key = e.event_key
     AND e.op_id <> 5
    WHERE
      t.id BETWEEN $1 AND $2
    ORDER BY
      t.id DESC,
      dependency_order,
      e.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.id, rec.audit_id, rec.old_data, rec.op_id, rec.table_name, rec.schema_name, rec.audit_id_column);
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;


/**********************************************************
* REVERT DISTINCT TRANSACTION
*
* Procedures to revert a single transaction or a range of
* transactions. For each distinct audit_id only the oldest
* operation is applied to make the revert process faster.
* This can be a fallback method for revert_transaction if
* foreign key violations are occurring.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.revert_distinct_transaction(tid INTEGER) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
      q.tid,
      q.audit_id,
      CASE WHEN e2.op_id > 6 THEN e2.op_id ELSE e1.op_id END AS op_id,
      q.old_data,
      a.table_name,
      a.schema_name,
      a.audit_id_column,
      rank() OVER (PARTITION BY e1.id ORDER BY q.row_log_id DESC) AS audit_order,
      CASE WHEN e1.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM (
      SELECT
        audit_id,
        table_name,
        schema_name,
        transaction_id AS tid,
        min(event_id) AS first_event,
        max(event_id) AS last_event,
        min(id) AS row_log_id,
        pgmemento.jsonb_merge(old_data ORDER BY id DESC) AS old_data
      FROM (
        SELECT
          r.id,
          r.audit_id,
          r.old_data,
          e.id AS event_id,
          e.table_name,
          e.schema_name,
          e.transaction_id,
          CASE WHEN r.audit_id IS NULL THEN e.id ELSE NULL END AS ddl_event
        FROM
          pgmemento.table_event_log e
        LEFT JOIN
          pgmemento.row_log r
          ON r.event_key = e.event_key
         AND e.op_id <> 5
        WHERE
          e.transaction_id = $1
      ) s
      GROUP BY
        audit_id,
        table_name,
        schema_name,
        ddl_event,
        transaction_id
    ) q
    JOIN
      pgmemento.table_event_log e1
      ON e1.id = q.first_event
    JOIN
      pgmemento.table_event_log e2
      ON e2.id = q.last_event
    JOIN
      pgmemento.audit_table_log a
      ON a.table_name = q.table_name
     AND a.schema_name = q.schema_name
     AND (a.txid_range @> q.tid::numeric
      OR lower(a.txid_range) = q.tid::numeric)
    LEFT JOIN pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    WHERE
      NOT (
        e1.op_id = 1
        AND e2.op_id = 9
      )
      AND NOT (
        e1.op_id = 21
        AND e2.op_id = 81
      )
      AND NOT (
        e1.op_id = 3
        AND (e2.op_id BETWEEN 7 AND 9)
      )
    ORDER BY
      dependency_order,
      e1.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.tid, rec.audit_id, rec.old_data, rec.op_id, rec.table_name, rec.schema_name, rec.audit_id_column);
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.revert_distinct_transactions(
  start_from_tid INTEGER,
  end_at_tid INTEGER
  ) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
      q.tid,
      q.audit_id,
      CASE WHEN e2.op_id > 6 THEN e2.op_id ELSE e1.op_id END AS op_id,
      q.old_data,
      a.table_name,
      a.schema_name,
      a.audit_id_column,
      rank() OVER (PARTITION BY e1.id ORDER BY q.row_log_id DESC) AS audit_order,
      CASE WHEN e1.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM (
      SELECT
        audit_id,
        table_name,
        schema_name,
        min(transaction_id) AS tid,
        min(event_id) AS first_event,
        max(event_id) AS last_event,
        min(id) AS row_log_id,
        pgmemento.jsonb_merge(old_data ORDER BY id DESC) AS old_data
      FROM (
        SELECT
          r.id,
          r.audit_id,
          r.old_data,
          e.id AS event_id,
          e.table_name,
          e.schema_name,
          e.transaction_id,
          CASE WHEN r.audit_id IS NULL THEN e.id ELSE NULL END AS ddl_event
        FROM
          pgmemento.table_event_log e
        LEFT JOIN
          pgmemento.row_log r
          ON r.event_key = e.event_key
         AND e.op_id <> 5
        WHERE
          e.transaction_id BETWEEN $1 AND $2
      ) s
      GROUP BY
        audit_id,
        table_name,
        schema_name,
        ddl_event
    ) q
    JOIN
      pgmemento.table_event_log e1
      ON e1.id = q.first_event
    JOIN
      pgmemento.table_event_log e2
      ON e2.id = q.last_event
    JOIN
      pgmemento.audit_table_log a
      ON a.table_name = q.table_name
     AND a.schema_name = q.schema_name
     AND (a.txid_range @> q.tid::numeric
      OR lower(a.txid_range) = q.tid::numeric)
    LEFT JOIN pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    WHERE
      NOT (
        e1.op_id = 1
        AND e2.op_id = 9
      )
      AND NOT (
        e1.op_id = 21
        AND e2.op_id = 81
      )
      AND NOT (
        e1.op_id = 3
        AND (e2.op_id BETWEEN 7 AND 9)
      )
    ORDER BY
      q.tid DESC,
      dependency_order,
      e1.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.tid, rec.audit_id, rec.old_data, rec.op_id, rec.table_name, rec.schema_name, rec.audit_id_column);
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;
