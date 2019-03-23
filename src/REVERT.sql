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
*     table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
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
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
DECLARE
  stmt TEXT;
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

  -- RENAME TABLE case
  WHEN $4 = 12 THEN
    BEGIN
      -- collect information of renamed table
      SELECT
        t_new.table_name
      INTO
        stmt
      FROM
        pgmemento.audit_table_log t_old,
        pgmemento.audit_table_log t_new
      WHERE
        t_old.log_id = t_new.log_id
        AND t_old.table_name = $5
        AND t_old.schema_name = $6
        AND upper(t_new.txid_range) = $1
        AND lower(t_old.txid_range) = $1;

      -- try to re-rename table
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', $6, $5, stmt);
      END IF;

      EXCEPTION
        WHEN undefined_table THEN
          RAISE NOTICE 'Could not revert RENAME TABLE event for table %.%: %', $6, $5, SQLERRM;
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
    PERFORM pgmemento.drop_table_audit($5, $6);


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
          'DELETE FROM %I.%I WHERE audit_id = $1',
          $6, $5)
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
              quote_ident(j.key) || '=' || quote_nullable(j.value->>0)
            END AS set_columns
          FROM
            jsonb_each($3) j
          LEFT JOIN
            pgmemento.audit_column_log c
            ON c.column_name = j.key
           AND jsonb_typeof(j.value) = 'object'
           AND upper(c.txid_range) IS NULL
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
          'UPDATE %I.%I t SET ' || stmt || ' WHERE t.audit_id = $1',
          $6, $5)
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
          format('ALTER COLUMN %I SET DATA TYPE %s USING pgmemento.restore_change(%L, audit_id, %L, NULL::%s)',
            c_new.column_name, c_old.data_type, $1, quote_ident(c_old.column_name), c_old.data_type),
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
        PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6);
      END IF;

      EXCEPTION
        WHEN duplicate_column THEN
          -- if column already exist just do an UPDATE
          PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6);
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
            PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6);
      END;
    END IF;

  -- DROP TABLE case
  WHEN $4 = 9 THEN
    -- collect information of columns of dropped table
    SELECT
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
      ) INTO stmt
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
      AND c_old.column_name <> 'audit_id'
      AND t_new.table_name IS NULL
      AND t.table_name = $5
      AND t.schema_name = $6;

    -- try to create table
    IF stmt IS NOT NULL THEN
      EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (' || stmt || ')', $6, $5);
    END IF;

    -- fill in truncated data with an INSERT statement if audit_id is set
    IF $2 IS NOT NULL THEN
      PERFORM pgmemento.recover_audit_version($1, $2, $3, 8, $5, $6);
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
      r.changes,
      e.op_id, 
      a.table_name,
      a.schema_name,
      rank() OVER (PARTITION BY r.event_id ORDER BY r.id DESC) AS audit_order,
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
     AND ((a.txid_range @> t.id::numeric AND e.op_id <> 12)
      OR lower(a.txid_range) = t.id::numeric)
    LEFT JOIN
      pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    LEFT JOIN
      pgmemento.row_log r
      ON r.event_id = e.id AND e.op_id <> 5
    WHERE
      t.id = $1
    ORDER BY
      dependency_order,
      e.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.id, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
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
      r.changes,
      e.op_id,
      a.table_name,
      a.schema_name,
      rank() OVER (PARTITION BY r.event_id ORDER BY r.id DESC) AS audit_order,
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
     AND ((a.txid_range @> t.id::numeric AND e.op_id <> 12)
      OR lower(a.txid_range) = t.id::numeric)
    LEFT JOIN
      pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    LEFT JOIN
      pgmemento.row_log r
      ON r.event_id = e.id AND e.op_id <> 5
    WHERE
      t.id BETWEEN $1 AND $2
    ORDER BY
      t.id DESC,
      dependency_order,
      e.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.id, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql STRICT;


/**********************************************************
* REVERT DISTINCT TRANSACTION
*
* Procedures to revert a single transaction or a range of
* transactions. For each distinct audit_it only the oldest 
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
      q.changes, 
      a.table_name,
      a.schema_name,
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
        pgmemento.jsonb_merge(changes ORDER BY id DESC) AS changes
      FROM (
        SELECT
          r.id,
          r.audit_id,
          r.changes,
          e.id AS event_id,
          e.table_name,
          e.schema_name,
          e.transaction_id,
          CASE WHEN r.audit_id IS NULL THEN e.id ELSE NULL END AS ddl_event
        FROM
          pgmemento.table_event_log e
        LEFT JOIN
          pgmemento.row_log r
          ON r.event_id = e.id AND e.op_id <> 5
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
      ON a.table_name = e.table_name
     AND a.schema_name = e.schema_name 
     AND ((a.txid_range @> q.tid::numeric AND e1.op_id <> 12)
      OR lower(a.txid_range) = q.tid::numeric)
    LEFT JOIN pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    WHERE
      NOT (
        e1.op_id = 1
        AND e2.op_id = 9
      )
      AND NOT (
        e1.op_id = 3
        AND (e2.op_id > 6 AND e2.op_id < 10)
      )
    ORDER BY
      dependency_order,
      e1.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.tid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
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
      q.changes, 
      a.table_name,
      a.schema_name,
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
        pgmemento.jsonb_merge(changes ORDER BY id DESC) AS changes
      FROM (
        SELECT
          r.id,
          r.audit_id,
          r.changes,
          e.id AS event_id,
          e.table_name,
          e.schema_name,
          e.transaction_id,
          CASE WHEN r.audit_id IS NULL THEN e.id ELSE NULL END AS ddl_event
        FROM
          pgmemento.table_event_log e
        LEFT JOIN
          pgmemento.row_log r
          ON r.event_id = e.id AND e.op_id <> 5
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
      ON a.table_name = e.table_name
     AND a.schema_name = e.schema_name 
     AND ((a.txid_range @> q.tid::numeric AND e1.op_id <> 12)
      OR lower(a.txid_range) = q.tid::numeric)
    LEFT JOIN pgmemento.audit_tables_dependency d
      ON d.table_log_id = a.log_id
    WHERE
      NOT (
        e1.op_id = 1
        AND e2.op_id = 9
      )
      AND NOT (
        e1.op_id = 3
        AND (e2.op_id > 6 AND e2.op_id < 10)
      )
    ORDER BY
      dependency_order,
      e1.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.tid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql STRICT;
