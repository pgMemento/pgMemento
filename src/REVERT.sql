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
*   recover_audit_version(tid BIGINT, aid BIGINT, changes JSONB, table_op INTEGER,
*     table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   revert_distinct_transaction(tid BIGINT) RETURNS SETOF VOID
*   revert_distinct_transactions(start_from_tid BIGINT, end_at_tid BIGINT) RETURNS SETOF VOID
*   revert_transaction(tid BIGINT) RETURNS SETOF VOID
*   revert_transactions(start_from_tid BIGINT, end_at_tid BIGINT) RETURNS SETOF VOID
***********************************************************/

/**********************************************************
* RECOVER
*
* Procedure to apply DML operations recovered from the logs
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.recover_audit_version(
  tid BIGINT,
  aid BIGINT, 
  changes JSONB,
  table_op INTEGER,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
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

  -- ADD COLUMN case
  WHEN $4 = 2 THEN
    -- collect added columns
    SELECT
      string_agg(
        'DROP COLUMN '
        || c.column_name,
        ', ' ORDER BY c.id DESC
      ) INTO stmt
    FROM
      pgmemento.audit_column_log c
    JOIN
      pgmemento.audit_table_log t
      ON c.audit_table_id = t.id
    WHERE
      lower(c.txid_range) = $1
      AND upper(c.txid_range) IS NULL
      AND t.table_name = $5
      AND t.schema_name = $6;

    BEGIN
      -- try to execute ALTER TABLE command
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert ADD COLUMN event for table %.%: %', $6, $5, SQLERRM;
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
    IF $3 IS NOT NULL AND $3 <> '{}'::jsonb THEN
      -- create SET part
      SELECT
        string_agg(key || '=' || quote_nullable(value),', ') INTO stmt
      FROM
        jsonb_each_text($3);

      BEGIN
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
    -- collect information of altered columns
    SELECT
      string_agg(
        'ALTER COLUMN '
        || c_new.column_name
        || ' SET DATA TYPE '
        || c_old.data_type
        || ' USING '
        || c_new.column_name
        || '::'
        || c_old.data_type,
        ', ' ORDER BY c_new.id
      ) INTO stmt
    FROM
      pgmemento.audit_column_log c_old,
      pgmemento.audit_column_log c_new,
      pgmemento.audit_table_log t
    WHERE
      c_old.audit_table_id = t.id
      AND c_new.audit_table_id = t.id
      AND t.table_name = $5
      AND t.schema_name = $6
      AND upper(c_old.txid_range) = $1
      AND lower(c_new.txid_range) = $1
      AND upper(c_new.txid_range) IS NULL
      AND c_old.ordinal_position = c_new.ordinal_position
      AND c_old.data_type <> c_new.data_type;

    BEGIN
      -- try to execute ALTER TABLE command
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      -- fill in data with an UPDATE statement
      PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6);

      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert ALTER COLUMN event for table %.% : %', $6, $5, SQLERRM;
    END;

  -- DROP COLUMN case
  WHEN $4 = 6 THEN
    -- collect information of dropped columns
    SELECT
      string_agg(
        'ADD COLUMN '
        || c_old.column_name
        || ' '
        || c_old.data_type
        || CASE WHEN c_old.column_default IS NOT NULL THEN ' DEFAULT ' || c_old.column_default ELSE '' END 
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
      ) c_new
      ON c_old.column_name = c_new.column_name
    WHERE
      upper(c_old.txid_range) = $1
      AND c_new.column_name IS NULL
      AND t.table_name = $5
      AND t.schema_name = $6;

    BEGIN
      -- try to execute ALTER TABLE command
      IF stmt IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ' || stmt , $6, $5);
      END IF;

      -- fill in data with an UPDATE statement
      PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6);

      EXCEPTION
        WHEN duplicate_column THEN
          -- if column already exist just do an UPDATE
          PERFORM pgmemento.recover_audit_version($1, $2, $3, 4, $5, $6);
	END;

  -- DELETE or TRUNCATE case
  WHEN $4 = 7 OR $4 = 8 THEN
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
        WHEN others THEN
          NULL;
    END;

  -- DROP TABLE case
  WHEN $4 = 9 THEN
    -- collect information of columns of dropped table
    SELECT
      string_agg(
        c_old.column_name
        || ' '
        || c_old.data_type
        || CASE WHEN c_old.column_default IS NOT NULL THEN ' DEFAULT ' || c_old.column_default ELSE '' END
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
        AND upper(txid_range) IS NULL
      ) t_new
      ON t.table_name = t_new.table_name
    WHERE
      upper(c_old.txid_range) = $1
      AND c_old.column_name <> 'audit_id'
      AND t_new.table_name IS NULL
      AND t.table_name = $5
      AND t.schema_name = $6;

    -- try to create table
    BEGIN
      IF stmt IS NOT NULL THEN
        EXECUTE format('CREATE TABLE %I.%I (' || stmt || ')', $6, $5);
      END IF;

      -- fill in truncated data
      PERFORM pgmemento.recover_audit_version($1, $2, $3, 8, $5, $6);

      EXCEPTION
        WHEN others THEN
          RAISE NOTICE 'Could not revert CREATE TABLE event for table %.% : %', $6, $5, SQLERRM;
    END;

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
CREATE OR REPLACE FUNCTION pgmemento.revert_transaction(tid BIGINT) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
      t.txid,
      r.audit_id, 
      r.changes,
      e.op_id, 
      a.table_name,
      a.schema_name,
      CASE WHEN e.op_id > 4 THEN
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id ASC, r.id DESC)
      ELSE
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id DESC, r.id DESC)
      END AS audit_order,
      CASE WHEN e.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM 
      pgmemento.transaction_log t 
    JOIN
      pgmemento.table_event_log e
      ON e.transaction_id = t.txid
    JOIN
      pgmemento.audit_table_log a 
      ON a.relid = e.table_relid
    LEFT JOIN
      pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
     AND d.schemaname = a.schema_name
    LEFT JOIN
      pgmemento.row_log r
      ON r.event_id = e.id
    WHERE
      t.txid = $1
    ORDER BY
      dependency_order,
      e.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.revert_transactions(
  start_from_tid BIGINT, 
  end_at_tid BIGINT
  ) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT
      t.txid,
      r.audit_id, 
      r.changes,
      e.op_id,
      a.table_name,
      a.schema_name,
      CASE WHEN e.op_id > 4 THEN
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id ASC, r.id DESC)
      ELSE
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id DESC, r.id DESC)
      END AS audit_order,
      CASE WHEN e.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM
      pgmemento.transaction_log t 
    JOIN
      pgmemento.table_event_log e
      ON e.transaction_id = t.txid
    JOIN
      pgmemento.audit_table_log a 
      ON a.relid = e.table_relid
    LEFT JOIN
      pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
     AND d.schemaname = a.schema_name
    LEFT JOIN
      pgmemento.row_log r
      ON r.event_id = e.id
    WHERE
      t.txid BETWEEN $1 AND $2
    ORDER BY
      t.id DESC,
      dependency_order,
      e.id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
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
CREATE OR REPLACE FUNCTION pgmemento.revert_distinct_transaction(tid BIGINT) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN 
    SELECT
      q.txid,
      q.audit_id,
      q.op_id, 
      q.changes, 
      a.table_name,
      a.schema_name,
      CASE WHEN q.op_id > 4 THEN
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id ASC)
      ELSE
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id DESC)
      END AS audit_order,
      CASE WHEN q.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM (
      SELECT DISTINCT ON (r.audit_id)
        t.txid,
        r.audit_id,
        r.event_id,
        e.table_relid,
        e.op_id,
        pgmemento.jsonb_merge(r.changes) OVER () AS changes
      FROM
        pgmemento.transaction_log t 
      JOIN
        pgmemento.table_event_log e
        ON e.transaction_id = t.txid
      LEFT JOIN
        pgmemento.row_log r
        ON r.event_id = e.id
      WHERE
        t.txid = $1
      ORDER BY
        r.audit_id,
        r.id
    ) q
    JOIN pgmemento.audit_table_log a
      ON a.relid = q.table_relid
    LEFT JOIN pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
      AND d.schemaname = a.schema_name
    ORDER BY
      dependency_order,
      q.event_id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.revert_distinct_transactions(
  start_from_tid BIGINT, 
  end_at_tid BIGINT
  ) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN 
    SELECT
      q.txid,
      q.audit_id,
      q.op_id, 
      q.changes, 
      a.table_name,
      a.schema_name,
      CASE WHEN q.op_id > 4 THEN
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id ASC)
      ELSE
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id DESC)
      END AS audit_order,
      CASE WHEN q.op_id > 4 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM (
      SELECT DISTINCT ON (r.audit_id)
        t.txid,
        r.audit_id,
        r.event_id,
        e.table_relid,
        e.op_id,
        pgmemento.jsonb_merge(r.changes) OVER () AS changes
      FROM
        pgmemento.transaction_log t 
      JOIN
        pgmemento.table_event_log e
        ON e.transaction_id = t.txid
      LEFT JOIN
        pgmemento.row_log r
        ON r.event_id = e.id
      WHERE
        t.txid BETWEEN $1 AND $2
      ORDER BY
        r.audit_id,
        r.id
    ) q
    JOIN pgmemento.audit_table_log a
      ON a.relid = q.table_relid
    LEFT JOIN pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
      AND d.schemaname = a.schema_name
    ORDER BY
      dependency_order,
      q.event_id DESC,
      audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql STRICT;