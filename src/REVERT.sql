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
  column_name TEXT;
  delimiter TEXT;
  update_set TEXT;
  join_columns TEXT := '';
BEGIN
  -- INSERT case
  IF $4 = 1 THEN
    BEGIN
      EXECUTE format(
        'DELETE FROM %I.%I WHERE audit_id = $1',
        $6, $5) 
        USING aid;

      -- row is already deleted
      EXCEPTION
        WHEN no_data_found THEN
          NULL;
    END;

  -- UPDATE case
  ELSIF $4 = 2 THEN
    -- update the row with values from changes
    IF $3 IS NOT NULL AND $3 <> '{}'::jsonb THEN
      -- begin UPDATE statement
      delimiter := '';
      update_set := format('UPDATE %I.%I t SET', $6, $5);

      -- loop over found keys and extend strings fpr UPDATE statement
      FOR column_name IN SELECT jsonb_object_keys($3) LOOP
        update_set := update_set || delimiter || format(' %I = j.%I', column_name, column_name);
		join_columns := join_columns || delimiter || format(' %I', column_name);
        delimiter := ',';
      END LOOP;

      -- complete statement and add condition
      update_set := update_set || ' FROM (SELECT ' || join_columns
        || format(' FROM jsonb_populate_record(null::%I.%I, %L)) j ', $6, $5, $3)
        || format('WHERE t.audit_id = %L', $2);

      EXECUTE update_set;
    END IF;

  -- DELETE and TRUNCATE case
  ELSE
    BEGIN
      EXECUTE format(
        'INSERT INTO %I.%I SELECT * FROM jsonb_populate_record(null::%I.%I, %L)',
         $6, $5, $6, $5, $3);

      -- row has already been re-inserted, so update it based on the values of this deleted version
      EXCEPTION
        WHEN unique_violation THEN
          -- merge changes with recent version of table record and update row
          PERFORM pgmemento.recover_audit_version($1, $2, $3, 2, $5, $6);
    END;
  END IF;
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
      CASE WHEN e.op_id > 2 THEN
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id ASC, r.id DESC)
      ELSE
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id DESC, r.id DESC)
      END AS audit_order,
      CASE WHEN e.op_id > 2 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM pgmemento.transaction_log t 
    JOIN pgmemento.table_event_log e
      ON e.transaction_id = t.txid
    JOIN pgmemento.row_log r
      ON r.event_id = e.id
    JOIN pgmemento.audit_table_log a 
      ON a.relid = e.table_relid
    JOIN pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
     AND d.schemaname = a.schema_name
     WHERE upper(a.txid_range) IS NULL
       AND t.txid = $1
       ORDER BY dependency_order, e.id DESC, audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql;

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
      CASE WHEN e.op_id > 2 THEN
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id ASC, r.id DESC)
      ELSE
        rank() OVER (PARTITION BY r.event_id ORDER BY r.audit_id DESC, r.id DESC)
      END AS audit_order,
      CASE WHEN e.op_id > 2 THEN
        rank() OVER (ORDER BY d.depth ASC)
      ELSE
        rank() OVER (ORDER BY d.depth DESC)
      END AS dependency_order
    FROM pgmemento.transaction_log t 
    JOIN pgmemento.table_event_log e
      ON e.transaction_id = t.txid
    JOIN pgmemento.row_log r
      ON r.event_id = e.id
    JOIN pgmemento.audit_table_log a 
      ON a.relid = e.table_relid
    JOIN pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
     AND d.schemaname = a.schema_name
      WHERE upper(a.txid_range) IS NULL
        AND t.txid BETWEEN $1 AND $2
        ORDER BY t.id DESC, dependency_order, e.id DESC, audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql;


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
      CASE WHEN q.op_id > 2 THEN
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id ASC)
      ELSE
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id DESC)
      END AS audit_order,
      CASE WHEN q.op_id > 2 THEN
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
      FROM pgmemento.row_log r
      JOIN pgmemento.table_event_log e ON e.id = r.event_id
      JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
        WHERE t.txid = $1
        ORDER BY r.audit_id, r.id
    ) q
    JOIN pgmemento.audit_table_log a
      ON a.relid = q.table_relid
    JOIN pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
      AND d.schemaname = a.schema_name
      ORDER BY dependency_order, q.event_id DESC, audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$
LANGUAGE plpgsql;

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
      CASE WHEN q.op_id > 2 THEN
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id ASC)
      ELSE
        rank() OVER (PARTITION BY q.event_id ORDER BY q.audit_id DESC)
      END AS audit_order,
      CASE WHEN q.op_id > 2 THEN
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
      FROM pgmemento.row_log r
      JOIN pgmemento.table_event_log e ON e.id = r.event_id
      JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
        WHERE t.txid BETWEEN $1 AND $2
        ORDER BY r.audit_id, r.id
    ) q
    JOIN pgmemento.audit_table_log a
      ON a.relid = q.table_relid
    JOIN pgmemento.audit_tables_dependency d
      ON d.tablename = a.table_name
      AND d.schemaname = a.schema_name
      ORDER BY dependency_order, q.event_id DESC, audit_order
  LOOP
    PERFORM pgmemento.recover_audit_version(rec.txid, rec.audit_id, rec.changes, rec.op_id, rec.table_name, rec.schema_name);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql;