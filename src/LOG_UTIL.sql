-- LOG_UTIL.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides utility functions for pgMemento and creates VIEWs
-- for document auditing and table dependencies
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                 | Author
-- 0.4.0     2017-03-06   new view for table dependencies               FKun
-- 0.3.0     2016-04-14   reflected changes in log tables               FKun
-- 0.2.1     2016-04-05   additional column in audit_tables view        FKun
-- 0.2.0     2016-02-15   get txids done right                          FKun
-- 0.1.0     2015-06-20   initial commit                                FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   delete_audit_table_log(table_oid INTEGER) RETURNS SETOF OID
*   delete_table_event_log(tid BIGINT, table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF INTEGER
*   delete_txid_log(t_id BIGINT) RETURNS BIGINT
*   get_max_txid_to_audit_id(aid BIGINT) RETURNS BIGINT
*   get_min_txid_to_audit_id(aid BIGINT) RETURNS BIGINT
*   get_txid_bounds_to_table(table_name TEXT, schema_name TEXT DEFAULT 'public', OUT txid_min BIGINT, OUT txid_max BIGINT) RETURNS RECORD
*   get_txids_to_audit_id(aid BIGINT) RETURNS SETOF BIGINT
*
* VIEW:
*   audit_tables
*   audit_tables_dependency
*
***********************************************************/

/**********************************************************
* GET TRANSACTION ID
*
* Simple functions to return the transaction_id related to
* certain database entities 
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_txids_to_audit_id(aid BIGINT) RETURNS SETOF BIGINT AS
$$
SELECT t.txid
  FROM pgmemento.transaction_log t
  JOIN pgmemento.table_event_log e ON e.transaction_id = t.txid
  JOIN pgmemento.row_log r ON r.event_id = e.id
    WHERE r.audit_id = $1;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgmemento.get_min_txid_to_audit_id(aid BIGINT) RETURNS BIGINT AS
$$
SELECT min(t.txid)
  FROM pgmemento.transaction_log t
  JOIN pgmemento.table_event_log e ON e.transaction_id = t.txid
  JOIN pgmemento.row_log r ON r.event_id = e.id
    WHERE r.audit_id = $1;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgmemento.get_max_txid_to_audit_id(aid BIGINT) RETURNS BIGINT AS
$$
SELECT max(t.txid)
  FROM pgmemento.transaction_log t
  JOIN pgmemento.table_event_log e ON e.transaction_id = t.txid
  JOIN pgmemento.row_log r ON r.event_id = e.id
    WHERE r.audit_id = $1;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgmemento.get_txid_bounds_to_table(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  OUT txid_min BIGINT,
  OUT txid_max BIGINT
  ) RETURNS RECORD AS
$$
SELECT min(transaction_id) AS txid_min, max(transaction_id) AS txid_max
  FROM pgmemento.table_event_log e 
    WHERE e.table_relid = ($2 || '.' || $1)::regclass::oid;
$$
LANGUAGE sql;


/**********************************************************
* DELETE LOGS
*
* Delete log information of a given transaction, event or
* audited tables / columns
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.delete_txid_log(t_id BIGINT) RETURNS BIGINT AS
$$
DELETE FROM pgmemento.transaction_log
  WHERE txid = $1 RETURNING txid;
$$
LANGUAGE sql;


CREATE OR REPLACE FUNCTION pgmemento.delete_table_event_log(
  tid BIGINT,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF INTEGER AS
$$
DELETE FROM pgmemento.table_event_log e
  USING pgmemento.audit_table_log a
  WHERE e.table_relid = a.relid
    AND e.transaction_id = $1
    AND a.schema_name = $3 AND a.table_name = $2
    AND a.txid_range @> $1::numeric
    RETURNING e.id;
$$
LANGUAGE sql;


CREATE OR REPLACE FUNCTION pgmemento.delete_audit_table_log(
  table_oid INTEGER
  ) RETURNS SETOF OID AS
$$
BEGIN
  -- only allow delete if table has already been dropped
  IF EXISTS (
    SELECT 1 FROM pgmemento.audit_table_log 
      WHERE relid = $1
        AND upper(txid_range) IS NOT NULL
  ) THEN
    -- remove corresponding table events from event log
    DELETE FROM pgmemento.table_event_log 
      WHERE table_relid = $1;

    RETURN QUERY
      DELETE FROM pgmemento.audit_table_log 
        WHERE relid = $1
          AND upper(txid_range) IS NOT NULL
          RETURNING relid;
  ELSE
    RAISE NOTICE 'Either audit table with relid % is not found or the table still exists.', $1; 
  END IF;
END;
$$
LANGUAGE plpgsql;


/***********************************************************
* AUDIT_TABLES VIEW
*
* A view that shows the user at which transaction auditing
* has been started. This will be useful when switching the
* production schema. (more functions to come in the near future)
***********************************************************/
CREATE OR REPLACE VIEW pgmemento.audit_tables AS
  SELECT
    t.schemaname, t.tablename, b.txid_min, b.txid_max, 
    CASE WHEN tg.tgenabled IS NOT NULL AND tg.tgenabled <> 'D' THEN
      TRUE
    ELSE
      FALSE
    END AS tg_is_active
  FROM pg_class c
  JOIN pg_namespace n ON c.relnamespace = n.oid
  JOIN pg_tables t ON c.relname = t.tablename
  JOIN pg_attribute a ON c.oid = a.attrelid
  LEFT JOIN (
    SELECT tgrelid, tgenabled FROM pg_trigger WHERE tgname = 'log_transaction_trigger'::name
  ) AS tg
  ON c.oid = tg.tgrelid
  JOIN LATERAL (
    SELECT * FROM pgmemento.get_txid_bounds_to_table(t.tablename, t.schemaname)
  ) b ON (true)
    WHERE n.nspname = t.schemaname 
      AND t.schemaname != 'pgmemento'
      AND a.attname = 'audit_id'
      ORDER BY schemaname, tablename;


/***********************************************************
* AUDIT_TABLES_DEPENDENCY VIEW
*
* This view is essential for reverting transactions.
* pgMemento can only log one INSERT/UPDATE/DELETE event per
* table per transaction which maps all changed rows to this
* one event even though it belongs to a subsequent one. 
* Therefore, knowledge about table dependencies is required
* to not violate foreign keys.
***********************************************************/
CREATE OR REPLACE VIEW pgmemento.audit_tables_dependency AS
  WITH RECURSIVE table_dependency(parent, child, schemaname, depth) AS (
    SELECT DISTINCT ON (tc.table_name)
      ccu.table_name AS parent,
      tc.table_name AS child,
      tc.table_schema AS schemaname,
      1 AS depth 
    FROM information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu 
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu 
      ON ccu.constraint_name = tc.constraint_name
    JOIN pgmemento.audit_table_log atl 
      ON atl.table_name = tc.table_name  
      WHERE constraint_type = 'FOREIGN KEY' 
        AND tc.table_name <> ccu.table_name
    UNION ALL
      SELECT DISTINCT ON (tc.table_name)
        ccu.table_name AS parent,
        tc.table_name AS child,
        tc.table_schema AS schemaname,
        t.depth + 1 AS depth
      FROM information_schema.table_constraints AS tc 
      JOIN information_schema.key_column_usage AS kcu 
        ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage AS ccu 
        ON ccu.constraint_name = tc.constraint_name
      JOIN table_dependency t 
        ON t.child = ccu.table_name
        WHERE constraint_type = 'FOREIGN KEY' 
          AND t.child <> tc.table_name
  )
  SELECT schemaname, tablename, depth FROM (
    SELECT schemaname, child AS tablename, max(depth) AS depth
      FROM table_dependency
      GROUP BY schemaname, child
    UNION ALL
      SELECT at.schemaname, at.tablename, 0 AS depth 
        FROM pgmemento.audit_tables at
        LEFT JOIN table_dependency d
          ON d.child = at.tablename
          WHERE d.child IS NULL
  ) t
  ORDER BY schemaname, depth, tablename;