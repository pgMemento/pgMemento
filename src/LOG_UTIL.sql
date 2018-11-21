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
-- Version | Date       | Description                                  | Author
-- 0.6.3     2018-11-20   new helper function to revert updates with     FKun
--                        composite data types
-- 0.6.2     2018-11-05   delete_table_event_log now takes OID           FKun
-- 0.6.1     2018-11-02   new functions to get historic table layouts    FKun
-- 0.6.0     2018-10-28   new function to update a key in logs           FKun
--                        new value filter in delete_key function
-- 0.5.1     2018-10-24   audit_table_check function moved here          FKun
-- 0.5.0     2018-07-16   reflect changes in transaction_id handling     FKun
-- 0.4.2     2017-07-26   new function to remove a key from all logs     FKun
-- 0.4.1     2017-04-11   moved VIEWs to SETUP.sql & added jsonb_merge   FKun
-- 0.4.0     2017-03-06   new view for table dependencies                FKun
-- 0.3.0     2016-04-14   reflected changes in log tables                FKun
-- 0.2.1     2016-04-05   additional column in audit_tables view         FKun
-- 0.2.0     2016-02-15   get txids done right                           FKun
-- 0.1.0     2015-06-20   initial commit                                 FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* AGGREGATE:
*   jsonb_merge(jsonb)
*
* FUNCTIONS:
*   audit_table_check(IN tid INTEGER, IN tab_name TEXT, IN tab_schema TEXT,
*     OUT log_tab_oid OID, OUT log_tab_name TEXT, OUT log_tab_schema TEXT, OUT log_tab_id INTEGER,
*     OUT recent_tab_name TEXT, OUT recent_tab_schema TEXT, OUT recent_tab_id INTEGER) RETURNS RECORD
*   delete_audit_table_log(table_oid OID) RETURNS SETOF OID
*   delete_key(aid BIGINT, key_name TEXT, old_value anyelement) RETURNS SETOF BIGINT
*   delete_table_event_log(tid INTEGER, table_oid OID) RETURNS SETOF INTEGER
*   delete_txid_log(tid INTEGER) RETURNS INTEGER
*   get_column_list_by_txid(tid INTEGER, table_name TEXT, schema_name TEXT DEFAULT 'public'::text,
*     OUT column_name TEXT, OUT data_type TEXT, OUT ordinal_position INTEGER) RETURNS SETOF RECORD
*   get_column_list_by_txid_range(start_from_tid INTEGER, end_at_tid INTEGER, table_oid OID,
*     OUT column_name TEXT, OUT column_count INTEGER, OUT data_type TEXT, OUT ordinal_position INTEGER,
*     OUT txid_range numrange) RETURNS SETOF RECORD
*   get_max_txid_to_audit_id(aid BIGINT) RETURNS INTEGER
*   get_min_txid_to_audit_id(aid BIGINT) RETURNS INTEGER
*   get_txids_to_audit_id(aid BIGINT) RETURNS SETOF INTEGER
*   jsonb_unroll_for_update(path TEXT, nested_value JSONB, complex_typname TEXT) RETURNS TEXT
*   update_key(aid BIGINT, path_to_key_name TEXT[], old_value anyelement, new_value anyelement) RETURNS SETOF BIGINT
*
***********************************************************/

/**********************************************************
* JSONB MERGE
*
* Custom aggregate function to merge several JSONB logs
* into one JSONB element eliminating redundant keys
***********************************************************/
CREATE AGGREGATE pgmemento.jsonb_merge(jsonb)
(
    sfunc = jsonb_concat(jsonb, jsonb),
    stype = jsonb,
    initcond = '{}'
);


/**********************************************************
* JSONB UNROLL
*
* Helper function to revert updates with composite datatypes
***********************************************************/
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
      pgmemento.jsonb_unroll_for_update($1 || '.' || j.key, j.value, p.typname)
    ELSE
      $1 || '.' || j.key || '=' || quote_nullable(j.value->>0)
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


/**********************************************************
* GET TRANSACTION ID
*
* Simple functions to return the transaction_id related to
* certain database entities 
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_txids_to_audit_id(aid BIGINT) RETURNS SETOF INTEGER AS
$$
SELECT
  t.id
FROM
  pgmemento.transaction_log t
JOIN
  pgmemento.table_event_log e
  ON e.transaction_id = t.id
JOIN
  pgmemento.row_log r
  ON r.event_id = e.id
WHERE
  r.audit_id = $1;
$$
LANGUAGE sql STABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.get_min_txid_to_audit_id(aid BIGINT) RETURNS INTEGER AS
$$
SELECT
  min(t.id)
FROM
  pgmemento.transaction_log t
JOIN
  pgmemento.table_event_log e
  ON e.transaction_id = t.id
JOIN
  pgmemento.row_log r
  ON r.event_id = e.id
WHERE
  r.audit_id = $1;
$$
LANGUAGE sql STABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.get_max_txid_to_audit_id(aid BIGINT) RETURNS INTEGER AS
$$
SELECT
  max(t.id)
FROM
  pgmemento.transaction_log t
JOIN
  pgmemento.table_event_log e
  ON e.transaction_id = t.id
JOIN
  pgmemento.row_log r
  ON r.event_id = e.id
WHERE
  r.audit_id = $1;
$$
LANGUAGE sql STABLE STRICT;


/**********************************************************
* DELETE LOGS
*
* Delete log information of a given transaction, event or
* audited tables / columns
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.delete_txid_log(tid INTEGER) RETURNS INTEGER AS
$$
DELETE FROM
  pgmemento.transaction_log
WHERE
  id = $1
RETURNING
  id;
$$
LANGUAGE sql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.delete_table_event_log(
  tid INTEGER,
  table_oid OID
  ) RETURNS SETOF INTEGER AS
$$
DELETE FROM
  pgmemento.table_event_log
WHERE
  transaction_id = $1
  AND table_relid = $2
RETURNING
  id;
$$
LANGUAGE sql STRICT;

CREATE OR REPLACE FUNCTION pgmemento.delete_audit_table_log(
  table_oid OID
  ) RETURNS SETOF OID AS
$$
BEGIN
  -- only allow delete if table has already been dropped
  IF EXISTS (
    SELECT
      1
    FROM
      pgmemento.audit_table_log 
    WHERE
      relid = $1
      AND upper(txid_range) IS NOT NULL
  ) THEN
    -- remove corresponding table events from event log
    DELETE FROM
      pgmemento.table_event_log 
    WHERE
      table_relid = $1;

    RETURN QUERY
      DELETE FROM
        pgmemento.audit_table_log 
      WHERE
        relid = $1
        AND upper(txid_range) IS NOT NULL
      RETURNING
        relid;
  ELSE
    RAISE NOTICE 'Either audit table with relid % is not found or the table still exists.', $1; 
  END IF;
END;
$$
LANGUAGE plpgsql STRICT;


/**********************************************************
* DATA CORRECTION
*
* Functions to delete or update a value for a given key
* inside the audit trail
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.delete_key(
  aid BIGINT,
  key_name TEXT,
  old_value anyelement
  ) RETURNS SETOF BIGINT AS
$$
UPDATE
  pgmemento.row_log
SET
  changes = changes - $2
WHERE
  audit_id = $1
  AND changes @> jsonb_build_object($2, $3)
RETURNING
  id;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgmemento.update_key(
  aid BIGINT,
  path_to_key_name TEXT[],
  old_value anyelement,
  new_value anyelement
  ) RETURNS SETOF BIGINT AS
$$
UPDATE
  pgmemento.row_log
SET
  changes = jsonb_set(changes, $2, to_jsonb($4), FALSE)
WHERE
  audit_id = $1
  AND changes @> jsonb_build_object($2[1], $3)
RETURNING
  id;
$$
LANGUAGE sql;


/**********************************************************
* AUDIT TABLE CHECK
*
* Helper function to check if requested table has existed
* before tid happened and if the name has been renamed 
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.audit_table_check(
  IN tid INTEGER,
  IN tab_name TEXT,
  IN tab_schema TEXT,
  OUT log_tab_oid OID,
  OUT log_tab_name TEXT,
  OUT log_tab_schema TEXT,
  OUT log_tab_id INTEGER,
  OUT recent_tab_name TEXT,
  OUT recent_tab_schema TEXT,
  OUT recent_tab_id INTEGER
  ) RETURNS RECORD AS
$$
BEGIN
  -- try to get OID of table
  log_tab_oid := ($3 || '.' || $2)::regclass::oid;

  -- check if the table has existed before tid happened
  -- save schema and name in case it was renamed
  SELECT
    t_old.id,
    t_old.schema_name,
    t_old.table_name,
    t_new.id,
    t_new.schema_name,
    t_new.table_name
  INTO
    log_tab_id,
    log_tab_schema,
    log_tab_name,
    recent_tab_id,
    recent_tab_schema,
    recent_tab_name    
  FROM
    pgmemento.audit_table_log t_new
  LEFT JOIN
    pgmemento.audit_table_log t_old
    ON t_old.relid = t_new.relid
   AND t_old.txid_range @> $1::numeric
  WHERE
    t_new.relid = log_tab_oid
    AND upper(t_new.txid_range) IS NULL
    AND lower(t_new.txid_range) IS NOT NULL;

  EXCEPTION
    WHEN OTHERS THEN
      -- check if the table exists in audit_table_log
      SELECT
        id,
        relid,
        schema_name,
        table_name
      INTO
        log_tab_id,
        log_tab_oid,
        log_tab_schema,
        log_tab_name
      FROM
        pgmemento.audit_table_log 
      WHERE
        table_name = $2
        AND schema_name = $3
        AND txid_range @> $1::numeric;
END;
$$
LANGUAGE plpgsql STABLE STRICT;


/**********************************************************
* GET COLUMN LIST BY TXID (RANGE)
*
* Returns column details of an audited table that have
* existed either before a given transaction ID or within
* a given ID range. When querying by range all different
* versions of a column appear in the result set. To avoid
* ambiguity a counter is returned as well.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.get_column_list_by_txid(
  tid INTEGER,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  OUT column_name TEXT,
  OUT data_type TEXT,
  OUT ordinal_position INTEGER
  ) RETURNS SETOF RECORD AS
$$
SELECT
  c.column_name,
  c.data_type,
  c.ordinal_position
FROM
  pgmemento.audit_column_log c
JOIN
  pgmemento.audit_table_log t
  ON t.id = c.audit_table_id
WHERE
  t.table_name = $2
  AND t.schema_name = $3
  AND t.txid_range @> $1::numeric
  AND c.txid_range @> $1::numeric;
$$
LANGUAGE sql STABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.get_column_list_by_txid_range(
  start_from_tid INTEGER,
  end_at_tid INTEGER,
  table_oid OID,
  OUT column_name TEXT,
  OUT column_count INTEGER,
  OUT data_type TEXT,
  OUT ordinal_position INTEGER,
  OUT txid_range numrange
  ) RETURNS SETOF RECORD AS
$$
SELECT
  column_name,
  (row_number() OVER (PARTITION BY column_name))::int AS column_count,
  data_type,
  ordinal_position,
  txid_range
FROM (
  SELECT
    c.column_name,
    c.data_type,
    c.ordinal_position,
    numrange(min(lower(c.txid_range)),max(COALESCE(upper(c.txid_range),$2::numeric))) AS txid_range
  FROM
    pgmemento.audit_column_log c
  JOIN
    pgmemento.audit_table_log t
    ON t.id = c.audit_table_id
  WHERE
    t.relid = $3
    AND t.txid_range && numrange(1::numeric, $2::numeric)
    AND c.txid_range && numrange(1::numeric, $2::numeric)
  GROUP BY
    c.column_name,
    c.data_type,
    c.ordinal_position
  ORDER BY
    c.ordinal_position
) t;
$$
LANGUAGE sql STABLE STRICT;