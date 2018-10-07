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
-- 0.5.2     2018-10-07   new function log_column_state                  FKun
-- 0.5.1     2018-09-24   new function column_array_to_column_list       FKun
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
*   column_array_to_column_list(columns TEXT[]) RETURNS TEXT
*   delete_audit_table_log(table_oid INTEGER) RETURNS SETOF VOID
*   delete_key(aid BIGINT, key_name TEXT) RETURNS SETOF BIGINT
*   delete_table_event_log(tid INTEGER, table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF INTEGER
*   delete_txid_log(tid INTEGER) RETURNS INTEGER
*   get_max_txid_to_audit_id(aid BIGINT) RETURNS INTEGER
*   get_min_txid_to_audit_id(aid BIGINT) RETURNS INTEGER
*   get_txids_to_audit_id(aid BIGINT) RETURNS SETOF INTEGER
*   log_column_state(e_id INTEGER, columns TEXT[], table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF VOID
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
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF INTEGER AS
$$
DELETE FROM
  pgmemento.table_event_log
WHERE
  transaction_id = $1
  AND table_relid = ($3 || '.' || $2)::regclass::oid
RETURNING
  id;
$$
LANGUAGE sql STRICT;


CREATE OR REPLACE FUNCTION pgmemento.delete_key(
  aid BIGINT,
  key_name TEXT
  ) RETURNS SETOF BIGINT AS
$$
UPDATE
  pgmemento.row_log
SET
  changes = changes - $2
WHERE
  audit_id = $1
RETURNING
  id;
$$
LANGUAGE sql STRICT;


CREATE OR REPLACE FUNCTION pgmemento.delete_audit_table_log(
  table_oid INTEGER
  ) RETURNS SETOF VOID AS
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


CREATE OR REPLACE FUNCTION pgmemento.column_array_to_column_list(columns TEXT[]) RETURNS TEXT AS
$$
SELECT
  array_to_string(array_agg(format('%L, %I', k, v)), ', ')
FROM
  unnest($1) k,
  unnest($1) v
WHERE
  k = v;
$$
LANGUAGE sql STRICT;


CREATE OR REPLACE FUNCTION pgmemento.log_column_state(
  e_id INTEGER,
  columns TEXT[],
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format(
    'INSERT INTO pgmemento.row_log(event_id, audit_id, changes)
       SELECT $1, t.audit_id, jsonb_build_object('||pgmemento.column_array_to_column_list($2)||') AS content FROM %I.%I t',
    $4, $3) USING $1;
END;
$$
LANGUAGE plpgsql STRICT;