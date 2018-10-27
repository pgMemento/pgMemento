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
-- 0.5.3     2018-10-24   audit_table_check function moved here          FKun
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
*   audit_table_check(IN tid INTEGER, IN tab_name TEXT, IN tab_schema TEXT,
*     OUT log_tab_oid OID, OUT log_tab_name TEXT, OUT log_tab_schema TEXT, OUT log_tab_id INTEGER,
*     OUT recent_tab_name TEXT, OUT recent_tab_schema TEXT, OUT recent_tab_id INTEGER) RETURNS RECORD
*   column_array_to_column_list(columns TEXT[]) RETURNS TEXT
*   delete_audit_table_log(table_oid INTEGER) RETURNS SETOF OID
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


/**********************************************************
* AUDIT TABLE CHECK
*
* Helper function to check if requested table has existed
* before tid happened and if the name has named 
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
DECLARE
  log_tab_upper_txid NUMERIC;
BEGIN
  -- try to get OID of table
  BEGIN
    log_tab_oid := ($3 || '.' || $2)::regclass::oid;

    EXCEPTION
      WHEN OTHERS THEN
        -- check if the table exists in audit_table_log
        SELECT
          relid INTO log_tab_oid
        FROM
          pgmemento.audit_table_log
        WHERE
          schema_name = $3
          AND table_name = $2
        LIMIT 1;

      IF log_tab_oid IS NULL THEN
        RAISE NOTICE 'Could not find table ''%'' in log tables.', $2;
        RETURN;
      END IF;
  END;

  -- check if the table has existed before tid happened
  -- save schema and name in case it was renamed
  SELECT
    id,
    schema_name,
    table_name,
    upper(txid_range)
  INTO
    log_tab_id,
    log_tab_schema,
    log_tab_name,
    log_tab_upper_txid 
  FROM
    pgmemento.audit_table_log 
  WHERE
    relid = log_tab_oid
    AND txid_range @> $1::numeric;

  IF NOT FOUND THEN
    RAISE NOTICE 'Table ''%'' does not exist for requested before transaction %.', $2, $1;
    RETURN;
  END IF;

  -- take into account that the table might not exist anymore or it has been renamed
  -- try to find out if there is an active table with the same oid
  IF log_tab_upper_txid IS NOT NULL THEN
    SELECT
      id,
      schema_name,
      table_name
    INTO
      recent_tab_id,
      recent_tab_schema,
      recent_tab_name
    FROM
      pgmemento.audit_table_log 
    WHERE
      relid = log_tab_oid
      AND upper(txid_range) IS NULL
      AND lower(txid_range) IS NOT NULL;
  END IF;

  -- if not, set new_tab_* attributes, as we need them later
  IF recent_tab_id IS NULL THEN
    recent_tab_id := log_tab_id;
    recent_tab_schema := log_tab_schema;
    recent_tab_name := log_tab_name;
  END IF;

  RETURN;
END;
$$
LANGUAGE plpgsql STABLE STRICT;