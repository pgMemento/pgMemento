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
*   delete_audit_table_log(table_oid INTEGER) RETURNS SETOF OID
*   delete_key(aid BIGINT, key_name TEXT) RETURNS SETOF BIGINT
*   delete_table_event_log(tid BIGINT, table_name TEXT, schema_name TEXT DEFAULT 'public'::text) RETURNS SETOF INTEGER
*   delete_txid_log(t_id BIGINT) RETURNS BIGINT
*   get_max_txid_to_audit_id(aid BIGINT) RETURNS BIGINT
*   get_min_txid_to_audit_id(aid BIGINT) RETURNS BIGINT
*   get_txids_to_audit_id(aid BIGINT) RETURNS SETOF BIGINT
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
CREATE OR REPLACE FUNCTION pgmemento.get_txids_to_audit_id(aid BIGINT) RETURNS SETOF BIGINT AS
$$
SELECT
  t.txid
FROM
  pgmemento.transaction_log t
JOIN
  pgmemento.table_event_log e
  ON e.transaction_id = t.txid
JOIN
  pgmemento.row_log r
  ON r.event_id = e.id
WHERE
  r.audit_id = $1;
$$
LANGUAGE sql STABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.get_min_txid_to_audit_id(aid BIGINT) RETURNS BIGINT AS
$$
SELECT
  min(t.txid)
FROM
  pgmemento.transaction_log t
JOIN
  pgmemento.table_event_log e
  ON e.transaction_id = t.txid
JOIN
  pgmemento.row_log r
  ON r.event_id = e.id
WHERE
  r.audit_id = $1;
$$
LANGUAGE sql STABLE STRICT;

CREATE OR REPLACE FUNCTION pgmemento.get_max_txid_to_audit_id(aid BIGINT) RETURNS BIGINT AS
$$
SELECT
  max(t.txid)
FROM
  pgmemento.transaction_log t
JOIN
  pgmemento.table_event_log e
  ON e.transaction_id = t.txid
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
CREATE OR REPLACE FUNCTION pgmemento.delete_txid_log(t_id BIGINT) RETURNS BIGINT AS
$$
DELETE FROM
  pgmemento.transaction_log
WHERE
  txid = $1
RETURNING
  txid;
$$
LANGUAGE sql STRICT;


CREATE OR REPLACE FUNCTION pgmemento.delete_table_event_log(
  tid BIGINT,
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF INTEGER AS
$$
DELETE FROM
  pgmemento.table_event_log e
USING
  pgmemento.audit_table_log a
WHERE
  e.table_relid = a.relid
  AND e.transaction_id = $1
  AND a.schema_name = $3
  AND a.table_name = $2
  AND a.txid_range @> $1::numeric
RETURNING
  e.id;
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