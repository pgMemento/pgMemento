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
-- This script provides functions to set up pgMemento for a schema in an 
-- PostgreSQL 9.4+ database.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                 | Author
-- 0.2.1     2016-04-05   additional column in audit_tables view        FKun
-- 0.2.0     2016-02-15   get txids done right                          FKun
-- 0.1.0     2015-06-20   initial commit                                FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   delete_table_event_log(t_id BIGINT, t_name TEXT, s_name TEXT DEFAULT 'public'::text) RETURNS SETOF INTEGER
*   delete_txid_log(t_id BIGINT) RETURNS BIGINT
*   get_max_txid_to_audit_id(aid BIGINT) RETURNS BIGINT
*   get_min_txid_to_audit_id(aid BIGINT) RETURNS BIGINT
*   get_txid_bounds_to_table(table_name TEXT, schema_name TEXT DEFAULT 'public', OUT txid_min BIGINT, OUT txid_max BIGINT) RETURNS RECORD
*   get_txids_to_audit_id(aid BIGINT) RETURNS SETOF BIGINT
*
* VIEW:
*   audit_tables
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
  SELECT t.txid FROM pgmemento.transaction_log t
    JOIN pgmemento.table_event_log e ON e.transaction_id = t.txid
    JOIN pgmemento.row_log r ON r.event_id = e.id
      WHERE r.audit_id = aid;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgmemento.get_min_txid_to_audit_id(aid BIGINT) RETURNS BIGINT AS
$$
  SELECT min(t.txid) FROM pgmemento.transaction_log t
    JOIN pgmemento.table_event_log e ON e.transaction_id = t.txid
    JOIN pgmemento.row_log r ON r.event_id = e.id
      WHERE r.audit_id = aid;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgmemento.get_max_txid_to_audit_id(aid BIGINT) RETURNS BIGINT AS
$$
  SELECT max(t.txid) FROM pgmemento.transaction_log t
    JOIN pgmemento.table_event_log e ON e.transaction_id = t.txid
    JOIN pgmemento.row_log r ON r.event_id = e.id
      WHERE r.audit_id = aid;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgmemento.get_txid_bounds_to_table(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  OUT txid_min BIGINT,
  OUT txid_max BIGINT
  ) RETURNS RECORD AS
$$
BEGIN
  EXECUTE format(
    'SELECT pgmemento.get_min_txid_to_audit_id(min(audit_id)) AS tmin, 
            pgmemento.get_max_txid_to_audit_id(max(audit_id)) AS tmax 
     FROM %I.%I', schema_name, table_name) INTO txid_min, txid_max;
  RETURN;
  
  EXCEPTION
    WHEN OTHERS THEN
	  NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* DELETE LOGS
*
* Delete log information of a given transaction  
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.delete_txid_log(t_id BIGINT) RETURNS BIGINT AS
$$
  DELETE FROM pgmemento.transaction_log WHERE txid = t_id RETURNING txid;
$$
LANGUAGE sql;


CREATE OR REPLACE FUNCTION pgmemento.delete_table_event_log(
  t_id BIGINT,
  t_name TEXT,
  s_name TEXT DEFAULT 'public'::text
  ) RETURNS SETOF INTEGER AS
$$
BEGIN
  RETURN QUERY
    DELETE FROM pgmemento.table_event_log 
      WHERE transaction_id = t_id
        AND schema_name = s_name
        AND table_name = t_name RETURNING id;
END;
$$
LANGUAGE plpgsql;


/***********************************************************
* CREATE VIEW
*
* A view that shows the user at which transaction auditing
* has been started. This will be useful when switch the
* production schema. (more functions to come in the near future)
***********************************************************/
CREATE OR REPLACE VIEW pgmemento.audit_tables AS
  SELECT
    t.schemaname, t.tablename, b.txid_min, b.txid_max, 
    CASE WHEN tg.tgenabled IS NOT NULL AND tg.tgenabled <> 'D' THEN TRUE ELSE FALSE END
    AS tg_is_active
  FROM pg_class c
  JOIN pg_namespace n ON c.relnamespace = n.oid
  JOIN pg_tables t ON c.relname = t.tablename
  JOIN pg_attribute a ON c.oid = a.attrelid
  LEFT JOIN pg_trigger tg ON c.oid = tg.tgrelid
    JOIN LATERAL (
      SELECT * FROM pgmemento.get_txid_bounds_to_table(t.tablename, t.schemaname)
    ) b ON (true)
  WHERE n.nspname = t.schemaname 
    AND t.schemaname != 'pgmemento'
    AND a.attname = 'audit_id'
    AND (tg.tgname = 'log_transaction_trigger' OR tg.tgname IS NULL)
    ORDER BY schemaname, tablename;