-- LOG_UTIL.sql
--
-- Author:      Felix Kunde <fkunde@virtualcitysystems.de>
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
-- 0.3.0     2015-06-20   initial commit                                FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   get_transaction_id(aid BIGINT) RETURNS BIGINT
*   get_transaction_id_bounds(table_name TEXT, schema_name TEXT DEFAULT 'public'::text, OUT txid_min BIGINT, OUT txid_max BIGINT) RETURNS RECORD
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
CREATE OR REPLACE FUNCTION pgmemento.get_transaction_id(aid BIGINT) RETURNS BIGINT AS
$$
  SELECT t.txid FROM pgmemento.transaction_log t
             JOIN pgmemento.table_event_log e ON e.transaction_id = t.txid
             JOIN pgmemento.row_log r ON r.event_id = e.id
             WHERE r.id = aid;
$$
LANGUAGE sql;


CREATE OR REPLACE FUNCTION pgmemento.get_transaction_id_bounds(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'::text,
  OUT txid_min BIGINT,
  OUT txid_max BIGINT
  ) RETURNS RECORD AS
$$
BEGIN
  EXECUTE format('SELECT pgmemento.get_transaction_id(min(audit_id)) AS tmin, pgmemento.get_transaction_id(max(audit_id)) AS tmax FROM %I.%I', schema_name, table_name) INTO txid_min, txid_max;
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


/***********************************************************
CREATE VIEW

***********************************************************/
CREATE OR REPLACE VIEW pgmemento.audit_tables AS
  SELECT
    t.schemaname, t.tablename, b.txid_min, b.txid_max 
  FROM pg_class c, pg_namespace n, pg_tables t, pg_attribute a
    JOIN LATERAL (
      SELECT * FROM pgmemento.get_transaction_id_bounds(t.tablename, t.schemaname)
    ) b ON (true)
  WHERE c.relname = t.tablename
    AND n.oid = c.relnamespace 
    AND n.nspname = t.schemaname 
    AND a.attrelid = c.oid
    AND a.attname = 'audit_id' 
    AND t.schemaname != 'pgmemento'
    ORDER BY schemaname, tablename;