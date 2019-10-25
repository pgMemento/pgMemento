-- UPGRADE.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script alters the tables to follow the new logging scheme of v0.7.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.2.0     2019-06-09   change to upgrade from v0.6.1 to v0.7            FKun
-- 0.1.1     2018-11-01   reflect range bounds change in audit tables      FKun
-- 0.1.0     2018-07-23   initial commit                                   FKun
--

-- TRANSACTION_LOG
-- rename stmt_time column

ALTER TABLE pgmemento.transaction_log
  RENAME txid_date TO txid_time;

COMMENT ON COLUMN pgmemento.transaction_log.txid_time IS 'Stores the result of transaction_timestamp() function';

-- TABLE_EVENT_LOG
-- replace table_relid with columns table_name and schema_name
ALTER TABLE pgmemento.table_event_log
  ADD COLUMN stmt_time TIMESTAMP WITH TIME ZONE
  ADD COLUMN table_name TEXT,
  ADD COLUMN schema_name TEXT;

-- fill new columns with values
UPDATE pgmemento.table_event_log e
   SET stmt_time = t.txid_time
  FROM pgmemento.transaction_log t
 WHERE e.transaction_id = t.id;

UPDATE pgmemento.table_event_log e
   SET table_name = atl.table_name,
       schema_name = atl.schema_name
  FROM pgmemento.audit_table_log atl
 WHERE atl.relid = e.table_relid
   AND (atl.txid_range @> e.transaction_id::numeric
    OR lower(atl.txid_range) = e.transaction_id::numeric);

-- update UNIQUE index and set columns to NOT NULL
DROP INDEX IF EXISTS table_event_log_unique_idx;

ALTER TABLE pgmemento.table_event_log
  DROP COLUMN table_relid,
  ALTER COLUMN stmt_time SET NOT NULL,
  ALTER COLUMN table_name SET NOT NULL,
  ALTER COLUMN schema_name SET NOT NULL;

COMMENT ON COLUMN pgmemento.table_event_log.stmt_time IS 'Stores the result of statement_timestamp() function';
COMMENT ON COLUMN pgmemento.table_event_log.table_name IS 'Name of table that fired the trigger';
COMMENT ON COLUMN pgmemento.table_event_log.schema_name IS 'Schema of firing table';

CREATE UNIQUE INDEX table_event_log_unique_idx ON pgmemento.table_event_log USING BTREE (transaction_id, stmt_time, table_name, schema_name, op_id);
CREATE INDEX table_even_log_time_idx ON pgmemento.table_event_log USING BTREE (stmt_time);

-- update table statistics
VACUUM ANALYZE pgmemento.table_event_log;

-- ROW_LOG
-- add two new timestamp columns and remove foreign key
ALTER TABLE pgmemento.row_log
  DROP CONSTRAINT row_log_table_fk,
  ALTER COLUMN event_id DROP NOT NULL,
  ADD COLUMN txid_time TIMESTAMP WITH TIME ZONE,
  ADD COLUMN stmt_time TIMESTAMP WITH TIME ZONE,
  ADD COLUMN op_id SMALLINT,
  ADD COLUMN table_name TEXT,
  ADD COLUMN schema_name TEXT;

-- fill new columns with values
UPDATE pgmemento.row_log r
   SET txid_time = e.stmt_time,
       stmt_time = e.stmt_time,
       op_id = e.op_id
       table_name = e.table_name
       schema_name = e.schema_name
  FROM pgmemento.table_event_log e
 WHERE r.event_id = e.id;

-- remove former foreign key index
DROP INDEX IF EXISTS row_log_event_idx;

ALTER TABLE pgmemento.row_log
  DROP CONSTRAINT row_log_table_fk,
  DROP COLUMN event_id,
  ALTER COLUMN txid_time SET NOT NULL,
  ALTER COLUMN stmt_time SET NOT NULL,
  ALTER COLUMN op_id SET NOT NULL,
  ALTER COLUMN table_name SET NOT NULL,
  ALTER COLUMN schema_name SET NOT NULL;

COMMENT ON COLUMN pgmemento.row_log.txid_time IS 'Stores the timestamp of the current transaction';
COMMENT ON COLUMN pgmemento.row_log.stmt_time IS 'Stores the timestamp of table event';

CREATE INDEX row_log_event_idx ON pgmemento.row_log USING BTREE (stmt_time, op_id, table_name, schema_name);

-- update table statistics
VACUUM ANALYZE pgmemento.row_log;

-- AUDIT_TABLE_LOG
-- introduce new column log_id with sequence
DROP SEQUENCE IF EXISTS pgmemento.table_log_id_seq;
CREATE SEQUENCE pgmemento.table_log_id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH 1
  CACHE 1
  NO CYCLE
  OWNED BY NONE;

ALTER TABLE pgmemento.audit_table_log
  ADD COLUMN log_id INTEGER;

COMMENT ON COLUMN pgmemento.audit_table_log.log_id IS 'ID to trace a changing table';
COMMENT ON COLUMN pgmemento.audit_table_log.relid IS '[DEPRECATED] The table''s OID to trace a table when changed';

-- generate log_ids
UPDATE pgmemento.audit_table_log atl
   SET log_id = s.log_id
  FROM (
       SELECT relid, nextval('pgmemento.table_log_id_seq') AS log_id
         FROM (
              SELECT DISTINCT relid
                FROM pgmemento.audit_table_log
            ORDER BY relid
              ) r
       ) s
 WHERE atl.relid = s.relid;

-- set log_id to NOT NULL and create index
ALTER TABLE pgmemento.audit_table_log
  ALTER COLUMN log_id SET NOT NULL;

ALTER INDEX IF EXISTS table_log_idx RENAME TO table_log_name_idx;
CREATE INDEX table_log_idx ON pgmemento.audit_table_log USING BTREE (log_id);

-- update table statistics
VACUUM ANALYZE pgmemento.audit_table_log;
