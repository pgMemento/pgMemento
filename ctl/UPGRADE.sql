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
-- 0.2.2     2020-02-29   reflect new schema of row_log table              FKun
-- 0.2.1     2020-02-01   reflect more changes in schema                   FKun
-- 0.2.0     2019-06-09   change to upgrade from v0.6.1 to v0.7            FKun
-- 0.1.1     2018-11-01   reflect range bounds change in audit tables      FKun
-- 0.1.0     2018-07-23   initial commit                                   FKun
--

-- TRANSACTION_LOG
-- rename stmt_date column
ALTER TABLE pgmemento.transaction_log
  RENAME stmt_date TO txid_time;

COMMENT ON COLUMN pgmemento.transaction_log.txid_time IS 'Stores the result of transaction_timestamp() function';

-- reverse unqiue index which makes other time index obsolete
CREATE UNIQUE INDEX IF NOT EXISTS transaction_log_unique_idx2 ON pgmemento.transaction_log USING BTREE (txid_time, txid);
DROP INDEX IF EXISTS transaction_log_unique_idx;
ALTER INDEX IF EXISTS transaction_log_unique_idx2 RENAME TO transaction_log_unique_idx;
DROP INDEX IF EXISTS transaction_log_date_idx;

-- TABLE_EVENT_LOG
-- replace table_relid with columns table_name and schema_name
ALTER TABLE pgmemento.table_event_log
  ALTER COLUMN table_operation TYPE TEXT,
  ADD COLUMN stmt_time TIMESTAMP WITH TIME ZONE,
  ADD COLUMN table_name TEXT,
  ADD COLUMN schema_name TEXT,
  ADD COLUMN event_key TEXT;

COMMENT ON COLUMN pgmemento.table_event_log.stmt_time IS 'Stores the result of statement_timestamp() function';
COMMENT ON COLUMN pgmemento.table_event_log.table_name IS 'Name of table that fired the trigger';
COMMENT ON COLUMN pgmemento.table_event_log.schema_name IS 'Schema of firing table';
COMMENT ON COLUMN pgmemento.table_event_log.event_key IS 'Concatenated information of most columns';

-- fill new columns with values
UPDATE pgmemento.table_event_log e
   SET stmt_time = t.txid_time,
       event_key = concat_ws(';', extract(epoch from t.txid_time), extract(epoch from t.txid_time), t.txid, e.op_id)
  FROM pgmemento.transaction_log t
 WHERE e.transaction_id = t.id;

UPDATE pgmemento.table_event_log e
   SET table_name = atl.table_name,
       schema_name = atl.schema_name,
       event_key = concat_ws(';', e.event_key, atl.table_name, atl.schema_name)
  FROM pgmemento.audit_table_log atl
 WHERE atl.relid = e.table_relid
   AND (atl.txid_range @> e.transaction_id::numeric
    OR lower(atl.txid_range) = e.transaction_id::numeric);

-- set columns to NOT NULL and update indexes
ALTER TABLE pgmemento.table_event_log
  DROP COLUMN table_relid,
  ALTER COLUMN stmt_time SET NOT NULL,
  ALTER COLUMN table_name SET NOT NULL,
  ALTER COLUMN schema_name SET NOT NULL,
  ALTER COLUMN event_key SET NOT NULL;

CREATE INDEX IF NOT EXISTS table_event_log_fk_idx ON pgmemento.table_event_log USING BTREE (transaction_id);
CREATE UNIQUE INDEX IF NOT EXISTS table_event_log_event_idx ON pgmemento.table_event_log USING BTREE (event_key);
DROP INDEX IF EXISTS table_event_log_unique_idx;

-- update table statistics
VACUUM ANALYZE pgmemento.table_event_log;

-- ROW_LOG
-- rename changes column to old_data because we can now have a new_data, too
-- remove foreign key but add event_key to link to table_event_log
ALTER TABLE pgmemento.row_log
  RENAME changes TO old_data;

ALTER TABLE pgmemento.row_log
  DROP CONSTRAINT row_log_table_fk,
  ALTER COLUMN event_id DROP NOT NULL,
  ADD COLUMN event_key TEXT,
  ADD COLUMN new_data JSONB;

COMMENT ON COLUMN pgmemento.row_log.event_key IS 'Concatenated information of table event';
COMMENT ON COLUMN pgmemento.row_log.old_data IS 'The old values of changed columns in a JSONB object';
COMMENT ON COLUMN pgmemento.row_log.new_data IS 'The new values of changed columns in a JSONB object';

-- fill new columns with values
UPDATE pgmemento.row_log r
   SET event_key = e.event_key
  FROM pgmemento.table_event_log e
 WHERE r.event_id = e.id;

-- create new index on event_key and remove former foreign key index
CREATE UNIQUE INDEX IF NOT EXISTS row_log_event_audit_idx ON pgmemento.row_log USING BTREE (event_key, audit_id);
DROP INDEX IF EXISTS row_log_event_idx;

ALTER TABLE pgmemento.row_log
  DROP COLUMN event_id,
  ALTER COLUMN event_key SET NOT NULL;

-- rename index on previous changes column and create GIN index on new_data
ALTER INDEX IF EXISTS row_log_changes_idx RENAME TO row_log_old_data_idx;
CREATE INDEX IF NOT EXISTS row_log_new_data_idx ON pgmemento.row_log USING GIN (new_data);

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
CREATE INDEX IF NOT EXISTS table_log_idx ON pgmemento.audit_table_log USING BTREE (log_id);

-- update table statistics
VACUUM ANALYZE pgmemento.audit_table_log;
