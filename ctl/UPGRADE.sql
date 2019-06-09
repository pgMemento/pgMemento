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

-- TABLE_EVENT_LOG
-- replace table_relid with columns table_name and schema_name
ALTER TABLE pgmemento.table_event_log
  ADD COLUMN table_name TEXT,
  ADD COLUMN schema_name TEXT;

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
  ALTER COLUMN table_name SET NOT NULL,
  ALTER COLUMN schema_name SET NOT NULL;

COMMENT ON COLUMN pgmemento.table_event_log.table_name IS 'Name of table that fired the trigger';
COMMENT ON COLUMN pgmemento.table_event_log.schema_name IS 'Schema of firing table';

CREATE UNIQUE INDEX table_event_log_unique_idx ON pgmemento.table_event_log USING BTREE (transaction_id, table_name, schema_name, op_id);


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

