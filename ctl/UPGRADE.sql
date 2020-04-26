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
-- 0.2.5     2020-04-25   copy log tables instead of updating them         FKun
-- 0.2.4     2020-04-20   update op_id of DROP AUDIT_ID events             FKun
-- 0.2.3     2020-03-30   create and fill new audit_schema_log table       FKun
-- 0.2.2     2020-02-29   reflect new schema of row_log table              FKun
-- 0.2.1     2020-02-01   reflect more changes in schema                   FKun
-- 0.2.0     2019-06-09   change to upgrade from v0.6.1 to v0.7            FKun
-- 0.1.1     2018-11-01   reflect range bounds change in audit tables      FKun
-- 0.1.0     2018-07-23   initial commit                                   FKun
--

\echo
\echo 'TRANSACTION_LOG: Create new unique index'
-- reverse unqiue index which makes other time index obsolete
CREATE UNIQUE INDEX IF NOT EXISTS transaction_log_unique_idx_2 ON pgmemento.transaction_log USING BTREE (stmt_date, txid);
DROP INDEX IF EXISTS transaction_log_unique_idx;
ALTER INDEX IF EXISTS transaction_log_unique_idx_2 RENAME TO transaction_log_unique_idx;
DROP INDEX IF EXISTS transaction_log_date_idx;

-- get current sequence values for copying
SELECT nextval('pgmemento.table_event_log_id_seq') AS curr_seq_event_log \gset
SELECT nextval('pgmemento.row_log_id_seq') AS curr_seq_row_log \gset

\echo
\echo 'TABLE_EVENT_LOG: Create copy with new columns'
CREATE TABLE IF NOT EXISTS pgmemento.table_event_log_2 AS
  SELECT
    e.id,
    e.transaction_id,
    t.stmt_date AS stmt_time,
    CASE WHEN e.table_operation = 'DROP AUDIT_ID' THEN 81::smallint ELSE e.op_id END AS op_id,
    table_operation,
    atl.table_name,
    atl.schema_name,
    concat_ws(';', extract(epoch from t.stmt_date), extract(epoch from t.stmt_date), t.txid, e.op_id, atl.table_name, atl.schema_name) AS event_key
  FROM
    pgmemento.table_event_log e
  JOIN
    pgmemento.transaction_log t
    ON t.id = e.transaction_id
  JOIN pgmemento.audit_table_log atl
    ON atl.relid = e.table_relid
   AND (atl.txid_range @> e.transaction_id::numeric
    OR (lower(atl.txid_range) = e.transaction_id::numeric AND NOT e.op_id = 12))
  WHERE e.id < :curr_seq_event_log;

-- set constraints
ALTER TABLE pgmemento.table_event_log_2
  ADD CONSTRAINT table_event_log_2_pk PRIMARY KEY (id),
  ADD CONSTRAINT table_event_log_txid_2_fk FOREIGN KEY (transaction_id)
    REFERENCES pgmemento.transaction_log (id) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE,
  ALTER COLUMN transaction_id SET NOT NULL,
  ALTER COLUMN stmt_time SET NOT NULL,
  ALTER COLUMN op_id SET NOT NULL,
  ALTER COLUMN table_operation SET NOT NULL,
  ALTER COLUMN table_name SET NOT NULL,
  ALTER COLUMN schema_name SET NOT NULL,
  ALTER COLUMN event_key SET NOT NULL;

-- created indexes
CREATE INDEX IF NOT EXISTS table_event_log_fk_idx ON pgmemento.table_event_log_2 USING BTREE (transaction_id);
CREATE UNIQUE INDEX IF NOT EXISTS table_event_log_event_idx ON pgmemento.table_event_log_2 USING BTREE (event_key);

-- update table statistics
VACUUM ANALYZE pgmemento.table_event_log_2;

\echo
\echo 'ROW_LOG: Create copy with new columns'
CREATE TABLE IF NOT EXISTS pgmemento.row_log_2 AS
  SELECT
    r.id,
    r.audit_id,
    e.event_key,
    r.changes AS old_data,
    NULL::jsonb AS new_data
  FROM
    pgmemento.row_log r
  JOIN
    pgmemento.table_event_log_2 e
    ON e.id = r.event_id
  WHERE
    r.id < :curr_seq_row_log;

-- set constraints
ALTER TABLE pgmemento.row_log_2
  ADD CONSTRAINT row_log_2_pk PRIMARY KEY (id),
  ALTER COLUMN audit_id SET NOT NULL,
  ALTER COLUMN event_key SET NOT NULL;

-- create index
CREATE INDEX IF NOT EXISTS row_log_2_audit_idx ON pgmemento.row_log_2 USING BTREE (audit_id);
CREATE UNIQUE INDEX IF NOT EXISTS row_log_event_audit_idx ON pgmemento.row_log_2 USING BTREE (event_key, audit_id);
CREATE INDEX IF NOT EXISTS row_log_old_data_idx ON pgmemento.row_log_2 USING GIN (old_data);
CREATE INDEX IF NOT EXISTS row_log_new_data_idx ON pgmemento.row_log_2 USING GIN (new_data);

-- update table statistics
VACUUM ANALYZE pgmemento.row_log;

\echo
\echo 'AUDIT_TABLE_LOG: Adding new columns'
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
  ADD COLUMN log_id INTEGER,
  ADD COLUMN audit_id_column TEXT,
  ADD COLUMN log_old_data BOOLEAN,
  ADD COLUMN log_new_data BOOLEAN;

COMMENT ON COLUMN pgmemento.audit_table_log.log_id IS 'ID to trace a changing table';
COMMENT ON COLUMN pgmemento.audit_table_log.relid IS '[DEPRECATED] The table''s OID to trace a table when changed';
COMMENT ON COLUMN pgmemento.audit_table_log.audit_id_column IS 'The name for the audit_id column added to the audited table';
COMMENT ON COLUMN pgmemento.audit_table_log.log_old_data IS 'Flag that shows if old values are logged for audited table';
COMMENT ON COLUMN pgmemento.audit_table_log.log_new_data IS 'Flag that shows if new values are logged for audited table';

-- generate log_ids
UPDATE pgmemento.audit_table_log atl
   SET log_id = s.log_id,
       audit_id_column = 'audit_id',
       log_old_data = TRUE,
       log_new_data = FALSE
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
  ALTER COLUMN log_id SET NOT NULL,
  ALTER COLUMN audit_id_column SET NOT NULL,
  ALTER COLUMN log_old_data SET NOT NULL,
  ALTER COLUMN log_new_data SET NOT NULL;

ALTER INDEX IF EXISTS table_log_idx RENAME TO table_log_name_idx;
CREATE INDEX IF NOT EXISTS table_log_idx ON pgmemento.audit_table_log USING BTREE (log_id);

-- update table statistics
VACUUM ANALYZE pgmemento.audit_table_log;

\echo
\echo 'AUDIT_SCHEMA_LOG: New log table'
CREATE TABLE pgmemento.audit_schema_log (
  id SERIAL,
  log_id INTEGER NOT NULL,
  schema_name TEXT NOT NULL,
  default_audit_id_column TEXT NOT NULL,
  default_log_old_data BOOLEAN DEFAULT TRUE,
  default_log_new_data BOOLEAN DEFAULT FALSE,
  trigger_create_table BOOLEAN DEFAULT FALSE,
  txid_range numrange
);

ALTER TABLE pgmemento.audit_schema_log
  ADD CONSTRAINT audit_schema_log_pk PRIMARY KEY (id);

COMMENT ON TABLE pgmemento.audit_schema_log IS 'Stores information about how pgMemento is configured in audited database schema';
COMMENT ON COLUMN pgmemento.audit_schema_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.audit_schema_log.log_id IS 'ID to trace a changing database schema';
COMMENT ON COLUMN pgmemento.audit_schema_log.schema_name IS 'The name of the database schema';
COMMENT ON COLUMN pgmemento.audit_schema_log.default_audit_id_column IS 'The default name for the audit_id column added to audited tables';
COMMENT ON COLUMN pgmemento.audit_schema_log.default_log_old_data IS 'Default setting for tables to log old values';
COMMENT ON COLUMN pgmemento.audit_schema_log.default_log_new_data IS 'Default setting for tables to log new values';
COMMENT ON COLUMN pgmemento.audit_schema_log.trigger_create_table IS 'Flag that shows if pgMemento starts auditing for newly created tables';
COMMENT ON COLUMN pgmemento.audit_schema_log.txid_range IS 'Stores the transaction IDs when pgMemento has been activated or stopped in the schema';

DROP SEQUENCE IF EXISTS pgmemento.schema_log_id_seq;
CREATE SEQUENCE pgmemento.schema_log_id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH 1
  CACHE 1
  NO CYCLE
  OWNED BY NONE;

\echo
\echo 'Drop log triggers'
DO
$$
DECLARE
  rec RECORD;
  remove_all_tx_tg BOOLEAN := TRUE; 
BEGIN
  FOR rec IN
    SELECT schemaname, tablename
      FROM pgmemento.audit_tables_copy
     ORDER BY schemaname, tablename
  LOOP
    -- the first iteration will remove all log_transaction_triggers
    IF remove_all_tx_tg THEN
      DROP FUNCTION IF EXISTS pgmemento.log_transaction() CASCADE;
      remove_all_tx_tg := FALSE;
    END IF;

    EXECUTE format('DROP TRIGGER IF EXISTS log_delete_trigger ON %I.%I', rec.schemaname, rec.tablename);
    EXECUTE format('DROP TRIGGER IF EXISTS log_update_trigger ON %I.%I', rec.schemaname, rec.tablename);
    EXECUTE format('DROP TRIGGER IF EXISTS log_insert_trigger ON %I.%I', rec.schemaname, rec.tablename);
    EXECUTE format('DROP TRIGGER IF EXISTS log_truncate_trigger ON %I.%I', rec.schemaname, rec.tablename);
  END LOOP;
END;
$$
LANGUAGE plpgsql;

\echo
\echo 'TABLE_EVENT_LOG & ROW_LOG: Swap rows created after copying'
INSERT INTO pgmemento.table_event_log_2
  SELECT
    e.id,
    e.transaction_id,
    t.stmt_date AS stmt_time,
    CASE WHEN e.table_operation = 'DROP AUDIT_ID' THEN 81::smallint ELSE e.op_id END AS op_id,
    table_operation,
    atl.table_name,
    atl.schema_name,
    concat_ws(';', extract(epoch from t.stmt_date), extract(epoch from t.stmt_date), t.txid, e.op_id, atl.table_name, atl.schema_name)
  FROM
    pgmemento.table_event_log e
  JOIN
    pgmemento.transaction_log t
    ON t.id = e.transaction_id
  JOIN pgmemento.audit_table_log atl
    ON atl.relid = e.table_relid
   AND (atl.txid_range @> e.transaction_id::numeric
    OR lower(atl.txid_range) = e.transaction_id::numeric)
  WHERE
    e.id >= :curr_seq_event_log;

INSERT INTO pgmemento.row_log_2
  SELECT
    r.id,
    r.audit_id,
    e.event_key,
    r.changes AS old_data,
    NULL::jsonb AS new_data
  FROM
    pgmemento.row_log r
  JOIN
    pgmemento.table_event_log_2 e
    ON e.id = r.event_id
  WHERE
    r.id >= :curr_seq_row_log;

\echo
\echo 'TABLE_EVENT_LOG & ROW_LOG: Drop original tables and rename copies'
SELECT nextval('pgmemento.row_log_id_seq') AS curr_seq_row_log \gset

DROP TABLE pgmemento.row_log;
ALTER TABLE pgmemento.row_log_2 RENAME TO row_log;
ALTER TABLE pgmemento.row_log RENAME CONSTRAINT row_log_2_pk TO row_log_pk;
ALTER INDEX IF EXISTS row_log_2_audit_idx RENAME TO row_log_audit_idx;

SELECT nextval('pgmemento.table_event_log_id_seq') AS curr_seq_event_log \gset

DROP TABLE pgmemento.table_event_log;
ALTER TABLE pgmemento.table_event_log_2 RENAME TO table_event_log;
ALTER TABLE pgmemento.table_event_log RENAME CONSTRAINT table_event_log_2_pk TO table_event_log_pk;
ALTER TABLE pgmemento.table_event_log RENAME CONSTRAINT table_event_log_txid_2_fk TO table_event_log_txid_fk;

-- recreate sequences
DROP SEQUENCE IF EXISTS pgmemento.row_log_id_seq;
CREATE SEQUENCE pgmemento.row_log_id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH :curr_seq_row_log
  CACHE 1
  NO CYCLE
  OWNED BY NONE;

ALTER TABLE pgmemento.row_log
  ALTER COLUMN id SET DEFAULT nextval('pgmemento.row_log_id_seq');

DROP SEQUENCE IF EXISTS pgmemento.table_event_log_id_seq;
CREATE SEQUENCE pgmemento.table_event_log_id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH :curr_seq_event_log
  CACHE 1
  NO CYCLE
  OWNED BY NONE;

ALTER TABLE pgmemento.table_event_log
  ALTER COLUMN id SET DEFAULT nextval('pgmemento.table_event_log_id_seq');

-- add comments
COMMENT ON TABLE pgmemento.table_event_log IS 'Stores metadata about different kind of events happening during one transaction against one table';
COMMENT ON COLUMN pgmemento.table_event_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.table_event_log.transaction_id IS 'Foreign Key to transaction_log table';
COMMENT ON COLUMN pgmemento.table_event_log.stmt_time IS 'Stores the result of statement_timestamp() function';
COMMENT ON COLUMN pgmemento.table_event_log.op_id IS 'ID of event type';
COMMENT ON COLUMN pgmemento.table_event_log.table_operation IS 'Text for of event type';
COMMENT ON COLUMN pgmemento.table_event_log.table_name IS 'Name of table that fired the trigger';
COMMENT ON COLUMN pgmemento.table_event_log.schema_name IS 'Schema of firing table';
COMMENT ON COLUMN pgmemento.table_event_log.event_key IS 'Concatenated information of most columns';

COMMENT ON TABLE pgmemento.row_log IS 'Stores the historic data a.k.a the audit trail';
COMMENT ON COLUMN pgmemento.row_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.row_log.audit_id IS ' The implicit link to a table''s row';
COMMENT ON COLUMN pgmemento.row_log.event_key IS 'Concatenated information of table event';
COMMENT ON COLUMN pgmemento.row_log.old_data IS 'The old values of changed columns in a JSONB object';
COMMENT ON COLUMN pgmemento.row_log.new_data IS 'The new values of changed columns in a JSONB object';

\echo
\echo 'TRANSACTION_LOG: Rename stmt_date column'
ALTER TABLE pgmemento.transaction_log
  RENAME stmt_date TO txid_time;

COMMENT ON COLUMN pgmemento.transaction_log.txid_time IS 'Stores the result of transaction_timestamp() function';
