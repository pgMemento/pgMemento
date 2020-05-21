-- SCHEMA.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script contains the database schema of pgMemento.
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                        | Author
-- 0.7.4     2020-03-23   add audit_id_column to audit_table_log               FKun
-- 0.7.3     2020-03-21   new audit_schema_log table                           FKun
-- 0.7.2     2020-02-29   new column in row_log to also audit new data         FKun
--                        new unique index on event_key and audit_id
-- 0.7.1     2020-02-02   put unique index of table_event_log on event_key     FKun
-- 0.7.0     2020-01-09   remove FK to events and use concatenated metakeys    FKun
--                        store more events with statement_timestamp
-- 0.6.2     2019-02-27   comments for tables and columns                      FKun
-- 0.6.1     2018-07-23   schema part cut from SETUP.sql                       FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* PGMEMENTO SCHEMA
*   Addtional schema that contains the log tables and
*   all functions to enable versioning of the database.
*
* TABLES:
*   audit_column_log
*   audit_table_log
*   audit_schema_log
*   row_log
*   table_event_log
*   transaction_log
*
* INDEXES:
*   column_log_column_idx
*   column_log_range_idx
*   column_log_table_idx
*   row_log_audit_idx
*   row_log_event_idx
*   row_log_new_data_idx
*   row_log_old_data_idx
*   table_event_log_event_idx
*   table_event_log_fk_idx
*   table_log_idx
*   table_log_range_idx
*   transaction_log_session_idx
*   transaction_log_txid_idx
*
* SEQUENCES:
*   audit_id_seq
*   schema_log_id_seq
*   table_log_id_seq
*
***********************************************************/
DROP SCHEMA IF EXISTS pgmemento CASCADE;
CREATE SCHEMA pgmemento;

-- transaction metadata is logged into the transaction_log table
DROP TABLE IF EXISTS pgmemento.transaction_log CASCADE;
CREATE TABLE pgmemento.transaction_log
(
  id SERIAL,
  txid BIGINT NOT NULL,
  txid_time TIMESTAMP WITH TIME ZONE NOT NULL,
  process_id INTEGER,
  user_name TEXT,
  client_name TEXT,
  client_port INTEGER,
  application_name TEXT,
  session_info JSONB
);

ALTER TABLE pgmemento.transaction_log
  ADD CONSTRAINT transaction_log_pk PRIMARY KEY (id);

COMMENT ON TABLE pgmemento.transaction_log IS 'Stores metadata about each transaction';
COMMENT ON COLUMN pgmemento.transaction_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.transaction_log.txid IS 'The internal transaction ID by PostgreSQL (can cycle)';
COMMENT ON COLUMN pgmemento.transaction_log.txid_time IS 'Stores the result of transaction_timestamp() function';
COMMENT ON COLUMN pgmemento.transaction_log.process_id IS 'Stores the result of pg_backend_pid() function';
COMMENT ON COLUMN pgmemento.transaction_log.user_name IS 'Stores the result of session_user function';
COMMENT ON COLUMN pgmemento.transaction_log.client_name IS 'Stores the result of inet_client_addr() function';
COMMENT ON COLUMN pgmemento.transaction_log.client_port IS 'Stores the result of inet_client_port() function';
COMMENT ON COLUMN pgmemento.transaction_log.application_name IS 'Stores the output of current_setting(''application_name'')';
COMMENT ON COLUMN pgmemento.transaction_log.session_info IS 'Stores any infos a client/user defines beforehand with set_config';

-- event on tables are logged into the table_event_log table
DROP TABLE IF EXISTS pgmemento.table_event_log CASCADE;
CREATE TABLE pgmemento.table_event_log
(
  id SERIAL,
  transaction_id INTEGER NOT NULL,
  stmt_time TIMESTAMP WITH TIME ZONE NOT NULL,
  op_id SMALLINT NOT NULL,
  table_operation TEXT,
  table_name TEXT NOT NULL,
  schema_name TEXT NOT NULL,
  event_key TEXT NOT NULL
);

ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_pk PRIMARY KEY (id);

COMMENT ON TABLE pgmemento.table_event_log IS 'Stores metadata about different kind of events happening during one transaction against one table';
COMMENT ON COLUMN pgmemento.table_event_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.table_event_log.transaction_id IS 'Foreign Key to transaction_log table';
COMMENT ON COLUMN pgmemento.table_event_log.stmt_time IS 'Stores the result of statement_timestamp() function';
COMMENT ON COLUMN pgmemento.table_event_log.op_id IS 'ID of event type';
COMMENT ON COLUMN pgmemento.table_event_log.table_operation IS 'Text for of event type';
COMMENT ON COLUMN pgmemento.table_event_log.table_name IS 'Name of table that fired the trigger';
COMMENT ON COLUMN pgmemento.table_event_log.schema_name IS 'Schema of firing table';
COMMENT ON COLUMN pgmemento.table_event_log.event_key IS 'Concatenated information of most columns';

-- all row changes are logged into the row_log table
DROP TABLE IF EXISTS pgmemento.row_log CASCADE;
CREATE TABLE pgmemento.row_log
(
  id BIGSERIAL,
  audit_id BIGINT NOT NULL,
  event_key TEXT NOT NULL,
  old_data JSONB,
  new_data JSONB
);

ALTER TABLE pgmemento.row_log
  ADD CONSTRAINT row_log_pk PRIMARY KEY (id);

COMMENT ON TABLE pgmemento.row_log IS 'Stores the historic data a.k.a the audit trail';
COMMENT ON COLUMN pgmemento.row_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.row_log.audit_id IS ' The implicit link to a table''s row';
COMMENT ON COLUMN pgmemento.row_log.event_key IS 'Concatenated information of table event';
COMMENT ON COLUMN pgmemento.row_log.old_data IS 'The old values of changed columns in a JSONB object';
COMMENT ON COLUMN pgmemento.row_log.new_data IS 'The new values of changed columns in a JSONB object';

-- if and how pgMemento is running, is logged in the audit_schema_log
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

-- liftime of audited tables is logged in the audit_table_log table
CREATE TABLE pgmemento.audit_table_log (
  id SERIAL,
  log_id INTEGER NOT NULL,
  relid OID,
  table_name TEXT NOT NULL,
  schema_name TEXT NOT NULL,
  audit_id_column TEXT NOT NULL,
  log_old_data BOOLEAN NOT NULL,
  log_new_data BOOLEAN NOT NULL,
  txid_range numrange
);

ALTER TABLE pgmemento.audit_table_log
  ADD CONSTRAINT audit_table_log_pk PRIMARY KEY (id);

COMMENT ON TABLE pgmemento.audit_table_log IS 'Stores information about audited tables, which is important when restoring a whole schema or database';
COMMENT ON COLUMN pgmemento.audit_table_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.audit_table_log.log_id IS 'ID to trace a changing table';
COMMENT ON COLUMN pgmemento.audit_table_log.relid IS '[DEPRECATED] The table''s OID to trace a table when changed';
COMMENT ON COLUMN pgmemento.audit_table_log.table_name IS 'The name of the table';
COMMENT ON COLUMN pgmemento.audit_table_log.schema_name IS 'The schema the table belongs to';
COMMENT ON COLUMN pgmemento.audit_table_log.audit_id_column IS 'The name for the audit_id column added to the audited table';
COMMENT ON COLUMN pgmemento.audit_table_log.log_old_data IS 'Flag that shows if old values are logged for audited table';
COMMENT ON COLUMN pgmemento.audit_table_log.log_new_data IS 'Flag that shows if new values are logged for audited table';
COMMENT ON COLUMN pgmemento.audit_table_log.txid_range IS 'Stores the transaction IDs when the table has been created and dropped';

-- lifetime of columns of audited tables is logged in the audit_column_log table
CREATE TABLE pgmemento.audit_column_log (
  id SERIAL,
  audit_table_id INTEGER NOT NULL,
  column_name TEXT NOT NULL,
  ordinal_position INTEGER,
  data_type TEXT,
  column_default TEXT,
  not_null BOOLEAN,
  txid_range numrange
);

ALTER TABLE pgmemento.audit_column_log
  ADD CONSTRAINT audit_column_log_pk PRIMARY KEY (id);

COMMENT ON TABLE pgmemento.audit_column_log IS 'Stores information about audited columns, which is important when restoring previous versions of tuples and tables';
COMMENT ON COLUMN pgmemento.audit_column_log.id IS 'The Primary Key';
COMMENT ON COLUMN pgmemento.audit_column_log.audit_table_id IS 'Foreign Key to pgmemento.audit_table_log';
COMMENT ON COLUMN pgmemento.audit_column_log.column_name IS 'The name of the column';
COMMENT ON COLUMN pgmemento.audit_column_log.ordinal_position IS 'The ordinal position within the table';
COMMENT ON COLUMN pgmemento.audit_column_log.data_type IS 'The column''s data type (incl typemods)';
COMMENT ON COLUMN pgmemento.audit_column_log.column_default IS 'The column''s default expression';
COMMENT ON COLUMN pgmemento.audit_column_log.not_null IS 'A flag to tell, if the column is a NOT NULL column or not';
COMMENT ON COLUMN pgmemento.audit_column_log.txid_range IS 'Stores the transaction IDs when the column has been created and dropped';

-- create foreign key constraints
ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_txid_fk
    FOREIGN KEY (transaction_id)
    REFERENCES pgmemento.transaction_log (id)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;

ALTER TABLE pgmemento.audit_column_log
  ADD CONSTRAINT audit_column_log_fk
    FOREIGN KEY (audit_table_id)
    REFERENCES pgmemento.audit_table_log (id)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;

-- create indexes on all columns that are queried later
DROP INDEX IF EXISTS transaction_log_unique_idx;
DROP INDEX IF EXISTS transaction_log_session_idx;
DROP INDEX IF EXISTS table_event_log_fk_idx;
DROP INDEX IF EXISTS table_event_log_event_idx;
DROP INDEX IF EXISTS row_log_audit_idx;
DROP INDEX IF EXISTS row_log_event_audit_idx;
DROP INDEX IF EXISTS row_log_old_data_idx;
DROP INDEX IF EXISTS row_log_new_data_idx;
DROP INDEX IF EXISTS table_log_idx;
DROP INDEX IF EXISTS table_log_range_idx;
DROP INDEX IF EXISTS column_log_table_idx;
DROP INDEX IF EXISTS column_log_column_idx;
DROP INDEX IF EXISTS column_log_range_idx;

CREATE UNIQUE INDEX transaction_log_unique_idx ON pgmemento.transaction_log USING BTREE (txid_time, txid);
CREATE INDEX transaction_log_session_idx ON pgmemento.transaction_log USING GIN (session_info);
CREATE INDEX table_event_log_fk_idx ON pgmemento.table_event_log USING BTREE (transaction_id);
CREATE UNIQUE INDEX table_event_log_event_idx ON pgmemento.table_event_log USING BTREE (event_key);
CREATE INDEX row_log_audit_idx ON pgmemento.row_log USING BTREE (audit_id);
CREATE UNIQUE INDEX row_log_event_audit_idx ON pgmemento.row_log USING BTREE (event_key, audit_id);
CREATE INDEX row_log_old_data_idx ON pgmemento.row_log USING GIN (old_data);
CREATE INDEX row_log_new_data_idx ON pgmemento.row_log USING GIN (new_data);
CREATE INDEX table_log_idx ON pgmemento.audit_table_log USING BTREE (log_id);
CREATE INDEX table_log_name_idx ON pgmemento.audit_table_log USING BTREE (table_name, schema_name);
CREATE INDEX table_log_range_idx ON pgmemento.audit_table_log USING GIST (txid_range);
CREATE INDEX column_log_table_idx ON pgmemento.audit_column_log USING BTREE (audit_table_id);
CREATE INDEX column_log_column_idx ON pgmemento.audit_column_log USING BTREE (column_name);
CREATE INDEX column_log_range_idx ON pgmemento.audit_column_log USING GIST (txid_range);


/***********************************************************
CREATE SEQUENCE

***********************************************************/
DROP SEQUENCE IF EXISTS pgmemento.audit_id_seq;
CREATE SEQUENCE pgmemento.audit_id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH 1
  CACHE 1
  NO CYCLE
  OWNED BY NONE;

DROP SEQUENCE IF EXISTS pgmemento.schema_log_id_seq;
CREATE SEQUENCE pgmemento.schema_log_id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH 1
  CACHE 1
  NO CYCLE
  OWNED BY NONE;

DROP SEQUENCE IF EXISTS pgmemento.table_log_id_seq;
CREATE SEQUENCE pgmemento.table_log_id_seq
  INCREMENT BY 1
  MINVALUE 0
  MAXVALUE 2147483647
  START WITH 1
  CACHE 1
  NO CYCLE
  OWNED BY NONE;
