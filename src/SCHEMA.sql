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
-- Version | Date       | Description                                       | Author
-- 0.6.1     2018-07-23   schema part cut from SETUP.sql                      FKun
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
*   row_log
*   table_event_log
*   transaction_log
*
* INDEXES:
*   column_log_column_idx
*   column_log_range_idx
*   column_log_table_idx
*   row_log_audit_idx
*   row_log_changes_idx
*   row_log_event_idx
*   table_event_log_unique_idx
*   table_log_idx
*   table_log_range_idx
*   transaction_log_date_idx
*   transaction_log_session_idx
*   transaction_log_txid_idx
*
* SEQUENCES:
*   audit_id_seq
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
  stmt_date TIMESTAMP WITH TIME ZONE NOT NULL,
  process_id INTEGER,
  user_name TEXT,
  client_name TEXT,
  client_port INTEGER,
  application_name TEXT,
  session_info JSONB
);

ALTER TABLE pgmemento.transaction_log
  ADD CONSTRAINT transaction_log_pk PRIMARY KEY (id),
  ADD CONSTRAINT transaction_log_unique_txid UNIQUE (txid, stmt_date);

-- event on tables are logged into the table_event_log table
DROP TABLE IF EXISTS pgmemento.table_event_log CASCADE;
CREATE TABLE pgmemento.table_event_log
(
  id SERIAL,
  transaction_id INTEGER NOT NULL,
  op_id SMALLINT NOT NULL,
  table_operation VARCHAR(18),
  table_relid OID NOT NULL
);

ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_pk PRIMARY KEY (id);

-- all row changes are logged into the row_log table
DROP TABLE IF EXISTS pgmemento.row_log CASCADE;
CREATE TABLE pgmemento.row_log
(
  id BIGSERIAL,
  event_id INTEGER NOT NULL,
  audit_id BIGINT NOT NULL,
  changes JSONB
);

ALTER TABLE pgmemento.row_log
  ADD CONSTRAINT row_log_pk PRIMARY KEY (id);

-- liftime of audited tables is logged in the audit_table_log table
CREATE TABLE pgmemento.audit_table_log (
  id SERIAL,
  relid OID,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  txid_range numrange  
);

ALTER TABLE pgmemento.audit_table_log
  ADD CONSTRAINT audit_table_log_pk PRIMARY KEY (id);

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

-- create foreign key constraints
ALTER TABLE pgmemento.table_event_log
  ADD CONSTRAINT table_event_log_txid_fk
    FOREIGN KEY (transaction_id)
    REFERENCES pgmemento.transaction_log (id)
    MATCH FULL
    ON DELETE CASCADE
    ON UPDATE CASCADE;

ALTER TABLE pgmemento.row_log
  ADD CONSTRAINT row_log_table_fk 
    FOREIGN KEY (event_id)
    REFERENCES pgmemento.table_event_log (id)
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
DROP INDEX IF EXISTS transaction_log_txid_idx;
DROP INDEX IF EXISTS transaction_log_date_idx;
DROP INDEX IF EXISTS transaction_log_session_idx;
DROP INDEX IF EXISTS table_event_log_unique_idx;
DROP INDEX IF EXISTS row_log_event_idx;
DROP INDEX IF EXISTS row_log_audit_idx;
DROP INDEX IF EXISTS row_log_changes_idx;
DROP INDEX IF EXISTS table_log_idx;
DROP INDEX IF EXISTS table_log_range_idx;
DROP INDEX IF EXISTS column_log_table_idx;
DROP INDEX IF EXISTS column_log_column_idx;
DROP INDEX IF EXISTS column_log_range_idx;

CREATE INDEX transaction_log_txid_idx ON pgmemento.transaction_log USING BTREE (txid);
CREATE INDEX transaction_log_date_idx ON pgmemento.transaction_log USING BTREE (stmt_date);
CREATE INDEX transaction_log_session_idx ON pgmemento.transaction_log USING GIN (session_info);
CREATE UNIQUE INDEX table_event_log_unique_idx ON pgmemento.table_event_log USING BTREE (transaction_id, table_relid, op_id);
CREATE INDEX row_log_event_idx ON pgmemento.row_log USING BTREE (event_id);
CREATE INDEX row_log_audit_idx ON pgmemento.row_log USING BTREE (audit_id);
CREATE INDEX row_log_changes_idx ON pgmemento.row_log USING GIN (changes);
CREATE INDEX table_log_idx ON pgmemento.audit_table_log USING BTREE (table_name, schema_name);
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