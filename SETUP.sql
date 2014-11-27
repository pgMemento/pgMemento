-- SETUP.sql
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
-- PostgreSQL 9.3+ database.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                       | Author
-- 0.2.0     2014-05-26   some intermediate version           FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* AUDIT SCHEMA
*   Addtional schema that contains the log tables and
*   all functions to enable versioning of the database.
*
* TABLES:
*   audit_log
*   table_templates
*   transaction_log
*
* INDEXES:
*   transaction_log_internal_idx;
*   transaction_log_op_idx;
*   transaction_log_table_idx;
*   transaction_log_date_idx;
*   audit_log_internal_idx;
*   audit_log_audit_idx;
*   templates_table_idx;
*   templates_date_idx;
*
* FUNCTIONS:
*   create_schema_audit(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_audit_id(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_log_trigger(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   create_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   create_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   create_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_schema_audit(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_audit_id(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_log_trigger(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_table_audit(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_audit_id(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_log_trigger(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*   log_schema_state(schema_name TEXT DEFAULT 'public', except_tables TEXT[] DEFAULT '{}') RETURNS SETOF VOID
*   log_table_state(table_name TEXT, schema_name TEXT DEFAULT 'public') RETURNS SETOF VOID
*
* TRIGGER FUNCTIONS
*   log_change() RETURNS trigger
*
***********************************************************/
DROP SCHEMA IF EXISTS pgmemento CASCADE;
CREATE SCHEMA pgmemento;

/***********************************************************
CREATE TABLES

***********************************************************/
-- transaction metadata is logged into the into the transaction_log table
DROP TABLE IF EXISTS pgmemento.transaction_log CASCADE;
CREATE TABLE pgmemento.transaction_log
(
  id SERIAL,
  internal_transaction_id BIGINT,
  table_operation TEXT,
  schema_name TEXT,
  table_name TEXT,
  table_relid OID,
  stmt_date TIMESTAMP,
  user_name TEXT,
  client_name TEXT,
  application_name TEXT
);

ALTER TABLE pgmemento.transaction_log
ADD CONSTRAINT transaction_log_pk PRIMARY KEY (id);

-- all row changes are logged into the audit_log table
DROP TABLE IF EXISTS pgmemento.audit_log CASCADE;
CREATE TABLE pgmemento.audit_log
(
  id SERIAL,
  internal_transaction_id BIGINT,
  table_relid OID,
  stmt_date TIMESTAMP,
  audit_id INTEGER,
  table_content JSON
);

ALTER TABLE pgmemento.audit_log
ADD CONSTRAINT audit_log_pk PRIMARY KEY (id);

-- need to somehow log the structure of a table
DROP TABLE IF EXISTS pgmemento.table_templates CASCADE;
CREATE TABLE pgmemento.table_templates
(
  id SERIAL,
  name TEXT,
  original_schema TEXT,
  original_table TEXT,
  original_relid OID,
  creation_date TIMESTAMP
);

ALTER TABLE pgmemento.table_templates
ADD CONSTRAINT table_templates_pk PRIMARY KEY (id);

-- create indexes on all columns that are queried later
DROP INDEX IF EXISTS transaction_log_internal_idx;
DROP INDEX IF EXISTS transaction_log_op_idx;
DROP INDEX IF EXISTS transaction_log_table_idx;
DROP INDEX IF EXISTS transaction_log_date_idx;
DROP INDEX IF EXISTS audit_log_internal_idx;
DROP INDEX IF EXISTS audit_log_audit_idx;
DROP INDEX IF EXISTS templates_table_idx;
DROP INDEX IF EXISTS templates_date_idx;

CREATE INDEX transaction_log_internal_idx ON pgmemento.transaction_log (internal_transaction_id, table_relid, stmt_date);
CREATE INDEX transaction_log_op_idx ON pgmemento.transaction_log (table_operation);
CREATE INDEX transaction_log_table_idx ON pgmemento.transaction_log (schema_name, table_name);
CREATE INDEX transaction_log_date_idx ON pgmemento.transaction_log (stmt_date);
CREATE INDEX audit_log_internal_idx ON pgmemento.audit_log (internal_transaction_id, table_relid, stmt_date);
CREATE INDEX audit_log_audit_idx ON pgmemento.audit_log (audit_id);
CREATE INDEX templates_table_idx ON pgmemento.table_templates (original_schema, original_table);
CREATE INDEX templates_date_idx ON pgmemento.table_templates (creation_date);


/**********************************************************
* ENABLE/DISABLE AUDIT
*
* Enables/disables pgMemento for a specified table/schema.
***********************************************************/
-- create pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.create_table_audit( 
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- create log trigger
  PERFORM pgmemento.create_table_log_trigger(table_name, schema_name);

  -- add audit_id column
  PERFORM pgmemento.create_table_audit_id(table_name, schema_name);
END;
$$
LANGUAGE plpgsql;

-- perform create_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.create_table_audit(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- drop pgMemento for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public' 
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- drop audit_id column
  PERFORM pgmemento.drop_table_audit_id(table_name, schema_name);

  -- drop log trigger
  PERFORM pgmemento.drop_table_log_trigger(table_name, schema_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.drop_table_audit(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* LOGGING TRIGGER
*
* Define trigger on a table to fire events when
*  - a statement is executed
*  - rows are inserted, updated or deleted 
*  - the table is truncated
***********************************************************/
-- create logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.create_table_log_trigger( 
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('CREATE TRIGGER log_trigger AFTER INSERT OR UPDATE OR DELETE ON %I.%I
                    FOR EACH ROW EXECUTE PROCEDURE pgmemento.log_change()', schema_name, table_name);
  EXECUTE format('CREATE TRIGGER log_truncate_trigger BEFORE TRUNCATE ON %I.%I
                    FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_change()', schema_name, table_name);
  EXECUTE format('CREATE TRIGGER log_transaction_trigger AFTER INSERT OR UPDATE OR DELETE ON %I.%I
                    FOR EACH STATEMENT EXECUTE PROCEDURE pgmemento.log_change()', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform create_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_log_trigger(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.create_table_log_trigger(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- drop logging triggers for one table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_log_trigger(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public' 
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('DROP TRIGGER IF EXISTS log_trigger ON %I.%I', schema_name, table_name);
  EXECUTE format('DROP TRIGGER IF EXISTS log_truncate_trigger ON %I.%I', schema_name, table_name);
  EXECUTE format('DROP TRIGGER IF EXISTS log_transaction_trigger ON %I.%I', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_log_trigger(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.drop_table_log_trigger(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* AUDIT ID COLUMN
*
* Add an extra column 'audit_id' to a table to trace 
* changes on rows over time.
***********************************************************/
-- add column 'audit_id' to a table
CREATE OR REPLACE FUNCTION pgmemento.create_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('ALTER TABLE %I.%I ADD COLUMN audit_id SERIAL', schema_name, table_name);
  EXECUTE format('CREATE INDEX %I ON %I.%I (audit_id)', table_name || '_audit_idx', schema_name, table_name); 
END;
$$
LANGUAGE plpgsql;

-- perform create_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.create_schema_audit_id(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.create_table_audit_id(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- drop column 'audit_id' from a table
CREATE OR REPLACE FUNCTION pgmemento.drop_table_audit_id(
  table_name TEXT,
  schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('DROP INDEX %I', table_name || '_audit_idx');
  EXECUTE format('ALTER TABLE %I.%I DROP COLUMN audit_id', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_audit_id(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.drop_table_audit_id(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* TRIGGER PROCEDURE log_change
*
* Procedure that is called when a trigger events are fired.
* Metadata of each statement is written to the transaction_log table. 
* Row-level changes are written to the audit_log table.
*   - INSERTs will be logged without specifying the content
*   - UPDATEs will produce a diff between OLD and NEW 
*     saving OLD values as JSON into the audit_log_table
*   - DELETEs and TRUNCATEs will log the complete row as JSON.
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_change() RETURNS trigger AS
$$
DECLARE
  rec RECORD;
  logged INTEGER := 0;
  json_diff JSON;
BEGIN
  -- handle statement-level trigger events
  IF TG_LEVEL = 'STATEMENT' THEN
    -- log row content affect by a TRUNCATE operation
    IF TG_OP = 'TRUNCATE' THEN
      FOR rec IN EXECUTE format('SELECT * FROM %I', TG_TABLE_NAME) LOOP
        EXECUTE 'INSERT INTO pgmemento.audit_log 
                   (id, internal_transaction_id, table_relid, stmt_date, audit_id, table_content)
                 VALUES 
                   (nextval(''pgmemento.AUDIT_LOG_ID_SEQ''), txid_current(), $1, statement_timestamp()::timestamp, $2, $3)' 
                 USING TG_RELID, rec.audit_id, row_to_json(OLD);
      END LOOP;
    END IF;

    -- log statement if not already happened during current transaction
    EXECUTE 'SELECT 1 FROM pgmemento.transaction_log
               WHERE (internal_transaction_id = $1 AND table_relid = $2 AND stmt_date = statement_timestamp()::timestamp)
                 AND table_operation = $3'
                 INTO logged USING txid_current(), TG_RELID, TG_OP;

    IF logged IS NULL THEN
      EXECUTE 'INSERT INTO pgmemento.transaction_log
                 (id, internal_transaction_id, table_operation, schema_name, table_name, table_relid,
                  stmt_date, user_name, client_name, application_name) 
               VALUES
                 (nextval(''pgmemento.TRANSACTION_LOG_ID_SEQ''), txid_current(), $1, $2, $3, $4,
                  statement_timestamp()::timestamp, current_user, inet_client_addr(), 
                  (SELECT setting FROM pg_settings WHERE name = ''application_name''))'
               USING TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_RELID;
    END IF;
  -- handle row-level trigger events
  ELSE
    IF TG_OP = 'INSERT' THEN
      EXECUTE 'INSERT INTO pgmemento.audit_log
                 (id, internal_transaction_id, table_relid, stmt_date, audit_id, table_content)
               VALUES 
                 (nextval(''pgmemento.AUDIT_LOG_ID_SEQ''), txid_current(), $1, statement_timestamp()::timestamp, $2, NULL)' 
               USING TG_RELID, NEW.audit_id;

	ELSIF TG_OP = 'UPDATE' THEN
      EXECUTE 'SELECT pgmemento.build_json(array_agg(to_json(old.key)), array_agg(old.value)) FROM json_each($1) old
                 LEFT OUTER JOIN json_each($2) new ON old.key = new.key
                   WHERE old.value::text <> new.value::text OR new.key IS NULL
		   HAVING array_agg(to_json(old.key)) IS NOT NULL
		   AND array_agg(old.value) IS NOT NULL' 
                   INTO json_diff USING row_to_json(OLD), row_to_json(NEW);
                   
	IF json_diff IS NOT NULL THEN

      EXECUTE 'INSERT INTO pgmemento.audit_log
                 (id, internal_transaction_id, table_relid, stmt_date, audit_id, table_content)
               VALUES 
                 (nextval(''pgmemento.AUDIT_LOG_ID_SEQ''), txid_current(), $1, statement_timestamp()::timestamp, $2, $3)' 
               USING TG_RELID, NEW.audit_id, json_diff;
        END IF;

	ELSIF TG_OP = 'DELETE' THEN
      EXECUTE 'INSERT INTO pgmemento.audit_log
                 (id, internal_transaction_id, table_relid, stmt_date, audit_id, table_content)
               VALUES 
                 (nextval(''pgmemento.AUDIT_LOG_ID_SEQ''), txid_current(), $1, statement_timestamp()::timestamp, $2, $3)' 
               USING TG_RELID, OLD.audit_id, row_to_json(OLD);
    END IF;
  END IF;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* LOG TABLE STATE
*
* Log table content in the audit_log table (as inserted values)
* to have a baseline for table versioning.
**********************************************************/
-- log all rows of a table in the audit_log table as inserted values
CREATE OR REPLACE FUNCTION pgmemento.log_table_state(
  original_table_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  transaction_id BIGINT;
  rec RECORD;
BEGIN
  transaction_id := txid_current();

  -- fill transaction_log table  
  EXECUTE 'INSERT INTO pgmemento.transaction_log 
             (id, internal_transaction_id, table_operation, schema_name, table_name, table_relid,
              stmt_date, user_name, client_name, application_name) 
           VALUES (nextval(''pgmemento.TRANSACTION_LOG_ID_SEQ''), $1, ''INSERT'', $2, $3, $4::regclass::oid,
             statement_timestamp()::timestamp, current_user, inet_client_addr(), 
             (SELECT setting FROM pg_settings WHERE name = ''application_name''))'
           USING transaction_id, original_schema_name, original_table_name, 
                 original_schema_name || '.' || original_table_name;

  -- fill audit_log table  
  FOR rec IN EXECUTE format('SELECT * FROM %I.%I', original_schema_name, original_table_name) LOOP
    EXECUTE 'INSERT INTO pgmemento.audit_log
               (id, internal_transaction_id, table_relid, stmt_date, audit_id, table_content)
             VALUES 
               (nextval(''pgmemento.AUDIT_LOG_ID_SEQ''), $1, $2::regclass::oid, statement_timestamp()::timestamp, $3, NULL)' 
             USING transaction_id, original_schema_name || '.' || original_table_name, rec.audit_id;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- perform log_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION pgmemento.log_schema_state(
  schema_name TEXT DEFAULT 'public',
  except_tables TEXT[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT pgmemento.log_table_state(tablename, schemaname) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* CREATE TABLE TEMPLATE
*
* To reproduce past tables from the JSON logs a table template
* is necessary. This is usually the audited table itself but
* if its structure has been changed the previous version of the
* table has to be recorded somehow.
*
* As for now this has to be done manually with create_table_template.
* The functions creates an empty copy of the table that will be
* changed (which means it has to be executed before the change).
* Every created copy is documented (with timestamp) in the 
* table_templates table within the pgmemento schema.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.create_table_template(
  original_table_name TEXT,
  original_schema_name TEXT DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  template_count INTEGER;
  template_name TEXT;
BEGIN
  template_count := nextval('pgmemento.TABLE_TEMPLATES_ID_SEQ');
  template_name := original_table_name || '_' || template_count;

  -- saving metadata of the template
  EXECUTE 'INSERT INTO pgmemento.table_templates (id, name, original_schema, original_table, original_relid, creation_date)
             VALUES ($1, $2, $3, $4, $5::regclass::oid, now()::timestamp)'
             USING template_count, template_name, original_schema_name, original_table_name,
                     original_schema_name || '.' || original_table_name;

  -- creating the template
  EXECUTE format('CREATE UNLOGGED TABLE pgmemento.%I AS SELECT * FROM %I.%I WHERE false',
                    template_name, original_schema_name, original_table_name);
END;
$$
LANGUAGE plpgsql;
