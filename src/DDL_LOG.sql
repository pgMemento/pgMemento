-- DDL_LOG.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides functions to track table changes in all database 
-- schemas using event triggers.
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                 | Author
-- 0.1.0     2016-04-14   initial commit                                FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   create_schema_event_trigger(trigger_create_table INTEGER DEFAULT 0) RETURNS SETOF VOID
*   drop_schema_event_trigger() RETURNS SETOF VOID
*   log_create_event() RETURNS SETOF VOID
*   log_drop_event(table_oid OID) RETURNS SETOF VOID
*   modify_audit_column_log() RETURNS SETOF VOID
*   modify_audit_table_log() RETURNS SETOF VOID
*
* TRIGGER FUNCTIONS:
*   schema_change_trigger() RETURNS event_trigger
*   schema_create_trigger() RETURNS event_trigger
*   schema_drop_trigger() RETURNS event_trigger
*
***********************************************************/

/***********************************************************
* MODIFY AUDIT_TABLE_LOG
*
* Procedures that updates the audit_table_log table which
* is called by event triggers (see below)
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.log_create_event() RETURNS SETOF VOID AS
$$
BEGIN
  -- EVENT: New table created
  -- enable auditing for newly created table
  -- but can only rely on schemas which already contain audited tables
  PERFORM pgmemento.create_table_audit(pgt.tablename, pgt.schemaname)
    FROM pg_tables pgt
    LEFT JOIN (
      SELECT schema_name, table_name 
        FROM pgmemento.audit_table_log
          WHERE upper(txid_range) IS NULL
      ) atl
      ON atl.schema_name = pgt.schemaname
      AND atl.table_name = pgt.tablename
    WHERE atl.table_name IS NULL
      AND pgt.schemaname IN (
        SELECT DISTINCT schemaname FROM pgmemento.audit_tables);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.modify_audit_table_log() RETURNS SETOF VOID AS
$$
BEGIN
  -- EVENT: New table created
  -- insert tables that do not exist in audit_table_log table
  INSERT INTO pgmemento.audit_table_log
    SELECT (pgt.schemaname || '.' || pgt.tablename)::regclass::oid,
      pgt.schemaname, pgt.tablename,
      numrange(txid_current(), NULL, '[)') AS txid_range
      FROM pgmemento.audit_tables tab
      JOIN pg_tables pgt 
        ON pgt.schemaname = tab.schemaname
       AND pgt.tablename = tab.tablename
      LEFT JOIN (
        SELECT schema_name, table_name 
          FROM pgmemento.audit_table_log
            WHERE upper(txid_range) IS NULL
        ) atl 
        ON atl.schema_name = tab.schemaname
        AND atl.table_name = tab.tablename
      WHERE tab.tg_is_active = TRUE
        AND atl.table_name IS NULL;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.log_drop_event(
  table_oid OID
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- EVENT: Table dropped
  -- update txid_range for removed table in audit_table_log table
  UPDATE pgmemento.audit_table_log
    SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
    WHERE relid = table_oid;

  -- update txid_range for removed columns in audit_column_log table
  UPDATE pgmemento.audit_column_log
    SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
    WHERE table_relid = table_oid;
END;
$$
LANGUAGE plpgsql;

/***********************************************************
* MODIFY AUDIT_COLUMN_LOG
*
* Procedure that updates the audit_column_log table which
* is called by event triggers (see functions below)
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.modify_audit_column_log() RETURNS SETOF VOID AS
$$
BEGIN
  -- EVENT: New table or new column created
  -- insert columns that do not exist in audit_column_log table
  INSERT INTO pgmemento.audit_column_log
    SELECT nextval('pgmemento.audit_column_log_id_seq'),
      (col.table_schema || '.' || col.table_name)::regclass::oid, col.column_name,
      col.ordinal_position, col.column_default, col.is_nullable,
      col.data_type, col.udt_name, col.character_maximum_length,
      col.numeric_precision, col.numeric_precision_radix, col.numeric_scale,
      col.datetime_precision, col.interval_type,
      numrange(txid_current(), NULL, '[)') AS txid_range
      FROM pgmemento.audit_tables tab
      JOIN information_schema.columns col 
        ON col.table_schema = tab.schemaname 
       AND col.table_name = tab.tablename
      LEFT JOIN (
        SELECT table_relid, column_name
          FROM pgmemento.audit_column_log
            WHERE upper(txid_range) IS NULL
        ) acl
        ON acl.table_relid = (tab.schemaname || '.' || tab.tablename)::regclass::oid
       AND acl.column_name = col.column_name
      WHERE tab.tg_is_active = TRUE
        AND acl.column_name IS NULL;

  -- EVENT: Column dropped
  -- update txid_range for removed columns in audit_column_log table
  WITH dropped_columns AS (
    SELECT acl.id FROM pgmemento.audit_column_log acl
      JOIN pgmemento.audit_table_log atl ON atl.relid = acl.table_relid
      LEFT JOIN information_schema.columns col 
             ON col.table_schema = atl.schema_name
            AND col.table_name = atl.table_name
            AND col.column_name = acl.column_name
      WHERE col.column_name IS NULL
        AND upper(acl.txid_range) IS NULL
  )
  UPDATE pgmemento.audit_column_log acol
    SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
    FROM dropped_columns dcol
    WHERE acol.id = dcol.id;

  -- EVENT: Column altered
  -- update txid_range for updated columns and insert new versions into audit_column_log table
  WITH updated_columns AS (
    SELECT acl.id, acl.table_relid, col.column_name,
        col.ordinal_position, col.column_default, col.is_nullable,
        col.data_type, col.udt_name, col.character_maximum_length,
        col.numeric_precision, col.numeric_precision_radix, col.numeric_scale,
        col.datetime_precision, col.interval_type
      FROM pgmemento.audit_column_log acl
      JOIN pgmemento.audit_table_log atl ON atl.relid = acl.table_relid
      LEFT JOIN information_schema.columns col 
             ON col.table_schema = atl.schema_name
            AND col.table_name = atl.table_name
            AND col.column_name = acl.column_name
      WHERE upper(acl.txid_range) IS NULL 
        AND (col.column_default <> acl.column_default
          OR col.is_nullable <> acl.is_nullable
          OR col.data_type <> acl.data_type
          OR col.udt_name <> acl.data_type_name
          OR col.character_maximum_length <> acl.char_max_length
          OR col.numeric_precision <> acl.numeric_precision
          OR col.numeric_precision_radix <> acl.numeric_precision_radix
          OR col.numeric_scale <> acl.numeric_scale
          OR col.datetime_precision <> acl.datetime_precision
          OR col.interval_type <> acl.interval_type)
  ), insert_new_versions AS (
    INSERT INTO pgmemento.audit_column_log
      SELECT nextval('pgmemento.audit_column_log_id_seq'), 
        table_relid, column_name,
        ordinal_position, column_default, is_nullable,
        data_type, udt_name, character_maximum_length,
        numeric_precision, numeric_precision_radix, numeric_scale,
        datetime_precision, interval_type,
        numrange(txid_current(), NULL, '[)') AS txid_range
        FROM updated_columns
  )
  UPDATE pgmemento.audit_column_log acol
    SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
    FROM updated_columns ucol
    WHERE ucol.id = acol.id;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE schema_create_trigger
*
* Procedure that is called when new tables are created
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.schema_create_trigger() RETURNS event_trigger AS
$$
BEGIN
  PERFORM pgmemento.log_create_event();
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE schema_change_trigger
*
* Procedure that is called when tables are altered
* e.g. to add, alter or drop columns
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.schema_change_trigger() RETURNS event_trigger AS
$$
BEGIN
  PERFORM pgmemento.modify_audit_table_log();
  PERFORM pgmemento.modify_audit_column_log();
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE schema_event_trigger
*
* Procedure that is called when tables are dropped
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.schema_drop_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj record;
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_dropped_objects()
  LOOP
    IF obj.schema_name <> 'pg_temp' THEN
      PERFORM pgmemento.log_drop_event(obj.objid);
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER
*
* Global event triggers that are fired when tables are
* created, altered or dropped
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.create_schema_event_trigger(
  trigger_create_table INTEGER DEFAULT 0
  ) RETURNS SETOF VOID AS
$$
DECLARE
  path_setting TEXT;
BEGIN
  PERFORM 1 FROM pg_event_trigger
    WHERE evtname = 'schema_change_trigger';

  IF NOT FOUND THEN
    CREATE EVENT TRIGGER schema_change_trigger ON ddl_command_end
      WHEN TAG IN ('ALTER TABLE')
        EXECUTE PROCEDURE pgmemento.schema_change_trigger();
  END IF;

  PERFORM 1 FROM pg_event_trigger
    WHERE evtname = 'schema_drop_trigger';

  IF NOT FOUND THEN
    CREATE EVENT TRIGGER schema_drop_trigger ON sql_drop
      WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE pgmemento.schema_drop_trigger();
  END IF;

  IF trigger_create_table <> 0 THEN
    PERFORM 1 FROM pg_event_trigger
      WHERE evtname = 'schema_create_trigger';

    IF NOT FOUND THEN
      CREATE EVENT TRIGGER schema_create_trigger ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE')
          EXECUTE PROCEDURE pgmemento.schema_create_trigger();
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.drop_schema_event_trigger() RETURNS SETOF VOID AS
$$
  DROP EVENT TRIGGER IF EXISTS schema_create_trigger;
  DROP EVENT TRIGGER IF EXISTS schema_change_trigger;
  DROP EVENT TRIGGER IF EXISTS schema_drop_trigger;
$$
LANGUAGE sql;