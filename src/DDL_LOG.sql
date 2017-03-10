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
-- 0.2.0     2017-03-13   update to Pg9.5 and adding more trigger       FKun
-- 0.1.0     2016-04-14   initial commit                                FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   create_schema_event_trigger(trigger_create_table INTEGER DEFAULT 0) RETURNS SETOF VOID
*   drop_schema_bypass(schema_name TEXT DEFAULT 'public', cascading INTEGER DEFAULT 0) RETURNS SETOF VOID
*   drop_table_bypass(table_name TEXT, schema_name TEXT DEFAULT 'public', cascading INTEGER DEFAULT 0) RETURNS SETOF VOID
*   drop_schema_event_trigger() RETURNS SETOF VOID
*
* TRIGGER FUNCTIONS:
*   schema_drop_pre_trigger() RETURNS event_trigger
*   table_alter_post_trigger() RETURNS event_trigger
*   table_alter_pre_trigger() RETURNS event_trigger
*   table_create_post_trigger() RETURNS event_trigger
*   table_drop_post_trigger() RETURNS event_trigger
*   table_drop_pre_trigger() RETURNS event_trigger
*
***********************************************************/

/**********************************************************
* EVENT TRIGGER PROCEDURE schema_drop_pre_trigger
*
* Procedure that is called BEFORE schema will be dropped.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.schema_drop_pre_trigger() RETURNS event_trigger AS
$$
BEGIN
  RAISE EXCEPTION 'If you want to drop a schema use function pgmemento.drop_schema_bypass';
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_alter_post_trigger
*
* Procedure that is called AFTER tables have been altered
* e.g. to add, alter or drop columns
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_alter_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj record;
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    -- first check if table is audited
    IF NOT EXISTS (
      SELECT 1 FROM pgmemento.audit_tables
        WHERE schemaname = obj.schema_name
          AND tablename = obj.objid::regclass::text
    ) THEN
      RETURN;
    ELSE
      -- check if affected table exists in 'audit_table_log' (with open range)
      IF NOT EXISTS (
        SELECT 1 FROM pgmemento.audit_table_log 
          WHERE relid = obj.objid
            AND upper(txid_range) IS NULL
      ) THEN
        -- EVENT: Activating auditing adds the audit_id column which fires the event trigger
        INSERT INTO pgmemento.audit_table_log
          (relid, schema_name, table_name, txid_range)
        VALUES 
          (obj.objid, obj.schema_name, obj.objid::regclass::text, numrange(txid_current(), NULL, '[)'));

        -- insert columns of new audited table into 'audit_column_log'
        INSERT INTO pgmemento.audit_column_log 
          (id, table_relid, column_name, ordinal_position, column_default, is_nullable, 
           data_type, data_type_name, char_max_length, numeric_precision, numeric_precision_radix, numeric_scale, 
           datetime_precision, interval_type, txid_range)
        (
          SELECT 
            nextval('pgmemento.audit_column_log_id_seq') AS id,
            obj.objid AS table_relid, column_name, ordinal_position, column_default, is_nullable,
            data_type, udt_name, character_maximum_length, numeric_precision, numeric_precision_radix, numeric_scale,
            datetime_precision, interval_type, numrange(txid_current(), NULL, '[)') AS txid_range
          FROM information_schema.columns
            WHERE table_schema = obj.schema_name 
              AND table_name = obj.objid::regclass::text
        );
      ELSE
        -- EVENT: New column created
        -- insert columns that do not exist in audit_column_log table
        INSERT INTO pgmemento.audit_column_log
          (id, table_relid, column_name, ordinal_position, column_default, is_nullable, 
           data_type, data_type_name, char_max_length, numeric_precision, numeric_precision_radix, numeric_scale, 
           datetime_precision, interval_type, txid_range)
        (
          SELECT 
            nextval('pgmemento.audit_column_log_id_seq') AS id, 
            obj.objid AS table_relid, col.column_name, col.ordinal_position, col.column_default, col.is_nullable,
            col.data_type, col.udt_name, col.character_maximum_length, col.numeric_precision, col.numeric_precision_radix, col.numeric_scale,
            col.datetime_precision, col.interval_type, numrange(txid_current(), NULL, '[)') AS txid_range
          FROM information_schema.columns col 
          LEFT JOIN (
            SELECT table_relid::regclass::text AS table_name, column_name, obj.schema_name
              FROM pgmemento.audit_column_log
                WHERE table_relid = obj.objid
                  AND upper(txid_range) IS NULL
          ) acl
          ON acl.table_name = col.table_name
          AND acl.column_name = col.column_name
          AND acl.schema_name = col.table_schema
            WHERE col.table_schema = obj.schema_name
              AND col.table_name = obj.objid::regclass::text
              AND acl.column_name IS NULL
        );

        -- EVENT: Column dropped
        -- update txid_range for removed columns in audit_column_log table
        WITH dropped_columns AS (
          SELECT acl.id 
            FROM pgmemento.audit_column_log acl
            LEFT JOIN (
              SELECT column_name, table_name
                FROM information_schema.columns
                WHERE table_schema = obj.schema_name
                  AND table_name = obj.objid::regclass::text
            ) col
            ON col.table_name = acl.table_relid::regclass::text
            AND col.column_name = acl.column_name
            WHERE acl.table_relid = obj.objid
              AND col.column_name IS NULL
              AND upper(acl.txid_range) IS NULL
        )
        UPDATE pgmemento.audit_column_log acol
           SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
           FROM dropped_columns dcol
           WHERE acol.id = dcol.id;

        -- EVENT: Column altered
        -- update txid_range for updated columns and insert new versions into audit_column_log table
        WITH updated_columns AS (
          SELECT acl.id, acl.table_relid, col.column_name, col.ordinal_position, col.column_default, col.is_nullable,
                 col.data_type, col.udt_name, col.character_maximum_length, col.numeric_precision, col.numeric_precision_radix, col.numeric_scale,
                 col.datetime_precision, col.interval_type
            FROM information_schema.columns col
            JOIN (
              SELECT *, obj.schema_name AS schema_name FROM pgmemento.audit_column_log
                WHERE table_relid = obj.objid
                  AND upper(txid_range) IS NULL 
            ) acl
            ON col.table_name = acl.table_relid::regclass::text
            AND col.column_name = acl.column_name
            AND col.table_schema = acl.schema_name
              WHERE (
                   col.column_default <> acl.column_default
                OR col.is_nullable <> acl.is_nullable
                OR col.data_type <> acl.data_type
                OR col.udt_name <> acl.data_type_name
                OR col.character_maximum_length <> acl.char_max_length
                OR col.numeric_precision <> acl.numeric_precision
                OR col.numeric_precision_radix <> acl.numeric_precision_radix
                OR col.numeric_scale <> acl.numeric_scale
                OR col.datetime_precision <> acl.datetime_precision
                OR col.interval_type <> acl.interval_type
              )
        ), insert_new_versions AS (
          INSERT INTO pgmemento.audit_column_log           
            (id, table_relid, column_name, ordinal_position, column_default, is_nullable, 
             data_type, data_type_name, char_max_length, numeric_precision, numeric_precision_radix, numeric_scale, 
             datetime_precision, interval_type, txid_range)
          (
            SELECT
              nextval('pgmemento.audit_column_log_id_seq') AS id, 
              table_relid, column_name, ordinal_position, column_default, is_nullable,
              data_type, udt_name, character_maximum_length, numeric_precision, numeric_precision_radix, numeric_scale,
              datetime_precision, interval_type, numrange(txid_current(), NULL, '[)') AS txid_range
            FROM updated_columns
          )
        )
        UPDATE pgmemento.audit_column_log acol
          SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
          FROM updated_columns ucol
            WHERE ucol.id = acol.id;
      END IF;
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_create_post_trigger
*
* Procedure that is called AFTER new tables have been created
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_create_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj record;
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_ddl_commands()
  LOOP
    IF obj.object_type = 'table' THEN
      PERFORM pgmemento.create_table_audit(obj.objid::regclass::text, obj.schema_name);
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_drop_post_trigger
*
* Procedure that is called AFTER tables have been dropped
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_drop_post_trigger() RETURNS event_trigger AS
$$
DECLARE
  obj record;
BEGIN
  FOR obj IN 
    SELECT * FROM pg_event_trigger_dropped_objects()
  LOOP
    IF obj.is_temporary = FALSE THEN
      -- update txid_range for removed table in audit_table_log table
      UPDATE pgmemento.audit_table_log
        SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
        WHERE relid = obj.objid;

      -- update txid_range for removed columns in audit_column_log table
      UPDATE pgmemento.audit_column_log
        SET txid_range = numrange(lower(txid_range), txid_current(), '[)') 
        WHERE table_relid = obj.objid
          AND upper(txid_range) IS NULL;
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* EVENT TRIGGER PROCEDURE table_drop_pre_trigger
*
* Procedure that is called BEFORE tables will be dropped.
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.table_drop_pre_trigger() RETURNS event_trigger AS
$$
BEGIN
  RAISE EXCEPTION 'If you want to drop a table use function pgmemento.drop_table_bypass';
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
BEGIN
  -- Create event trigger for DROP SCHEMA events to log data
  -- before it is lost
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'schema_drop_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER schema_drop_pre_trigger ON ddl_command_start
      WHEN TAG IN ('DROP SCHEMA')
        EXECUTE PROCEDURE pgmemento.schema_drop_pre_trigger();
  END IF;

  -- Create event trigger for ALTER TABLE events to update 'audit_column_log' table
  -- after table is altered
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'table_alter_post_trigger'
  ) THEN
    CREATE EVENT TRIGGER table_alter_post_trigger ON ddl_command_end
      WHEN TAG IN ('ALTER TABLE')
        EXECUTE PROCEDURE pgmemento.table_alter_post_trigger();
  END IF;

  -- Create event trigger for CREATE TABLE events to automatically start auditing on new tables
  -- The user can decide if he wants this behaviour during initializing pgMemento.
  IF trigger_create_table <> 0 THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_event_trigger
        WHERE evtname = 'table_create_post_trigger'
    ) THEN
      CREATE EVENT TRIGGER table_create_post_trigger ON ddl_command_end
        WHEN TAG IN ('CREATE TABLE')
          EXECUTE PROCEDURE pgmemento.table_create_post_trigger();
    END IF;
  END IF;

  -- Create event trigger for DROP TABLE events to update tables 'audit_table_log' and 'audit_column_log'
  -- after table is dropped
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'table_drop_post_trigger'
  ) THEN
    CREATE EVENT TRIGGER table_drop_post_trigger ON sql_drop
      WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE pgmemento.table_drop_post_trigger();
  END IF;

  -- Create event trigger for DROP TABLE events to log data
  -- before it is lost
  IF NOT EXISTS (
    SELECT 1 FROM pg_event_trigger
      WHERE evtname = 'table_drop_pre_trigger'
  ) THEN
    CREATE EVENT TRIGGER table_drop_pre_trigger ON ddl_command_start
      WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE pgmemento.table_drop_pre_trigger();
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmemento.drop_schema_event_trigger() RETURNS SETOF VOID AS
$$
  DROP EVENT TRIGGER IF EXISTS schema_drop_pre_trigger;
  DROP EVENT TRIGGER IF EXISTS table_alter_post_trigger;
  DROP EVENT TRIGGER IF EXISTS table_create_post_trigger;
  DROP EVENT TRIGGER IF EXISTS table_drop_post_trigger;
  DROP EVENT TRIGGER IF EXISTS table_drop_pre_trigger;
$$
LANGUAGE sql;


/**********************************************************
* Bypass functions for DROP events
*
* pgMemento only logs deleted versions of the data.
* Therefore, DROP events would cause data loss if no
* DML operation is used in advance
**********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.drop_schema_bypass(
  schema_name TEXT DEFAULT 'public',
  cascading INTEGER DEFAULT 0
  ) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  -- first drop triggers preventing drop events
  ALTER EVENT TRIGGER schema_drop_pre_trigger DISABLE;
  ALTER EVENT TRIGGER table_drop_pre_trigger DISABLE;

  -- second: TRUNCATE tables to log the data
  FOR rec IN 
    SELECT p.schemaname, p.tablename 
      FROM pg_tables p, pgmemento.table_dependency d
      WHERE p.schemaname = d.schemname
        AND p.tablename = d.tablename
        AND p.schemaname = $1
  LOOP
    IF $2 <> 0 THEN
      EXECUTE format('TRUNCATE %I.%I CASCADE', rec.schema_name, rec.table_name);
    ELSE
      EXECUTE format('TRUNCATE %I.%I', rec.schema_name, rec.table_name);
    END IF;
  END LOOP;

  -- third: drop the schema
  IF $2 <> 0 THEN
    EXECUTE format('DROP SCHEMA %I CASCADE', $1);
  ELSE
    EXECUTE format('DROP SCHEMA %I', $1);
  END IF;

  -- third: recreate event trigger
  ALTER EVENT TRIGGER table_drop_pre_trigger ENABLE;
  ALTER EVENT TRIGGER schema_drop_pre_trigger ENABLE;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION pgmemento.drop_table_bypass(
  table_name TEXT, 
  schema_name TEXT DEFAULT 'public',
  cascading INTEGER DEFAULT 0
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- first: drop trigger preventing drop tables events
  ALTER EVENT TRIGGER table_drop_pre_trigger DISABLE;

  -- second: TRUNCATE table to log the data and drop it afterwards
  IF $3 <> 0 THEN
    EXECUTE format('TRUNCATE %I.%I CASCADE', $2, $1);
    EXECUTE format('DROP TABLE %I.%I CASCADE', $2, $1);
  ELSE
    EXECUTE format('TRUNCATE %I.%I', $2, $1);
    EXECUTE format('DROP TABLE %I.%I', $2, $1);
  END IF;

  -- third: recreate event trigger
  ALTER EVENT TRIGGER table_drop_pre_trigger ENABLE;
END;
$$
LANGUAGE plpgsql;