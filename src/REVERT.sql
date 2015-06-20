-- REVERT.sql
--
-- Author:      Felix Kunde <fkunde@virtualcitysystems.de>
--
--              This skript is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This script provides functions to revert single transactions and entire database
-- states.
-- 
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                   | Author
-- 0.2.0     2015-02-26   added revert_transaction procedure              FKun
-- 0.1.0     2014-11-26   initial commit                                  FKun
--

/**********************************************************
* C-o-n-t-e-n-t:
*
* FUNCTIONS:
*   revert_transaction(tid BIGINT) RETURNS SETOF VOID
***********************************************************/
CREATE OR REPLACE FUNCTION pgmemento.revert_transaction(tid BIGINT) RETURNS SETOF VOID AS
$$
DECLARE
  r RECORD;
  column_name TEXT;
  delimeter VARCHAR(1);
  update_stmt TEXT;
  updated_changes JSONB;
BEGIN
  SET CONSTRAINTS ALL DEFERRED;

  FOR r IN EXECUTE 
    'SELECT * FROM (
       (SELECT r.audit_id, r.changes, e.schema_name, e.table_name, e.op_id
          FROM pgmemento.row_log r
          JOIN pgmemento.table_event_log e ON r.event_id = e.id
          JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
          WHERE t.txid = $1 AND e.op_id > 2
          ORDER BY r.audit_id ASC)
        UNION ALL
       (SELECT r.audit_id, r.changes, e.schema_name, e.table_name, e.op_id
          FROM pgmemento.row_log r
          JOIN pgmemento.table_event_log e ON r.event_id = e.id
          JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
          WHERE t.txid = $1 AND e.op_id = 2
          ORDER BY r.audit_id DESC)
        UNION ALL
       (SELECT r.audit_id, r.changes, e.schema_name, e.table_name, e.op_id
          FROM pgmemento.row_log r
          JOIN pgmemento.table_event_log e ON r.event_id = e.id
          JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
          WHERE t.txid = $1 AND e.op_id = 1
          ORDER BY r.audit_id DESC)
     ) txid_content
     ORDER BY op_id DESC' USING tid LOOP

    -- INSERT case
    IF r.op_id = 1 THEN
      EXECUTE format('DELETE FROM %I.%I WHERE audit_id = %L', r.schema_name, r.table_name, r.audit_id);

    -- UPDATE case
    ELSIF r.op_id = 2 THEN
      -- set variables for update statement
      delimeter := '';
      update_stmt := format('UPDATE %I.%I SET', r.schema_name, r.table_name);

      -- loop over found keys
      FOR column_name IN EXECUTE 'SELECT jsonb_object_keys($1)' USING r.changes LOOP
        update_stmt := update_stmt || delimeter ||
                         format(' %I = (SELECT %I FROM jsonb_populate_record(null::%I.%I, %L))',
                         column_name, column_name, r.schema_name, r.table_name, r.changes);
        delimeter := ',';
      END LOOP;

      -- add condition and execute
      update_stmt := update_stmt || format(' WHERE audit_id = %L', r.audit_id);
      EXECUTE update_stmt;

    -- DELETE and TRUNCATE case
    ELSE
      EXECUTE 'WITH json_update AS
                (SELECT * FROM jsonb_each_text($1)
                   UNION ALL
                 SELECT * FROM jsonb_each_text($2)
                )
                SELECT json_object_agg(key, value)::jsonb FROM json_update'
                INTO updated_changes USING r.changes, json_object_agg('audit_id',nextval('pgmemento.audit_id_seq'))::jsonb;

      EXECUTE format('INSERT INTO %I.%I
                        SELECT * FROM jsonb_populate_record(null::%I.%I, %L)',
                        r.schema_name, r.table_name, r.schema_name, r.table_name, updated_changes);
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;