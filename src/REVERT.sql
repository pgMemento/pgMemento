-- REVERT.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
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
-- 0.2.1     2016-02-14   removed dynamic sql code                        FKun
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
  rec RECORD;
  column_name TEXT;
  delimeter VARCHAR(1);
  update_stmt TEXT;
  updated_changes JSONB;
BEGIN
  SET CONSTRAINTS ALL DEFERRED;

  FOR rec IN 
    SELECT r.audit_order, r.audit_id, r.changes, 
           e.schema_name, e.table_name, e.op_id
      FROM pgmemento.table_event_log e
      JOIN pgmemento.transaction_log t ON t.txid = e.transaction_id
      JOIN LATERAL (
        SELECT 
          CASE WHEN e.op_id > 2 THEN
            rank() OVER (ORDER BY audit_id ASC)
          ELSE
            rank() OVER (ORDER BY audit_id DESC)
          END AS audit_order,
          audit_id, changes 
        FROM pgmemento.row_log 
          WHERE event_id = e.id
      ) r ON (true)
      WHERE t.txid = tid
        ORDER BY e.id DESC, audit_order ASC
  LOOP
    -- INSERT case
    IF rec.op_id = 1 THEN
      EXECUTE format('DELETE FROM %I.%I WHERE audit_id = $1', rec.schema_name, rec.table_name) USING rec.audit_id;

    -- UPDATE case
    ELSIF rec.op_id = 2 THEN
      IF rec.changes IS NOT NULL AND rec.changes <> '{}'::jsonb THEN
        -- set variables for update statement
        delimeter := '';
        update_stmt := format('UPDATE %I.%I SET', rec.schema_name, rec.table_name);

        -- loop over found keys
        FOR column_name IN SELECT jsonb_object_keys(rec.changes) LOOP
          update_stmt := update_stmt || delimeter ||
                           format(' %I = (SELECT %I FROM jsonb_populate_record(null::%I.%I, $1))',
                           column_name, column_name, rec.schema_name, rec.table_name);
          delimeter := ',';
        END LOOP;

        -- add condition and execute
        update_stmt := update_stmt || ' WHERE audit_id = $2';
        EXECUTE update_stmt USING rec.changes, rec.audit_id;
      END IF;

    -- DELETE and TRUNCATE case
    ELSE
      -- re-insertion of deleted rows will use new audit_ids
      WITH json_update AS (
        SELECT * FROM jsonb_each_text(rec.changes)
          UNION ALL
        SELECT * FROM jsonb_each_text(
          (SELECT json_object_agg('audit_id',nextval('pgmemento.audit_id_seq'))::jsonb))
      )
      SELECT json_object_agg(key, value)::jsonb INTO updated_changes FROM json_update;

      EXECUTE format('INSERT INTO %I.%I
                        SELECT * FROM jsonb_populate_record(null::%I.%I, $1)',
                        rec.schema_name, rec.table_name, rec.schema_name, rec.table_name)
                        USING updated_changes;
    END IF;
  END LOOP;
END;
$$ 
LANGUAGE plpgsql;