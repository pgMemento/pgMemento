-- TEST_INSTALL.sql
--
-- Author:      Felix Kunde <felix-kunde@gmx.de>
--
--              This script is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- Script that tests for - correct installation of pgMemento
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                                    | Author
-- 0.1.0     2017-07-20   initial commit                                   FKun
--

-- get test number
SELECT nextval('pgmemento.test_seq') AS n \gset

\echo
\echo 'TEST ':n': pgMemento setup'

DO
$$
DECLARE
  pgm_objects TEXT[] := '{}';
BEGIN
  -- check for log tables
  SELECT
    array_agg(c.relname ORDER BY c.relname) INTO pgm_objects
  FROM
    pg_class c,
    pg_namespace n
  WHERE
    c.relnamespace = n.oid
    AND n.nspname = 'pgmemento'
    AND c.relkind = 'r';

  ASSERT array_length(pgm_objects,1) = 5, 'Error: Incorrect number of audit tables!';
  ASSERT pgm_objects[1] = 'audit_column_log', 'Error: audit_column_log table not found!';
  ASSERT pgm_objects[2] = 'audit_table_log', 'Error: audit_table_log table not found!';
  ASSERT pgm_objects[3] = 'row_log', 'Error: row_log table not found!';
  ASSERT pgm_objects[4] = 'table_event_log', 'Error: table_event_log table not found!';
  ASSERT pgm_objects[5] = 'transaction_log', 'Error: transaction_log table not found!';

  -- check for views
  SELECT
    array_agg(c.relname ORDER BY c.relname) INTO pgm_objects
  FROM
    pg_class c,
    pg_namespace n
  WHERE
    c.relnamespace = n.oid
    AND n.nspname = 'pgmemento'
    AND c.relkind = 'v';

  ASSERT array_length(pgm_objects,1) = 2, 'Error: Incorrect number of audit views!';
  ASSERT pgm_objects[1] = 'audit_tables', 'Error: audit_tables view not found!';
  ASSERT pgm_objects[2] = 'audit_tables_dependency', 'Error: audit_tables_dependency view not found!';

  -- check for sequences
  SELECT
    array_agg(c.relname ORDER BY c.relname) INTO pgm_objects
  FROM
    pg_class c,
    pg_namespace n
  WHERE
    c.relnamespace = n.oid
    AND n.nspname = 'pgmemento'
    AND c.relkind = 'S'
    AND c.relname <> 'test_seq';

  ASSERT array_length(pgm_objects,1) = 6, 'Error: Incorrect number of sequences!';
  ASSERT pgm_objects[1] = 'audit_column_log_id_seq', 'Error: audit_column_log_id_seq not found!';
  ASSERT pgm_objects[2] = 'audit_id_seq', 'Error: audit_id_seq not found!';
  ASSERT pgm_objects[3] = 'audit_table_log_id_seq', 'Error: audit_table_log_id_seq not found!';
  ASSERT pgm_objects[4] = 'row_log_id_seq', 'Error: row_log_id_seq not found!';
  ASSERT pgm_objects[5] = 'table_event_log_id_seq', 'Error: table_event_log_id_seq not found!';
  ASSERT pgm_objects[6] = 'transaction_log_id_seq', 'Error: transaction_log_id_seq not found!';

  -- check for stored procedures
  SELECT
    array_agg(
      p.proname || ';' ||
      pg_catalog.pg_get_function_result(p.oid) ||
      CASE WHEN pg_catalog.pg_get_function_arguments(p.oid) = '' 
        THEN ''::text
        ELSE ';' || pg_catalog.pg_get_function_arguments(p.oid)
      END
      ORDER BY p.proname
    ) INTO pgm_objects
  FROM
    pg_proc p,
    pg_namespace n
  WHERE
    p.pronamespace = n.oid
    AND n.nspname = 'pgmemento';

  ASSERT array_length(pgm_objects,1) = 75, 'Error: Incorrect number of stored procedures!';
  ASSERT pgm_objects[1] = 'audit_table_check;record;tid integer, tab_name text, tab_schema text, OUT log_tab_oid oid, OUT log_tab_name text, OUT log_tab_schema text, OUT log_tab_id integer, OUT recent_tab_name text, OUT recent_tab_schema text, OUT recent_tab_id integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[2] = 'column_array_to_column_list;text;columns text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[3] = 'create_restore_template;SETOF void;until_tid integer, template_name text, table_name text, schema_name text, preserve_template boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[4] = 'create_schema_audit;SETOF void;schema_name text DEFAULT ''public''::text, log_state boolean DEFAULT true, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[5] = 'create_schema_audit_id;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[6] = 'create_schema_event_trigger;SETOF void;trigger_create_table boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[7] = 'create_schema_log_trigger;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[8] = 'create_table_audit;SETOF void;table_name text, schema_name text DEFAULT ''public''::text, log_state boolean DEFAULT true', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[9] = 'create_table_audit_id;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[10] = 'create_table_log_trigger;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[11] = 'delete_audit_table_log;SETOF oid;table_oid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[12] = 'delete_key;SETOF bigint;aid bigint, key_name text, old_value anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[13] = 'delete_table_event_log;SETOF integer;tid integer, table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[14] = 'delete_txid_log;integer;tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[15] = 'drop_schema_audit;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[16] = 'drop_schema_audit_id;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[17] = 'drop_schema_event_trigger;SETOF void', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[18] = 'drop_schema_log_trigger;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[19] = 'drop_schema_state;SETOF void;target_schema_name text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[20] = 'drop_table_audit;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[21] = 'drop_table_audit_id;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[22] = 'drop_table_log_trigger;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[23] = 'drop_table_state;SETOF void;table_name text, target_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[24] = 'fkey_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[25] = 'fkey_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[26] = 'get_column_list_by_txid;SETOF record;tid integer, table_name text, schema_name text, OUT column_name text, OUT data_type text, OUT ordinal_position integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[27] = 'get_column_list_by_txid_range;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, OUT column_name text, OUT column_count integer, OUT data_type text, OUT ordinal_position integer, OUT txid_range numrange', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[28] = 'get_ddl_from_context;text;stack text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[29] = 'get_max_txid_to_audit_id;integer;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[30] = 'get_min_txid_to_audit_id;integer;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[31] = 'get_txid_bounds_to_table;record;table_name text, schema_name text DEFAULT ''public''::text, OUT txid_min integer, OUT txid_max integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[32] = 'get_txids_to_audit_id;SETOF integer;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[33] = 'index_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[34] = 'index_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[35] = 'jsonb_merge;jsonb;jsonb', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[36] = 'jsonb_populate_value;anyelement;jsonb_log jsonb, column_name text, INOUT template anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[37] = 'log_column_state;SETOF void;e_id integer, columns text[], table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[38] = 'log_delete;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[39] = 'log_insert;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[40] = 'log_schema_state;SETOF void;schemaname text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[41] = 'log_table_event;integer;event_txid bigint, table_oid oid, op_type text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[42] = 'log_table_state;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[43] = 'log_transaction;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[44] = 'log_truncate;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[45] = 'log_update;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[46] = 'modify_ddl_log_tables;SETOF void;tablename text, schemaname text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[47] = 'move_schema_state;SETOF void;target_schema_name text, source_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[], copy_data boolean DEFAULT true', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[48] = 'move_table_state;SETOF void;table_name text, target_schema_name text, source_schema_name text, copy_data boolean DEFAULT true', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[49] = 'pkey_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[50] = 'pkey_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[51] = 'recover_audit_version;SETOF void;tid integer, aid bigint, changes jsonb, table_op integer, table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[52] = 'register_audit_table;integer;audit_table_name text, audit_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[53] = 'restore_change;anyelement;during_tid integer, aid bigint, column_name text, INOUT restored_value anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[54] = 'restore_query;text;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, aid bigint DEFAULT NULL::bigint, all_versions boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[55] = 'restore_record;record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, aid bigint, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[56] = 'restore_record_definition;text;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, all_versions boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[57] = 'restore_records;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, aid bigint, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[58] = 'restore_recordset;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[59] = 'restore_recordsets;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[60] = 'restore_schema_state;SETOF void;start_from_tid integer, end_at_tid integer, original_schema_name text, target_schema_name text, target_table_type text DEFAULT ''VIEW''::text, update_state boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[61] = 'restore_table_state;SETOF void;start_from_tid integer, end_at_tid integer, original_table_name text, original_schema_name text, target_schema_name text, target_table_type text DEFAULT ''VIEW''::text, update_state boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[62] = 'restore_value;anyelement;until_tid integer, aid bigint, column_name text, INOUT restored_value anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[63] = 'revert_distinct_transaction;SETOF void;tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[64] = 'revert_distinct_transactions;SETOF void;start_from_tid integer, end_at_tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[65] = 'revert_transaction;SETOF void;tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[66] = 'revert_transactions;SETOF void;start_from_tid integer, end_at_tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[67] = 'schema_drop_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[68] = 'sequence_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[69] = 'table_alter_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[70] = 'table_alter_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[71] = 'table_create_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[72] = 'table_drop_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[73] = 'table_drop_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[74] = 'unregister_audit_table;SETOF void;audit_table_name text, audit_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[75] = 'update_key;SETOF bigint;aid bigint, path_to_key_name text[], old_value anyelement, new_value anyelement', 'Error: Expected different function and/or arguments';  
END
$$
LANGUAGE plpgsql;
