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
-- Version | Date       | Description                                  | Author
-- 0.2.0     2020-02-29   reflect all changes of 0.7 release             FKun
-- 0.1.0     2017-07-20   initial commit                                 FKun
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

  ASSERT array_length(pgm_objects,1) = 6, 'Error: Incorrect number of audit tables!';
  ASSERT pgm_objects[1] = 'audit_column_log', 'Error: audit_column_log table not found!';
  ASSERT pgm_objects[2] = 'audit_schema_log', 'Error: audit_schema_log table not found!';
  ASSERT pgm_objects[3] = 'audit_table_log', 'Error: audit_table_log table not found!';
  ASSERT pgm_objects[4] = 'row_log', 'Error: row_log table not found!';
  ASSERT pgm_objects[5] = 'table_event_log', 'Error: table_event_log table not found!';
  ASSERT pgm_objects[6] = 'transaction_log', 'Error: transaction_log table not found!';

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

  ASSERT array_length(pgm_objects,1) = 9, 'Error: Incorrect number of sequences!';
  ASSERT pgm_objects[1] = 'audit_column_log_id_seq', 'Error: audit_column_log_id_seq not found!';
  ASSERT pgm_objects[2] = 'audit_id_seq', 'Error: audit_id_seq not found!';
  ASSERT pgm_objects[3] = 'audit_schema_log_id_seq', 'Error: audit_schema_log_id_seq not found!';
  ASSERT pgm_objects[4] = 'audit_table_log_id_seq', 'Error: audit_table_log_id_seq not found!';
  ASSERT pgm_objects[5] = 'row_log_id_seq', 'Error: row_log_id_seq not found!';
  ASSERT pgm_objects[6] = 'schema_log_id_seq', 'Error: schema_log_id_seq not found!';
  ASSERT pgm_objects[7] = 'table_event_log_id_seq', 'Error: table_event_log_id_seq not found!';
  ASSERT pgm_objects[8] = 'table_log_id_seq', 'Error: table_log_id_seq not found!';
  ASSERT pgm_objects[9] = 'transaction_log_id_seq', 'Error: transaction_log_id_seq not found!';

  -- check for stored procedures
  SELECT
    array_agg(
      p.proname || ';' ||
      pg_catalog.pg_get_function_result(p.oid) ||
      CASE WHEN pg_catalog.pg_get_function_arguments(p.oid) = ''
        THEN ''::text
        ELSE ';' || pg_catalog.pg_get_function_arguments(p.oid)
      END
      ORDER BY p.proname, p.oid
    ) INTO pgm_objects
  FROM
    pg_proc p,
    pg_namespace n
  WHERE
    p.pronamespace = n.oid
    AND n.nspname = 'pgmemento';

  ASSERT array_length(pgm_objects,1) = 94, 'Error: Incorrect number of stored procedures!';
  ASSERT pgm_objects[1] = 'audit_table_check;record;tid integer, tab_name text, tab_schema text, OUT table_log_id integer, OUT log_tab_name text, OUT log_tab_schema text, OUT log_audit_id_column text, OUT log_tab_id integer, OUT recent_tab_name text, OUT recent_tab_schema text, OUT recent_audit_id_column text, OUT recent_tab_id integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[2] = 'column_array_to_column_list;text;columns text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[3] = 'create_restore_template;SETOF void;until_tid integer, template_name text, table_name text, schema_name text DEFAULT ''public''::text, preserve_template boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[4] = 'create_schema_audit;SETOF void;schemaname text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_old_data boolean DEFAULT true, log_new_data boolean DEFAULT false, log_state boolean DEFAULT false, trigger_create_table boolean DEFAULT false, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[5] = 'create_schema_audit_id;SETOF void;schemaname text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[6] = 'create_schema_event_trigger;SETOF void;trigger_create_table boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[7] = 'create_schema_log_trigger;SETOF void;schemaname text DEFAULT ''public''::text, log_old_data boolean DEFAULT true, log_new_data boolean DEFAULT false, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[8] = 'create_table_audit;SETOF void;tablename text, schemaname text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_old_data boolean DEFAULT true, log_new_data boolean DEFAULT false, log_state boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[9] = 'create_table_audit_id;SETOF void;table_name text, schema_name text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[10] = 'create_table_log_trigger;SETOF void;table_name text, schema_name text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_old_data boolean DEFAULT true, log_new_data boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[11] = 'delete_audit_table_log;SETOF integer;tablename text, schemaname text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[12] = 'delete_key;SETOF bigint;aid bigint, key_name text, old_value anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[13] = 'delete_table_event_log;SETOF integer;tid integer, tablename text, schemaname text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[14] = 'delete_table_event_log;SETOF integer;tablename text, schemaname text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[15] = 'delete_txid_log;integer;tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[16] = 'drop;text;schemaname text DEFAULT ''public''::text, log_state boolean DEFAULT true, drop_log boolean DEFAULT false, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[17] = 'drop_schema_audit;SETOF void;schema_name text DEFAULT ''public''::text, log_state boolean DEFAULT true, drop_log boolean DEFAULT false, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[18] = 'drop_schema_audit_id;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[19] = 'drop_schema_event_trigger;SETOF void', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[20] = 'drop_schema_log_trigger;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[21] = 'drop_schema_state;SETOF void;target_schema_name text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[22] = 'drop_table_audit;SETOF void;table_name text, schema_name text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_state boolean DEFAULT true, drop_log boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[23] = 'drop_table_audit_id;SETOF void;table_name text, schema_name text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[24] = 'drop_table_log_trigger;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[25] = 'drop_table_state;SETOF void;table_name text, target_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[26] = 'fetch_ident;text;context text, fetch_count integer DEFAULT 1', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[27] = 'fkey_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[28] = 'fkey_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[29] = 'flatten_ddl;text;ddl_command text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[30] = 'get_column_list;SETOF record;start_from_tid integer, end_at_tid integer, table_log_id integer, table_name text, schema_name text DEFAULT ''public''::text, all_versions boolean DEFAULT false, OUT column_name text, OUT column_count integer, OUT data_type text, OUT ordinal_position integer, OUT txid_range numrange', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[31] = 'get_column_list_by_txid;SETOF record;tid integer, table_name text, schema_name text DEFAULT ''public''::text, OUT column_name text, OUT data_type text, OUT ordinal_position integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[32] = 'get_column_list_by_txid_range;SETOF record;start_from_tid integer, end_at_tid integer, table_log_id integer, OUT column_name text, OUT column_count integer, OUT data_type text, OUT ordinal_position integer, OUT txid_range numrange', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[33] = 'get_ddl_from_context;text;stack text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[34] = 'get_max_txid_to_audit_id;integer;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[35] = 'get_min_txid_to_audit_id;integer;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[36] = 'get_operation_id;smallint;operation text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[37] = 'get_table_oid;oid;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[38] = 'get_txid_bounds_to_table;record;table_log_id integer, OUT txid_min integer, OUT txid_max integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[39] = 'get_txids_to_audit_id;SETOF integer;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[40] = 'index_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[41] = 'index_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[42] = 'init;text;schemaname text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_old_data boolean DEFAULT true, log_new_data boolean DEFAULT false, log_state boolean DEFAULT false, trigger_create_table boolean DEFAULT false, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[43] = 'jsonb_merge;jsonb;jsonb', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[44] = 'jsonb_populate_value;anyelement;jsonb_log jsonb, column_name text, INOUT template anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[45] = 'jsonb_unroll_for_update;text;path text, nested_value jsonb, complex_typname text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[46] = 'log_delete;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[47] = 'log_insert;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[48] = 'log_new_table_state;SETOF void;columns text[], tablename text, schemaname text, table_event_key text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[49] = 'log_old_table_state;SETOF void;columns text[], tablename text, schemaname text, table_event_key text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[50] = 'log_schema_baseline;SETOF void;audit_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[51] = 'log_statement;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[52] = 'log_table_baseline;SETOF void;table_name text, schema_name text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_new_data boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[53] = 'log_table_event;text;tablename text, schemaname text, op_type text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[54] = 'log_transaction;integer;current_txid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[55] = 'log_truncate;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[56] = 'log_update;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[57] = 'modify_ddl_log_tables;SETOF void;tablename text, schemaname text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[58] = 'modify_row_log;SETOF void;tablename text, schemaname text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[59] = 'move_schema_state;SETOF void;target_schema_name text, source_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[], copy_data boolean DEFAULT true', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[60] = 'move_table_state;SETOF void;table_name text, target_schema_name text, source_schema_name text, copy_data boolean DEFAULT true', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[61] = 'pkey_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[62] = 'pkey_table_state;SETOF void;target_table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[63] = 'recover_audit_version;SETOF void;tid integer, aid bigint, changes jsonb, table_op integer, tab_name text, tab_schema text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[64] = 'register_audit_table;integer;audit_table_name text, audit_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[65] = 'reinit;text;schemaname text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_old_data boolean DEFAULT true, log_new_data boolean DEFAULT false, trigger_create_table boolean DEFAULT false, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[66] = 'restore_change;anyelement;during_tid integer, aid bigint, column_name text, INOUT restored_value anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[67] = 'restore_query;text;start_from_tid integer, end_at_tid integer, table_name text, schema_name text DEFAULT ''public''::text, aid bigint DEFAULT NULL::bigint, all_versions boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[68] = 'restore_record;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, aid bigint, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[69] = 'restore_record_definition;text;tid integer, table_name text, schema_name text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[70] = 'restore_record_definition;text;start_from_tid integer, end_at_tid integer, table_log_id integer, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[71] = 'restore_records;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text, aid bigint, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[72] = 'restore_recordset;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text DEFAULT ''public''::text, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[73] = 'restore_recordsets;SETOF record;start_from_tid integer, end_at_tid integer, table_name text, schema_name text DEFAULT ''public''::text, jsonb_output boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[74] = 'restore_schema_state;SETOF void;start_from_tid integer, end_at_tid integer, original_schema_name text, target_schema_name text, target_table_type text DEFAULT ''VIEW''::text, update_state boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[75] = 'restore_table_state;SETOF void;start_from_tid integer, end_at_tid integer, original_table_name text, original_schema_name text, target_schema_name text, target_table_type text DEFAULT ''VIEW''::text, update_state boolean DEFAULT false', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[76] = 'restore_value;anyelement;until_tid integer, aid bigint, column_name text, INOUT restored_value anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[77] = 'revert_distinct_transaction;SETOF void;tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[78] = 'revert_distinct_transactions;SETOF void;start_from_tid integer, end_at_tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[79] = 'revert_transaction;SETOF void;tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[80] = 'revert_transactions;SETOF void;start_from_tid integer, end_at_tid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[81] = 'schema_drop_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[82] = 'sequence_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[83] = 'split_table_from_query;record;INOUT query text, OUT audit_table_name text, OUT audit_schema_name text, OUT audit_table_log_id integer, OUT audit_id_column_name text, OUT audit_old_data boolean', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[84] = 'start;text;schemaname text DEFAULT ''public''::text, audit_id_column_name text DEFAULT ''pgmemento_audit_id''::text, log_old_data boolean DEFAULT true, log_new_data boolean DEFAULT false, trigger_create_table boolean DEFAULT false, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[85] = 'stop;text;schemaname text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[86] = 'table_alter_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[87] = 'table_alter_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[88] = 'table_create_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[89] = 'table_drop_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[90] = 'table_drop_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[91] = 'trim_outer_quotes;text;quoted_string text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[92] = 'unregister_audit_table;SETOF void;audit_table_name text, audit_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[93] = 'update_key;SETOF bigint;aid bigint, path_to_key_name text[], old_value anyelement, new_value anyelement', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[94] = 'version;record;OUT full_version text, OUT major_version integer, OUT minor_version integer, OUT revision integer, OUT build_id text', 'Error: Expected different function and/or arguments';
END;
$$
LANGUAGE plpgsql;
