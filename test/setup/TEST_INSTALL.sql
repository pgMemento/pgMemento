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

  ASSERT array_length(pgm_objects,1) = 64, 'Error: Incorrect number of stored procedures!';
  ASSERT pgm_objects[1] = 'audit_table_check;record;tid bigint, tab_name text, tab_schema text, OUT log_tab_oid oid, OUT log_tab_name text, OUT log_tab_schema text, OUT log_tab_id integer, OUT recent_tab_name text, OUT recent_tab_schema text, OUT recent_tab_id integer, OUT recent_tab_upper_txid numeric', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[2] = 'create_restore_template;SETOF void;tid bigint, template_name text, table_name text, schema_name text, preserve_template integer DEFAULT 0', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[3] = 'create_schema_audit;SETOF void;schema_name text DEFAULT ''public''::text, log_state integer DEFAULT 1, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[4] = 'create_schema_audit_id;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[5] = 'create_schema_event_trigger;SETOF void;trigger_create_table integer DEFAULT 0', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[6] = 'create_schema_log_trigger;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[7] = 'create_table_audit;SETOF void;table_name text, schema_name text DEFAULT ''public''::text, log_state integer DEFAULT 1', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[8] = 'create_table_audit_id;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[9] = 'create_table_log_trigger;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[10] = 'delete_audit_table_log;SETOF oid;table_oid integer', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[11] = 'delete_key;SETOF bigint;aid bigint, key_name text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[12] = 'delete_table_event_log;SETOF integer;tid bigint, table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[13] = 'delete_txid_log;bigint;t_id bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[14] = 'drop_schema_audit;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[15] = 'drop_schema_audit_id;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[16] = 'drop_schema_event_trigger;SETOF void', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[17] = 'drop_schema_log_trigger;SETOF void;schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[18] = 'drop_schema_state;SETOF void;target_schema_name text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[19] = 'drop_table_audit;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[20] = 'drop_table_audit_id;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[21] = 'drop_table_log_trigger;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[22] = 'drop_table_state;SETOF void;table_name text, target_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[23] = 'fkey_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[24] = 'fkey_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[25] = 'generate_log_entries;SETOF jsonb;start_from_tid bigint, end_at_tid bigint, table_name text, schema_name text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[26] = 'generate_log_entry;jsonb;start_from_tid bigint, end_at_tid bigint, table_name text, schema_name text, aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[27] = 'get_ddl_from_context;text;stack text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[28] = 'get_max_txid_to_audit_id;bigint;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[29] = 'get_min_txid_to_audit_id;bigint;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[30] = 'get_txid_bounds_to_table;record;table_name text, schema_name text DEFAULT ''public''::text, OUT txid_min bigint, OUT txid_max bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[31] = 'get_txids_to_audit_id;SETOF bigint;aid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[32] = 'index_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[33] = 'index_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[34] = 'jsonb_merge;jsonb;jsonb', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[35] = 'log_ddl_event;integer;table_name text, schema_name text, op_type integer, op_text text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[36] = 'log_delete;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[37] = 'log_insert;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[38] = 'log_schema_state;SETOF void;schemaname text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[39] = 'log_table_state;SETOF void;table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[40] = 'log_transaction;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[41] = 'log_truncate;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[42] = 'log_update;trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[43] = 'modify_ddl_log_tables;SETOF void;tablename text, schemaname text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[44] = 'move_schema_state;SETOF void;target_schema_name text, source_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[], copy_data integer DEFAULT 1', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[45] = 'move_table_state;SETOF void;table_name text, target_schema_name text, source_schema_name text, copy_data integer DEFAULT 1', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[46] = 'pkey_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text, except_tables text[] DEFAULT ''{}''::text[]', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[47] = 'pkey_table_state;SETOF void;table_name text, target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[48] = 'recover_audit_version;SETOF void;tid bigint, aid bigint, changes jsonb, table_op integer, table_name text, schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[49] = 'register_audit_table;integer;audit_table_name text, audit_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[50] = 'restore_query;text;start_from_tid bigint, end_at_tid bigint, table_name text, schema_name text, aid bigint DEFAULT NULL::bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[51] = 'restore_schema_state;SETOF void;start_from_tid bigint, end_at_tid bigint, original_schema_name text, target_schema_name text, target_table_type text DEFAULT ''VIEW''::text, update_state integer DEFAULT 0', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[52] = 'restore_table_state;SETOF void;start_from_tid bigint, end_at_tid bigint, original_table_name text, original_schema_name text, target_schema_name text, target_table_type text DEFAULT ''VIEW''::text, update_state integer DEFAULT 0', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[53] = 'revert_distinct_transaction;SETOF void;tid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[54] = 'revert_distinct_transactions;SETOF void;start_from_tid bigint, end_at_tid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[55] = 'revert_transaction;SETOF void;tid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[56] = 'revert_transactions;SETOF void;start_from_tid bigint, end_at_tid bigint', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[57] = 'schema_drop_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[58] = 'sequence_schema_state;SETOF void;target_schema_name text, original_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[59] = 'table_alter_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[60] = 'table_alter_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[61] = 'table_create_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[62] = 'table_drop_post_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[63] = 'table_drop_pre_trigger;event_trigger', 'Error: Expected different function and/or arguments';
  ASSERT pgm_objects[64] = 'unregister_audit_table;SETOF void;audit_table_name text, audit_schema_name text DEFAULT ''public''::text', 'Error: Expected different function and/or arguments';
END
$$
LANGUAGE plpgsql;

\echo
\echo 'TEST ':n': pgMemento setup - correct!'