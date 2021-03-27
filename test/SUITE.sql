-- set test counter
CREATE SEQUENCE pgmemento.test_seq;

-- create table for testing
CREATE TABLE public.object (
  id INTEGER PRIMARY KEY,
  lineage TEXT
);

INSERT INTO
  public.object(id, lineage)
VALUES
  (1, NULL);

-- test schema
\i test/setup/TEST_INSTALL.sql

-- test setup functions
\i test/setup/TEST_INIT.sql

-- test DDL logging
\i test/ddl_log/TEST_CREATE_TABLE.sql
\i test/ddl_log/TEST_ALTER_TABLE.sql
\i test/ddl_log/TEST_ADD_COLUMN.sql
\i test/ddl_log/TEST_ALTER_COLUMN.sql
\i test/ddl_log/TEST_DROP_COLUMN.sql
\i test/ddl_log/TEST_DROP_TABLE.sql

-- test DML logging
\i test/dml_log/TEST_INSERT.sql
\i test/dml_log/TEST_UPDATE.sql
\i test/dml_log/TEST_DELETE.sql
\i test/dml_log/TEST_TRUNCATE.sql

-- test reverts
\i test/revert/TEST_REVERT_DROP_TABLE.sql;
\i test/revert/TEST_REVERT_DROP_COLUMN.sql;
\i test/revert/TEST_REVERT_ALTER_COLUMN.sql;
\i test/revert/TEST_REVERT_ADD_COLUMN.sql;
\i test/revert/TEST_REVERT_ALTER_TABLE.sql;
\i test/revert/TEST_REVERT_CREATE_TABLE.sql;

\i test/revert/TEST_REVERT_TRUNCATE.sql;
\i test/revert/TEST_REVERT_DELETE.sql;
\i test/revert/TEST_REVERT_UPDATE.sql;
\i test/revert/TEST_REVERT_INSERT.sql;

-- test restore
\i test/restore/TEST_RESTORE_RECORD.sql;
\i test/restore/TEST_RESTORE_RECORDS.sql;
\i test/restore/TEST_RESTORE_RECORDSET.sql;
\i test/restore/TEST_RESTORE_RECORDSETS.sql;
\i test/restore/TEST_RESTORE_TABLE_STATE.sql;

-- test util functions
\i test/log_util/TEST_DELETE_LOGS.sql

-- test CTL functions and uninstalling
\i test/setup/TEST_STOP_START.sql
\i test/setup/TEST_UNINSTALL.sql

\echo
\echo 'Uninstall test tables'
DROP TABLE object;
DROP TABLE util_test;

DROP SEQUENCE pgmemento.test_seq;
