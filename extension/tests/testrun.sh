#!/bin/bash

set -e

psql postgres postgres -c 'DROP DATABASE IF EXISTS pgmemento_test;'
psql postgres postgres -c 'CREATE DATABASE pgmemento_test;'

cd /home/pgmemento;

echo "Running extension tests...";
psql pgmemento_test postgres -f /home/pgmemento/extension/tests/TEST.sql

### Test we have backups covered

echo "Running backup tests...";
psql postgres postgres -c 'CREATE DATABASE pgmemento_backup';

echo "Creating initial data for backup";
cat <<'EOF' | psql pgmemento_backup postgres
  CREATE EXTENSION pgmemento;
  SELECT pgmemento.init('public', 'pgmemento_audit_id', true, true, false, true);

  CREATE TABLE valuable_data (id INTEGER, value VARCHAR);
  INSERT INTO valuable_data (id, value) VALUES (1, 'one'), (2, 'two'), (3, 'three');
  UPDATE valuable_data SET value = 'mutated two' WHERE id = 2;
EOF

echo "Query state of 'two' before update";
cat <<'EOF' | psql pgmemento_backup postgres -qAt > /tmp/restore_data.sql
  SELECT format(
    'SELECT set_config(''pgmemento.restore_value'', pgmemento.restore_value(%1$s, %2$s, ''value'', NULL::varchar), FALSE) AS restore',
    (SELECT txid_max FROM pgmemento.audit_tables WHERE tablename = 'valuable_data'),
    (SELECT pgmemento_audit_id FROM valuable_data WHERE id = 2)
  );
EOF

echo "Taking the mutated data backup";
pg_dump -U postgres -Ox pgmemento_backup > /tmp/backup.sql;

echo "Drop the original database";
psql postgres postgres -c 'drop database pgmemento_backup;'

echo "Generating new database";

psql postgres postgres -c 'drop database if exists pgmemento_restore;'
psql postgres postgres -c 'create database pgmemento_restore;'

echo "Restoring the data from backup";
psql pgmemento_restore postgres < /tmp/backup.sql

echo "Validating we have mutated data restored";

read -r -d '\0' TEST_MUTATED <<'EOF'
  DO $$ BEGIN
    ASSERT (SELECT value FROM valuable_data WHERE id = 2) = 'mutated two';
  END; $$;\0
EOF

psql pgmemento_restore postgres -c "$TEST_MUTATED";

echo "Validating restore query works";

read -r -d '\0' TEST_RESTORE <<'EOF'
  DO $$ BEGIN
    ASSERT (SELECT current_setting('pgmemento.restore_value')) = 'two';
  END; $$;\0
EOF

echo "$(cat /tmp/restore_data.sql); $TEST_RESTORE" | psql pgmemento_restore postgres -1

echo "SUCCESS";
