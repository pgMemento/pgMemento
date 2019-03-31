#!/bin/bash

set -e

psql postgres postgres -c 'drop database if exists pgmemento_test;'
psql postgres postgres -c 'create database pgmemento_test;'

cd /home/pgmemento;

echo "Running extension tests...";
psql pgmemento_test postgres -f /home/pgmemento/extension/tests/TEST.sql

### Test we have backups covered

if [[ "$NO_BACKUP_RECOVERY" != "" ]] ; then
  exit
fi

echo "Running backup tests...";
psql postgres postgres -c 'create database pgmemento_backup';

echo "Creating initial data for backup";
cat <<'EOF' | psql pgmemento_backup postgres
  create extension pgmemento;
  select pgmemento.create_schema_event_trigger(true);
  select pgmemento.create_schema_audit('public', true);

  create table valuable_data (id int, value varchar);
  insert into valuable_data (id, value) values (1, 'one'), (2, 'two'), (3, 'three');
EOF


echo "Taking the data history point snapshot";
cat <<'EOF' | psql pgmemento_backup postgres -qAt > /tmp/revert_data.sql
  select format(
    'select pgmemento.revert_transaction(%1$s)',
    (select max(id) from pgmemento.transaction_log)
  );
EOF


echo "Mutating the data after the snapshot";
psql pgmemento_backup postgres -c "update valuable_data set value = 'mutated two' where id = 2"

echo "Taking the mutated data backup";
pg_dump -U postgres -Oxo pgmemento_backup > /tmp/backup.sql;

echo "Drop the original database";
psql postgres postgres -c 'drop database pgmemento_backup;'

echo "Generating new database";

psql postgres postgres -c 'drop database if exists pgmemento_restore;'
psql postgres postgres -c 'create database pgmemento_restore;'

echo "Restoring the data from backup";
psql pgmemento_restore postgres < /tmp/backup.sql

echo "Validating we have mutated data restored";

read -r -d '\0' TEST_MUTATED <<'EOF'
  do $$ begin
    assert (select value from valuable_data where id=2) = 'mutated two';
  end; $$;\0
EOF

psql pgmemento_restore postgres -c "$TEST_MUTATED";

echo "Trying to revert the mutated data";
psql pgmemento_restore postgres < /tmp/revert_data.sql

read -r -d '\0' TEST_REVERTED <<'EOF'
  do $$ begin
    assert (select value from valuable_data where id=2) = 'two';
  end; $$;\0
EOF

psql pgmemento_restore postgres -c "$TEST_REVERTED"

echo "SUCCESS";
