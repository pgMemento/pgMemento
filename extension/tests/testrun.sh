#!/bin/bash

set -e

if [[ "$PG_EXTENSION_DIR" == "" ]]; then
    PG_EXTENSION_DIR=$(psql postgres postgres -t -P format=unaligned -c "select setting || '/extension/' from pg_config where name = 'SHAREDIR';");
fi

EXTVERSION=$(grep default_version /home/pgmemento/extension/pgmemento.control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

cp /home/pgmemento/extension/pgmemento.control $PG_EXTENSION_DIR/.;
cd /home/pgmemento/extension && ./compile.sh > $PG_EXTENSION_DIR/pgmemento--$EXTVERSION.sql


psql postgres postgres -c 'drop database if exists pgmemento_test;'
psql postgres postgres -c 'create database pgmemento_test;'

cd /home/pgmemento;

echo "Running extension tests...";
psql pgmemento_test postgres -f /home/pgmemento/extension/tests/TEST.sql


### Test we have backups covered

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
cat <<'EOF' | psql pgmemento_backup postgres -qAt > /tmp/restore_data.sql
  create function
    generate_restore_query()
    returns varchar
  as $$
  declare
    txid_min_ int;
    txid_max_ int;
    audit_id_ int;
    result_ varchar;

  begin
    select audit_id into audit_id_ from valuable_data where id = 2;

    select
      txid_min,
      txid_max
    into
      txid_min_,
      txid_max_
    from
      pgmemento.audit_tables
    where
      schemaname like 'public'
    and
      tablename like 'valuable_data';

    select
      format('select * from pgmemento.restore_record(%1$s, %2$s, ''valuable_data'', ''public'', %3$s) ', txid_min_, txid_max_, audit_id_)
      || (select pgmemento.restore_record_definition(txid_max_, 'valuable_data', 'public')) into result_;

    return result_;
  end; $$ language plpgsql;

  select generate_restore_query();
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

echo "Trying to revert the mutated data back to the snapshot";
psql pgmemento_restore postgres < /tmp/restore_data.sql

read -r -d '\0' TEST_RESTORED <<'EOF'
  do $$ begin
    assert (select value from valuable_data where id=2) = 'two';
  end; $$;\0
EOF

psql pgmemento_restore postgres -c "$TEST_RESTORED"

echo "SUCCESS";
