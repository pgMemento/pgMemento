#!/bin/bash

cd $(dirname "$0");

cat "../src/SCHEMA.sql" | grep -v 'DROP SCHEMA' | grep -v 'CREATE SCHEMA'

scripts=("SETUP.sql" "LOG_UTIL.sql" "DDL_LOG.sql" "RESTORE.sql" "REVERT.sql" "SCHEMA_MANAGEMENT.sql" "CTL.sql");

for script in ${scripts[@]}; do
    cat "../src/$script";
    echo -e "\n\n\n";
done

echo "-- make all the data available for pg_dump";
read -r -d '' CONFIG_DUMP <<'EOF'
do language plpgsql
$$
declare
  name_ varchar;
begin
  for name_ in select sequence_schema || '.' || sequence_name from information_schema.sequences where sequence_schema = 'pgmemento' loop
    perform pg_catalog.pg_extension_config_dump(name_, '');
  end loop;

  for name_ in select table_schema || '.' || table_name from information_schema.tables where table_schema = 'pgmemento' loop
    perform pg_catalog.pg_extension_config_dump(name_, '');
  end loop;
end
$$;
EOF
echo "$CONFIG_DUMP";
