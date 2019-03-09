#!/bin/bash

set -e

if [[ "$PG_EXTENSION_DIR" == "" ]]; then
    PG_EXTENSION_DIR=$(psql postgres postgres -t -P format=unaligned -c "select setting || '/extension/' from pg_config where name = 'SHAREDIR';");
fi

EXTVERSION=$(grep default_version /home/pgmemento/extension/pgmemento.control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

cp /home/pgmemento/extension/pgmemento.control $PG_EXTENSION_DIR/.;
cd /home/pgmemento/extension && ./compile.sh > $PG_EXTENSION_DIR/pgmemento--$EXTVERSION.sql
