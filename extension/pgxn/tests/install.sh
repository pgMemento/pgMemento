#!/bin/bash

set -e

# Install PGXN client
pip install pgxnclient


# build the extension archive
cd /home/pgmemento/extension/pgxn
rm -f ./dist/pgmemento-*.zip
./build.sh

# installing the extension from the local archive
pgxn install --verbose --yes ./dist/*.zip

# # Use these to install with PGXS (cmake) instead of PGXN
# unzip ./dist/pgmemento-*.zip -d /tmp/pgmemento-extension
# cd /tmp/pgmemento-extension/*
# make && make install

# Testing stuff
cd /home/pgmemento;

psql postgres postgres -c 'create database pgmemento_test;'

echo "Running extension tests...";
psql pgmemento_test postgres -f /home/pgmemento/extension/tests/TEST.sql
