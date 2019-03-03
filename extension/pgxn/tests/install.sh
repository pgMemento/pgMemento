#!/bin/bash

set -e

cd /home/pgmemento/extension/pgxn

rm -f ./dist/pgmemento-*.zip

./build.sh

unzip ./dist/pgmemento-*.zip -d /tmp/pgmemento-extension

cd /tmp/pgmemento-extension/*

make && make install

cd /home/pgmemento;

psql postgres postgres -c 'create database pgmemento_test;'

echo "Running extension tests...";
psql pgmemento_test postgres -f /home/pgmemento/extension/tests/TEST.sql
