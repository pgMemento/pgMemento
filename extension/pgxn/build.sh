#!/bin/bash

set -e

cd $(dirname "$0")

echo -n "Copying the control file ... ";
VERSION=$(grep default_version ../pgmemento.control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")
EXTFOLDER="pgmemento-$VERSION/"
echo "done";


echo -n "Preparing the filesystem structure ... ";
mkdir -p $EXTFOLDER/sql ;
mkdir -p $EXTFOLDER/doc ;
mkdir -p $EXTFOLDER/doc/pgmemento-docs-$VERSION ;
mkdir -p $EXTFOLDER/test ;
echo "done";

echo -n "Compiling the extension SQL ... ";
../compile.sh > $EXTFOLDER/sql/pgmemento.sql ;
cp ../../UPGRADE_v07_to_v073.sql $EXTFOLDER/sql/pgmemento--0.7--$VERSION.sql ;
cp ../../UPGRADE_v07_to_v073.sql $EXTFOLDER/sql/pgmemento--0.7.1--$VERSION.sql ;
cp ../../UPGRADE_v07_to_v073.sql $EXTFOLDER/sql/pgmemento--0.7.2--$VERSION.sql ;
cp ../pgmemento.control $EXTFOLDER/. ;
cp Makefile $EXTFOLDER/. ;
cp META.json $EXTFOLDER/. ;
echo "done";

echo -n "Copying the documentation ... ";
cp ../../README.md $EXTFOLDER/doc/README.pgmemento ;
cp ../../LICENSE $EXTFOLDER/doc/LICENSE.pgmemento ;
cp -r ../../doc/. $EXTFOLDER/doc/pgmemento-docs-$VERSION ;
echo "done";

echo -n "Building the archive ... "
mkdir -p ./dist
zip -rm "dist/pgmemento-$VERSION.zip" $EXTFOLDER
echo "done";
