#!/bin/bash

set -e

cd $(dirname "$0");

PG_VERSION=$1
NO_BACKUP_RECOVERY=$2

TESTCMD="docker exec -it --env NO_BACKUP_RECOVERY=\$NO_BACKUP_RECOVERY \$CONTAINER bash -c 'cd /home/pgmemento/extension/pgxn/tests && /bin/bash install.sh && /bin/bash ../../tests/testrun.sh';"

docker build -t pgmemento-test--$PG_VERSION - < ../../tests/Dockerfile-$PG_VERSION;
CONTAINER=$(docker run --rm -d -v $(pwd)/../../..:/home/pgmemento pgmemento-test--$PG_VERSION);
sleep 5;
eval $TESTCMD
docker stop $CONTAINER
