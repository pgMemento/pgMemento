#!/bin/bash

set -e

cd $(dirname "$0");

NO_BACKUP_RECOVERY=$1

TESTCMD="docker exec -it --env NO_BACKUP_RECOVERY=\$NO_BACKUP_RECOVERY \$CONTAINER bash -c 'cd /home/pgmemento/extension/tests && /bin/bash install.sh && /bin/bash testrun.sh';"

docker build -t pgmemento-test--11 - < ./Dockerfile-11;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--11);
sleep 5;
eval $TESTCMD
docker stop $CONTAINER


docker build -t pgmemento-test--10 - < ./Dockerfile-10;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--10);
sleep 5;
eval $TESTCMD
docker stop $CONTAINER


docker build -t pgmemento-test--96 - < ./Dockerfile-96;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--96);
sleep 5;
eval $TESTCMD
docker stop $CONTAINER


docker build -t pgmemento-test--95 - < ./Dockerfile-95;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--95);
sleep 5;
eval $TESTCMD
docker stop $CONTAINER
