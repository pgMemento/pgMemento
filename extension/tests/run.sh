#!/bin/bash

set -e

cd $(dirname "$0");

docker build -t pgmemento-test--11 - < ./Dockerfile-11;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--11);
sleep 5;
docker exec -it $CONTAINER bash -c 'cd /home/pgmemento/extension/tests && /bin/bash testrun.sh';
docker stop $CONTAINER


docker build -t pgmemento-test--10 - < ./Dockerfile-10;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--10);
sleep 5;
docker exec -it $CONTAINER bash -c 'cd /home/pgmemento/extension/tests && /bin/bash testrun.sh';
docker stop $CONTAINER


docker build -t pgmemento-test--96 - < ./Dockerfile-96;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--96);
sleep 5;
docker exec -it $CONTAINER bash -c 'cd /home/pgmemento/extension/tests && /bin/bash testrun.sh';
docker stop $CONTAINER


docker build -t pgmemento-test--95 - < ./Dockerfile-95;
CONTAINER=$(docker run --rm -d -v $(pwd)/../..:/home/pgmemento pgmemento-test--95);
sleep 5;
docker exec -it $CONTAINER bash -c 'cd /home/pgmemento/extension/tests && /bin/bash testrun.sh';
docker stop $CONTAINER
