#!/bin/bash

set -e

cd $(dirname "$0");

PG_VERSION=$1

TESTCMD="docker exec -it --env \$CONTAINER bash -c 'cd /home/pgmemento/extension/pgxn/tests && /bin/bash install.sh && /bin/bash ../../tests/testrun.sh';"

/bin/bash ../../tests/container.sh "$PG_VERSION" "$TESTCMD"
