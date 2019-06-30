#!/bin/bash

set -e

cd $(dirname "$0");

PG_VERSION=$1

TESTCMD="docker exec -it \$CONTAINER bash -c 'cd /home/pgmemento/extension/tests && /bin/bash install.sh && /bin/bash testrun.sh';"

/bin/bash ./container.sh "$PG_VERSION" "$TESTCMD"
