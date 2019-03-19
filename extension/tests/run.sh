#!/bin/bash

set -e

cd $(dirname "$0");

PG_VERSION=$1
NO_BACKUP_RECOVERY=$2

TESTCMD="docker exec -it --env NO_BACKUP_RECOVERY=\$NO_BACKUP_RECOVERY \$CONTAINER bash -c 'cd /home/pgmemento/extension/tests && /bin/bash install.sh && /bin/bash testrun.sh';"

/bin/bash ./container.sh $PG_VERSION $NO_BACKUP_RECOVERY $TESTCMD
