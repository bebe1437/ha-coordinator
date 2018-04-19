#!/usr/bin/env bash

#set -xv
source config.sh

if [ ! -z "$1" ]; then
        echo "config:$1"
        config="$1"
fi

java -Dconfig.resource=$config \
-Dlogback.root.level=INFO \
-jar start.jar & echo $! >$pidfile.tmp
