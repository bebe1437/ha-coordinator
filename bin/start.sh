#!/usr/bin/env bash

#set -xv
source config.sh

if [ ! -d "log" ]; then
	mkdir log
fi

./ha-start.sh >> log/nohup.log 2>&1 &

sleep 3
pid=$(cat $pidfile.tmp)

running=$(ps -o pid= -p $pid | wc -l)

if [ $running -gt 0 ]; then
        echo "ha is running..."
	cat $pidfile.tmp > $pidfile
	rm $pidfile.tmp
else
        echo "Fail to start ha."
	rm $pidfile.tmp
fi 