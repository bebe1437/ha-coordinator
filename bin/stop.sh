#!/usr/bin/env bash

#set -xv

source config.sh

if [ -f $pidfile ]; then
	cat $pidfile | xargs kill -9
	rm $pidfile
else
	echo "ha.pid NOT FOUND"
fi


if [ -f $pidfile.tmp ]; then
	cat $pidfile.tmp | xargs kill -9
	rm $pidfile.tmp
fi


if [ -f $processPidfile ]; then
	pid=$(cat $processPidfile)
	ps hopid --ppid $pid | xargs kill -9
	kill -9 $pid
	rm $processPidfile
fi