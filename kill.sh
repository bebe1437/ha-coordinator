#!/usr/bin/env bash

pn=$(ps hopid --ppid $1 | wc -l)

if [ "$pn" -gt 0 ]; then
	ps hopid --ppid $1 | xargs kill -9
fi