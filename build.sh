#!/usr/bin/env bash

DIR="ha-coordinator"

function clean(){
	if [ -d $DIR ]; then
		rm -rf $DIR
	fi

	rm -rf *.pid

	if [ -d target ]; then
		rm -rf target
	fi
}

function timestamp() {
  date +"%Y%m%d_%H%M%S"
}

clean
mvn -U clean install -Dmaven.test.skip=true

DIR="ha-coordinator"

if [ -d target ]; then
	mkdir $DIR
	cp -r target/libs $DIR/
	cp target/*.jar $DIR/
	cp -r target/conf $DIR/ 
	cp bin/*.sh $DIR/

	tar zcvf $DIR."$(timestamp)".tar.gz $DIR/
	clean
fi
