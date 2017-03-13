#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

cd $bin/..
mvn clean install

if [ -z ${ZEPPELIN_HOME} ]; 
then 
	echo "ZEPPELIN_HOME is not set"
else 
	rm -rf $ZEPPELIN_HOME/interpreter/dashbase
	rm -rf $ZEPPELIN_HOME/local-repo/io/dashbase
	exec $ZEPPELIN_HOME/bin/install-interpreter.sh -n dashbase -t io.dashbase:dashbase-zeppelin:0.0.1-SNAPSHOT
fi
