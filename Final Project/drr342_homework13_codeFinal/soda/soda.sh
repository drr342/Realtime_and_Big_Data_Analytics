#!/bin/bash

HDFSIN=$1
WAIT=$2
HDFSLIB=/user/drr342/lib

if [ -z "$HDFSIN" ]; then
	echo "Path to input directory in HDFS is required as first argument"
	exit 1
fi
if [ -z "$WAIT" ]; then
	WAIT=true
fi
hadoop fs -test -d $HDFSIN
if [ $? != 0 ]; then
    echo "Provide path to INPUT DIRECTORY, not file"
	exit 1
fi

HDFSOUT=$HDFSIN"WithCA"
hadoop fs -rm -r -f $HDFSOUT

find . -name '*.class' -exec rm {} \;
find . -maxdepth 1 -name '*.jar' -exec rm {} \;
javac -classpath `yarn classpath`:"./lib/*" -d . *.java
jar -cvf soda.jar *.class

MAIN=SodaDriver
hadoop jar soda.jar $MAIN $HDFSIN $HDFSOUT $HDFSLIB $WAIT
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Soda Job did not complete successfully..."
	hadoop fs -rm -r -f $HDFSOUT
    exit $retVal
fi
