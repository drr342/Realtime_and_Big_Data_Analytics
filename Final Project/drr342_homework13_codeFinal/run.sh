#!/bin/bash

HDFSPATH=$1

if [ -z "$HDFSPATH" ]; then
	echo "Path to output directory in HDFS is required as first argument"
	exit 1
fi

hadoop fs -test -d $HDFSPATH
if [ $? != 0 ]; then
    hadoop fs -mkdir $HDFSPATH
    retVal=$?
    if [ $retVal -ne 0 ]; then
        exit $retVal
    fi
fi

echo "=================== PHASE 1: PREPROCESSING IN MAPREDUCE ==================="
./runMR.sh $HDFSPATH
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Map Reduce stage did not complete successfully..."
    exit $retVal
fi

echo "=================== PHASE 2: ANALYTICS IN IMPALA ==================="
./runAnalytic.sh $HDFSPATH
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Impala stage did not complete successfully..."
    exit $retVal
fi
