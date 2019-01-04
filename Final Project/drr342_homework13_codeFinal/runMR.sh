#!/bin/bash

HDFSIN=/user/drr342/project/chicago
HDFSOUT=$1

if [ -z "$HDFSOUT" ]; then
	echo "Path to output directory in HDFS is required as first argument"
	exit 1
fi

hadoop fs -test -d $HDFSOUT
if [ $? != 0 ]; then
    hadoop fs -mkdir $HDFSOUT
    retVal=$?
    if [ $retVal -ne 0 ]; then
        exit $retVal
    fi
fi

cd business
find . -name '*.class' -exec rm {} \;
find . -name '*.jar' -exec rm {} \;
javac -classpath `yarn classpath` -d . *.java
jar -cvf business.jar *.class

MAIN=BusinessDriver
IN=$HDFSIN/businessWithCA
OUT=$HDFSOUT/business
hadoop fs -rm -r -f $OUT
hadoop jar business.jar $MAIN $IN $OUT
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Business Job did not complete successfully..."
	hadoop fs -rm -r -f $OUT
    exit $retVal
fi

cd ../crime
find . -name '*.class' -exec rm {} \;
find . -name '*.jar' -exec rm {} \;
javac -classpath `yarn classpath` -d . *.java
jar -cvf crime.jar *.class

MAIN=CrimeDriver
IN=$HDFSIN/crime
OUT=$HDFSOUT/crime
hadoop fs -rm -r -f $OUT
TYPES=$HDFSIN/types.csv
hadoop jar crime.jar $MAIN $IN $OUT $TYPES
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Crime Job did not complete successfully..."
	hadoop fs -rm -r -f $OUT
    exit $retVal
fi

cd ../demographics
find . -name '*.class' -exec rm {} \;
find . -name '*.jar' -exec rm {} \;
javac -classpath `yarn classpath` -d . *.java
jar -cvf demographics.jar *.class

MAIN=DemographicsDriver
IN=$HDFSIN/demographics
OUT=$HDFSOUT/demographics
hadoop fs -rm -r -f $OUT
MAP=$HDFSIN/tract2ca.csv
hadoop jar demographics.jar $MAIN $IN $OUT $MAP
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Demographics Job did not complete successfully..."
	hadoop fs -rm -r -f $OUT
    exit $retVal
fi

cd ../traffic
find . -name '*.class' -exec rm {} \;
find . -name '*.jar' -exec rm {} \;
javac -classpath `yarn classpath` -d . *.java
jar -cvf traffic.jar *.class

MAIN=TrafficDriver
IN=$HDFSIN/traffic
OUT=$HDFSOUT/traffic
hadoop fs -rm -r -f $OUT
MAP=$HDFSIN/traffic2ca.csv
hadoop jar traffic.jar $MAIN $IN $OUT $MAP
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Traffic Job did not complete successfully..."
	hadoop fs -rm -r -f $OUT
    exit $retVal
fi

cd ../train
find . -name '*.class' -exec rm {} \;
find . -name '*.jar' -exec rm {} \;
javac -classpath `yarn classpath` -d . *.java
jar -cvf train.jar *.class

MAIN=TrainDriver
IN=$HDFSIN/train
OUT=$HDFSOUT/train
hadoop fs -rm -r -f $OUT
MAP=$HDFSIN/train2ca.csv
hadoop jar train.jar $MAIN $IN $OUT $MAP
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Trains Job did not complete successfully..."
	hadoop fs -rm -r -f $OUT
    exit $retVal
fi

echo "Preprocessing jobs completed successfully!"
echo "Results in HDFS:"
hadoop fs -du -h $HDFSOUT
