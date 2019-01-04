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

# hadoop fs -rm -r -f $HDFSPATH/joins
hadoop fs -rm -r -f $HDFSPATH/correlations
hadoop fs -mkdir $HDFSPATH/correlations
hdfs dfs -setfacl -R -m user:impala:rwx $HDFSPATH

echo "Creating required tables..."
cd analytic
impala-shell -i compute-3-7 --quiet --var=basePath=$HDFSPATH --var=user=$USER -f createTables.sql
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Required tables could not be created..."
    exit $retVal
fi

echo "Required tables created successfully!"
echo "Populating correlation tables..."

impala-shell -i compute-3-7 -B --quiet \
             -q 'use drr342; describe chicago_business; describe chicago_demo; describe chicago_traffic; describe chicago_train; describe chicago_crime;' \
             -o temp --output_delimiter=','
find . -name '*.class' -exec rm {} \;
javac -d . GenerateQuery.java
java GenerateQuery
impala-shell -i compute-3-7 --quiet --var=user=$USER -f correlation.sql
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Correlation tables could not be created..."
    exit $retVal
fi

echo "Correlation tables created successfully!"
echo "Results in HDFS:"
hadoop fs -du -h $HDFSPATH/correlations

echo "Generating Final Correlation Table..."
mkdir temp
total=$(hadoop fs -ls -C $HDFSPATH/correlations | wc -l)
for i in $(seq 1 $total); do
	f=$(hadoop fs -ls -C $HDFSPATH/correlations | sed -n $i'p')
	hadoop fs -getmerge $f ./temp/$i'.csv'
done;
find . -name '*.class' -exec rm {} \;
javac -d . SortCorrelations.java
java SortCorrelations
rm -rf temp
