#!/bin/bash

PRG="$0"
HOME=`cd $(dirname "$PRG");pwd`
echo $HOME


if [ $# -eq 1 ] ; then
	time=$1
else
	echo "please input time...."
	exit 1
fi

echo $time

name="LogETL"

spark-submit \
--master yarn \
--name "$name-$time" \
--conf "spark.time=$time" \
--class com.ruozedata.LogETL.spark.LogETLApp \
/home/hadoop/job/emp/lib/LogETL-1.0.jar
