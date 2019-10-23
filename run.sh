#!/bin/bash

# Variables
hadoop="${HADOOP_HOME}/bin/hadoop"
hdfs="${HADOOP_HOME}/bin/hdfs"

if [[ "$1" == "test" ]]; then
  mvn clean && mvn package
  ${hdfs} dfs -rm -r -f /cs435/testout
  ${hdfs} dfs -rm -r -f /cs435/tmp
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.JobDriver /cs435/testfiles/ /cs435/testout
else
  echo "use test"
fi
