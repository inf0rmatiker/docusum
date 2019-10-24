#!/bin/bash

# Variables
hadoop="${HADOOP_HOME}/bin/hadoop"
hdfs="${HADOOP_HOME}/bin/hdfs"

mvn clean && mvn package
if [[ "$1" == "test" ]]; then
  ${hdfs} dfs -rm -r -f /cs435/testout
  ${hdfs} dfs -rm -r -f /cs435/tmp
  ${hdfs} dfs -rm -r -f /cs435/tmp2
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileA /cs435/testfiles/
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileB /cs435/testfiles /cs435/testout 
else
  ${hdfs} dfs -rm -r -f /cs435/pa2-profileB-out /cs435/tmp /cs435/tmp2 /cs435/pa2-profileA-out /cs435/pa2-temp-out
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileA /cs435/PA2Dataset/
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileB /cs435/PA2Dataset/ /cs435/pa2-profileB-out   
fi
