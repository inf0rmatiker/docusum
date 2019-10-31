#!/bin/bash

# Variables
hadoop="${HADOOP_HOME}/bin/hadoop"
hdfs="${HADOOP_HOME}/bin/hdfs"

mvn clean && mvn package
if [[ "$1" == "demo" ]]; then
  echo "Running demo..."
  #${hdfs} dfs -rm -r -f /cs435/PA2demo/tmp /cs435/PA2demo/tmp2 /cs435/PA2demo/output
  #${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileA /cs435/PA2demo/inputfiles/
  ${hdfs} dfs -rm -r -f /cs435/PA2demo/output
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileB /cs435/PA2demo/inputfiles/ /cs435/PA2demo/output
else
  echo "Running on 1 GB dataset"
  ${hdfs} dfs -rm -r -f /cs435/pa2-profileB-out /cs435/tmp /cs435/tmp2 /cs435/pa2-profileA-out /cs435/pa2-temp-out
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileA /cs435/PA2Dataset/
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileB /cs435/PA2Dataset/ /cs435/pa2-profileB-out
fi
