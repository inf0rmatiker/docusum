#!/bin/bash

# Variables
hadoop="${HADOOP_HOME}/bin/hadoop"
hdfs="${HADOOP_HOME}/bin/hdfs"


if [[ $# -ge 2 ]]; then
  echo -e "Cleaning and building the runnable jar...\n"
  mvn clean && mvn package 

  echo -e "\nRemoving any intermediate output directories...\n"
  ${hdfs} dfs -rm -r -f /tempOut1 /tempOut2 $2  

  echo -e "\nRunning MapReduce jobs...\n"
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileA $1
  ${hadoop} jar target/docusum-1.0-SNAPSHOT.jar driver.ProfileB $1 $2
else
  echo "Usage: ./run.sh <input_path> <output_path>"
fi
