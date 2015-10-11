#!/bin/bash

echo "${green}Prepare the HDFS${reset}"
hadoop fs -rm -r -f $HDFS_HOME/
hadoop fs -mkdir -p $HDFS_HOME/data

hadoop fs -put $DATA_HOME/* $HDFS_HOME/data/

echo "${green}Done${reset}"