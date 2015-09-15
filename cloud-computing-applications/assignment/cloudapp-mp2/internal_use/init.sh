#!/bin/bash

echo "${green}Prepare the Environment${reset}"
rm -rf $PREFIX
mkdir -p $PREFIX
bash $XL_HOME/install.sh


echo "${green}Prepare the HDFS${reset}"
hadoop fs -rm -r -f $HDFS_HOME/
hadoop fs -mkdir -p $HDFS_HOME/links $HDFS_HOME/titles/ $HDFS_HOME/misc

hadoop fs -put $DATA_HOME/links/* $HDFS_HOME/links/
hadoop fs -put $DATA_HOME/titles/* $HDFS_HOME/titles/
hadoop fs -put $DATA_HOME/misc/* $HDFS_HOME/misc/

echo "${green}Done${reset}"