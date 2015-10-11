#!/bin/bash
export HDFS_HOME=/mp5
export XL_HOME=./internal_use
export PREFIX=$XL_HOME/tmp
export DATA_HOME=$XL_HOME/dataset
export LOG=$PREFIX/logs.txt
# export ERR=$PREFIX/errors.txt
export ERR=$LOG
# export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

export red=`tput setaf 1`
export green=`tput setaf 2`
export yellow=`tput setaf 3`
export reset=`tput sgr0`
