#!/bin/bash
export HDFS_HOME=/mp2
export XL_HOME=./internal_use
export PREFIX=$XL_HOME/tmp
export DATA_HOME=$PREFIX/dataset
export LOG=$PREFIX/logs.txt
# export ERR=$PREFIX/errors.txt
export ERR=$LOG
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
export N=${3-10}
export DATASET=mini

export red=`tput setaf 1`
export green=`tput setaf 2`
export yellow=`tput setaf 3`
export reset=`tput sgr0`
