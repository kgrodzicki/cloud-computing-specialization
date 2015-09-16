#!/bin/bash
export XL_HOME=./internal_use
export CLASSPATH=`hbase classpath`
export PREFIX=$XL_HOME/tmp

export red=`tput setaf 1`
export green=`tput setaf 2`
export yellow=`tput setaf 3`
export reset=`tput sgr0`
