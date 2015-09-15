#!/bin/bash

source settings.sh
source $XL_HOME/user_setting.sh

echo "${green}Reset the Environment${reset}"
bash $XL_HOME/init.sh

echo "${green}Running Codes${reset}"
bash $XL_HOME/run.sh

echo "${green}Processing the Results${reset}"
python $XL_HOME/submit.py

echo "${green}Cleaning UP${reset}"
hadoop fs -rm -r -f $HDFS_HOME/
rm -rf $PREFIX
bash $XL_HOME/init.sh

echo "${green}Done${reset}"
