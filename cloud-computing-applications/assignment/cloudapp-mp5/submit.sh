#!/bin/bash

source settings.sh

echo "${green}Reset the Environment${reset}"
bash start.sh
rm -rf $PREFIX

echo "${green}Compile Codes${reset}"
mvn clean package

mkdir -p $PREFIX
bash $XL_HOME/run.sh

echo "${green}Processing the Results${reset}"
python $XL_HOME/submit.py

echo "${green}Housekeeping${reset}"
rm -rf $PREFIX
bash start.sh

echo "${green}Done${reset}"
