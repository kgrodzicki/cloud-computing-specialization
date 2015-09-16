#!/bin/bash

source settings.sh

bash $XL_HOME/run.sh

echo "${green}Processing the Results${reset}"
python $XL_HOME/submit.py

echo "${green}Cleaning UP${reset}"
echo "disable 'powers';drop 'powers'" | hbase shell
rm -rf $PREFIX

echo "${green}Done${reset}"
