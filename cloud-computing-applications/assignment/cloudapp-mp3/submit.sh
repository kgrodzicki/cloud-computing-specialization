#!/bin/bash

source settings.sh

echo "${green}Processing the Results${reset}"
python $XL_HOME/submit.py

echo "${green}Done${reset}"
