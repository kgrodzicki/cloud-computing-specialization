#!/bin/bash
cat src/main/resources/input.txt | kafka-console-producer.sh --broker-list $1 --topic $2
