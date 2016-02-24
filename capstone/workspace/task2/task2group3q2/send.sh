#!/bin/bash
KAFKA=52.73.246.241:9092
cat src/main/resources/input.txt | kafka-console-producer.sh --broker-list $KAFKA --topic Topic-2-3-2
