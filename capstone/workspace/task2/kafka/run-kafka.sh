#!/bin/bash

mkdir -p kafka-ex/{data,logs}
LPWD=${PWD}
docker run -d \
    --hostname localhost \
    --name kafka \
    --publish 9092:9092 --publish 7203:7203 \
    --env KAFKA_ADVERTISED_HOST_NAME=$1 \
    --env ZOOKEEPER_CONNECTION_STRING=$2:2181 \
    --env KAFKA_BROKER_ID=0 \
    ches/kafka
