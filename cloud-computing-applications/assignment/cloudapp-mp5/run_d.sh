#!/usr/bin/env bash
spark-submit --class RandomForestMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar  /mp5/data/training.data /mp5/data/test.data /mp5/output/part-d/