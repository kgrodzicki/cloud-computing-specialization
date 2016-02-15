#!/bin/bash
sbt package && spark-submit --class "App" --master local[4] target/scala-2.10/spark-app_2.10-1.0.jar
