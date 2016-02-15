#!/bin/bash
sbt package && spark-submit --class "App" --master local[4] target/scala-2.11/spark-app_2.11-1.0.jar

