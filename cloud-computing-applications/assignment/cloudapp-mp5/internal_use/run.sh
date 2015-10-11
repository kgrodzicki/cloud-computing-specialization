#!/bin/bash

run_assignment() {
	echo "${yellow}	Run${reset}"
	$2 2>&1 | tee $PREFIX/$1.log

	echo "${yellow}	Collect the Output${reset}"
	hadoop fs -cat $HDFS_HOME/output/part-$1/* 2>&1 | tee $PREFIX/$1.output
}

echo "${green}Running Part A${reset}"
run_assignment a "hadoop jar target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner ConnectedComponentsComputation -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat -vip /mp5/data/graph.data  -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /mp5/output/part-a -w 1 -ca giraph.SplitMasterWorker=false"

echo "${green}Running Part B${reset}"
run_assignment b "spark-submit --class KMeansMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar /mp5/data/cars.data /mp5/output/part-b/"

echo "${green}Running Part C${reset}"
run_assignment c "hadoop jar target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner ShortestPathsComputation -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat -vip /mp5/data/graph.data  -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /mp5/output/part-c -w 1 -ca giraph.SplitMasterWorker=false -ca SimpleShortestPathsVertex.sourceId=3"

echo "${green}Running Part D${reset}"
run_assignment d "spark-submit --class RandomForestMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar  /mp5/data/training.data /mp5/data/test.data /mp5/output/part-d/"