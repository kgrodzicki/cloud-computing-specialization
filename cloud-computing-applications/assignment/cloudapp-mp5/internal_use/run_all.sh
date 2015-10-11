#!/bin/bash
bash start.sh

hadoop jar target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner ConnectedComponentsComputation -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat -vip /mp5/data/graph.data  -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /mp5/output/part-a -w 1 -ca giraph.SplitMasterWorker=false | tee  log-part-a.txt

# spark-submit --class NaiveBayesMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar /mp5/data/training.data /mp5/data/test.data /mp5/output/part-b/ | tee log-part-b.txt

spark-submit --class KMeansMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar /mp5/data/cars.data /mp5/output/part-b/ | tee log-part-b.txt

hadoop jar target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner ShortestPathsComputation -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat -vip /mp5/data/graph.data  -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /mp5/output/part-c -w 1 -ca giraph.SplitMasterWorker=false -ca SimpleShortestPathsVertex.sourceId=3 | tee log-part-c.txt

# spark-submit --class GaussianMixtureMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar 3 /mp5/data/cars.data | tee >  output-part-d.txt 2> log-part-d.txt

# spark-submit --class SVMMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar  /mp5/data/training.data /mp5/data/test.data /mp5/output/part-d/ | tee log-part-d.txt

spark-submit --class RandomForestMP target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar  /mp5/data/training.data /mp5/data/test.data /mp5/output/part-d/ | tee log-part-d.txt