#!/usr/bin/env bash
hadoop jar target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner ShortestPathsComputation -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat -vip /mp5/data/graph.data  -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /mp5/output/part-c -w 1 -ca giraph.SplitMasterWorker=false -ca SimpleShortestPathsVertex.sourceId=3

