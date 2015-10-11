#!/usr/bin/env bash
hadoop jar target/mp5-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner ConnectedComponentsComputation -vif org.apache.giraph.io.formats.IntIntNullTextInputFormat -vip /mp5/data/graph.data  -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /mp5/output/part-a -w 1 -ca giraph.SplitMasterWorker=false
