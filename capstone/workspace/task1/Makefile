transform-1-1-1:
	mkdir -p /tmp/input-1-1-1
	python filter_csv.py /mnt/transportation/aviation/airline_origin_destination/ /tmp/input-1-1-1 Origin Dest

transform-1-1-2:
	mkdir -p /tmp/input-1-2
	python filter_csv.py /mnt/transportation/aviation/airline_ontime/ /tmp/input-1-2 AirlineID ArrDelay Cancelled
	hdfs dfs -mkdir /input-1-2
	hdfs dfs -copyFromLocal /tmp/input-1-2 /
run-1-1-2:
	hadoop jar t112.jar AirlinePerformance /input-1-2 /output-1-2

transform-1-2-1:
	mkdir -p /tmp/input-1-2-1
	python filter_csv.py /mnt/transportation/aviation/airline_ontime/ /tmp/input-2-1 Origin UniqueCarrier DepDelayMinutes CarrierDelay Cancelled
	hdfs dfs -mkdir /input-1-2-1
	hdfs dfs -copyFromLocal /tmp/input-1-2-1/ /input-1-2-1
run-1-2-1:
	hadoop jar t121.jar CarrierPerformance /input-1-2-1 /

transform-1-2-2:
	mkdir -p /tmp/input-1-2-2
	python filter_csv.py /mnt/transportation/aviation/airline_ontime/ /tmp/input-1-2-2 Origin DepDelayMinutes Dest
	hdfs dfs -mkdir /input-1-2-2
	hdfs dfs -copyFromLocal /tmp/input-1-2-2/ /
run-1-2-2:
	hadoop jar t122.jar DestPerformance /input-1-2-2 /output-1-2-2

transform-1-2-4:
	mkdir -p /tmp/input-1-2-4
	python filter_csv.py /mnt/transportation/aviation/airline_ontime/ /tmp/input-1-2-4 Origin Dest ArrDelay
	hdfs dfs -mkdir /input-1-2-4
	hdfs dfs -copyFromLocal /tmp/input-1-2-4/ /
run-1-2-4:
	hadoop jar t1244.jar OriginDestMeanApp /input-1-2-4

transform-1-3-1:
	echo "reusing same input as 1-1-1"
run-1-3-1:
	hadoop jar t131.jar TopPopularAirportAll /input-1-1-1

transform-1-3-2:
	mkdir -p /tmp/input-1-3-2
	python filter_csv.py /mnt/transportation/aviation/airline_ontime/ /tmp/input-1-3-2 Origin Dest FlightDate FlightNum DepTime ArrDelay Cancelled
	hdfs dfs -mkdir /input-1-3-2
	hdfs dfs -copyFromLocal /tmp/input-1-3-2 /
run-1-3-2:
	hadoop jar t132.jar MergeFlights /input-1-3-2 /output-1-3-2
