transform-1-1:
	mkdir -p /tmp/input-1-1
	python filter_csv.py /mnt/transportation/aviation/airline_origin_destination/ /tmp/input-1-1 Origin Dest
	hdfs dfs -mkdir /input-1-1
	hdfs dfs -copyFromLocal /tmp/input-1-1/ /input-1-1
run-1-1:
	hadoop jar t111.jar TopPopularAirport /input-1-1 /output-1-1

transform-3-2:
	mkdir -p /mnt/transportationtransformed/input-3-2
	python filter_csv.py /mnt/transportation/aviation/airline_ontime/ /mnt/transportationtransformed/input-3-2 Origin Dest FlightDate FlightNum DepTime ArrDelay Cancelled
	hdfs dfs -mkdir /input-3-2
	hdfs dfs -copyFromLocal /mnt/transportationtransformed/input-3-2/ /input-3-2
run-3-2:
	hadoop jar task1group3q21hadoop-1.0-SNAPSHOT.jar MergeFlights /input-3-2 /output-3-2
