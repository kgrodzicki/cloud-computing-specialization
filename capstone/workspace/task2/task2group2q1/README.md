# Rank the top 10 airlines by on-time arrival performance.

# Use spark-submit to run your application
$ ./run.sh 192.168.99.101:9092 Topic-2-2-1 192.168.99.101:9042

# Send data to Kafka broker
$ ./send 192.168.99.101:9092 Topic-2-2-1

# Set up cassandra
CREATE KEYSPACE capstone
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
  
CREATE TABLE capstone.airport (
  code text PRIMARY KEY,
  top_carriers list<text>
);


@kgrodzicki
