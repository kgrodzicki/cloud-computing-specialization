send-2-1-1:
	cat $1 | bash kafka-console-producer.sh --broker-list $2 --topic Topic-2-1-1
run-2-1-1:
	spark/bin/spark-submit --class "App" --master spark://ec2-52-90-76-138.compute-1.amazonaws.com:7077 /root/app/task2group1q1-assembly-1.0.jar 54.88.106.120:9092 Topic-2-1-1

send-2-1-2:
	cat $1 | bash kafka-console-producer.sh --broker-list $2 --topic Topic-2-1-2
run-2-1-2:
	spark-submit --class "App" --master local[4] task2group1q2-assembly-1.0.jar 54.88.106.120:9092 Topic-2-1-2
