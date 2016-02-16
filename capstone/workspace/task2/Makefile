cp-2-1-1:
	aws s3 cp task2group1q1/target/scala-2.10/task2group1q1-assembly-1.0.jar s3://capstone-jars/
cp-2-1-2:
	aws s3 cp task2group1q2/target/scala-2.10/task2group1q2-assembly-1.0.jar s3://capstone-jars/
cp-2-2-1:
	aws s3 cp task2group2q1/target/scala-2.10/task2group2q1-assembly-1.0.jar s3://capstone-jars/
cp-2-2-2:
	aws s3 cp task2group2q2/target/scala-2.10/task2group2q2-assembly-1.0.jar s3://capstone-jars/
cp-2-2-4:
	aws s3 cp task2group2q4/target/scala-2.10/task2group1q2-assembly-1.0.jar s3://capstone-jars/
cp-2-3-2:
	aws s3 cp task2group3q2/target/scala-2.10/task2group1q4-assembly-1.0.jar s3://capstone-jars/

run-cassandra:
	docker run --name cassandra -p 9042:9042 -d cassandra

cqlsh:
	docker run -it --link cassandra:cassandra --rm cassandra cqlsh cassandra
