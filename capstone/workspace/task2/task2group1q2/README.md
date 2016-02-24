# Rank the top 10 airlines by on-time arrival performance.

# Use spark-submit to run your application
$ ./run.sh 192.168.99.101:9092 Topic-2-1-2 1

# Send data to Kafka broker
$ ./send.sh 192.168.99.101:9092 Topic-2-1-2

# Aws
aws emr create-cluster --applications Name=Ganglia Name=Spark --ec2-attributes '{"KeyName":"hadoop-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-1df35e6b","EmrManagedSlaveSecurityGroup":"sg-a9db23d1","EmrManagedMasterSecurityGroup":"sg-b7db23cf"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://capstone-cloud-computing/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","App","s3://capstone-jars/task2group1q2-assembly-1.0.jar","52.73.246.241:9092","Topic-2-1-2","5"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"task2-group1q2"}]' --name 'spark-cluster-task2-group1q2' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"},{"InstanceCount":1,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --region us-east-1 --auto-terminate

## Send data to kafka cluster
export KAFKA=52.90.32.107:9092
cat ~/input/input-1-1-2/* | ./kafka-console-producer.sh --broker-list $KAFKA --topic Topic-2-1-2

## Spark Arguments:
--class "App"

## App arguments:
52.73.246.241:9092 Topic-2-1-2 5

## Kafka Producer Message/Sec
624306.67
542853.33
247048.23
	
## Result 
-------------------------------------------
Time: 1455925080000 ms
-------------------------------------------

-------------------------------------------
Time: 1455925140000 ms
-------------------------------------------
(19805,5.06123E7)
(19790,4.33674E7)
(19977,3.96589E7)
(19704,3.22615E7)
(20355,3.19251E7)
(19386,2.62656E7)
(19707,2.48697E7)
(19393,2.12692E7)
(19822,1.87864E7)
(20211,1.64429E7)
...

-------------------------------------------
Time: 1455925200000 ms
-------------------------------------------
(20355,2.901567E8)
(19805,2.874103E8)
(19790,2.630868E8)
(19977,2.398754E8)
(19386,2.003243E8)
(19393,1.882496E8)
(19704,1.696555E8)
(20211,9.75475E7)
(19991,6.80002E7)
(19707,4.12897E7)
...

-------------------------------------------
Time: 1455925260000 ms
-------------------------------------------
(19805,6.163321E8)
(19393,6.155836E8)
(19790,6.062101E8)
(20355,5.693363E8)
(19977,5.335941E8)
(19386,4.478106E8)
(19704,3.401208E8)
(20211,1.810726E8)
(19991,1.438966E8)
(20398,9.40128E7)
...

-------------------------------------------
Time: 1455925320000 ms
-------------------------------------------
(19805,6.163321E8)
(19393,6.155836E8)
(19790,6.062101E8)
(20355,5.693363E8)
(19977,5.335941E8)
(19386,4.478106E8)
(19704,3.401208E8)
(20211,1.810726E8)
(19991,1.438966E8)
(20398,9.40128E7)
...

-------------------------------------------
Time: 1455925380000 ms
-------------------------------------------
(19805,6.163321E8)
(19393,6.155836E8)
(19790,6.062101E8)
(20355,5.693363E8)
(19977,5.335941E8)
(19386,4.478106E8)
(19704,3.401208E8)
(20211,1.810726E8)
(19991,1.438966E8)
(20398,9.40128E7)
...

-------------------------------------------
Time: 1455925440000 ms
-------------------------------------------
(19805,6.163321E8)
(19393,6.155836E8)
(19790,6.062101E8)
(20355,5.693363E8)
(19977,5.335941E8)
(19386,4.478106E8)
(19704,3.401208E8)
(20211,1.810726E8)
(19991,1.438966E8)
(20398,9.40128E7)
...

https://console.aws.amazon.com/s3/home?region=us-east-1&bucket=capstone-cloud-computing&prefix=j-286GF9X96003G/containers/application_1455924917176_0001/container_1455924917176_0001_01_000001/


@kgrodzicki
