# For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X

# Use spark-submit to run your application
$ ./run.sh 192.168.99.101:9092 Topic-2-2-4 192.168.99.101:9042

# Send data to Kafka broker
$ ./send.sh 192.168.99.101:9092 Topic-2-2-4

# Aws
aws emr create-cluster --applications Name=Ganglia Name=Spark --ec2-attributes '{"KeyName":"hadoop-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-1df35e6b","EmrManagedSlaveSecurityGroup":"sg-a9db23d1","EmrManagedMasterSecurityGroup":"sg-b7db23cf"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://capstone-cloud-computing/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","App","s3://capstone-jars/task2group2q4-assembly-1.0.jar","52.73.246.241:9092","Topic-2-2-4","52.73.246.241:9042","3"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"task2-group2q4"}]' --name 'spark-cluster-task2-group2q4' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.2xlarge","Name":"Master Instance Group"},{"InstanceCount":3,"InstanceGroupType":"CORE","InstanceType":"m3.2xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --region us-east-1 --auto-terminate

## Send data to kafka cluster
export KAFKA=52.73.246.241:9092
cat ~/input/input-1-2-4/* | ./kafka-console-producer.sh --broker-list $KAFKA --topic Topic-2-2-4

## Kafka Producer Message/Sec
552126.67
558166.67
546893.33

## Result
### Query
select * from capstone.meanarrdelay where origin='LGA' and dest='BOS';
select * from capstone.meanarrdelay where origin='BOS' and dest='LGA';
select * from capstone.meanarrdelay where origin='OKC' and dest='DFW';
select * from capstone.meanarrdelay where origin='MSP' and dest='ATL';

cassandra@cqlsh:capstone> select * from capstone.meanarrdelay where origin='LGA' and dest='BOS';

 origin | dest | mean
--------+------+------
    LGA |  BOS |    1

(1 rows)
cassandra@cqlsh:capstone> select * from capstone.meanarrdelay where origin='BOS' and dest='LGA';

 origin | dest | mean
--------+------+------
    BOS |  LGA |    3

(1 rows)
cassandra@cqlsh:capstone> select * from capstone.meanarrdelay where origin='OKC' and dest='DFW';

 origin | dest | mean
--------+------+------
    OKC |  DFW |    5

(1 rows)
cassandra@cqlsh:capstone> select * from capstone.meanarrdelay where origin='MSP' and dest='ATL';

 origin | dest | mean
--------+------+------
    MSP |  ATL |    7

(1 rows)


@kgrodzicki
