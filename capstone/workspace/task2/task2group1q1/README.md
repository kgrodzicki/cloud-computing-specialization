# Rank the top 10 most popular airports by numbers of flights to/from the airport.

# Use spark-submit to run your application
$ ./run.sh 192.168.99.101:9092 Topic-2-1-1

# Send data to Kafka broker
$ ./send.sh 192.168.99.101:9092 Topic-2-1-1 1
$ ./send.sh 52.90.32.107:9092 Topic-2-1-1


# Aws
aws emr create-cluster --applications Name=Ganglia Name=Spark --ec2-attributes '{"KeyName":"hadoop-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-351a8c08","EmrManagedSlaveSecurityGroup":"sg-a9db23d1","EmrManagedMasterSecurityGroup":"sg-b7db23cf"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://capstone-cloud-computing/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","App","s3://capstone-jars/task2group1q1-assembly-1.0.jar","54.174.3.86:9092","Topic-2-1-1","15"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"task2group1q1"}]' --name 'spark-cluster-task2group1q1' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.2xlarge","Name":"Master Instance Group"},{"InstanceCount":1,"InstanceGroupType":"CORE","InstanceType":"m3.2xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --region us-east-1 --auto-terminate

## Send data to kafka cluster
export KAFKA=52.90.32.107:9092
cat ~/input/input-1-1-1/* | ./kafka-console-producer.sh --broker-list $KAFKA --topic Topic-2-1-1

## Spark Arguments:
--class "App"

## App arguments:
54.174.3.86:9092 Topic-2-1-1

## Kafka Producer Message/Sec
133906.67 600386.67
	
## Result 
16/02/17 20:14:30 INFO App$: Top 10 most popular airports by numbers of flights to/from the airport.: (ORD,16633518),(ATL,16013716),(DFW,15086365),(DEN,9519605),(LAX,7787836),(CLT,7314252),(PIT,6858597),(MSP,6756307),(SFO,6743748),(STL,6585285)

https://console.aws.amazon.com/s3/home?region=us-east-1&bucket=capstone-cloud-computing&prefix=j-G23V63QJZQOI/containers/application_1455739021822_0001/container_1455739021822_0001_01_000001/

@kgrodzicki



