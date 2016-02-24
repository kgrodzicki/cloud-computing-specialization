# For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X

# Use spark-submit to run your application
$ ./run.sh 192.168.99.101:9092 Topic-2-2-2 192.168.99.101:9042

# Send data to Kafka broker
$ ./send.sh 192.168.99.101:9092 Topic-2-2-2

# Aws
aws emr create-cluster --applications Name=Ganglia Name=Spark --ec2-attributes '{"KeyName":"hadoop-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-1df35e6b","EmrManagedSlaveSecurityGroup":"sg-a9db23d1","EmrManagedMasterSecurityGroup":"sg-b7db23cf"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://capstone-cloud-computing/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","App","s3://capstone-jars/task2group2q2-assembly-1.0.jar","52.73.246.241:9092","Topic-2-2-2","52.73.246.241:9042","5"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"task2-group2q2"}]' --name 'spark-cluster-task2-group2q2' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.2xlarge","Name":"Master Instance Group"},{"InstanceCount":3,"InstanceGroupType":"CORE","InstanceType":"m3.2xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --region us-east-1 --auto-terminate

## Send data to kafka cluster
export KAFKA=52.73.246.241:9092
cat ~/input/input-1-2-2/* | ./kafka-console-producer.sh --broker-list $KAFKA --topic Topic-2-2-2

## App arguments:

## Kafka Producer Message/Sec
538822.04
558685.29
552420.00

## Result 
### Query
select code, top_dest from airport where code in ('SRQ', 'CMH', 'JFK', 'SEA', 'BOS');

cassandra@cqlsh:capstone> select code, top_dest from airport where code in ('SRQ', 'CMH', 'JFK', 'SEA', 'BOS');

 code | top_dest
------+------------------------------------------------------------------------------------------------------------------------------------------------------------------
  BOS |     ['(ONT,100)', '(MDW,83.868)', '(LGA,79.68)', '(MKE,70.974)', '(BDL,65.323)', '(SJC,63.716)', '(DTW,62.709)', '(RSW,62.465)', '(JFK,61.564)', '(BTV,61.544)']
  CMH | ['(SDF,84.874)', '(DTW,82.546)', '(MSP,82.206)', '(CLE,79.407)', '(BNA,78.855)', '(MEM,76.471)', '(STL,73.425)', '(CAK,72.536)', '(RSW,71.186)', '(DFW,69.683)']
  JFK |       ['(TUS,100)', '(ABQ,100)', '(DAB,82.278)', '(SNA,74.715)', '(STT,70.317)', '(SJC,68.447)', '(PSE,66.667)', '(BDL,66.422)', '(BQN,65.808)', '(PBI,64.966)']
  SEA | ['(PSP,82.531)', '(ONT,81.698)', '(SNA,77.044)', '(BOS,76.923)', '(LGB,75.806)', '(OGG,75.294)', '(SJC,75.276)', '(BUR,74.491)', '(GEG,74.109)', '(SAN,73.219)']
  SRQ | ['(DCA,86.702)', '(BNA,81.899)', '(MEM,81.712)', '(TPA,81.015)', '(BWI,79.339)', '(MSP,78.378)', '(CLE,78.147)', '(RSW,77.083)', '(MDW,75.179)', '(MIA,74.691)']

(5 rows)


@kgrodzicki
