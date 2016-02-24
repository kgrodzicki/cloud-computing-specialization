# For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.

# Use spark-submit to run your application
export KAFKA=192.168.99.100:9092
export CASSANDRA=192.168.99.100:9042
./run.sh $KAFKA Topic-2-2-1 $CASSANDRA 2

# Send data to Kafka broker
$ ./send 192.168.99.101:9092 Topic-2-2-1

# Aws
aws emr create-cluster --applications Name=Ganglia Name=Spark --ec2-attributes '{"KeyName":"hadoop-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-1df35e6b","EmrManagedSlaveSecurityGroup":"sg-a9db23d1","EmrManagedMasterSecurityGroup":"sg-b7db23cf"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://capstone-cloud-computing/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","App","s3://capstone-jars/task2group2q1-assembly-1.0.jar","52.73.246.241:9092","Topic-2-2-1","52.73.246.241:9042","5"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"task2-group2q1"}]' --name 'spark-cluster-task2-group2q1' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.2xlarge","Name":"Master Instance Group"},{"InstanceCount":1,"InstanceGroupType":"CORE","InstanceType":"m3.2xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --region us-east-1 --auto-terminate

## Send data to kafka cluster
export KAFKA=52.73.246.241:9092
cat ~/input/input-1-2-1/* | ./kafka-console-producer.sh --broker-list $KAFKA --topic Topic-2-2-1

## Kafka Producer Message/Sec
539066.67

## Issue 1
log4j:ERROR Could not read configuration file from URL [file:/etc/spark/conf/log4j.properties].
java.io.FileNotFoundException: /etc/spark/conf/log4j.properties (No such file or directory)
	at java.io.FileInputStream.open(Native Method)
	at java.io.FileInputStream.<init>(FileInputStream.java:146)
	at java.io.FileInputStream.<init>(FileInputStream.java:101)
	at sun.net.www.protocol.file.FileURLConnection.connect(FileURLConnection.java:90)
	at sun.net.www.protocol.file.FileURLConnection.getInputStream(FileURLConnection.java:188)
	at org.apache.log4j.PropertyConfigurator.doConfigure(PropertyConfigurator.java:557)
	at org.apache.log4j.helpers.OptionConverter.selectAndConfigure(OptionConverter.java:526)
	at org.apache.log4j.LogManager.<clinit>(LogManager.java:127)
	at org.apache.spark.Logging$class.initializeLogging(Logging.scala:121)
	at org.apache.spark.Logging$class.initializeIfNecessary(Logging.scala:106)
	at org.apache.spark.Logging$class.log(Logging.scala:50)
	at org.apache.spark.deploy.yarn.ApplicationMaster$.log(ApplicationMaster.scala:635)
	at org.apache.spark.deploy.yarn.ApplicationMaster$.main(ApplicationMaster.scala:649)
	at org.apache.spark.deploy.yarn.ApplicationMaster.main(ApplicationMaster.scala)
log4j:ERROR Ignoring configuration file [file:/etc/spark/conf/log4j.properties].
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/mnt/yarn/usercache/hadoop/filecache/11/spark-assembly-1.6.0-hadoop2.7.1-amzn-0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/lib/hadoop/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
16/02/19 20:56:29 INFO ApplicationMaster: Registered signal handlers for [TERM, HUP, INT]
16/02/19 20:56:30 INFO ApplicationMaster: ApplicationAttemptId: appattempt_1455915241628_0001_000001
16/02/19 20:56:30 INFO SecurityManager: Changing view acls to: yarn,hadoop
16/02/19 20:56:30 INFO SecurityManager: Changing modify acls to: yarn,hadoop
16/02/19 20:56:30 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: Set(yarn, hadoop); users with modify permissions: Set(yarn, hadoop)
16/02/19 20:56:31 INFO ApplicationMaster: Starting the user application in a separate Thread
16/02/19 20:56:31 INFO ApplicationMaster: Waiting for spark context initialization
16/02/19 20:56:31 INFO ApplicationMaster: Waiting for spark context initialization ... 
16/02/19 20:56:31 ERROR ApplicationMaster: User class threw exception: java.lang.ExceptionInInitializerError
java.lang.ExceptionInInitializerError
	at com.datastax.spark.connector.cql.DefaultConnectionFactory$.clusterBuilder(CassandraConnectionFactory.scala:35)
	at com.datastax.spark.connector.cql.DefaultConnectionFactory$.createCluster(CassandraConnectionFactory.scala:87)
	at com.datastax.spark.connector.cql.CassandraConnector$.com$datastax$spark$connector$cql$CassandraConnector$$createSession(CassandraConnector.scala:153)
	at com.datastax.spark.connector.cql.CassandraConnector$$anonfun$2.apply(CassandraConnector.scala:148)
	at com.datastax.spark.connector.cql.CassandraConnector$$anonfun$2.apply(CassandraConnector.scala:148)
	at com.datastax.spark.connector.cql.RefCountedCache.createNewValueAndKeys(RefCountedCache.scala:31)
	at com.datastax.spark.connector.cql.RefCountedCache.acquire(RefCountedCache.scala:56)
	at com.datastax.spark.connector.cql.CassandraConnector.openSession(CassandraConnector.scala:81)
	at com.datastax.spark.connector.cql.CassandraConnector.withSessionDo(CassandraConnector.scala:109)
	at App$.main(App.scala:62)
	at App.main(App.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at org.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:542)
Caused by: java.lang.IllegalStateException: Detected Guava issue #1635 which indicates that a version of Guava less than 16.01 is in use.  This introduces codec resolution issues and potentially other incompatibility issues in the driver.  Please upgrade to Guava 16.01 or later.
	at com.datastax.driver.core.SanityChecks.checkGuava(SanityChecks.java:62)
	at com.datastax.driver.core.SanityChecks.check(SanityChecks.java:36)
	at com.datastax.driver.core.Cluster.<clinit>(Cluster.java:67)
	... 16 more
16/02/19 20:56:31 INFO ApplicationMaster: Final app status: FAILED, exitCode: 15, (reason: User class threw exception: java.lang.ExceptionInInitializerError)
16/02/19 20:56:41 ERROR ApplicationMaster: SparkContext did not initialize after waiting for 100000 ms. Please check earlier log output for errors. Failing the application.
16/02/19 20:56:41 INFO ShutdownHookManager: Shutdown hook called

## Solution 
shade guava lib

## Issue 2
16/02/19 22:19:19 ERROR ApplicationMaster: Uncaught exception: 
java.lang.SecurityException: Invalid signature file digest for Manifest main attributes

## Solution 2
exclude from uber jar: META-INF/*.SF, META-INF/*.DSA and META-INF/*.RSA

## Results
### Query
select * from capstone.airport where code in ('SRQ', 'CMH', 'JFK', 'SEA', 'BOS');

cassandra@cqlsh:capstone> select * from capstone.airport where code in ('SRQ', 'CMH', 'JFK', 'SEA', 'BOS');

 code | top_carriers
------+--------------------------------------------------------------------------------------------------------------------------------------------------------
  BOS |    ['(FL,87.535)', '(CO,86.235)', '(EV,81.69)', '(XE,80.533)', '(YV,80.488)', '(TZ,76.18)', '(9E,73.535)', '(UA,73.21)', '(DL,71.116)', '(AA,66.688)']
  CMH |  ['(DH,85.323)', '(AA,84.793)', '(9E,84.204)', '(UA,84.198)', '(CO,82.262)', '(XE,81.389)', '(DL,80.229)', '(B6,70.95)', '(MQ,70.935)', '(OH,63.401)']
  JFK |  ['(XE,77.027)', '(UA,77.022)', '(DH,72.004)', '(YV,71.528)', '(EV,66.748)', '(MQ,63.712)', '(OH,61.75)', '(DL,57.577)', '(US,54.179)', '(NW,52.719)']
  SEA | ['(FL,76.369)', '(XE,70.523)', '(UA,69.351)', '(B6,67.947)', '(CO,67.312)', '(TZ,65.464)', '(DL,59.993)', '(F9,59.765)', '(AA,56.157)', '(YV,53.333)']
  SRQ |  ['(9E,91.667)', '(FL,86.303)', '(TZ,83.607)', '(CO,83.275)', '(B6,78.779)', '(DL,73.009)', '(XE,69.504)', '(YV,67.778)', '(EV,65.789)', '(NW,56.25)']

(5 rows)


@kgrodzicki
