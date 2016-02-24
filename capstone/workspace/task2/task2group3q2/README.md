# For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X

# Use spark-submit to run your application
$ ./run.sh 192.168.99.101:9092 Topic-2-3-2 192.168.99.101:9042

# Send data to Kafka broker
$ ./send.sh 192.168.99.101:9092 Topic-2-3-2

# Aws
aws emr create-cluster --applications Name=Ganglia Name=Spark --ec2-attributes '{"KeyName":"hadoop-key-pair","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-1df35e6b","EmrManagedSlaveSecurityGroup":"sg-a9db23d1","EmrManagedMasterSecurityGroup":"sg-b7db23cf"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-4.3.0 --log-uri 's3n://capstone-cloud-computing/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","App","s3://capstone-jars/task2group3q2-assembly-1.0.jar","52.73.246.241:9092","Topic-2-3-2","52.73.246.241:9042","60", "500"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"task2-group3q2"}]' --name 'spark-cluster-task2-group3q2' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"},{"InstanceCount":1,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"},"Configurations":[]}]' --region us-east-1 --auto-terminate

## Send data to kafka cluster
export KAFKA=52.73.246.241:9092
cat ~/input/input-1-3-2/* | ./kafka-console-producer.sh --broker-list $KAFKA --topic Topic-2-3-2

## Kafka Producer Message/Sec
330840.00
440193.33
506120.00
	
## Query
select * from capstone.flightxyz where x='BOX' and y='ATL' and z='LAX' and xDepDate='2008-04-03';
select * from capstone.flightxyz where x='DFW' and y='STL' and z='ORD' and xDepDate='2008-01-24';
select * from capstone.flightxyz where x='PHX' and y='JFK' and z='MSP' and xDepDate='2008-09-07';
select * from capstone.flightxyz where x='LAX' and y='MIA' and z='LAX' and xDepDate='2008-05-16';

## Result
cqlsh:capstone> select * from capstone.flightxyz where x='BOX' and y='ATL' and z='LAX' and xDepDate='2008-04-03';

 x | y | z | xdepdate | xdeptime | xyflightnr | ydepdate | ydeptime | yzflightnr
---+---+---+----------+----------+------------+----------+----------+------------

(0 rows)
cqlsh:capstone> select * from capstone.flightxyz where x='DFW' and y='STL' and z='ORD' and xDepDate='2008-01-24';

 x   | y   | z   | xdepdate   | xdeptime | xyflightnr | ydepdate   | ydeptime | yzflightnr
-----+-----+-----+------------+----------+------------+------------+----------+------------
 DFW | STL | ORD | 2008-01-24 |      657 |       1336 | 2008-01-26 |     1654 |       2245

(1 rows)
cqlsh:capstone> select * from capstone.flightxyz where x='PHX' and y='JFK' and z='MSP' and xDepDate='2008-09-07';

 x   | y   | z   | xdepdate   | xdeptime | xyflightnr | ydepdate   | ydeptime | yzflightnr
-----+-----+-----+------------+----------+------------+------------+----------+------------
 PHX | JFK | MSP | 2008-09-07 |     1127 |        178 | 2008-09-09 |     1747 |        609

(1 rows)
cqlsh:capstone> select * from capstone.flightxyz where x='LAX' and y='MIA' and z='LAX' and xDepDate='2008-05-16';

 x   | y   | z   | xdepdate   | xdeptime | xyflightnr | ydepdate   | ydeptime | yzflightnr
-----+-----+-----+------------+----------+------------+------------+----------+------------
 LAX | MIA | LAX | 2008-05-16 |     2257 |        280 | 2008-05-18 |     1925 |        456

(1 rows)


# Issue 1
16/02/20 12:57:27 WARN Session: Error creating pool to /172.17.0.2:9042
com.datastax.driver.core.exceptions.ConnectionException: [/172.17.0.2] Pool was closed during initialization
	at com.datastax.driver.core.HostConnectionPool$2.onSuccess(HostConnectionPool.java:149)
	at com.datastax.driver.core.HostConnectionPool$2.onSuccess(HostConnectionPool.java:135)
	at shadeio.common.util.concurrent.Futures$4.run(Futures.java:1181)
	at shadeio.common.util.concurrent.MoreExecutors$SameThreadExecutorService.execute(MoreExecutors.java:297)
	at shadeio.common.util.concurrent.ExecutionList.executeListener(ExecutionList.java:156)
	at shadeio.common.util.concurrent.ExecutionList.execute(ExecutionList.java:145)
	at shadeio.common.util.concurrent.AbstractFuture.set(AbstractFuture.java:185)
	at shadeio.common.util.concurrent.Futures$CombinedFuture.setOneValue(Futures.java:1626)
	at shadeio.common.util.concurrent.Futures$CombinedFuture.access$400(Futures.java:1470)
	at shadeio.common.util.concurrent.Futures$CombinedFuture$2.run(Futures.java:1548)
	at shadeio.common.util.concurrent.MoreExecutors$SameThreadExecutorService.execute(MoreExecutors.java:297)
	at shadeio.common.util.concurrent.ExecutionList.executeListener(ExecutionList.java:156)
	at shadeio.common.util.concurrent.ExecutionList.execute(ExecutionList.java:145)
	at shadeio.common.util.concurrent.AbstractFuture.set(AbstractFuture.java:185)
	at shadeio.common.util.concurrent.Futures$FallbackFuture$1$1.onSuccess(Futures.java:475)
	at shadeio.common.util.concurrent.Futures$4.run(Futures.java:1181)
	at shadeio.common.util.concurrent.MoreExecutors$SameThreadExecutorService.execute(MoreExecutors.java:297)
	at shadeio.common.util.concurrent.Futures$ImmediateFuture.addListener(Futures.java:102)
	at shadeio.common.util.concurrent.Futures.addCallback(Futures.java:1184)
	at shadeio.common.util.concurrent.Futures$FallbackFuture$1.onFailure(Futures.java:472)
	at shadeio.common.util.concurrent.Futures$4.run(Futures.java:1172)
	at shadeio.common.util.concurrent.MoreExecutors$SameThreadExecutorService.execute(MoreExecutors.java:297)
	at shadeio.common.util.concurrent.ExecutionList.executeListener(ExecutionList.java:156)
	at shadeio.common.util.concurrent.ExecutionList.execute(ExecutionList.java:145)
	at shadeio.common.util.concurrent.AbstractFuture.setException(AbstractFuture.java:202)
	at shadeio.common.util.concurrent.Futures$FallbackFuture$1$1.onFailure(Futures.java:483)
	at shadeio.common.util.concurrent.Futures$4.run(Futures.java:1172)
	at shadeio.common.util.concurrent.MoreExecutors$SameThreadExecutorService.execute(MoreExecutors.java:297)
	at shadeio.common.util.concurrent.ExecutionList.executeListener(ExecutionList.java:156)
	at shadeio.common.util.concurrent.ExecutionList.add(ExecutionList.java:101)
	at shadeio.common.util.concurrent.AbstractFuture.addListener(AbstractFuture.java:170)
	at shadeio.common.util.concurrent.Futures.addCallback(Futures.java:1184)
	at shadeio.common.util.concurrent.Futures$FallbackFuture$1.onFailure(Futures.java:472)
	at shadeio.common.util.concurrent.Futures$4.run(Futures.java:1172)
	at shadeio.common.util.concurrent.MoreExecutors$SameThreadExecutorService.execute(MoreExecutors.java:297)
	at shadeio.common.util.concurrent.ExecutionList.executeListener(ExecutionList.java:156)
	at shadeio.common.util.concurrent.ExecutionList.execute(ExecutionList.java:145)
	at shadeio.common.util.concurrent.AbstractFuture.setException(AbstractFuture.java:202)
	at shadeio.common.util.concurrent.Futures$ChainingListenableFuture.run(Futures.java:857)
	at shadeio.common.util.concurrent.MoreExecutors$SameThreadExecutorService.execute(MoreExecutors.java:297)
	at shadeio.common.util.concurrent.ExecutionList.executeListener(ExecutionList.java:156)
	at shadeio.common.util.concurrent.ExecutionList.execute(ExecutionList.java:145)
	at shadeio.common.util.concurrent.AbstractFuture.setException(AbstractFuture.java:202)
	at shadeio.common.util.concurrent.SettableFuture.setException(SettableFuture.java:68)
	at com.datastax.driver.core.Connection$1.operationComplete(Connection.java:157)
	at com.datastax.driver.core.Connection$1.operationComplete(Connection.java:140)
	at io.netty.util.concurrent.DefaultPromise.notifyListener0(DefaultPromise.java:680)
	at io.netty.util.concurrent.DefaultPromise.notifyListeners0(DefaultPromise.java:603)
	at io.netty.util.concurrent.DefaultPromise.notifyListeners(DefaultPromise.java:563)
	at io.netty.util.concurrent.DefaultPromise.tryFailure(DefaultPromise.java:424)
	at io.netty.channel.epoll.AbstractEpollStreamChannel$EpollStreamUnsafe$1.run(AbstractEpollStreamChannel.java:656)
	at io.netty.util.concurrent.PromiseTask$RunnableAdapter.call(PromiseTask.java:38)
	at io.netty.util.concurrent.ScheduledFutureTask.run(ScheduledFutureTask.java:120)
	at io.netty.util.concurrent.SingleThreadEventExecutor.runAllTasks(SingleThreadEventExecutor.java:357)
	at io.netty.channel.epoll.EpollEventLoop.run(EpollEventLoop.java:258)
	at io.netty.util.concurrent.SingleThreadEventExecutor$2.run(SingleThreadEventExecutor.java:111)
	at java.lang.Thread.run(Thread.java:745)
	
# Solution 1
Fix rpc. Use public IP when creating a cassandra cluster. With current configuration only first node is accessible 

@kgrodzicki
