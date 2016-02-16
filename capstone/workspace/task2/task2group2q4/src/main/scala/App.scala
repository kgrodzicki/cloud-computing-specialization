import java.lang.System._

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SomeColumns, _}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.immutable

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
object App {


  object Model {

    final case class OriginDestArrDelay(origin: String, dest: String, arrDelay: Option[Double])

  }

  def parseDouble(s: String) = try
    Some(s.toDouble)
  catch {
    case _: Throwable => None
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      err.println(
        s"""
           | Usage: App <brokers> <topics> <brokers>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <brokers> is a list of one or more Cassandra brokers
           |
        """.stripMargin)
      exit(1)
    }

    val Array(kafkaBrokers, topics, cassandraBrokers) = args
    val batchDuration: Duration = Seconds(5)
    val cassandraHost: String = cassandraBrokers.split(":")(0)
    val cassandraPort: String = cassandraBrokers.split(":")(1)
    val keepAlliveMs = batchDuration.+(Seconds(5)).milliseconds // keep connection alive between batch sizes

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort)
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .set("spark.cassandra.connection.keep_alive_ms", s"$keepAlliveMs")
      .setAppName("Spark Task 2 group 2 question 4")

    /** Creates the keyspace and table in Cassandra. */
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS capstone WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      session.execute(s"CREATE TABLE IF NOT EXISTS capstone.meanarrdelay(origin text, dest text, mean text, PRIMARY KEY(origin, dest));")
      session.execute(s"TRUNCATE capstone.meanarrdelay")
    }

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    import Model.OriginDestArrDelay
    val result: DStream[(String, String, Int)] = lines.map { line: String =>
      // split each line
      line.split(",") match {
        case Array(origin, dest, arrDelay) => OriginDestArrDelay(origin, dest, parseDouble(arrDelay))
        case Array(origin, dest) => OriginDestArrDelay(origin, dest, None)
      }
    }.filter(_.arrDelay match {
      case Some(d) => true
      // filter all where arrival delay is not provided
      case _ => false
    }).map {
      // make a tuple for each origin dest and arrDelay
      case a@OriginDestArrDelay(origin, dest, arrDelay) => (origin, (dest, arrDelay.get.toInt))
    }.groupByKey()
      .updateStateByKey(updateState)
      .map(a => {
        val origin: String = a._1
        val dests: Seq[(String, Int)] = a._2

        dests.groupBy(_._1).map(each => {
          val dest = each._1
          val delays = each._2.map(_._2)
          val meanArrDelay: Double = delays.sum / delays.size

          (origin, dest, meanArrDelay.toInt)
        })
      }).flatMap(_.map(a => a))

    result.foreachRDD(_.saveToCassandra("capstone", "meanarrdelay", SomeColumns("origin", "dest", "mean")))

    ssc.start()
    ssc.awaitTermination()
  }

  def updateState(newValues: Seq[Iterable[(String, Int)]], runningCount: Option[Iterable[(String, Int)]]): Option[Seq[(String, Int)]] = {
    runningCount match {
      case Some(rc) =>
        Some(newValues.flatten ++ rc)
      case _ =>
        Some(newValues.flatten)
    }
  }

}
