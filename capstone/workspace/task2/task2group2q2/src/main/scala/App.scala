import java.lang.System._

import com.datastax.driver.core.exceptions.QueryExecutionException
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.datastax.spark.connector._

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
object App {

  object Model {

    final case class OriginDest(origin: String, depDelayMinutes: Option[Double], dest: String)

  }

  def parseDouble(s: String) = try {
    Some(s.toDouble)
  } catch {
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

    val cassandraHost: String = cassandraBrokers.split(":")(0)
    val cassandraPort: String = cassandraBrokers.split(":")(1)
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort)
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .setAppName("Spark Task 2 group 2 question 1")

    /** Creates the keyspace and table in Cassandra. */
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS capstone WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS capstone.airport (code text PRIMARY KEY, top_carriers list<text>)")
      try {
        session.execute(s"alter table capstone.airport add top_dest list<text>")
      }
      catch {
        case qee: QueryExecutionException =>
        //pass as column already exists
      }
      session.execute(s"TRUNCATE capstone.airport")
    }

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    import Model.OriginDest
    val result: DStream[(String, Seq[(String, Double)])] = lines.map { line: String =>
      // split each line
      line.split(",") match {
        case Array(origin, depDelayMinutes, dest) => OriginDest(origin, parseDouble(depDelayMinutes), dest)
      }
    }.filter(_.depDelayMinutes match {
      case Some(d) => true
      case _ => false
    }).map {
      // make a tuple for each airport
      case a@OriginDest(origin, depDelayMinutes, dest) => (origin, (dest, depDelayMinutes.get))
    }.groupByKey()
      .updateStateByKey(updateState)
      .map(a => {
        val origin: String = a._1
        val dests: Iterable[(String, Double)] = a._2

        val topTenDest = dests.groupBy(_._1).map(each => {
          val dest = each._1
          val delays = each._2.map(_._2)
          val performance: Double = (delays.sum / delays.size) * 100

          (dest, performance)
        }).toSeq.sortWith(_._2 > _._2).take(10)

        (origin, topTenDest)
      })

    result.foreachRDD(_.saveToCassandra("capstone", "airport", SomeColumns("code", "top_dest")))

    ssc.start()
    ssc.awaitTermination()

  }

  def updateState(newValues: Seq[Iterable[(String, Double)]], runningCount: Option[Iterable[(String, Double)]]): Option[Seq[(String, Double)]] = {
    runningCount match {
      case Some(rc) => Some(newValues.flatten ++ rc)
      case _ => Some(newValues.flatten)
    }
  }
}
