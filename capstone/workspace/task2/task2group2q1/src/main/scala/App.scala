import java.lang.System.{err, exit}
import java.text.DecimalFormat

import _root_.kafka.serializer.StringDecoder
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._

import com.datastax.spark.connector._

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
object App {
  val logger = Logger.getLogger(getClass.getName)

  object Model {

    final case class AirportCarrier(origin: String, uniqueCarrier: String, depDelayMinutes: Option[Double], carrierDelay: Option[Double], cancelled: Option[Double])

  }

  def parseDouble(s: String) = try {
    Some(s.toDouble)
  } catch {
    case _: Throwable => None
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      err.println(
        s"""
           | Usage: App <brokers> <topics> <brokers>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <brokers> is a list of one or more Cassandra brokers
           |  <timeout> await termination in minutes
           |
        """.stripMargin)
      exit(1)
    }

    val Array(kafkaBrokers, topics, cassandraBrokers, timeout) = args
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
      .set("spark.cassandra.output.consistency.level", "ANY") // no need for strong consistency here
      .setAppName("Spark Task 2 group 2 question 1")

    /** Creates the keyspace and table in Cassandra. */
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS capstone WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS capstone.airport (code text PRIMARY KEY, top_carriers list<text>)")
      session.execute(s"TRUNCATE capstone.airport")
    }

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    val formatter = new DecimalFormat("#.###")
    import Model.AirportCarrier
    val result = lines.filter(_.contains(",")).map { line: String =>
      // split each line
      // Origin UniqueCarrier DepDelayMinutes CarrierDelay Cancelled
      line.split(",") match {
        case Array(origin, uniqueCarrier, depDelayMinutes, carrierDelay, cancelled) => AirportCarrier(origin, uniqueCarrier, parseDouble(depDelayMinutes), parseDouble(carrierDelay), parseDouble(cancelled))
        case line => logger.warn(s"unable to match $line")
          AirportCarrier("", "", None, None, None)
      }
    }.filter(isCorrect).map {
      // map if delayed or not
      case a@AirportCarrier(_, _, _, Some(carrierDelay), _) if carrierDelay <= 0.0 => (a.origin, (a.uniqueCarrier, 1))
      case a => (a.origin, (a.uniqueCarrier, 0))
    }.groupByKey()
      .updateStateByKey(updateState)
      .map(a => {
        val airpo = a._1

        val carrDelay: Iterable[(String, Int)] = a._2
        val tenBestCarriers = carrDelay.groupBy(_._1).map(eachDelay => {
          val carrier: String = eachDelay._1
          val status: Iterable[(String, Int)] = eachDelay._2

          val nrOfFlights = status.map(_._2).size
          val onTime = status.map(_._2).sum
          (carrier, (onTime / nrOfFlights.toDouble) * 100)
        })
          // sort it and take 10 best
          .toSeq.sortWith(_._2 > _._2).take(10)
          .map(e => (e._1, formatter.format(e._2)))

        (airpo, tenBestCarriers)
      })

    result.foreachRDD(_.saveToCassandra("capstone", "airport", SomeColumns("code", "top_carriers")))

    ssc.start()
    ssc.awaitTerminationOrTimeout(Minutes(timeout.toLong).milliseconds)
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  def updateState(newValues: Seq[Iterable[(String, Int)]], runningCount: Option[Iterable[(String, Int)]]): Option[Seq[(String, Int)]] = {
    runningCount match {
      case Some(rc) => Some(newValues.flatten ++ rc)
      case _ => Some(newValues.flatten)
    }
  }

  import Model.AirportCarrier

  def isCorrect(airportCarrier: AirportCarrier) = {
    val notCancelled = airportCarrier.cancelled match {
      // filter all which wehere cancelled
      case Some(c) if c == 0.0 => true
      case _ => false
    }

    val isCarrierDelayProvided = airportCarrier.carrierDelay match {
      case Some(c) => true
      case _ => false
    }

    notCancelled && isCarrierDelayProvided
  }
}
