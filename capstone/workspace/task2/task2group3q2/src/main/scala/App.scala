import java.lang.Math.abs
import java.lang.System._
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import App.Model.{Flight, FlightInfo}
import _root_.kafka.serializer.StringDecoder
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
object App {

  val LOG = Logger.getLogger(App.getClass)

  object Model {

    final case class FlightInfo(origin: String, dest: String, flightDate: String, flightNum: String, depTime: Option[Int], arrDelay: Option[Double], cancelled: Option[Double])

    final case class Flight(origin: String, dest: String, flightDate: Date, depTime: Int, flightNum: String, arrDelay: Double)

  }

  def parseDouble(s: String) = try
    Some(s.toDouble)
  catch {
    case _: Throwable => None
  }

  def parseInt(s: String) = try
    Some(s.toInt)
  catch {
    case _: Throwable => None
  }

  def parseDate(s: String) = try {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dateFormatter.parse(s)
  } catch {
    case _: Throwable => throw new RuntimeException("unable to parse date for string:[" + s + "]")
  }

  def isNotCancelledAndArrDelayIsPresent(flightInfo: FlightInfo): Boolean = flightInfo match {
    case f@FlightInfo(_, _, _, _, _, Some(_), Some(cancelled)) if cancelled == 0.00 => true
    case _ => false
  }

  def differenceInDays(flightDate: Date, flightDate1: Date): Long = try {
    val diff = flightDate.getTime - flightDate1.getTime
    abs(TimeUnit.DAYS.convert(diff, MILLISECONDS))
  }
  catch {
    case e: Throwable => throw new RuntimeException("not able to get difference")
  }

  def isCorrect(firstLeg: Flight, secondLeg: Flight): Boolean = {
    val isFirstLegBeforeSecond: Boolean = firstLeg.flightDate.before(secondLeg.flightDate)
    val isTwoDaysDifference: Boolean = differenceInDays(firstLeg.flightDate, secondLeg.flightDate) == 2
    val isFirstLegDestEqualToSecondLegOrigin = firstLeg.dest.equals(secondLeg.origin)
    val isFirstLegBefore12 = firstLeg.depTime < 1200
    val isSecondLegAfter12 = secondLeg.depTime >= 1200

    LOG.info(firstLeg, secondLeg, isFirstLegBeforeSecond, differenceInDays(firstLeg.flightDate, secondLeg.flightDate), isFirstLegDestEqualToSecondLegOrigin)
    if (isFirstLegBeforeSecond && isTwoDaysDifference && isFirstLegDestEqualToSecondLegOrigin && isFirstLegBefore12 && isSecondLegAfter12)
      true
    else
      false

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
    val batchDuration: Duration = Seconds(50)
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
      session.execute(
        """
          |CREATE TABLE IF NOT EXISTS capstone.flightconnections (
          |  x text,
          |  y text,
          |  z text,
          |  xDepDate date,
          |  xDepTime text,
          |  yDepDate date,
          |  yDepTime text,
          |  xyflightnr int,
          |  yzflightnr int,
          |  PRIMARY KEY(x, y, z, xDepDate))""".stripMargin)
      session.execute(s"TRUNCATE capstone.flightconnections")
    }

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    import Model._
    val bestFlights = lines.map { line: String =>
      // split each line
      line.split(",") match {
        // Origin Dest FlightDate FlightNum DepTime ArrDelay Cancelled
        case Array(origin, dest, flightDate, flightNum, depTime, arrDelay, cancelled) => FlightInfo(origin, dest, flightDate, flightNum, parseInt(depTime), parseDouble(arrDelay), parseDouble(cancelled))
        // TODO [grokrz]: all bad caseses should be filtered out
        case Array(origin, dest, flightDate, flightNum, depTime, arrDelay) => FlightInfo(origin, dest, flightDate, flightNum, parseInt(depTime), parseDouble(arrDelay), None)
        case Array(origin, dest, flightDate, flightNum, depTime) => FlightInfo(origin, dest, flightDate, flightNum, parseInt(depTime), None, None)
      }
    }.filter(isNotCancelledAndArrDelayIsPresent).map(a => {
      val key = (a.origin, a.dest, a.flightDate)
      val value = (a.flightNum, a.depTime.get, a.arrDelay.get)
      (key, value)
    }).groupByKey().map(f => {
      val best: (String, Int, Double) = f._2.toSeq.sortBy(_._3).take(1).head
      Flight(f._1._1, f._1._2, parseDate(f._1._3), best._2, best._1, best._3)
    })

    val result = bestFlights.transform(rdd => rdd.cartesian(rdd).filter {
      case (firstLeg, secondLeg) if isCorrect(firstLeg, secondLeg) => true
      case _ => false
    }).map { case (firsLeg: Flight, secondLeg: Flight) =>
      // merge it
      val x = firsLeg.origin
      val y = firsLeg.dest
      val z = secondLeg.dest
      val xDepDate = firsLeg.flightDate
      val xDepTime = firsLeg.depTime.toString.reverse.padTo(4, "0").reverse.mkString
      val yDepDate = secondLeg.flightDate
      val yDepTime = secondLeg.depTime.toString.reverse.padTo(4, "0").reverse.mkString
      val xyFlightNr = firsLeg.flightNum
      val yzFlightNr = secondLeg.flightNum
      (x, y, z, xDepDate, xDepTime, yDepDate, yDepTime, xyFlightNr, yzFlightNr)
    }

    result.foreachRDD(_.saveToCassandra("capstone", "flightconnections", SomeColumns("x", "y", "z", "xdepdate", "xdeptime", "ydepdate", "ydeptime", "xyflightnr", "yzflightnr")))

    ssc.start()
    ssc.awaitTermination()
  }
}
