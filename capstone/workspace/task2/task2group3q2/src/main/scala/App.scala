import java.lang.Math.abs
import java.lang.System._
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.{Calendar, Date}

import App.Model.{Flight, FlightInfo}
import _root_.kafka.serializer.StringDecoder
import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
//TODO [grokrz]: refactor
object App {
  val logger = Logger.getLogger(App.getClass)

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

  def formatDate(date: Date) = try {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dateFormatter.format(date)
  } catch {
    case _: Throwable => throw new RuntimeException("unable to format date for string:[" + date + "]")
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

    if (isFirstLegBeforeSecond && isTwoDaysDifference && isFirstLegDestEqualToSecondLegOrigin && isFirstLegBefore12 && isSecondLegAfter12) {
      logger.debug(firstLeg, secondLeg, isFirstLegBeforeSecond, differenceInDays(firstLeg.flightDate, secondLeg.flightDate), isFirstLegDestEqualToSecondLegOrigin)
      true
    } else
      false
  }

  def main(args: Array[String]) {
    if (args.length < 5) {
      err.println(
        s"""
           | Usage: App <brokers> <topics> <brokers>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <brokers> is a list of one or more Cassandra brokers
           |  <timeout> await termination in minutes
           |  <maxRate> max rate per partition
           |
        """.stripMargin)
      exit(1)
    }

    val Array(kafkaBrokers, topics, cassandraBrokers, timeout, maxRate) = args
    val batchDuration: Duration = Seconds(10)
    val cassandraHost: String = cassandraBrokers.split(":")(0)
    val cassandraPort: String = cassandraBrokers.split(":")(1)
    //    val keepAlliveMs = batchDuration.+(Seconds(300)).milliseconds // keep connection alive between batch sizes

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort)
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      //      .set("spark.cassandra.connection.keep_alive_ms", s"$keepAlliveMs")
      .set("spark.cassandra.output.consistency.level", "ONE") // no need for strong consistency here
      .set("spark.streaming.receiver.maxRate", maxRate)
      .set("spark.streaming.kafka.maxRatePerPartition", maxRate)
      .set("spark.streaming.backpressure.enabled", "true")
      .setAppName("Spark Task 2 group 3 question 2")

    /** Creates the keyspace and table in Cassandra. */
    val cc = CassandraConnector(conf)
    cc.withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS capstone WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
      session.execute(
        """
          |CREATE TABLE IF NOT EXISTS capstone.flightxyz (
          |  x text,
          |  y text,
          |  z text,
          |  xDepDate text,
          |  xDepTime text,
          |  yDepDate text,
          |  yDepTime text,
          |  xyflightnr int,
          |  yzflightnr int,
          |  PRIMARY KEY(x, y, z, xDepDate))""".stripMargin)
      session.execute(s"TRUNCATE capstone.flightxyz")
      session.execute(
        """
          |CREATE TABLE IF NOT EXISTS capstone.bestbefore12 (
          |  origin text,
          |  dest text,
          |  flightDate text,
          |  depTime int,
          |  flightNum text,
          |  arrDelay int,
          |  PRIMARY KEY(dest, flightDate, origin))""".stripMargin)
      session.execute(s"TRUNCATE capstone.bestbefore12")
      session.execute(
        """
          |CREATE TABLE IF NOT EXISTS capstone.bestafter12 (
          |  origin text,
          |  dest text,
          |  flightDate text,
          |  depTime int,
          |  flightNum text,
          |  arrDelay int,
          |  PRIMARY KEY(origin, flightDate, dest))""".stripMargin)
      session.execute(s"TRUNCATE capstone.bestafter12")
    }

    val ssc = new StreamingContext(conf, batchDuration)
    val sc = ssc.sparkContext
    //    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val windowDuration: Duration = batchDuration.times(2)
    val slideDuration: Duration = batchDuration.times(1)
    //    val lines: DStream[String] = messages.window(windowDuration, slideDuration).map(_._2)
    val lines: DStream[String] = messages.map(_._2)


    import Model._
    // every two time steps, we compute a result over the previous 2 time steps
    val inputWindow = lines.filter(_.contains(",")).map { line: String =>
      // split each line
      line.split(",") match {
        case Array(origin, dest, flightDate, flightNum, depTime, arrDelay, cancelled) => FlightInfo(origin, dest, flightDate, flightNum, parseInt(depTime), parseDouble(arrDelay), parseDouble(cancelled))
        case _ => FlightInfo("", "", "", "", None, None, None)
      }
    }

    val filteredLines: DStream[((String, String, String, String), (String, Int, Double))] = inputWindow.filter(isNotCancelledAndArrDelayIsPresent).map(a => {
      val dayPart: String = if (a.depTime.get > 1200)
        "AFTER12"
      else
        "BEFORE12"
      val key: (String, String, String, String) = (a.origin, a.dest, a.flightDate, dayPart)
      val value: (String, Int, Double) = (a.flightNum, a.depTime.get, a.arrDelay.get)
      (key, value)
    })

    val bestFlights = filteredLines
      .groupByKey()
      .map(f => {
        val best: (String, Int, Double) = f._2.toSeq.sortBy(_._3).take(1).head
        Flight(f._1._1, f._1._2, parseDate(f._1._3), best._2, best._1, best._3)
      })

    bestFlights.foreachRDD(rdd => {
      val flights = rdd.collect()
      logger.debug(s"SAVE $flights")
      flights.foreach(f => {
        if (f.depTime > 1200) {
          logger.debug(s"BEST AFTER 12: $f , ${f.depTime}")
          rdd.map(f => (f.origin, f.dest, formatDate(f.flightDate), f.depTime, f.flightNum, f.arrDelay)).saveToCassandra("capstone", "bestafter12", SomeColumns("origin", "dest", "flightdate", "deptime", "flightnum", "arrdelay"))
        } else {
          logger.debug(s"BEST BEFORE 12: $f, ${f.depTime}")
          rdd.map(f => (f.origin, f.dest, formatDate(f.flightDate), f.depTime, f.flightNum, f.arrDelay)).saveToCassandra("capstone", "bestbefore12", SomeColumns("origin", "dest", "flightdate", "deptime", "flightnum", "arrdelay"))
        }
      })
    })

    def addDate(date: Date, nr: Int): Date = {
      val c = Calendar.getInstance()
      c.setTime(date)
      c.add(Calendar.DATE, nr)
      c.getTime
    }

    def before(origin: String, date: String): Array[Flight] = {
      cc.withSessionDo(s => {
        val rows: util.List[Row] = s.execute(s"select * from capstone.bestbefore12 where dest='$origin' and flightdate='$date'").all()
        var result = new ListBuffer[Flight]
        for (row: Row <- rows) {
          val f = Flight(row.getString("origin"), row.getString("dest"), parseDate(row.getString("flightdate")), row.getInt("deptime"), row.getString("flightnum"), row.getInt("arrdelay"))
          result += f
        }
        logger.debug(s"found before ${result.toArray.mkString(",")}")
        result.toArray
      })
    }

    def after(origin: String, date: String): Array[Flight] = {
      cc.withSessionDo(s => {
        val query: String = s"select * from capstone.bestafter12 where origin='$origin' and flightdate='$date'"
        logger.debug(query)
        val rows: util.List[Row] = s.execute(query).all()
        var result = new ListBuffer[Flight]
        for (row: Row <- rows) {
          val f = Flight(row.getString("origin"), row.getString("dest"), parseDate(row.getString("flightdate")), row.getInt("deptime"), row.getString("flightnum"), row.getInt("arrdelay"))
          result += f
        }
        logger.debug(s"found after ${result.toArray.mkString(",")}")
        result.toArray
      })
    }

    val connections = bestFlights.flatMap(f => {
      logger.debug(s"MERGE $f")
      if (f.depTime <= 1200) {
        val plus2Days: Date = addDate(f.flightDate, 2)
        after(f.dest, formatDate(plus2Days)).map(merge(f, _))
      } else {
        val minus2Days: Date = addDate(f.flightDate, -2)
        before(f.origin, formatDate(minus2Days)).map(merge(_, f))
      }
    }).foreachRDD(rdd => {
      logger.debug(s"SAVE")
      rdd.saveToCassandra("capstone", "flightxyz", SomeColumns("x", "y", "z", "xdepdate", "xdeptime", "ydepdate", "ydeptime", "xyflightnr", "yzflightnr"))
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(Minutes(timeout.toLong).milliseconds)
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  def updateState(newValues: Seq[(String, Int, Double)], runningCount: Option[Seq[(String, Int, Double)]]): Option[Seq[(String, Int, Double)]] = {
    runningCount match {
      case Some(rc) =>
        Some(newValues ++ rc)
      case _ =>
        Some(newValues)
    }
  }

  def merge(firsLeg: Flight, secondLeg: Flight): (String, String, String, String, Int, String, Int, String, String) = {
    logger.debug(s"MERGE $firsLeg $secondLeg")
    val x = firsLeg.origin
    val y = firsLeg.dest
    val z = secondLeg.dest
    val xDepDate = formatDate(firsLeg.flightDate)
    val xDepTime = firsLeg.depTime
    val yDepDate = formatDate(secondLeg.flightDate)
    val yDepTime = secondLeg.depTime
    val xyFlightNr = firsLeg.flightNum
    val yzFlightNr = secondLeg.flightNum
    val merged = (x, y, z, xDepDate, xDepTime, yDepDate, yDepTime, xyFlightNr, yzFlightNr)
    logger.warn(s"MERGED $merged")
    merged
  }

}
