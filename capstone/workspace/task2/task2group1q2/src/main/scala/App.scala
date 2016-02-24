import java.lang.System._

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils.createDirectStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 11/02/16.
  */
object App {
  val logger = Logger.getLogger(getClass.getName)

  object Model {

    final case class Airline(airlineId: String, ArrDelay: Option[Double], cancelled: Option[Double])

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
           |Usage: App <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <timeout> await termination in minutes
           |
        """.stripMargin)
      exit(1)
    }

    val Array(brokers, topics, timeout) = args

    val conf = new SparkConf().setAppName("Spark Task 2 group 1 question 1")
    val ssc = new StreamingContext(conf, Seconds(60))
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    import Model.Airline
    val result = lines.filter(_.contains(","))
      .map { line: String =>
        // split each line
        line.split(",") match {
          case Array(airlineId, arrDelay, cancelled) => Airline(airlineId, parseDouble(arrDelay), parseDouble(cancelled))
          case arg => logger.error(s"Unable to parse: ${arg.mkString}")
            Airline("", None, None)
        }
      }
      // filter out where we failed to parse
      .filter(!_.airlineId.isEmpty)
      .filter(_.cancelled match {
        // filter all which where cancelled
        case Some(c) if c == 0.0 => true
        case _ => false
      }).map {
      // map if flight was delayed or not
      case a@Airline(_, Some(arrDelay), _) if arrDelay <= 0.0 => (a.airlineId, 1)
      case a => (a.airlineId, 0)
    }
      // save state
      .updateStateByKey(updateState)
      .groupByKey()
      // calculate percentage of on time flights
      .map(a => {
      val airlineId = a._1
      val nrOfFLights: Double = a._2.size
      val nrOnTimeFlights = a._2.sum
      (airlineId, (nrOnTimeFlights / nrOfFLights) * 100)
    }).transform(_.sortBy(_._2, ascending = false)).print(10)

    ssc.start()
    ssc.awaitTerminationOrTimeout(Minutes(timeout.toLong).milliseconds)
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  def updateState(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newSum: Int = newValues.sum
    val total = runningCount match {
      case Some(rc) => rc + newSum
      case _ => newSum
    }
    Some(total)
  }
}
