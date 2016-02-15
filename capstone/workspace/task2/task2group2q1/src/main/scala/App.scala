import java.lang.System.{err, exit}

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
object App {

  object Model {

    final case class AirportCarrier(origin: String, uniqueCarrier: String, depDelayMinutes: Option[Double], carrierDelay: Option[Double], cancelled: Option[Double])

  }

  def parseDouble(s: String) = try {
    Some(s.toDouble)
  } catch {
    case _: Throwable => None
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      err.println(
        s"""
           | Usage: App <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      exit(1)
    }


    val Array(brokers, topics) = args

    val conf = new SparkConf().setAppName("Spark Task 2 group 2 question 1")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    import Model.AirportCarrier
    lines.map { line: String =>
      // split each line
      // Origin UniqueCarrier DepDelayMinutes CarrierDelay Cancelled
      line.split(",") match {
        case Array(origin, uniqueCarrier, depDelayMinutes, carrierDelay, cancelled) => AirportCarrier(origin, uniqueCarrier, parseDouble(depDelayMinutes), parseDouble(carrierDelay), parseDouble(cancelled))
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

          val nrOfFlights: Double = status.map(_._2).size
          val onTime = status.map(_._2).sum
          (carrier, (onTime / nrOfFlights) * 100)
        })
          // sort it and take 10 best
          .toSeq.sortWith(_._2 > _._2).take(10)

        (airpo, tenBestCarriers)
      }).print(10)

    ssc.start()
    ssc.awaitTermination()
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
