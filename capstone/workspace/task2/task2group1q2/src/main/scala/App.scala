import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils.createDirectStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 11/02/16.
  */
object App {

  object Model {

    final case class Airline(airlineId: String, ArrDelay: Option[Double], cancelled: Option[Double])

  }

  def parseDouble(s: String) = try {
    Some(s.toDouble)
  } catch {
    case _: Throwable => None
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: App <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val conf = new SparkConf().setAppName("Spark Task 2 group 1 question 1")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    import Model.Airline
    val result = lines.map { line: String =>
      // split each line
      line.split(",") match {
        case Array(airlineId, arrDelay, cancelled) => Airline(airlineId, parseDouble(arrDelay), parseDouble(cancelled))
      }
    }.filter(_.cancelled match {
      // filter all which ehere cancelled
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
    }).transform(_.sortBy(_._2, ascending = false)).print()


    ssc.start()
    ssc.awaitTermination()
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
