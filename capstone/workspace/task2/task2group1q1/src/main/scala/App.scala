import java.lang.System.{exit, err}
import java.util.concurrent.TimeUnit

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.log4j.Logger

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 11/02/16.
  */

case class Airport(code: String)

object App {
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    if (args.length < 3) {
      err.println(
        s"""
           | Usage: App <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <timeout> await termination in minutes
           |
        """.stripMargin)
      exit(1)
    }

    val Array(brokers, topics, timeout) = args

    val conf = new SparkConf().setAppName("Spark Task 2 group 1 question 1")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines: DStream[String] = messages.map(_._2)

    lines.flatMap(_.split(",")).map((_, 1))
      .reduceByKey(_ + _)
      .updateStateByKey(updateState)
      .transform(rdd => {
        val result = rdd.sortBy(_._2, ascending = false)
        val bestTen = result.take(10).mkString(",")
        logger.info(s"Top 10 most popular airports by numbers of flights to/from the airport.: $bestTen")
        result
      })
      .print(10)

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
