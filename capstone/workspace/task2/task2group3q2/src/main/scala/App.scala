import java.lang.Math.abs
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import App.Model.{Flight, FlightInfo}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
    val inputFile = "./src/main/resources/input.txt"
    val conf = new SparkConf().setAppName("Spark Task 2 group 3 question 2")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputFile, 2).cache()

    import Model._
    val bestFlights: RDD[Flight] = data.map { line: String =>
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
      println("ELEMENT: ", f)
      val best: (String, Int, Double) = f._2.toSeq.sortBy(_._3).take(1).head
      Flight(f._1._1, f._1._2, parseDate(f._1._3), best._2, best._1, best._3)
    })

    val merged = bestFlights.cartesian(bestFlights).distinct().filter {
      case (firstLeg, secondLeg) if isCorrect(firstLeg, secondLeg) => true
      case _ => false
    }.distinct()

    if (!merged.isEmpty())
      merged.foreach(println)
    else
      LOG.info("empty results")
  }

}
