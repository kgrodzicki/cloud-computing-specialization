import org.apache.spark.{SparkConf, SparkContext}

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
    val inputFile = "./src/main/resources/input.txt"
    val conf = new SparkConf().setAppName("Spark Task 2 group 2 question 2")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputFile, 2).cache()

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

    val result = data.map { line: String =>
      // split each line
      // Origin UniqueCarrier DepDelayMinutes CarrierDelay Cancelled
      line.split(",") match {
        case Array(origin, uniqueCarrier, depDelayMinutes, carrierDelay, cancelled) => AirportCarrier(origin, uniqueCarrier, parseDouble(depDelayMinutes), parseDouble(carrierDelay), parseDouble(cancelled))
      }
    }.filter(isCorrect).map {
      // map if delayed or not
      case a@AirportCarrier(_, _, _, Some(carrierDelay), _) if carrierDelay <= 0.0 => (a.origin, (a.uniqueCarrier, 1))
      case a => (a.origin, (a.uniqueCarrier, 0))
    }.groupByKey().map(a => {
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
    })

    result.foreach(println)
  }

  /**
    * import Model.Airline
    * val result = data.map { line: String =>
    * // split each line
    * line.split(",") match {
    * case Array(airlineId, arrDelay, cancelled) => Airline(airlineId, parseDouble(arrDelay), parseDouble(cancelled))
    * }
    * }.filter(_.cancelled match {
    * // filter all which ehere cancelled
    * case Some(c) if c == 0.0 => true
    * case _ => false
    * }).map {
    * // map if flight was delayed or not
    * case a@Airline(_, Some(arrDelay), _) if arrDelay <= 0.0 => (a.airlineId, 1)
    * case a => (a.airlineId, 0)
    * }.groupByKey()
    * // calculate percentage of on time flights
    * .map(a => {
    * val airlineId = a._1
    * val nrOfFLights: Double = a._2.size
    * val nrOnTimeFlights = a._2.sum
    * (airlineId, (nrOnTimeFlights / nrOfFLights) * 100)
    * }).sortBy(_._2, ascending = false).take(10)

    * result.foreach(println)
    */
}
