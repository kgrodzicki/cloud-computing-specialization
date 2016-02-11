import org.apache.spark.{SparkConf, SparkContext}

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
    val inputFile = "./src/main/resources/input.txt"
    val conf = new SparkConf().setAppName("Spark Task 2 group 1 question 2")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputFile, 2).cache()

    import Model.Airline
    val result = data.map { line: String =>
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
    }.groupByKey()
      // calculate percentage of on time flights
      .map(a => {
      val airlineId = a._1
      val nrOfFLights: Double = a._2.size
      val nrOnTimeFlights = a._2.sum
      (airlineId, (nrOnTimeFlights / nrOfFLights) * 100)
    }).sortBy(_._2, ascending = false).take(10)

    result.foreach(println)
  }
}
