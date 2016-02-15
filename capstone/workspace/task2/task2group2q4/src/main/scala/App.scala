import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
object App {

  object Model {

    final case class OriginDestArrDelay(origin: String, dest: String, arrDelay: Option[Double])

  }

  def parseDouble(s: String) = try
    Some(s.toDouble)
  catch {
    case _: Throwable => None
  }

  def main(args: Array[String]) {
    val inputFile = "./src/main/resources/input.txt"
    val conf = new SparkConf().setAppName("Spark Task 2 group 2 question 4")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputFile, 2).cache()

    import Model.OriginDestArrDelay
    val result = data.map { line: String =>
      // split each line
      line.split(",") match {
        case Array(origin, dest, arrDelay) => OriginDestArrDelay(origin, dest, parseDouble(arrDelay))
        case Array(origin, dest) => OriginDestArrDelay(origin, dest, None)
      }
    }.filter(_.arrDelay match {
      case Some(d) => true
      // filter all where arrival delay is not provided
      case _ => false
    }).map {
      // make a tuple for each origin dest and arrDelay
      case a@OriginDestArrDelay(origin, dest, arrDelay) => (origin, (dest, arrDelay.get))
    }.groupByKey().map(a => {
      val origin: String = a._1
      val dests: Iterable[(String, Double)] = a._2

      val originDestArrDelayMean = dests.groupBy(_._1).map(each => {
        val dest = each._1
        val delays = each._2.map(_._2)
        val meanArrDelay: Double = delays.sum / delays.size
        (origin, dest, meanArrDelay)
      })

      originDestArrDelayMean
    })

    result.foreach(println)
  }

}
