import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 13/02/16.
  */
object App {

  object Model {

    final case class OriginDest(origin: String, depDelayMinutes: Option[Double], dest: String)

  }

  def parseDouble(s: String) = try {
    Some(s.toDouble)
  } catch {
    case _: Throwable => None
  }

  def main(args: Array[String]) {
    val inputFile = "./src/main/resources/input-test.txt"
    val conf = new SparkConf().setAppName("Spark Task 2 group 2 question 2")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputFile, 2).cache()

    import Model.OriginDest
    val result = data.map { line: String =>
      // split each line
      line.split(",") match {
        case Array(origin, depDelayMinutes, dest) => OriginDest(origin, parseDouble(depDelayMinutes), dest)
      }
    }.filter(_.depDelayMinutes match {
      case Some(d) => true
      case _ => false
    }).map {
      // make a tuple for each airport
      case a@OriginDest(origin, depDelayMinutes, dest) => (origin, (dest, depDelayMinutes.get))
    }.groupByKey().map(a => {
      val origin: String = a._1
      val dests: Iterable[(String, Double)] = a._2

      val topTenDest = dests.groupBy(_._1).map(each => {
        val dest = each._1
        val delays = each._2.map(_._2)
        val performance: Double = (delays.sum / delays.size) * 100

        (dest, performance)
      }).toSeq.sortWith(_._2 > _._2).take(10)

      (origin, topTenDest)
    })

    result.foreach(println)
  }
}
