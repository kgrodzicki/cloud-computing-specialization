import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author <a href="mailto:kgrodzicki@gmail.com">Krzysztof Grodzicki</a> 11/02/16.
  */

case class Airport(code: String)

object App {
  def main(args: Array[String]) {
    val inputFile = "./src/main/resources/input.txt"
    val conf = new SparkConf().setAppName("Spark Task 2 group 1 question 1")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputFile, 2).cache()

    val result = data.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(10)

    result.foreach(println)
  }
}
