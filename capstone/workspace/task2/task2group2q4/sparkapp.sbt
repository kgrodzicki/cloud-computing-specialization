name := "task2group2q4"

version := "1.0"

scalaVersion := "2.10.5"

mainClass in Compile := Some("App")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}
