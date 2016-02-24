name := "task2group3q2"

version := "1.0"

scalaVersion := "2.10.5"

mainClass in Compile := Some("App")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case s: String if s.contains("META-INF/") && (s.contains(".SF") || s.contains(".DSA") || s.contains(".RSA")) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
