name := "spark-load-perf"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.0-M1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.4" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"


