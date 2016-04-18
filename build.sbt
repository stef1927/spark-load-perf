name := "spark-load-perf"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.4" excludeAll ExclusionRule(organization = "javax.servlet")
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("io", "netty", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
    case y => old(y)
  }
}

