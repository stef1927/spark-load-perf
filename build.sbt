name := "spark-load-perf"

version := "1.0"

scalaVersion := "2.10.5"

//Either use a published connector if it supports streaming or, manually build the connector with sbt assembly
//and copy the assembly jar to a top-level folder called "lib" (unmanaged dependency)
//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.4" excludeAll ExclusionRule(organization = "javax.servlet")
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("org", "joda", "time", xs @ _*) => MergeStrategy.last
    case PathList("org", "objectweb", "asm", xs @ _*) => MergeStrategy.last
    case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
    case PathList("io", "netty", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.first
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case "META-INF/native/libnetty-transport-native-epoll.so" => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "overview.html" => MergeStrategy.rename
    case "plugin.xml" => MergeStrategy.rename
    case "parquet.thrift" => MergeStrategy.rename
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
    case y => old(y)
  }
}

