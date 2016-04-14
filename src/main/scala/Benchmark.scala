import com.datastax.spark.connector._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import sys.process._

object Benchmark {
  type OptionMap = Map[Symbol, Any]

  def main(args: Array[String]) {
    val options = parseOptions(args)

    val conf = new SparkConf()
                 .setAppName("spark-load-test")
                 .setMaster(options('sparkMaster).asInstanceOf[String])
                 .set("spark.cassandra.connection.host", options('cassandraHost).asInstanceOf[String])

    val sc = new SparkContext(conf)
    try {
      new Run(options).run(sc)
    }
    finally {
      sc.stop()
    }
  }

  def parseOptions(args: Array[String]) = {
    val usage = "Usage: sbt run\n" +
      "\t--cassandra-host 127.0.0.1\n" +
      "\t--spark-master local[4]\n" +
      "\t--hdfs-host hdfs://localhost:9000\n" +
      "\t--hdfs-path /user\n" +
      "\t--schema 1\n" +
      "\t--num-records 100000\n" +
      "\t--num-clusterings 100\n" +
      "\t--key-length 25\n" +
      "\t--flush-os-cache\n" +
      "WARNING: only flush the OS cache in test environments!!!"

    val arglist = args.toList

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--cassandra-host" :: value :: tail =>
          nextOption(map - 'cassandraHost ++ Map('cassandraHost -> value.toString), tail)
        case "--spark-master" :: value :: tail =>
          nextOption(map - 'sparkMaster ++ Map('sparkMaster -> value.toString), tail)
        case "--hdfs-host" :: value :: tail =>
          nextOption(map - 'hdfsHost ++ Map('hdfsHost -> value.toString), tail)
        case "--hdfs-path" :: value :: tail =>
          nextOption(map - 'hdfsPath ++ Map('hdfsPath -> value.toString), tail)
        case "--schema" :: value :: tail =>
          nextOption(map - 'schema ++ Map('schema -> Schema.create(value.toInt)), tail)
        case "--num-records" :: value :: tail =>
          nextOption(map - 'numRecords ++ Map('numRecords -> value.toInt), tail)
        case "--num-clusterings" :: value :: tail =>
          nextOption(map - 'numClusterings ++ Map('numClusterings -> value.toInt), tail)
        case "--key-length" :: value :: tail =>
          nextOption(map - 'keyLength ++ Map('keyLength -> value.toInt), tail)
        case "--flush-os-cache" :: tail =>
          nextOption(map - 'flushOSCache ++ Map('flushOSCache -> true), tail)
        case option :: tail =>
          println("Unknown option " + option)
          println(usage)
          sys.exit(0)
      }
    }

    val options = nextOption(Map('cassandraHost -> "127.0.0.1",
                                 'sparkMaster -> "local[4]",
                                 'hdfsHost -> "hdfs://localhost:9000",
                                 'hdfsPath -> "/user",
                                 'schema -> Schema.create(1),
                                 'numRecords -> 100000,
                                 'numClusterings -> 100,
                                 'keyLength -> 25,
                                 'flushOSCache -> false),
                             arglist)
    println(options)
    options
  }

  @SerialVersionUID(100L)
  class Run(options: OptionMap) extends Serializable {
    val hdfsHost = options('hdfsHost).asInstanceOf[String]
    val csvFile = hdfsHost + options('hdfsPath).asInstanceOf[String] + "/benchmark.csv"
    val parquetFile = hdfsHost + options('hdfsPath).asInstanceOf[String] + "/benchmark.parquet"
    val numRecords = options('numRecords).asInstanceOf[Int]
    val schema = options('schema).asInstanceOf[Schema]
    val flushOSCache = options('flushOSCache).asInstanceOf[Boolean]
    val numClusterings = options('numClusterings).asInstanceOf[Int]
    val keyLength = options('keyLength).asInstanceOf[Int]

    def run( sc: SparkContext): Unit = {
      val sql = new SQLContext(sc)
      generateSchema(sc)
      clearHDFSData()

      time(loadData(sql))

      time(testParquet(sql))
      maybeFlushOSCache(sc)

      time(testCSV(sc))
      maybeFlushOSCache(sc)

      time(testCassandra(sc))
      maybeFlushOSCache(sc)
    }

    def generateSchema(sc: SparkContext) = {
      println("**** Generating schema...")
      schema.asInstanceOf[Schema].create(sc.getConf)
    }

    def loadData(sql: SQLContext) = {

      val sc = sql.sparkContext
      val numPartitions = sc.defaultParallelism
      val recordsPerPartition = numRecords / numPartitions

      val collection = sc.parallelize(Seq[Int](), numPartitions)
                         .mapPartitions { _ => {
                           println(s"**** Generating $recordsPerPartition rows...")
                           schema.generateRows(recordsPerPartition, numClusterings, keyLength).iterator
                         }}.cache()

      collection.saveToCassandra(schema.keyspace, schema.table, schema.columns())
      collection.saveAsTextFile(csvFile)

      val df = sql.createDataFrame(collection)
      df.write.parquet(parquetFile)

      "Data generation"
    }

    def clearHDFSData() = {
      println("**** Clearing HDFS data...")
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsHost), hadoopConf)

      hdfs.delete(new org.apache.hadoop.fs.Path(csvFile), true)
      hdfs.delete(new org.apache.hadoop.fs.Path(parquetFile), true)
    }

    def time[A](f: => A) = {
      val s = System.nanoTime
      val testName = f
      val time = (System.nanoTime - s) / 1e9
      println(s"**** Total time for $testName: $time s")
    }

    def maybeFlushOSCache(sc: SparkContext): Unit = {
      if (!flushOSCache)
        return

      val empty = sc.parallelize(Seq[Int]()).mapPartitions { _ => {
        println("Flushing OS cache...")
        println("free && sync && echo 3 > /proc/sys/vm/drop_caches && free" !!)
        Seq[Int]().iterator
      }}

      empty.foreach(_ => ()) //just to make sure the RDD is created so the action in mapPartitions() is done
    }

    def testCassandra(sc: SparkContext) = {
      processRdd(sc.cassandraTable(schema.keyspace, schema.table).map(r => schema.fromCassandra(r)), "Cassandra")
    }

    def testParquet(sql: SQLContext) = {
      processRdd(sql.read.parquet(parquetFile).rdd.map(r => schema.fromParquet(r)), "HDFS Parquet")
    }

    def testCSV(sc: SparkContext) = {
      processRdd(sc.textFile(csvFile).map(s => schema.fromString(s)), "HDFS csv")
    }

    def processRdd(rdd: RDD[Row], testName: String) = {
      val total = rdd.count()
      val result = rdd.map(row => schema.max(row)).reduce((a, b) => if (a > b) a else b)
      println(s"**** $testName - max of $total is $result")
      testName
    }
  }
}
