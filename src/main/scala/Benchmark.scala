import com.datastax.spark.connector._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext

import sys.process._
import language.postfixOps

object Benchmark {
  type OptionMap = Map[Symbol, Any]

  def main(args: Array[String]) {
    val options = parseOptions(args)

    val conf = new SparkConf()
                 .setAppName("spark-load-test")
                 .setMaster(options('sparkMaster).asInstanceOf[String])
                 .set("spark.cassandra.connection.host", options('cassandraHost).asInstanceOf[String])

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    try {
      for (schema <- options('schemas).asInstanceOf[Seq[Schema]])
        new Run(options, schema).run(sc, sqlContext)
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
      "\t--schemas 1,3\n" +
      "\t--num-records 100000\n" +
      "\t--num-clusterings 100\n" +
      "\t--key-length 25\n" +
      "\t--flush-os-cache\n" +
      "WARNING: only flush the OS cache in test environments!!!\n" +
      "To select the schemas to test pass a number between 1 and 4 " +
      "or a command separated list of numbers between 1 and 4"

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
        case "--schemas" :: value :: tail =>
          nextOption(map - 'schemas ++ Map('schemas -> value.split(',').map(n => Schema.create(n.trim.toInt))), tail)
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
                                 'schemas -> Seq(Schema.create(1), Schema.create(3)),
                                 'numRecords -> 1000,
                                 'numClusterings -> 100,
                                 'keyLength -> 25,
                                 'flushOSCache -> false),
                             arglist)
    println(options)
    options
  }

  @SerialVersionUID(100L)
  class Run(options: OptionMap, schema: Schema) extends Serializable {
    val hdfsHost = options('hdfsHost).asInstanceOf[String]
    val csvFile = hdfsHost + options('hdfsPath).asInstanceOf[String] + "/benchmark.csv"
    val parquetFile = hdfsHost + options('hdfsPath).asInstanceOf[String] + "/benchmark.parquet"
    val numRecords = options('numRecords).asInstanceOf[Int]
    val flushOSCache = options('flushOSCache).asInstanceOf[Boolean]
    val numClusterings = options('numClusterings).asInstanceOf[Int]
    val keyLength = options('keyLength).asInstanceOf[Int]

    def run( sc: SparkContext, sqlContext: SQLContext): Unit = {
      println(s"Testing $schema...")

      generateSchema(sc)
      clearHDFSData()

      loadData(sqlContext)

      val parquetRDD = test(testParquet_RDD(sqlContext), sc)
      val parquetDF = test(testParquet_DF(sqlContext), sc)

      val csvRDD = test(testCSV_RDD(sc), sc)
      val csvDF = test(testCSV_DF(sqlContext), sc)

      val cassandraRDD = test(testCassandra_RDD(sc), sc)
      val cassandraDF = test(testCassandra_DF(sc), sc)

      println("Test         |       Count|      Result|      Time")
      println("Parquet RDD  |%12d|%12d|%f".format(parquetRDD._1, parquetRDD._2, parquetRDD._3));
      println("Parquet DF   |%12d|%12d|%f".format(parquetDF._1, parquetDF._2, parquetDF._3));
      println("Csv RDD      |%12d|%12d|%f".format(csvRDD._1, csvRDD._2, csvRDD._3));
      println("Csv DF       |%12d|%12d|%f".format(csvDF._1, csvDF._2, csvDF._3));
      println("Cassandra RDD|%12d|%12d|%f".format(cassandraRDD._1, cassandraRDD._2, cassandraRDD._3));
      println("Cassandra DF |%12d|%12d|%f".format(cassandraDF._1, cassandraDF._2, cassandraDF._3));
    }

    def generateSchema(sc: SparkContext) = {
      schema.asInstanceOf[Schema].create(sc.getConf)
    }

    def loadData(sqlContext: SQLContext) = {
      val s = System.nanoTime
      val sc = sqlContext.sparkContext
      val numPartitions = sc.defaultParallelism
      val recordsPerPartition = numRecords / numPartitions

      val collection = sc.parallelize(Seq[Int](), numPartitions)
                         .mapPartitions { _ => {
                           schema.generateRows(recordsPerPartition, numClusterings, keyLength).iterator
                         }}.cache()

      val df = schema.createDataFrame(sqlContext, collection)
      df.write.parquet(parquetFile)

      df.write.format("org.apache.spark.sql.cassandra")
              .options(Map( "table" -> schema.table, "keyspace" -> schema.keyspace))
              .mode(SaveMode.Append)
              .save()

      df.write.format("com.databricks.spark.csv")
              .option("header", "false")
              .save(csvFile)

      val time = (System.nanoTime - s) / 1e9
      println(s"Data loading took $time seconds")
    }

    def clearHDFSData() = {
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsHost), hadoopConf)

      hdfs.delete(new org.apache.hadoop.fs.Path(csvFile), true)
      hdfs.delete(new org.apache.hadoop.fs.Path(parquetFile), true)
    }

    def test[A, B](f: => (A, B), sc: SparkContext) = {
      val ret = time(f)
      maybeFlushOSCache(sc)
      ret
    }

    def time[A, B](f: => (A, B)) = {
      val s = System.nanoTime
      val ret = f
      (ret._1, ret._2, (System.nanoTime - s) / 1e9)
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

    def testCassandra_RDD(sc: SparkContext) = {
      processRdd(sc.cassandraTable(schema.keyspace, schema.table).map(r => schema.fromCassandra(r)))
    }

    def testCassandra_DF(sc: SparkContext) = {
      val cc = new CassandraSQLContext(sc)
      val cols = schema.getSelectColumns.mkString(",")
      val table = schema.keyspace + "." + schema.table
      processDataFrame(cc.sql(s"SELECT $cols FROM $table"))
    }

    def testParquet_RDD(sql: SQLContext) = {
      processRdd(sql.read.parquet(parquetFile).rdd.map(r => schema.fromParquet(r)))
    }

    def testParquet_DF(sqlContext: SQLContext) = {
      val cols = schema.getSelectColumns.mkString(",")
      processDataFrame(sqlContext.sql(s"SELECT $cols FROM parquet.`$parquetFile`"))
    }

    def testCSV_RDD(sc: SparkContext) = {
      processRdd(sc.textFile(csvFile).map(s => schema.fromString(s)))
    }

    def testCSV_DF(sqlContext: SQLContext) = {
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .schema(schema.getCSVSchema)
        .load(csvFile)

      val columns = schema.getSelectColumns
      processDataFrame(df.select(columns(0), columns.drop(1):_*))
    }

    def processRdd(rdd: RDD[ResultRow]) = {
      (rdd.count(), rdd.map(row => schema.max(row)).reduce((a, b) => if (a > b) a else b))
    }

    def processDataFrame(df: DataFrame) = {
      (df.count(), df.map(row => schema.max(row)).reduce((a, b) => if (a > b) a else b))
    }
  }
}
