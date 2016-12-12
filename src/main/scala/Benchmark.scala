import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext


import scala.collection.mutable
import scala.util.Random
import sys.process._
import language.postfixOps

object Benchmark {
  type OptionMap = Map[Symbol, Any]

  def main(args: Array[String]) {
    val options = parseOptions(args)

    val conf = new SparkConf()
                 .setAppName("spark-load-test")
                 .set("spark.cassandra.connection.host", options('cassandraHost).asInstanceOf[String])
                 .set("spark.cassandra.input.metrics", "false")
                 .set("spark.cassandra.input.split.size_in_mb", options('splitSize).asInstanceOf[String])


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
      "\t--hdfs-host hdfs://localhost:9000\n" +
      "\t--hdfs-path /user\n" +
      "\t--schemas 1,3\n" +
      "\t--num-records 100000\n" +
      "\t--num-timestamps 100\n" +
      "\t--key-length 25\n" +
      "\t--num-generate-partitions 10\n" +
      "\t--split-size-mb 64\n" +
      "\t--num-repetitions 1\n" +
      "\t--flush-os-cache\n" +
      "\t--compact\n" +
      "\t--workers localhost\n" +
      "\t--max-pages-second 0\n" +
       "WARNING: only flush the OS cache in test environments!!!\n" +
      "To select the schemas to test pass a number between 1 and 4 " +
      "or a command separated list of numbers between 1 and 4"

    val arglist = args.toList

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--cassandra-host" :: value :: tail =>
          nextOption(map - 'cassandraHost ++ Map('cassandraHost -> value.toString), tail)
        case "--hdfs-host" :: value :: tail =>
          nextOption(map - 'hdfsHost ++ Map('hdfsHost -> value.toString), tail)
        case "--hdfs-path" :: value :: tail =>
          nextOption(map - 'hdfsPath ++ Map('hdfsPath -> value.toString), tail)
        case "--schemas" :: value :: tail =>
          nextOption(map - 'schemas ++ Map('schemas -> value.split(',').map(n => Schema.create(n.trim.toInt)).to[Seq]), tail)
        case "--num-records" :: value :: tail =>
          nextOption(map - 'numRecords ++ Map('numRecords -> value.toInt), tail)
        case "--num-timestamps" :: value :: tail =>
          nextOption(map - 'numTimestamps ++ Map('numTimestamps -> value.toInt), tail)
        case "--key-length" :: value :: tail =>
          nextOption(map - 'keyLength ++ Map('keyLength -> value.toInt), tail)
        case "--num-generate-partitions" :: value :: tail =>
          nextOption(map - 'generatePartitions ++ Map('generatePartitions -> value.toInt), tail)
        case "--split-size-mb" :: value :: tail =>
          nextOption(map - 'splitSize ++ Map('splitSize -> value.toString), tail)
        case "--num-repetitions" :: value :: tail =>
          nextOption(map - 'numRepetitions ++ Map('numRepetitions -> value.toInt), tail)
        case "--flush-os-cache" :: tail =>
          nextOption(map - 'flushOSCache ++ Map('flushOSCache -> true), tail)
        case "--compact" :: tail =>
          nextOption(map - 'compact ++ Map('compact -> true), tail)
        case "--workers" :: value :: tail =>
          nextOption(map - 'workers ++ Map('workers -> value.split(',').toSeq), tail)
        case "--max-pages-second" :: value :: tail =>
          nextOption(map - 'maxPagesSecond ++ Map('maxPagesSecond -> value.toInt), tail)
        case option :: tail =>
          println("Unknown option " + option)
          println(usage)
          sys.exit(0)
      }
    }

    val options = nextOption(Map('cassandraHost -> "127.0.0.1",
                                 'hdfsHost -> "hdfs://localhost:9000",
                                 'hdfsPath -> "/user",
                                 'schemas -> Seq(Schema.create(1), Schema.create(3)),
                                 'numRecords -> 1000,
                                 'numTimestamps -> 100,
                                 'keyLength -> 25,
                                 'generatePartitions -> 10,
                                 'splitSize -> "64",
                                 'flushOSCache -> false,
                                 'numRepetitions -> 1,
                                 'compact -> false,
                                 'workers -> Seq("localhost"),
                                 'maxPagesSecond -> 0),
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
    val numTimestamps = options('numTimestamps).asInstanceOf[Int]
    val keyLength = options('keyLength).asInstanceOf[Int]
    val generatePartitions = options('generatePartitions).asInstanceOf[Int]
    val numRepetitions = options('numRepetitions).asInstanceOf[Int]
    val compact = options('compact).asInstanceOf[Boolean]
    val workers = options('workers).asInstanceOf[Seq[String]]
    val maxPagesSecond = options('maxPagesSecond).asInstanceOf[Int]

    val seed = System.nanoTime();
    println(s"Using seed $seed")
    Random.setSeed(seed)

    def run( sc: SparkContext, sqlContext: SQLContext): Unit = {
      println(s"Testing $schema...")

      setUp(sc, sqlContext)

      type ResultMap = Map[String, Seq[(Long, Int, Double)]]

      val tests = mutable.LinkedHashMap(
        "parquet_rdd" -> testParquet_RDD _,
        "parquet_df" -> testParquet_DF _,
        "csv_rdd" -> testCSV_RDD _,
        "csv_df" -> testCSV_DF _,
        "cassandra_rdd" -> testCassandra_RDD _,
        "cassandra_rdd_async" -> testCassandra_RDD_async _,
        "cassandra_df" -> testCassandra_DF _,
        "cassandra_df_async" -> testCassandra_DF_async _
      )


      val testNames = tests.keys.toList

      def nextTest(results: ResultMap, testNames: Seq[String]): ResultMap = testNames match {
        case Nil => results
        case name :: tail => nextTest(results ++ Map(name -> Seq(test(name, tests(name), sqlContext))), tail)
      }

      def nextRep(rep: Int): ResultMap = {
        println(s"Repetitions nr. $rep")
        val results = nextTest(Map(), Random.shuffle(testNames))

        println("                     Test|       Count|      Result|      Time")
        for (test <- testNames) {
          val result = results(test)
          println("%25s|%12d|%12d|%f".format(test, result(0)._1, result(0)._2, result(0)._3));
        }

        results
      }

      val results = (1 to numRepetitions).map(r => nextRep(r))
                                         .reduce((m1, m2) => m1.flatMap{ case (k,v) =>
                                           Map(k -> (m1.getOrElse(k, Seq()) ++ m2.getOrElse(k, Seq())))})

      println("");
      println("#############")
      println("# Averages: #")
      println("#############")

      println("                     Test|        Time|    Std. Dev")
      for (test <- testNames) {
        val times = results.getOrElse(test, Seq((0L, 0L, 0.))).drop(1).map(r => r._3)
        val count = times.length
        val mean = times.sum / count
        val devs = times.map(t => (t - mean) * (t - mean))
        val stddev = Math.sqrt(devs.sum / count)
        println("%25s|%12f|%12f".format(test, mean, stddev));
      }
    }

    def setUp(sc: SparkContext, sqlContext: SQLContext) = {
      generateSchema(sc)
      clearHDFSData()

      loadData(sqlContext)

      maybeCompactCassandraTables(sc)
      maybeFlushOSCache(sc)
    }

    def generateSchema(sc: SparkContext) = {
      schema.asInstanceOf[Schema].create(sc.getConf)
    }

    def clearHDFSData() = {
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsHost), hadoopConf)

      hdfs.delete(new org.apache.hadoop.fs.Path(csvFile), true)
      hdfs.delete(new org.apache.hadoop.fs.Path(parquetFile), true)
    }

    def loadData(sqlContext: SQLContext) = {
      val s = System.nanoTime
      val sc = sqlContext.sparkContext
      val recordsPerPartition = numRecords / generatePartitions

      val collection = sc.parallelize(Seq[Int](), generatePartitions)
                         .mapPartitions { _ => {
                           schema.generateRows(recordsPerPartition, numTimestamps, keyLength).iterator
                         }}

      val df = schema.createDataFrame(sqlContext, collection).cache()
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

      df.unpersist(true)
    }

    def maybeCompactCassandraTables(sc: SparkContext) {
      if (!compact)
        return

      val ks = schema.keyspace
      val table = schema.table

      workers.par.map(worker => {
        println(s"Flushing Cassandra tables on $worker...")
        println(s"ssh $worker ~/cassandra-src/bin/nodetool flush $ks $table" !)

        println(s"Compacting Cassandra tables on $worker...")
        println(s"ssh $worker ~/cassandra-src/bin/nodetool compact $ks $table" !)

        println(s"Refreshing size estimates on $worker...")
        println(s"ssh $worker ~/cassandra-src/bin/nodetool refreshsizeestimates" !)
      })

    }

    def maybeFlushOSCache(sc: SparkContext) {
      if (!flushOSCache)
        return

      workers.par.map(worker => {
        s"ssh $worker sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null" !

        s"ssh $worker ~/cassandra-src/bin/nodetool invalidatekeycache" !

        s"ssh $worker ~/cassandra-src/bin/nodetool invalidaterowcache" !
      })
    }

    def test[A, B](testName: String, f: SQLContext => (A, B), sqlContext: SQLContext) = {
      print(s"Starting $testName...")
      val ret = time(f(sqlContext))
      println("...OK")
      maybeFlushOSCache(sqlContext.sparkContext)
      ret
    }

    def time[A, B](f: => (A, B)) = {
      val s = System.nanoTime
      val ret = f
      (ret._1, ret._2, (System.nanoTime - s) / 1e9)
    }

    def testCassandra_RDD(sqlContext: SQLContext) = {
      _testCassandra_RDD(sqlContext, false)
    }


    def testCassandra_RDD_async(sqlContext: SQLContext) = {
      _testCassandra_RDD(sqlContext, true)
    }

    /**
      * Test a Cassandra RDDs using Case Classes.
      *
      * @param sqlContext the Spark SQL context
      * @param asyncPaging indicates if we should use asynchronous paging
      * @return
      */
    def _testCassandra_RDD(sqlContext: SQLContext, asyncPaging: Boolean) = {
      val sc = sqlContext.sparkContext
      val conf = sc.getConf
      conf.set("spark.cassandra.input.async.paging.enabled", asyncPaging.toString)
      conf.set("spark.cassandra.input.async.paging.max_pages_second", maxPagesSecond.toString)

      schema.num match {
        case 1 | 2 =>
          processRdd(sc.cassandraTable[ValuesRow](schema.keyspace, schema.table)
                       .withReadConf(ReadConf.fromSparkConf(conf))
                       .map(r => ResultRow(r.val2, r.val3)))
        case 3 | 4 =>
          processRdd(sc.cassandraTable[BlobRow](schema.keyspace, schema.table)
            .withReadConf(ReadConf.fromSparkConf(conf))
            .map(r => schema.dataToResult( r.data)))
      }
    }

    def testCassandra_DF(sqlContext: SQLContext) = {
      _testCassandra_DF(sqlContext, false)
    }

    def testCassandra_DF_async(sqlContext: SQLContext) = {
      _testCassandra_DF(sqlContext, true)
    }

    /**
      * Test a Cassandra data frames (DFs). Note that we also push the predicate
      * to the server so this test will only decode 2 rows, rather than the full table.
      *
      * @param sqlContext the Spark SQL context
      * @param asyncPaging indicates if we should use asynchrouns paging
      * @return
      */
    def _testCassandra_DF(sqlContext: SQLContext, asyncPaging: Boolean) = {
      val sc = sqlContext.sparkContext
      val cc = new HiveContext(sc)
      cc.setConf("spark.cassandra.input.async.paging.enabled", asyncPaging.toString)
      cc.setConf("spark.cassandra.input.async.paging.max_pages_second", maxPagesSecond.toString)
      val cols = schema.getSelectColumns.mkString(",")
      val table = schema.keyspace + "." + schema.table
      processDataFrame(cc.sql(s"SELECT $cols FROM $table"))
    }

    def testParquet_RDD(sqlContext: SQLContext) = {
      processRdd(sqlContext.read.parquet(parquetFile).rdd.map(r => schema.fromParquet(r)))
    }

    def testParquet_DF(sqlContext: SQLContext) = {
      val cols = schema.getSelectColumns.mkString(",")
      processDataFrame(sqlContext.sql(s"SELECT $cols FROM parquet.`$parquetFile`"))
    }

    def testCSV_RDD(sqlContext: SQLContext) = {
      val sc = sqlContext.sparkContext
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

    def processRdd(rdd: RDD[ResultRow]): (Long, Int) = {
      val ret = (rdd.count(), rdd.map(row => schema.max(row)).reduce((a, b) => if (a > b) a else b))
      rdd.unpersist(false)
      ret
    }

    def processDataFrame(df: DataFrame): (Long, Int) = {
      val ret = (df.count(), df.map(row => schema.max(row)).reduce((a, b) => if (a > b) a else b))
      df.unpersist(false)
      ret
    }
  }
}
