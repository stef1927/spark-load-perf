import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object Benchmark {
  val appName = "spark-load-test"
  val sparkMaster = "local[4]"
  val cassandraHost = "127.0.0.1"
  val hdfsHost = "hdfs://localhost:9000"

  val csvFile = hdfsHost + "/user/stef/test.csv"
  val parquetFile = hdfsHost + "/user/stef/test.parquet"

  val keyspace = "ks"
  val table = "table1"

  val keyLength = 10
  val numRecords = 100000

  case class Row(id: String, val1: Int, val2: Int) {
    override def toString() = s"$id,$val1,$val2"
  }

  object Row {
    def declaration() = "(id text PRIMARY KEY, val1 int, val2 int)"

    def columns() = SomeColumns("id", "val1", "val2")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(appName)
      .setMaster(sparkMaster)
      .set("spark.cassandra.connection.host", cassandraHost)

    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    createSchema(conf)
    loadData(sql)

    time(testParquet(sql))
    time(testHDFS(sc))
    time(testCassandra(sc))

    sc.stop()
  }

  def createSchema(conf: SparkConf) = {
    println("**** Creating schema...")
    val tableColumns = Row.declaration()
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table $tableColumns")
      session.execute(s"TRUNCATE $keyspace.$table")
    }

    clearHDFSData()
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

  def testParquet(sql: SQLContext) = {
    processRdd(sql.read.parquet(parquetFile).rdd.map(r => Row(r.getString(0), r.getInt(1), r.getInt(2))), "Parquet")
  }

  def testHDFS(sc: SparkContext)  = {
    processRdd(sc.textFile(csvFile).map(s => s.split(',')).map(p => Row(p(0), p(1).trim.toInt, p(2).trim.toInt)), "HDFS")
  }

  def testCassandra(sc: SparkContext) = {
    processRdd(sc.cassandraTable(keyspace, table), "Cassandra")
  }

  def processRdd(rdd: RDD[Row], testName: String) = {
    val total = rdd.count()
    val result = rdd.map(row => Math.max(row.val1, row.val2)).reduce((a,b) => if (a > b) a else b)
    println(s"**** $testName - max of $total is $result")
    testName
  }

  def loadData(sql: SQLContext) = {
    println("**** Generating data...")
    val collection = sql.sparkContext.parallelize(Seq.fill(numRecords)(new Row(randomString(keyLength), randomInt(), randomInt())))

    collection.saveToCassandra(keyspace, table, Row.columns())
    collection.saveAsTextFile(csvFile)

    val df = sql.createDataFrame(collection)
    df.write.parquet(parquetFile)
  }

  def randomString(length: Int) = scala.util.Random.alphanumeric.take(length).mkString
  def randomInt() = scala.util.Random.nextInt()
}
