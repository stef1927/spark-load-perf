import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._

object Schema {
  def create(num: Int) = {
    num match {
      case 1 => new Schema("schema1", 1)
      case 2 => new Schema("schema2", 2)
      case 3 => new Schema("schema3", 3)
      case 4 => new Schema("schema4", 4)
    }
  }
}

/**
  * The full row as it is stored in Cassandra, Parquet or CSV. This is used by schema 1 and 2, which
  * store multiple values directly in the data store.
  */
case class ValuesRow(id: String, timestamp: Long, val1: Int, val2: Int, val3: Int, val4: Int,
               val5: Int, val6: Int, val7: Int, val8: Int, val9: Int, val10: Int) {

  override def toString = s"$id,$timestamp,$val1,$val2,$val3,$val4,$val5,$val6,$val7,$val8,$val9,$val10"
}

object ValuesRow {
  val csvSchema = StructType(Array(
      StructField("id", StringType, nullable=false),
      StructField("tstamp", LongType, nullable=false),
      StructField("val1", IntegerType),
      StructField("val2", IntegerType),
      StructField("val3", IntegerType),
      StructField("val4", IntegerType),
      StructField("val5", IntegerType),
      StructField("val6", IntegerType),
      StructField("val7", IntegerType),
      StructField("val8", IntegerType),
      StructField("val9", IntegerType),
      StructField("val10", IntegerType)))
}


/**
  * The full row as it is stored in Cassandra, Parquet or CSV. This is used by schema 3 and 4, which
  * store multiple values as a blob.
  */
case class BlobRow(id: String, timestamp: Long, data: String) {

  override def toString = s"$id,$timestamp,$data"
}

object BlobRow {
  val csvSchema = StructType(Array(
    StructField("id", StringType, nullable=false),
    StructField("tstamp", LongType, nullable=false),
    StructField("data", StringType)))

  val sep = '|'
}

/**
  * A view on the full row as returned by the SELECT query (SELECT val2, val3)
  */
case class ResultRow(val2: Int, val3: Int)

@SerialVersionUID(100L)
class Schema(name: String, num: Int) extends Serializable {
  val table = name
  val keyspace = "ks"

  def create(conf: SparkConf) = {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")

      num match {
        case 1 =>
          session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (id TEXT, timestamp BIGINT, " +
                           "val1 INT, val2 INT, val3 INT, val4 INT, val5 INT, val6 INT, val7 INT, val8 INT, val9 INT, val10 INT, " +
                           "PRIMARY KEY (id, timestamp))") // partition and clustering keys
        case 2 =>
          session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (id TEXT, timestamp BIGINT, " +
                           "val1 INT, val2 INT, val3 INT, val4 INT, val5 INT, val6 INT, val7 INT, val8 INT, val9 INT, val10 INT, " +
                           "PRIMARY KEY ((id, timestamp)))") // composite partition key, no clustering key

        case 3 =>
          session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (id TEXT, timestamp BIGINT, data TEXT," +
                           "PRIMARY KEY (id, timestamp))") // partition and clustering keys

        case 4 =>
          session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (id TEXT, timestamp BIGINT, data TEXT," +
                           "PRIMARY KEY ((id, timestamp)))") // composite partition key, no clustering key
      }

      session.execute(s"TRUNCATE $keyspace.$table")
    }
  }

  def getCSVSchema = num match {
    case 1 | 2 =>
      ValuesRow.csvSchema
    case 3 | 4 =>
      BlobRow.csvSchema
  }

  def generateRows(numRecords: Int, numClusterings: Int, keyLength: Int) = {
    val numPartitions = numRecords / numClusterings
    val ret = (1 to numPartitions).map(_ => randomString(keyLength))
                           .flatMap(id => (1 to numClusterings).map(_ => randomRow(id)))

    val left = numRecords % numClusterings
    if (left > 0) {
      val id = randomString(keyLength)
      ret ++ (1 to left).map(_ => randomRow(id))
    }
    else {
      ret
    }
  }

  def randomString(len: Int) = scala.util.Random.alphanumeric.take(len).mkString

  def randomRow(id: String) = {
    val values = (1 to 10).map(_ => randomInt)
    num match {
      case 1 | 2 =>
        Seq(id, randomLong) ++ values
      case 3 | 4 =>
        Seq(id, randomLong, values.mkString(BlobRow.sep.toString))
    }
  }

  def randomInt = scala.util.Random.nextInt

  def randomLong = scala.util.Random.nextLong

  def createDataFrame(sqlContext: SQLContext, rdd: RDD[Seq[Any]]) = num match {
    case 1 | 2 =>
      sqlContext.createDataFrame(rdd.map(r => ValuesRow(r(0).asInstanceOf[String],
        r(1).asInstanceOf[Long], r(2).asInstanceOf[Int], r(3).asInstanceOf[Int], r(4).asInstanceOf[Int],
        r(5).asInstanceOf[Int], r(6).asInstanceOf[Int], r(7).asInstanceOf[Int], r(8).asInstanceOf[Int],
        r(9).asInstanceOf[Int], r(10).asInstanceOf[Int], r(11).asInstanceOf[Int])))
    case 3 | 4 =>
      sqlContext.createDataFrame(rdd.map(r => BlobRow(r(0).asInstanceOf[String],
        r(1).asInstanceOf[Long], r(2).asInstanceOf[String])))
  }

  def getSelectColumns = num match {
    case 1 | 2 =>
      Array[String]("val2", "val3")
    case 3 | 4 =>
      Array[String]("data")
  }

  def fromCassandra(row: com.datastax.spark.connector.CassandraRow) = num match {
    case 1 | 2 =>
      ResultRow(row.getInt("val2"), row.getInt("val3"))
    case 3 | 4 =>
      val s = row.getString("data").split(BlobRow.sep)
      ResultRow(s(1).trim.toInt, s(2).trim.toInt)
  }

  def fromParquet(row: org.apache.spark.sql.Row) = num match {
    case 1 | 2 =>
      ResultRow(row.getInt(3), row.getInt(4))
    case 3 | 4 =>
      val s = row.getString(2).split(BlobRow.sep)
      ResultRow(s(1).trim.toInt, s(2).trim.toInt)
  }

  def fromString(row: String) = {
    val s = row.split(',')
    num match {
      case 1 | 2 =>
        ResultRow(s(3).trim.toInt, s(4).trim.toInt)
      case 3| 4 =>
        val fields = s(2).split(BlobRow.sep)
        ResultRow(fields(1).trim.toInt, fields(2).trim.toInt)
    }
  }

  def max(row: ResultRow) = Math.max(row.val2, row.val3)

  def max(row: org.apache.spark.sql.Row) = num match {
    case 1 | 2 =>
      Math.max(row.getInt(0), row.getInt(1))
    case 3 | 4 =>
      val values = row.getString(0).split(BlobRow.sep)
      Math.max(values(1).trim.toInt, values(2).trim.toInt)
  }

  override def toString = s"$keyspace.$table"
}

