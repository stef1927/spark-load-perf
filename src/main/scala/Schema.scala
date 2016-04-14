import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

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

case class Row(id: String, timestamp: Long, vals: Array[Int]) {
  val val1 = vals(0)  // this is for the Cassandra columns schemas
  val val2 = vals(1)
  val val3 = vals(2)
  val val4 = vals(3)
  val val5 = vals(4)
  val val6 = vals(5)
  val val7 = vals(6)
  val val8 = vals(7)
  val val9 = vals(8)
  val val10 = vals(9)

  val data = vals.mkString(",") // this is for the Cassandra blob schemas

  override def toString = s"$id,$timestamp,$data" // this is for the CSV writer
}

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

  def columns() = num match {
    case 1 | 2 =>
      SomeColumns("id", "timestamp", "val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9", "val10")
    case 3 | 4 =>
      SomeColumns("id", "timestamp", "data")
  }

  def generateRows(numRecords: Int, numClusterings: Int, keyLength: Int) = {
    val numPartitions = numRecords / numClusterings
    val ret = (1 to numPartitions).map(_ => randomString(keyLength))
                           .map(id => (1 to numClusterings)
                             .map(_ => Row(id, randomLong(), randomVals(10)))).flatten

    val left = numRecords % numClusterings
    if (left > 0) {
      val id = randomString(keyLength)
      ret ++ (1 to left).map(_ => Row(id, randomLong(), randomVals(10)))
    }
    else {
      ret
    }
  }

  def fromCassandra(row: com.datastax.spark.connector.CassandraRow) = num match {
    case 1 | 2 =>
      Row(row.getString(0), row.getLong(1), Array[Int]() ++ row.iterator.drop(2).map(v => v.asInstanceOf[Int]))
    case 3 | 4 =>
      Row(row.getString(0), row.getLong(1), row.getString(2).split(',').map(s => s.trim.toInt))
  }

  def fromParquet(row: org.apache.spark.sql.Row) = {
      Row(row.getString(0), row.getLong(1), Array[Int]() ++ row.getSeq[Int](2))
  }

  def fromString(s: String) = {
    val fields = s.split(',')
    Row(fields(0), fields(1).trim.toLong, Array[Int]() ++ fields.iterator.drop(2).map(s => s.trim.toInt))
  }

  // just pick the max between the second and third value columns
  def max(row: Row) = Math.max(row.vals(1), row.vals(2))

  def randomString(len: Int) = scala.util.Random.alphanumeric.take(len).mkString

  def randomVals(len: Int): Array[Int] = Array[Int]() ++ (0 until len).map(_ => randomInt)

  def randomInt() = scala.util.Random.nextInt

  def randomLong() = scala.util.Random.nextLong

  override def toString = s"$keyspace.$table"
}

