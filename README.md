# spark-load-perf

A benchmark for comparing Cassandra and HDFS load times into Spark.

Data is loaded from the following sources:

* Cassandra
* A CSV file stored in HDFS
* A Parquet file stored in HDFS

The benchmark starts by generating random data and saving it into the data sources. The data saved in all sources is identical and is generated in parallel, by default with 100 parallel jobs. The CSV and Parquet files in HDFS are stored with a corresponding number of partitions.

The benchmark either retrieves a Spark RDD (Resilient Distributed Datasets) or a DF (Data Frame) via the Spark SQL context. The difference between the two is that the RDD contains the entire table or file data, whilst the data frame only contains the two columns that are used to produce the final result.

Either RDD or DF are processed by selecting the global maximum of the maximum of two columns for each row.

In total, the following tests are performed, in random order:

* **Cassandra RDD:** the entire Cassandra table is loaded into an RDD via `sc.cassandraTable`;
* **Cassandra DF:** a SELECT predicate is pushed to the server via `CassandraSQLContext`, to retrieve two columns that are saved into a data frame;
* **CSV RDD:** the CSV data is loaded into an RDD via `sc.textFile`;
* **CSV DF**: the CSV data is loaded into a DF via the spark SQL context using the `com.databricks.spark.csv` format;
* **Parquet RDD:** the Parquet data is loaded into an RDD via `sqlContext.read.parquet`;
* **Parquet DF:** a SELECT predicate is used via `SQLContext`, to retrieve two columns that are saved into a data frame.

The benchmark supports four schemas:

`CREATE TABLE ks.schema1 (id TEXT, timestamp BIGINT, val1 INT, val2 INT, val3 INT, val4 INT, val5 INT, val6 INT, val7 INT, val8 INT, val9 INT, val10 INT, PRIMARY KEY (id, timestamp))`

`CREATE TABLE ks.schema2 (id TEXT, timestamp BIGINT, val1 INT, val2 INT, val3 INT, val4 INT, val5 INT, val6 INT, val7 INT, val8 INT, val9 INT, val10 INT, PRIMARY KEY ((id, timestamp)))`

`CREATE TABLE ks.schema3 (id TEXT, timestamp BIGINT, data TEXT, PRIMARY KEY (id, timestamp))`

`CREATE TABLE ks.schama4 (id TEXT, timestamp BIGINT, data TEXT, PRIMARY KEY ((id, timestamp)))`

The first two schemas are identical except that the second schema uses a composite partition key whist the first one uses a clustering key. The same is true for the third and forth schemas. The difference between the first two schemas and the last twos is that the 10 integer values are encoded into a string in the last two schemas. This was done to measure the impact of reading multiple values from Cassandra, whilst the impact of clustering rows can be measured by comparing schemas one and two or three and four.


## Building

Use `sbt package` to build a jar file that can be submitted to a local Spark cluster, or `sbt assembly` to build a jar that can be submitted to a distributed cluster.

## Running

Submit it to a Spark cluster via `spark-summit.sh`. For example:

`spark-submit --class Benchmark --master spark://spark_master:7077 target/scala-2.10/spark-load-perf-assembly-1.0.jar --hdfs-host hdfs://hdfs_host:9000`

The following command line arguments are supported:

* `--cassandra-host 127.0.0.1` - the initial Cassandra host to contact
* `--hdfs-host hdfs://localhost:9000` - the HDFS host
* `--hdfs-path /user` - the HDFS path where files will be stored
* `--schemas 1,3` - the schemas to test
* `--num-records 100000` - the total number of rows to generate
* `--num-timestamps 100` - the number of timestamps per primary partition keys
* `--key-length 25` - the length of the primary partition key
* `--num-generate-partitions 10` - the number of parallel jobs used to generate data
* `--split-size-mb 64` - the size of each C* RDD partition in MB
* `--num-repetitions 1` - the number of times each test is repeated
* `--flush-os-cache` - when specified,  the OS cache will be flushed via `sync && echo 3 | sudo tee /proc/sys/vm/drop_caches`
* `--compact` - when specified, Cassandra tables will be flushed and compacted before starting the tests.

Use split-size-mb and num-generate-partitions to ensure the number of C* and HDFS RDD tasks is the same or comparable: num-generate-partitions 
controls the number of HDFS blocks created and hence the HDFS partitions, split-size-mb should be reduced to increase C* RDD partitions, increasing
the number of records increases the datasize and therefore the C* partitions as well.

## Creating a distributed cluster

The `bin` folder contains scripts that can be used to create a distributed cluster running Cassandra, HDFS and Spark. The scripts are based on a private tool but give nonetheless an idea of how a test environment can be set-up.

