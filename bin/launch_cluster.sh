#!/bin/bash

# Create the cluster
c launch -i n1-standard-8 ste 5

# Install and Start Cassandra
c install -i source --branch-name=trunk --cass-git-repo=https://github.com/stef1927/cassandra.git -n 256 -s 5 ste cassandra
c start -s ste cassandra
c run ste 0 'nodetool status'

# Extract cluster private ip addresses
hosts=`c info ste | grep "private hostname" | cut -d' ' -f 3 | tr '\n' ','`
echo ${hosts}

# Install and Start HDFS
# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html
c scp ste all bin/install_hdfs.sh /home/automaton/install_hdfs.sh
c run ste all 'chmod +x install_hdfs.sh && ./install_hdfs.sh ${hosts}'
c run ste all "sed -i'' -e 's/\${JAVA_HOME}/\/usr\/lib\/jvm\/jdk1.8.0_40/' ~/hadoop-2.6.4/etc/hadoop/hadoop-env.sh"
c run ste 0 './hadoop-2.6.4/bin/hdfs namenode -format ste_hdfs'
c run ste 0 './hadoop-2.6.4/sbin/start-dfs.sh'
c run ste 0 './hadoop-2.6.4/bin/hdfs dfs -mkdir /user'

# Install and Start Spark
# http://spark.apache.org/docs/latest/spark-standalone.html
c scp ste all bin/install_spark.sh /home/automaton/install_spark.sh
c run ste all 'chmod +x install_spark.sh && ./install_spark.sh ${hosts}'
c run ste 0 'spark-1.6.1-bin-hadoop2.6/sbin/start-all.sh'


# Monitoring
# ssh port forwarding:
# c ssh ste 0 -- -L 8080:localhost:8080 -L 50070:localhost:50070 -L 9000:localhost:9000 -L 7077:localhost:7077 -L 9042:localhost:9042
# To check which ports are open: "sudo netstap -tulpn | grep LISTEN"
# Monitor the status of the HDFS cluster via the name node Web API: http://name_node_host:50070/ (must forward port via SSH or use public IP)
# Monitor the status of the Spark cluster via the master Web API: http://spark_master_host:8080/ (must forward port via SSH or use public IP)

# Running the client on the master host
# c ssh ste 0

# echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
# sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
# sudo apt-get update
# sudo apt-get install sbt
# git clone https://github.com/stef1927/spark-load-perf.git
# cd spark-load-perf/
# sbt assembly

# ../spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class Benchmark \
# --master spark://10.240.0.2:7077 target/scala-2.10/spark-load-perf-assembly-1.0.jar \
# --hdfs-host hdfs://10.240.0.2:9000 --num-records 15000000 --flush-os-cache --compact \
# --num-generate-partitions 1000 --num-repetitions 3 --schemas 4 | tee results.txt