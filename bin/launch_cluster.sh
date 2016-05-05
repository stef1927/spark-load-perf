#!/bin/bash

shopt -s expand_aliases
source ~/.bash_aliases

set -xv #echo on

ROOT_PATH=/home/stefi/git/cstar/benchmarks/spark-load-perf

# Create the cluster
c launch -i n1-standard-8 ste 5

# Install and Start Cassandra
#c install -i source --branch-name=trunk --cass-git-repo=https://github.com/stef1927/cassandra.git -n 256 -s 5 ste cassandra
c install -i source --branch-name=9259 --cass-git-repo=https://github.com/stef1927/cassandra.git -n 256 -s 5 ste cassandra
c start -s ste cassandra
c run ste 0 'nodetool status'

# Extract cluster private ip addresses
hosts=`c info ste | grep "private hostname" | cut -d' ' -f 3 | tr '\n' ','`
echo ${hosts}


# Install and Start HDFS
# https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html
c scp ste all ${ROOT_PATH}/bin/install_hdfs.sh /home/automaton/install_hdfs.sh
c run ste all "chmod +x install_hdfs.sh && ./install_hdfs.sh ${hosts}"
c run ste all "sed -i'' -e 's/\${JAVA_HOME}/\/usr\/lib\/jvm\/jdk1.8.0_40/' ~/hadoop-2.6.4/etc/hadoop/hadoop-env.sh"
c run ste 0 './hadoop-2.6.4/bin/hdfs namenode -format ste_hdfs'
c run ste 0 './hadoop-2.6.4/sbin/start-dfs.sh'
c run ste 0 './hadoop-2.6.4/bin/hdfs dfs -mkdir /user'

# Install and Start Spark
# http://spark.apache.org/docs/latest/spark-standalone.html
c scp ste all ${ROOT_PATH}/bin/install_spark.sh /home/automaton/install_spark.sh
c run ste all "chmod +x install_spark.sh && ./install_spark.sh ${hosts}"
c run ste 0 'spark-1.6.1-bin-hadoop2.6/sbin/start-all.sh'


# Monitoring
# ssh port forwarding:
# c ssh ste 0 -- -L 8080:localhost:8080 -L 50070:localhost:50070 -L 9000:localhost:9000 -L 7077:localhost:7077 -L 9042:localhost:9042
# To check which ports are open: "sudo netstap -tulpn | grep LISTEN"
# Monitor the status of the HDFS cluster via the name node Web API: http://name_node_host:50070/ (must forward port via SSH or use public IP)
# Monitor the status of the Spark cluster via the master Web API: http://spark_master_host:8080/ (must forward port via SSH or use public IP)

# Running the client on the master host
c run ste 0 'echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list'
c run ste 0 "sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823"
c run ste 0 "sudo apt-get -y update"
c run ste 0 "sudo apt-get -y install sbt"
c run ste 0 "git clone https://github.com/stef1927/spark-load-perf.git"
c run ste 0 "mkdir spark-load-perf/lib && mkdir spark-load-perf/lib-pre-optim"
c scp ste 0 ${ROOT_PATH}/lib-pre-optim/spark-cassandra-connector-assembly-1.6.0-M2.jar /home/automaton/spark-load-perf/lib-pre-optim
c scp ste 0 ${ROOT_PATH}/lib/spark-cassandra-connector-assembly-1.6.0-M2.jar /home/automaton/spark-load-perf/lib
c scp ste all ${ROOT_PATH}/profiling-advanced.jfc /home/automaton

# Install some monitoring utilities
c run ste all 'sudo apt-get -y install dstat htop'

#Build
c run ste 0 "cd spark-load-perf && sbt assembly"

echo "Sample launch command"
echo "../spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class Benchmark --master spark://10.240.0.2:7077 \
     target/scala-2.10/spark-load-perf-assembly-1.0.jar --hdfs-host hdfs://10.240.0.2:9000 --num-records 15000000 \
     --flush-os-cache --compact --workers ${hosts} --num-generate-partitions 30 --split-size-mb 32 --num-repetitions 5 --schemas 1 | tee results.txt"

set +xv #echo off

# Schema 1 parameters to ensure 30-40 partitions:
# --num-generate-partitions 30 --split-size-mb 32 --num-repetitions 5 --schemas 1

# Schema 3 parameters to ensure 30-40 partitions:
# --num-generate-partitions 30 --split-size-mb 64 --num-repetitions 5 --schemas 3


# To launch with JFR add use jcmd on the spark executors (the JVM options are set in spark-defaults.conf):
#alias jcmd='/usr/lib/jvm/jdk1.8.0_40/bin/jcmd'
#jcmd <= will show the spark executors
#jcmd PID JFR.start settings=/home/automaton/profiling-advanced.jfc filename=benchmark.jfr dumponexit=true
#jcmd PID JFR.dump recording=1 filename=/home/automaton/benchmark.jfr

#To record JFR files for cassandra:
#export JVM_OPTS="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints"
#and restart, then use jcmd to start and stop recording

# Sample launch command for testing locally:
#$SPARK_HOME/bin/spark-submit --class Benchmark --master local[4] target/scala-2.10/spark-load-perf-assembly-1.0.jar --num-records 100000 --schemas 1| tee results.txt

# Sample launch command for profiling locally with JFR
#$SPARK_HOME/bin/spark-submit --class Benchmark --master local[4] --conf "spark.driver.extraJavaOptions=-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints" target/scala-2.10/spark-load-perf-assembly-1.0.jar --num-records 2500000 --schemas 1 --split-size-mb 16 --num-repetitions 3 | tee results.txt
# Then use:jcmd PID JFR.start settings=/home/stefi/profiling-advanced.jfc filename=benchmark.jfr dumponexit=true