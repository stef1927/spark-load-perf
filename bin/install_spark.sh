#!/bin/bash

master_ip=$1
slave_ips=$2
local_ip=`hostname -I | xargs` 

echo "Master IP ${master_ip}, Local IP ${local_ip}"
echo "Slaves: ${slave_ips}"

# http://spark.apache.org/docs/latest/spark-standalone.html
wget http://apache.communilink.net/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
tar -xvf spark-1.6.1-bin-hadoop2.6.tgz && rm spark-1.6.1-bin-hadoop2.6.tgz

if [ "${master_ip}" != "${local_ip}" ]
then
    echo "Not the master host, exiting"
    exit
fi

echo "Configuring spark for master host"

echo "${master_ip}" > spark-1.6.1-bin-hadoop2.6/conf/slaves
export IFS=","
for ip in ${slave_ips}; do
  echo "$ip" >> spark-1.6.1-bin-hadoop2.6/conf/slaves
done

cat >spark-1.6.1-bin-hadoop2.6/conf/spark-env.sh <<EOL
SPARK_MASTER_IP=$master_ip
SPARK_LOCAL_IP=$master_ip
SPARK_PUBLIC_DNS=$master_ip
#SPARK_WORKER_CORES=8
EOL

