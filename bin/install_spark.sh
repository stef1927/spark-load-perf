#!/bin/bash

export IFS=","
read -r -a addresses <<< "$1"

master_ip=${addresses[0]}
local_ip=`hostname -I | xargs` 

echo "Master IP ${master_ip}, Local IP ${local_ip}"
echo "All addresses: ${addresses[@]}"

wget http://apache.communilink.net/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
tar -xvf spark-1.6.1-bin-hadoop2.6.tgz && rm spark-1.6.1-bin-hadoop2.6.tgz

if [ "${master_ip}" != "${local_ip}" ]
then
    echo "Not the master host, exiting"
    exit
fi

echo "Configuring spark for master host"

slaves_file=spark-1.6.1-bin-hadoop2.6/conf/slaves

if [ -f ${slaves_file} ];
then
    rm ${slaves_file}
fi

for ip in "${addresses[@]}"; do
  echo "$ip" >> ${slaves_file}
done

cat >spark-1.6.1-bin-hadoop2.6/conf/spark-env.sh <<EOL
SPARK_MASTER_IP=$master_ip
SPARK_LOCAL_IP=$master_ip
SPARK_PUBLIC_DNS=$master_ip
#SPARK_WORKER_CORES=8
EOL

