#!/bin/bash

export IFS=","
read -r -a addresses <<< "$1"

master_ip=${addresses[0]}
local_ip=`hostname -I | xargs` 

echo "Master IP ${master_ip}, Local IP ${local_ip}"
echo "All addresses: ${addresses[@]}"

wget http://apache.01link.hk/hadoop/common/hadoop-2.6.4/hadoop-2.6.4.tar.gz
tar -xvf hadoop-2.6.4.tar.gz

cd hadoop-2.6.4
mkdir data
mkdir data/datanode
mkdir data/namenode

cd etc/hadoop

cat >core-site.xml <<EOL
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://${master_ip}:9000</value>
</property>
</configuration>
EOL

cat >hdfs-site.xml <<EOL
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>

    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/home/automaton/hadoop-2.6.4/data/namenode</value>
    </property>

    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/home/automaton/hadoop-2.6.4/data/datanode</value>
    </property>
</configuration>
EOL

slaves_file=slaves

if [ -f ${slaves_file} ];
then
    rm ${slaves_file}
fi

for ip in "${addresses[@]}"; do
  echo "$ip" >> ${slaves_file}
done

cd ~
rm hadoop-2.6.4.tar.gz