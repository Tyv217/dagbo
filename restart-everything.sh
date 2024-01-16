#!/bin/bash

~/hadoop/sbin/stop-all.sh
hdfs namenode -format
python3 change_cluster_id.py
~/hadoop/sbin/start-all.sh
cd ~/HiBench
mvn -Psparkbench -Dmodules -Psql -Dspark=2.4 -Dscala=2.11 clean package
bin/workloads/sql/aggregation/prepare/prepare.sh
cd ~/dagbo
