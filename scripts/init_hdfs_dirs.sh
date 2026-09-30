#!/usr/bin/env bash
set -euo pipefail

docker exec namenode bash -lc '
  /opt/hadoop/bin/hdfs dfs -mkdir -p /tmp /user/spark /warehouse/silver/transactions
  /opt/hadoop/bin/hdfs dfs -chmod 1777 /tmp
  /opt/hadoop/bin/hdfs dfs -chown -R spark:supergroup /user/spark /warehouse
  /opt/hadoop/bin/hdfs dfs -ls /
  /opt/hadoop/bin/hdfs dfs -ls /user
'
