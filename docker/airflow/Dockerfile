# Extends the official Airflow image with the docker CLI only (no daemon --
# the container talks to the HOST's docker daemon via the socket bind-mount
# in docker-compose.yml). This is what lets fraud_gold_dag.py's BashOperator
# tasks run `docker exec spark-master spark-submit ...` and
# `docker exec hive beeline ...` without a second, separately-versioned
# Spark/Hive client installed inside the Airflow image itself.
#
# requests is added for the WebHDFS partition-existence check in the DAG.
FROM apache/airflow:2.8.1-python3.11

USER root
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-25.0.3.tgz \
      -o /tmp/docker.tgz \
    && tar xzf /tmp/docker.tgz -C /tmp \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && rm -rf /tmp/docker.tgz /tmp/docker

USER airflow
RUN pip install --no-cache-dir requests==2.31.0
