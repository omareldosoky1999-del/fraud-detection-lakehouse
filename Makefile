# Fraud Detection Lakehouse - one-command workflows.
# Docker targets mirror .github/workflows/full-stack-e2e.yml (the proven sequence).
#   make local-e2e     Docker-free check: train -> promote -> score (needs Java + dev reqs)
#   make test          Unit tests
#   make e2e           Full Docker stack: up -> init -> train -> promote -> stream -> produce -> verify

COMPOSE   := docker compose -f docker/docker-compose.yml -f docker/docker-compose.lakehouse.yml -f docker/docker-compose.mlflow.yml
PROFILES  := --profile lakehouse --profile mlops --profile serving
# The worker advertises ALL host cores; a 2 GB executor running 16 tasks dies with OOM
# ("Connection reset" / executor ids climbing). Bound cores and memory explicitly.
SPARK_RES ?= --executor-memory 2g --driver-memory 2g --executor-cores 4 --total-executor-cores 4 --conf spark.sql.shuffle.partitions=8 --conf spark.default.parallelism=8
SUBMIT    := /opt/spark/bin/spark-submit --master spark://spark-master:7077 $(SPARK_RES)
EXEC      := docker exec -w /app
N_TRAIN   ?= 5000
N_EVENTS  ?= 1500
START     ?= 2026-09-01T10:00:00

.PHONY: help up down ps init hdfs-ready train promote stream stream-log produce verify e2e test local-e2e

help:
	@grep -E '^# ' Makefile | head -6; echo; echo "Targets: up init train promote stream produce verify e2e down test local-e2e"

up:
	$(COMPOSE) $(PROFILES) up -d --build

ps:
	$(COMPOSE) $(PROFILES) ps

down:
	$(COMPOSE) $(PROFILES) down

init:
	bash scripts/init_hdfs_dirs.sh
	docker exec kafka bash /kafka-scripts/create-topics.sh kafka:9092
	@echo "waiting for HBase master..."
	@for i in $$(seq 1 180); do docker exec hbase bash -lc "curl -fsS http://localhost:16010/master-status >/dev/null" && break; sleep 2; done
	@echo "waiting for serving API..."
	@for i in $$(seq 1 40); do curl -fsS http://localhost:8095/health >/dev/null && break; sleep 3; done

# Training saves model copies through Spark's default filesystem (HDFS in this stack),
# which needs /user/spark to exist and be owned by 'spark'. Create it if missing.
hdfs-ready:
	@docker exec namenode /opt/hadoop/bin/hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (1)" \
	  || { echo "HDFS has no live datanode (see RUNBOOK: Incompatible clusterIDs: docker rm -f datanode && make up)"; exit 1; }
	@{ docker exec namenode /opt/hadoop/bin/hdfs dfs -test -d /user/spark \
	   && docker exec namenode /opt/hadoop/bin/hdfs dfs -test -d /checkpoints; } \
	  || { echo "HDFS dirs (/user/spark, /checkpoints) missing -> running scripts/init_hdfs_dirs.sh"; bash scripts/init_hdfs_dirs.sh; }

train: hdfs-ready
	$(EXEC) -e GIT_PYTHON_REFRESH=quiet -e GIT_COMMIT_SHA="$$(git rev-parse HEAD)" spark-master bash -lc 'set -o pipefail; \
	  $(SUBMIT) /app/processing/ml/train.py --n $(N_TRAIN) --tracking-uri http://mlflow:5000 \
	  --promote-alias candidate --ml-threshold 0.70 2>&1 | tee /tmp/fraud-training.log'

promote:
	@RUN_ID=$$(docker exec spark-master bash -lc "grep '\[MLFLOW\] run_id=' /tmp/fraud-training.log | tail -1 | sed 's/.*run_id=//'"); \
	test -n "$$RUN_ID" || { echo "no run_id found - run 'make train' first"; exit 1; }; \
	echo "promoting run $$RUN_ID"; \
	$(EXEC) -e GIT_PYTHON_REFRESH=quiet spark-master $(SUBMIT) /app/scripts/promote_ensemble.py \
	  --tracking-uri http://mlflow:5000 --expected-run-id "$$RUN_ID"

stream: hdfs-ready
	$(EXEC) -d spark-master bash -lc 'rm -f /tmp/full-stack-stream.log; nohup $(SUBMIT) \
	  /app/processing/streaming/bronze_silver_job.py --topic transactions --alerts-topic fraud.alerts \
	  --trigger "2 seconds" --max-offsets-per-trigger 500 > /tmp/full-stack-stream.log 2>&1'
	@sleep 20; docker exec spark-master bash -lc 'ps aux | grep -q "[b]ronze_silver_job"' \
	  || { echo "stream job is NOT running. First errors:"; \
	       docker exec spark-master bash -lc "grep -nE 'Caused by|ERROR|Exception' /tmp/full-stack-stream.log | head -10"; exit 1; }
	@docker exec spark-master bash -lc 'tail -15 /tmp/full-stack-stream.log || true'

stream-log:
	docker exec spark-master bash -lc 'tail -f /tmp/full-stack-stream.log'

produce:
	python ingestion/producer/transaction_producer.py --n $(N_EVENTS) --speed 0 --start $(START) \
	  --flush-every 100 --reset --checkpoint /tmp/fraud-producer-checkpoint.json \
	  --labels-out /tmp/fraud-ground-truth.csv

verify:
	bash scripts/wait_for_trino.sh
	@echo "waiting for the first batches (model loading takes ~1-2 min)..."
	@for i in $$(seq 1 90); do \
	  out=$$(docker exec trino trino --server http://localhost:8080 --execute "SELECT count(*) FROM polaris.gold.fraud_decisions" 2>&1 | tr -d '\r\"'); \
	  case "$$out" in \
	    ''|*[!0-9]*) echo "[$$i/90] not ready yet: $$(echo $$out | cut -c1-110)";; \
	    *) echo "decision_rows=$$out"; \
	       if [ "$$out" -gt 0 ]; then echo "OK: decisions are landing in Iceberg"; exit 0; fi;; \
	  esac; sleep 4; done; \
	echo "FAILED: no decisions after ~6 min. Is the stream alive?"; \
	docker exec spark-master bash -lc 'ps aux | grep -c "[b]ronze_silver_job"; grep -nE "Caused by|ERROR|Exception" /tmp/full-stack-stream.log | head -10; tail -40 /tmp/full-stack-stream.log'; exit 1

e2e: up init train promote stream produce verify

test:
	python -m pytest tests -q

local-e2e:
	python scripts/local_e2e.py
