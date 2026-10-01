# Fraud Detection Lakehouse - one-command workflows.
# Docker targets mirror .github/workflows/full-stack-e2e.yml (the proven sequence).
#   make local-e2e     Docker-free check: train -> promote -> score (needs Java + dev reqs)
#   make test          Unit tests
#   make e2e           Full Docker stack: up -> init -> train -> promote -> stream -> produce -> verify

COMPOSE   := docker compose -f docker/docker-compose.yml -f docker/docker-compose.lakehouse.yml -f docker/docker-compose.mlflow.yml
PROFILES  := --profile lakehouse --profile mlops --profile serving
SUBMIT    := /opt/spark/bin/spark-submit --master spark://spark-master:7077
N_TRAIN   ?= 5000
N_EVENTS  ?= 1500
START     ?= 2026-09-01T10:00:00

.PHONY: help up down ps init train promote stream stream-log produce verify e2e test local-e2e

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

train:
	docker exec -e GIT_COMMIT_SHA="$$(git rev-parse HEAD)" spark-master bash -lc 'set -o pipefail; \
	  $(SUBMIT) /app/processing/ml/train.py --n $(N_TRAIN) --tracking-uri http://mlflow:5000 \
	  --promote-alias candidate --ml-threshold 0.70 2>&1 | tee /tmp/fraud-training.log'

promote:
	@RUN_ID=$$(docker exec spark-master bash -lc "grep '\[MLFLOW\] run_id=' /tmp/fraud-training.log | tail -1 | sed 's/.*run_id=//'"); \
	test -n "$$RUN_ID" || { echo "no run_id found - run 'make train' first"; exit 1; }; \
	echo "promoting run $$RUN_ID"; \
	docker exec spark-master $(SUBMIT) /app/scripts/promote_ensemble.py \
	  --tracking-uri http://mlflow:5000 --expected-run-id "$$RUN_ID"

stream:
	docker exec -d spark-master bash -lc 'rm -f /tmp/full-stack-stream.log; nohup $(SUBMIT) \
	  /app/processing/streaming/bronze_silver_job.py --topic transactions --alerts-topic fraud.alerts \
	  --trigger "2 seconds" --max-offsets-per-trigger 500 > /tmp/full-stack-stream.log 2>&1'
	@sleep 10; docker exec spark-master bash -lc 'tail -40 /tmp/full-stack-stream.log || true'

stream-log:
	docker exec spark-master bash -lc 'tail -f /tmp/full-stack-stream.log'

produce:
	python ingestion/producer/transaction_producer.py --n $(N_EVENTS) --speed 0 --start $(START) \
	  --flush-every 100 --reset --checkpoint /tmp/fraud-producer-checkpoint.json \
	  --labels-out /tmp/fraud-ground-truth.csv

verify:
	bash scripts/wait_for_trino.sh
	@for i in $$(seq 1 60); do \
	  c=$$(docker exec trino trino --server http://localhost:8080 --execute "SELECT count(*) FROM polaris.gold.fraud_decisions" --output-format CSV_HEADER=false 2>/dev/null | tr -d '\r\"' || true); \
	  echo "decision_rows=$$c"; \
	  if [ "$${c:-0}" -gt 0 ] 2>/dev/null; then echo "OK: decisions are landing in Iceberg"; exit 0; fi; sleep 3; done; \
	echo "FAILED: no decisions - stream log:"; docker exec spark-master bash -lc 'tail -120 /tmp/full-stack-stream.log'; exit 1

e2e: up init train promote stream produce verify

test:
	python -m pytest tests -q

local-e2e:
	python scripts/local_e2e.py
