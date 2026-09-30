"""Consume fraud.alerts and de-duplicate by deterministic alert_id."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from confluent_kafka import Consumer, KafkaError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="fraud.alerts")
    parser.add_argument("--group", default="fraud-alert-sink")
    parser.add_argument("--db", default="serving-data/alerts.db")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(str(db_path))
    db.execute(
        "create table if not exists alerts("
        "alert_id text primary key,"
        "payload text not null,"
        "received_at text default current_timestamp)"
    )
    db.commit()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            payload = json.loads(msg.value().decode())
            alert_id = payload["alert_id"]

            cur = db.execute(
                "insert or ignore into alerts(alert_id,payload) values(?,?)",
                (alert_id, json.dumps(payload, separators=(",", ":"))),
            )
            db.commit()

            # Commit the Kafka offset only after the SQLite transaction commits.
            consumer.commit(message=msg, asynchronous=False)

            if cur.rowcount:
                print(json.dumps(payload, ensure_ascii=False), flush=True)
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
