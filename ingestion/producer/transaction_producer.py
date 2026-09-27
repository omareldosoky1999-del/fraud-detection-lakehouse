"""Kafka producer: replays the labeled generator into Kafka as Avro.

Fixes vs. the old ingestion/data/Kafka_producer.py
--------------------------------------------------
* No hard-coded D:\\ paths; everything comes from env vars / CLI flags.
* Checkpoint is advanced ONLY after Kafka acknowledged the batch (flush() with
  zero delivery errors). The old one saved after produce(), i.e. before the ack,
  so a failed send was silently lost.
* Delivery is at-least-once; the Spark job de-duplicates on Trans_id, so the
  end result is effectively exactly-once.
* Records that cannot be serialized (schema violation) go to the DLQ topic with
  the error reason instead of killing the run (the old DLQ code was never called).
* Key = Clt_id (not Trans_id): all events of one customer land in the same
  partition, so per-customer ordering is preserved for the fraud rules.
* Idempotent producer (enable.idempotence) + acks=all.
* One process, one loop, deterministic order (the old one used 10 threads that
  slept 0.5s per record => ~42 minutes per 5,000 records).

Time compression
----------------
--speed S replays S virtual seconds per real second (0 = as fast as possible).
Event timestamps are the generator's virtual clock (set --start to the past, or
let the producer default it), so event-time logic in Spark/watermarks stays
consistent while a "day" of customer behaviour is demoed in minutes.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.generator.transaction_generator import (  # noqa: E402
    DEFAULT_DATA_DIR, generate, summarize, write_labels)

SCHEMA_PATH = ROOT / "ingestion" / "schema" / "transaction.avsc"


class DeliveryTracker:
    """Counts delivery results reported by the Kafka client callbacks."""

    def __init__(self):
        self.delivered = 0
        self.errors = []

    def __call__(self, err, msg):
        if err is not None:
            self.errors.append(str(err))
        else:
            self.delivered += 1


def load_checkpoint(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_checkpoint(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)  # atomic on the same filesystem


def run_producer(events, producer, serialize, *, topic, dlq_topic, checkpoint_path,
                 checkpoint_meta, speed=0.0, flush_every=200, sleep=time.sleep,
                 clock=time.monotonic, log=print):
    """Send `events` (already filtered to those not yet acknowledged).

    `producer` needs produce/poll/flush (confluent_kafka.Producer API);
    `serialize(record)` returns Avro bytes or raises. Dependencies are injected
    so the logic is unit-testable without Kafka.
    Returns a stats dict. Raises RuntimeError if a batch is not acknowledged.
    """
    tracker = DeliveryTracker()
    stats = {"sent": 0, "dlq": 0, "acked_batches": 0}
    if not events:
        return stats

    first_ts = events[0].ts
    t0 = clock()
    pending_last_id = None

    def commit(last_id):
        producer.flush()
        if tracker.errors:
            raise RuntimeError(
                f"{len(tracker.errors)} message(s) not acknowledged, checkpoint NOT advanced "
                f"(first error: {tracker.errors[0]})")
        save_checkpoint(checkpoint_path, {**checkpoint_meta, "last_acked_id": last_id})
        stats["acked_batches"] += 1

    for ev in events:
        if speed > 0:
            wait = (ev.ts - first_ts).total_seconds() / speed - (clock() - t0)
            if wait > 0:
                sleep(wait)

        rec = ev.rec
        key = str(rec["Clt_id"])
        try:
            value = serialize(rec)
            target = topic
        except Exception as e:  # schema violation etc. -> DLQ, keep going
            value = json.dumps({"reason": f"serialization_failed: {e}",
                                "record": {k: str(v) for k, v in rec.items()}}).encode()
            target = dlq_topic
            stats["dlq"] += 1

        while True:
            try:
                producer.produce(topic=target, key=key, value=value, on_delivery=tracker)
                break
            except BufferError:  # local queue full -> serve callbacks and retry
                producer.poll(0.5)
        producer.poll(0)
        stats["sent"] += 1
        pending_last_id = rec["Trans_id"]

        if stats["sent"] % flush_every == 0:
            commit(pending_last_id)
            log(f"[producer] acked up to Trans_id={pending_last_id} "
                f"(sent={stats['sent']}, dlq={stats['dlq']})")

    commit(pending_last_id)
    log(f"[producer] DONE sent={stats['sent']} dlq={stats['dlq']} last_id={pending_last_id}")
    return stats


def build_kafka(bootstrap, registry_url):
    """Real Kafka/Schema Registry objects (imported lazily so tests don't need them)."""
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "client.id": "transaction-producer",
    })
    sr = SchemaRegistryClient({"url": registry_url})
    avro = AvroSerializer(sr, SCHEMA_PATH.read_text())

    def serialize(rec, topic=os.getenv("TOPIC_TRANSACTIONS", "transactions")):
        return avro(rec, SerializationContext(topic, MessageField.VALUE))

    return producer, serialize


def main(argv=None):
    ap = argparse.ArgumentParser(description="Replay labeled synthetic transactions into Kafka (Avro).")
    ap.add_argument("--n", type=int, default=5000, help="approx. number of events")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--speed", type=float, default=60.0,
                    help="virtual seconds per real second; 0 = no throttling")
    ap.add_argument("--start", type=datetime.fromisoformat, default=None,
                    help="virtual start (ISO). Default: now - n/250h, stored in the checkpoint")
    ap.add_argument("--start-id", type=int, default=100_001)
    ap.add_argument("--fraud-rate", type=float, default=0.025)
    ap.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    ap.add_argument("--labels-out", default=os.getenv("LABELS_PATH", "labels/ground_truth.csv"))
    ap.add_argument("--checkpoint", default=os.getenv("PRODUCER_CHECKPOINT", ".state/producer_checkpoint.json"))
    ap.add_argument("--reset", action="store_true", help="ignore the checkpoint and start from scratch")
    ap.add_argument("--flush-every", type=int, default=200)
    a = ap.parse_args(argv)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    registry = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("TOPIC_TRANSACTIONS", "transactions")
    dlq = os.getenv("TOPIC_DLQ", "transactions.dlq")

    ckpt_path = Path(a.checkpoint)
    ckpt = {} if a.reset else load_checkpoint(ckpt_path)
    params = {"seed": a.seed, "n": a.n, "start_id": a.start_id, "fraud_rate": a.fraud_rate}
    if ckpt and {k: ckpt.get(k) for k in params} != params:
        sys.exit(f"Checkpoint {ckpt_path} was made with different parameters ({ckpt}). "
                 f"Use --reset to start over.")

    # The start time must survive restarts, otherwise resumed events would differ.
    start = a.start or (datetime.fromisoformat(ckpt["start"]) if ckpt.get("start") else
                        datetime.now().replace(microsecond=0) - timedelta(hours=max(a.n / 250.0, 1.0)))
    meta = {**params, "start": start.isoformat()}

    events = generate(a.n, a.seed, start, a.fraud_rate, start_id=a.start_id, data_dir=a.data_dir)
    write_labels(events, a.labels_out)
    print(summarize(events))

    last = ckpt.get("last_acked_id", a.start_id - 1)
    todo = [e for e in events if e.rec["Trans_id"] > last]
    print(f"[producer] resuming after Trans_id={last}: {len(todo)}/{len(events)} events to send "
          f"-> {topic} @ {bootstrap} (speed x{a.speed:g})")

    producer, serialize = build_kafka(bootstrap, registry)
    run_producer(todo, producer, serialize, topic=topic, dlq_topic=dlq,
                 checkpoint_path=ckpt_path, checkpoint_meta=meta,
                 speed=a.speed, flush_every=a.flush_every)


if __name__ == "__main__":
    main()
