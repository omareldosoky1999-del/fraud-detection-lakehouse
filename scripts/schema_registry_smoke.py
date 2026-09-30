"""Schema Registry contract smoke test for Confluent Avro ingestion."""
from __future__ import annotations

import json
import struct
import sys
from datetime import datetime
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from ingestion.generator.transaction_generator import generate

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "ingestion" / "schema" / "transaction.avsc"
TARGET_TOPIC = "transactions"
SUBJECT = TARGET_TOPIC + "-value"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    import os

    registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    client = SchemaRegistryClient({"url": registry_url})
    schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
    serializer = AvroSerializer(client, schema_text)

    event = generate(
        1,
        seed=2026,
        start=datetime(2026, 9, 1),
        data_dir=str(ROOT / "ingestion" / "data"),
    )[0]
    payload = serializer(
        event.rec,
        SerializationContext(TARGET_TOPIC, MessageField.VALUE),
    )

    if payload[0] != 0:
        raise RuntimeError("Unexpected Confluent magic byte: " + str(payload[0]))
    schema_id = struct.unpack(">I", payload[1:5])[0]
    latest = client.get_latest_version(SUBJECT)

    if schema_id != latest.schema_id:
        raise RuntimeError("Header schema id does not match Schema Registry latest version")

    compatibility = client.get_compatibility()
    print(json.dumps({
        'subject': SUBJECT,
        'schema_id': schema_id,
        'latest_version': latest.version,
        'compatibility': compatibility,
        'payload_bytes': len(payload),
    }, indent=2))


if __name__ == "__main__":
    main()
