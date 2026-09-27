"""Replay manually corrected DLQ JSONL records to the Avro transactions topic."""
from __future__ import annotations
import argparse, json
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def main():
 p=argparse.ArgumentParser(); p.add_argument("--input",required=True); p.add_argument("--bootstrap",default="localhost:29092"); p.add_argument("--schema-registry",default="http://localhost:8081"); p.add_argument("--topic",default="transactions"); p.add_argument("--schema",default="ingestion/schema/transaction.avsc"); a=p.parse_args()
 schema=Path(a.schema).read_text(); sr=SchemaRegistryClient({"url":a.schema_registry}); serializer=AvroSerializer(sr,schema)
 prod=Producer({"bootstrap.servers":a.bootstrap,"enable.idempotence":True,"acks":"all"})
 with open(a.input,encoding="utf-8") as fh:
  for line in fh:
   if not line.strip(): continue
   rec=json.loads(line); key=str(rec["Clt_id"]); value=serializer(rec,SerializationContext(a.topic,MessageField.VALUE)); prod.produce(a.topic,key=key,value=value)
   prod.poll(0)
 prod.flush()
if __name__=="__main__": main()
