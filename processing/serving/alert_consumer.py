"""Consume fraud.alerts and de-duplicate by deterministic alert_id."""
from __future__ import annotations
import argparse, json, sqlite3
from confluent_kafka import Consumer, KafkaError

def main():
 p=argparse.ArgumentParser(); p.add_argument("--bootstrap",default="kafka:9092"); p.add_argument("--topic",default="fraud.alerts"); p.add_argument("--group",default="fraud-alert-sink"); p.add_argument("--db",default="serving-data/alerts.db"); a=p.parse_args()
 db=sqlite3.connect(a.db); db.execute("create table if not exists alerts(alert_id text primary key,payload text not null,received_at text default current_timestamp)"); db.commit()
 c=Consumer({"bootstrap.servers":a.bootstrap,"group.id":a.group,"auto.offset.reset":"earliest","enable.auto.commit":False}); c.subscribe([a.topic])
 try:
  while True:
   msg=c.poll(1.0)
   if msg is None: continue
   if msg.error():
    # _PARTITION_EOF just means "caught up to the end of this partition for
    # now" -- a normal, expected condition, not a failure. Only genuine
    # errors should stop the consumer.
    if msg.error().code() == KafkaError._PARTITION_EOF: continue
    raise RuntimeError(msg.error())
   payload=json.loads(msg.value().decode()); aid=payload["alert_id"]
   cur=db.execute("insert or ignore into alerts(alert_id,payload) values(?,?)",(aid,json.dumps(payload,separators=(",",":"))))
   db.commit(); c.commit(message=msg,asynchronous=False)
   if cur.rowcount: print(json.dumps(payload,ensure_ascii=False))
 finally: c.close(); db.close()
if __name__=="__main__": main()
