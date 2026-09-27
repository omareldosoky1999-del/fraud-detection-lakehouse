import json
from datetime import datetime, timedelta

import pytest

from ingestion.generator.transaction_generator import Event
from ingestion.producer.transaction_producer import (
    load_checkpoint, run_producer, save_checkpoint)


def mk_events(n, start_id=1):
    t = datetime(2026, 9, 1)
    return [Event(t + timedelta(seconds=i), {"Trans_id": start_id + i, "Clt_id": 10 + i % 3})
            for i in range(n)]


class FakeProducer:
    """Mimics confluent_kafka.Producer: callbacks fire on flush()."""

    def __init__(self, fail_from=None):
        self.buffer, self.sent, self.fail_from = [], [], fail_from

    def produce(self, topic, key, value, on_delivery):
        self.buffer.append((topic, key, value, on_delivery))

    def poll(self, t=0):
        pass

    def flush(self):
        for topic, key, value, cb in self.buffer:
            if self.fail_from is not None and len(self.sent) >= self.fail_from:
                cb("broker down", None)
            else:
                self.sent.append((topic, key, value))
                cb(None, None)
        self.buffer = []


def kwargs(tmp_path, **kw):
    base = dict(topic="transactions", dlq_topic="transactions.dlq",
                checkpoint_path=tmp_path / "ckpt.json", checkpoint_meta={"seed": 1},
                flush_every=10, log=lambda *_: None)
    base.update(kw)
    return base


def test_checkpoint_written_after_ack_and_key_is_client(tmp_path):
    p = FakeProducer()
    run_producer(mk_events(25), p, lambda r: b"x", **kwargs(tmp_path))
    assert load_checkpoint(tmp_path / "ckpt.json")["last_acked_id"] == 25
    assert [k for _, k, _ in p.sent[:3]] == ["10", "11", "12"]  # Clt_id, not Trans_id
    assert len(p.sent) == 25


def test_checkpoint_not_advanced_when_delivery_fails(tmp_path):
    p = FakeProducer(fail_from=10)  # first 10 ack, then broker "dies"
    with pytest.raises(RuntimeError, match="NOT advanced"):
        run_producer(mk_events(25), p, lambda r: b"x", **kwargs(tmp_path))
    # only the first fully-acked batch (ids 1..10) is checkpointed -> nothing is lost
    assert load_checkpoint(tmp_path / "ckpt.json")["last_acked_id"] == 10


def test_resume_sends_only_unacked(tmp_path):
    save_checkpoint(tmp_path / "ckpt.json", {"seed": 1, "last_acked_id": 10})
    ckpt = load_checkpoint(tmp_path / "ckpt.json")
    todo = [e for e in mk_events(25) if e.rec["Trans_id"] > ckpt["last_acked_id"]]
    p = FakeProducer()
    run_producer(todo, p, lambda r: b"x", **kwargs(tmp_path))
    assert len(p.sent) == 15
    assert load_checkpoint(tmp_path / "ckpt.json")["last_acked_id"] == 25


def test_serialization_error_goes_to_dlq_and_run_continues(tmp_path):
    def ser(rec):
        if rec["Trans_id"] == 3:
            raise ValueError("bad field")
        return b"ok"

    p = FakeProducer()
    stats = run_producer(mk_events(5), p, ser, **kwargs(tmp_path))
    topics = [t for t, _, _ in p.sent]
    assert topics.count("transactions.dlq") == 1 and topics.count("transactions") == 4
    assert stats["dlq"] == 1
    dlq_payload = json.loads([v for t, _, v in p.sent if t == "transactions.dlq"][0])
    assert "bad field" in dlq_payload["reason"]


def test_speed_paces_by_virtual_time(tmp_path):
    sleeps, now = [], [0.0]

    def fake_sleep(s):
        sleeps.append(round(s, 3))
        now[0] += s

    p = FakeProducer()
    run_producer(mk_events(4), p, lambda r: b"x", **kwargs(tmp_path, speed=2.0),
                 sleep=fake_sleep, clock=lambda: now[0])
    # events are 1 virtual second apart, speed x2 -> 0.5 real seconds between sends
    assert sleeps == [0.5, 0.5, 0.5]
