from datetime import datetime

from processing.serving.hbase_sink import ensure_table, write_partition


class Batch:
    def __init__(self, store):
        self.store = store
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return None
    def put(self, key, data):
        self.store[key] = data


class Table:
    def __init__(self, store, rows=None):
        self.store = store
        self.rows = rows or {}
    def batch(self, batch_size=500):
        return Batch(self.store)
    def row(self, key, columns=None):
        return self.rows.get(key, {})


class Conn:
    def __init__(self):
        self._tables = set()
        self.created = []
        self.store = {}
        self.table_rows = {}
        self.closed = False
    def tables(self):
        return list(self._tables)
    def create_namespace(self, name):
        pass
    def create_table(self, name, families):
        self._tables.add(name.encode())
        self.created.append((name, families))
    def table(self, name):
        return Table(self.store, self.table_rows)
    def close(self):
        self.closed = True


def decision(client_id=10, transaction_id=1, when=datetime(2026, 9, 1, 12, 0, 0)):
    return {
        "client_id": client_id,
        "transaction_id": transaction_id,
        "decision": "FLAG",
        "risk_score": 20,
        "matched_rules": ["VELOCITY"],
        "event_time": when,
        "ingest_time": when,
    }


def test_ensure_table_is_idempotent():
    c = Conn()
    ensure_table(c)
    ensure_table(c)
    assert c.created == [("fraud:client_risk", {"cf": {}})]


def test_write_partition_does_not_regress_older_client_state():
    c = Conn()
    c.table_rows[b"10"] = {
        b"cf:last_event_time": b"2026-09-01T13:00:00",
        b"cf:last_transaction_id": b"99",
    }
    write_partition([decision(client_id=10, transaction_id=5)], lambda: c)
    assert c.store == {}
    assert c.closed
