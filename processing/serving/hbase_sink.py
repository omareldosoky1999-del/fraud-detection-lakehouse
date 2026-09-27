"""Write fraud decisions to HBase for low-latency point lookups
(e.g. "what is client X's current risk state" for a card-authorization check).

This is intentionally NOT the system of record (that's the Silver/Gold
Parquet tables on HDFS, queryable historically via Hive). HBase here is a
serving cache: one row per client, overwritten on every new decision, keyed
so a lookup by client_id is O(1).

Design notes
------------
* Runs inside `foreachPartition` (called from bronze_silver_job.py), not
  `foreach`/collected-to-driver: each Spark partition opens its own HBase
  connection, writes its rows, and closes it. This scales with the cluster
  instead of funnelling every write through the driver.
* A batch `put` per partition (not one RPC per row) to keep throughput
  reasonable.
* If HBase is unreachable, the write is logged and swallowed rather than
  failing the whole micro-batch: the serving cache is a convenience, and a
  serving-layer outage should not stop the pipeline from writing Silver/Gold
  (the source of truth) or publishing alerts. This is a deliberate
  availability trade-off -- call it out explicitly if asked in the defense.
"""
import logging

logger = logging.getLogger(__name__)

TABLE = "fraud:client_risk"
COLUMN_FAMILY = b"cf"


def row_key(client_id) -> bytes:
    return str(client_id).encode("utf-8")



def _as_iso(value) -> str:
    """Serialize datetime-like values consistently for HBase cells."""
    if value is None:
        return ""
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        return iso()
    return str(value)


def to_hbase_row(record: dict) -> dict:
    """One decisioned transaction row -> the column values HBase should hold.

    Only the LATEST state per client is kept (the put is a plain overwrite),
    which is what a "current risk" serving lookup needs; full history lives
    in the Gold table, not here.
    """
    cf = COLUMN_FAMILY.decode()
    return {
        f"{cf}:last_transaction_id": str(record["transaction_id"]),
        f"{cf}:last_decision": str(record["decision"]),
        f"{cf}:last_risk_score": str(record["risk_score"]),
        f"{cf}:last_matched_rules": ",".join(record.get("matched_rules") or []),
        f"{cf}:last_event_time": _as_iso(record["event_time"]),
        f"{cf}:updated_at": _as_iso(record.get("ingest_time", "")),
    }


def ensure_table(conn) -> None:
    """Ensure the HBase namespace/table used by the serving cache exists.

    Real HBase starts empty in a clean deployment. HappyBase exposes the
    administration methods we need on the connection object. The helper is
    intentionally tolerant of an already-created namespace/table and of test
    fakes that do not implement HBase admin methods.
    """
    tables_fn = getattr(conn, "tables", None)
    create_table = getattr(conn, "create_table", None)
    if not callable(tables_fn) or not callable(create_table):
        return

    encoded_table = TABLE.encode("utf-8")
    try:
        existing = {t.encode("utf-8") if isinstance(t, str) else t for t in tables_fn()}
    except Exception:
        existing = set()
    if encoded_table in existing:
        return

    namespace = TABLE.split(":", 1)[0]
    create_namespace = getattr(conn, "create_namespace", None)
    if callable(create_namespace):
        try:
            create_namespace(namespace)
        except Exception:
            # Namespace may already exist; table creation below is still safe.
            pass

    try:
        create_table(TABLE, {COLUMN_FAMILY.decode("utf-8"): {}})
    except Exception:
        # Another executor/service may have created it concurrently. Confirm
        # before deciding that the failure is real.
        try:
            existing_after = {t.encode("utf-8") if isinstance(t, str) else t for t in tables_fn()}
        except Exception:
            existing_after = set()
        if encoded_table not in existing_after:
            raise


def write_partition(records, connection_factory):
    """Write one Spark partition's worth of decision records to HBase.

    `records`: iterable of dict-like rows (a partition passed to
    `foreachPartition` after `.asDict()`/similar upstream).
    `connection_factory`: zero-arg callable returning a happybase-style
    connection (`.table(name)` -> object with `.batch()` context manager
    exposing `.put(row_key, data)`). Injected so this is testable with a fake
    connection and swappable for a different HBase client if needed.
    """
    records = list(records)
    if not records:
        return
    try:
        conn = connection_factory()
    except Exception:
        logger.exception("HBase connection failed; skipping serving-layer write for %d rows", len(records))
        return

    try:
        ensure_table(conn)
        table = conn.table(TABLE)
        with table.batch(batch_size=500) as batch:
            row_reader = getattr(table, "row", None)
            for r in records:
                rec = r.asDict() if hasattr(r, "asDict") else dict(r)
                key = row_key(rec["client_id"])
                incoming_time = _as_iso(rec.get("event_time"))
                incoming_id = int(rec["transaction_id"])

                # Spark already reduces each micro-batch to one latest row per
                # client. This second guard protects the serving cache from a
                # late-arriving event in a later batch regressing an already
                # newer state. Test fakes may not implement table.row(), in
                # which case the normal overwrite semantics are retained.
                if callable(row_reader):
                    existing = row_reader(key, columns=[b"cf:last_event_time", b"cf:last_transaction_id"])
                    if existing:
                        old_time = existing.get(b"cf:last_event_time", b"").decode("utf-8")
                        try:
                            old_id = int(existing.get(b"cf:last_transaction_id", b"-1"))
                        except (TypeError, ValueError):
                            old_id = -1
                        if old_time > incoming_time or (old_time == incoming_time and old_id >= incoming_id):
                            continue
                batch.put(key, to_hbase_row(rec))
    except Exception:
        logger.exception("HBase write failed for a partition of %d rows", len(records))
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass


def build_connection_factory(host: str, port: int = 9090):
    """Real happybase connection factory (imported lazily; not needed for tests)."""
    def factory():
        import happybase
        return happybase.Connection(host=host, port=port, timeout=5000)
    return factory
