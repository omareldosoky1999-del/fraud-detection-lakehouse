import json
from datetime import datetime
from pathlib import Path

import fastavro
import pytest
from fastavro.validation import validate

from common.fx import to_usd
from ingestion.generator.transaction_generator import (
    DATE_FMT, DEFAULT_DATA_DIR, FRAUD_PATTERNS, generate, write_labels)

START = datetime(2026, 9, 1)
SCHEMA = fastavro.parse_schema(json.loads(
    (Path(__file__).resolve().parents[1] / "ingestion/schema/transaction.avsc").read_text()))


@pytest.fixture(scope="module")
def events():
    return generate(20000, seed=7, start=START, data_dir=DEFAULT_DATA_DIR)


def test_deterministic_for_same_seed():
    a = generate(2000, seed=1, start=START)
    b = generate(2000, seed=1, start=START)
    assert [e.rec for e in a] == [e.rec for e in b]
    assert [e.pattern for e in a] == [e.pattern for e in b]


def test_different_seed_differs():
    a = generate(2000, seed=1, start=START)
    b = generate(2000, seed=2, start=START)
    assert [e.rec for e in a] != [e.rec for e in b]


def test_every_record_matches_avro_schema(events):
    for e in events:
        validate(e.rec, SCHEMA)  # raises on violation


def test_ids_sequential_unique_and_ref_no_unique(events):
    ids = [e.rec["Trans_id"] for e in events]
    assert ids == list(range(100_001, 100_001 + len(events)))
    refs = [e.rec["Trans_Ref_No"] for e in events]
    assert len(set(refs)) == len(refs)


def test_time_sorted_and_date_format_roundtrips(events):
    ts = [e.ts for e in events]
    assert ts == sorted(ts)
    for e in events[:200]:
        parsed = datetime.strptime(e.rec["Trans_date"], DATE_FMT)
        assert abs((parsed - e.ts).total_seconds()) < 1


def test_fraud_rate_close_to_target_and_all_patterns_present(events):
    fraud = [e for e in events if e.is_fraud]
    assert 0.024 <= len(fraud) / len(events) <= 0.032
    assert {e.pattern for e in fraud} == set(FRAUD_PATTERNS)
    assert min(sum(e.pattern == p for e in fraud) for p in FRAUD_PATTERNS) >= 20


def test_referential_integrity_card_belongs_to_client(events):
    import fastavro as fa
    with open(DEFAULT_DATA_DIR / "cards.avro", "rb") as f:
        owner = {c["Card_id"]: c["Clt_id"] for c in fa.reader(f)}
    assert all(owner[e.rec["Card_id"]] == e.rec["Clt_id"] for e in events)


def test_pattern_shapes(events):
    by_client = {}
    for e in events:
        by_client.setdefault(e.rec["Clt_id"], []).append(e)
    assert all(to_usd(e.rec["Trans_amount"], e.rec["Currency"]) >= 25000 * 0.99
               for e in events if e.pattern == "HIGH_AMOUNT")
    assert all(8400 <= to_usd(e.rec["Trans_amount"], e.rec["Currency"]) <= 9950
               for e in events if e.pattern == "STRUCTURING")
    assert all(e.rec["Country_Src"] != "Egypt" for e in events if e.pattern == "IMPOSSIBLE_TRAVEL")
    mules = [e for e in events if e.pattern == "MULE"]
    assert {e.rec["Trans_type"] for e in mules} == {"Deposit", "Withdrawal"}


def test_labels_file(tmp_path, events):
    out = tmp_path / "gt.csv"
    write_labels(events, out)
    lines = out.read_text().strip().splitlines()
    assert lines[0] == "Trans_id,is_fraud,pattern"
    assert len(lines) == len(events) + 1
