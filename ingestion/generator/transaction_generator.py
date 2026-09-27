"""Labeled synthetic transaction generator.

Why this exists
---------------
The original 50K dataset is uniformly random: no customer behaviour and no
fraud labels, so any rule/ML result on it is meaningless. This generator builds
realistic per-customer behaviour and injects KNOWN fraud campaigns, so the
pipeline can be evaluated (precision / recall per rule).

Design notes
------------
* Output records match ingestion/schema/transaction.avsc exactly (no schema
  change -> Avro/Spark/Hive work keeps compatibility).
* The ground truth is NOT part of the event. It is written to a separate CSV
  (Trans_id, is_fraud, pattern) and joined only by the evaluation step.
* Fully deterministic for a given (seed, n, start, ...), which is what makes
  the producer restart-safe.
* Event time is a *virtual* clock. The producer can replay it faster than real
  time (--speed), so a "day" of customer behaviour can be demoed in minutes.

Fraud patterns (every event of an injected campaign is labeled fraud, except
the legit "anchor" event of IMPOSSIBLE_TRAVEL):
  VELOCITY               8-15 small txns by one client within 2-8 minutes
  IMPOSSIBLE_TRAVEL      txn from a far country minutes after a home-country txn
  STRUCTURING            4-6 txns of 8,500-9,900 USD within ~20-50 minutes
  MULE                   large deposit then ~90-98% withdrawn within 5-20 minutes
  NEW_DEVICE_HIGH_VALUE  unknown device + foreign country + 5,000-15,000 USD
  HIGH_AMOUNT            single 25,000-60,000 USD txn (huge vs. client profile)

Hard negatives (so precision is < 100% like in real life): ~1.5% of normal
events are big legit purchases (1,000-7,000 USD) and ~3% happen abroad.
"""
import argparse
import csv
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import fastavro

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.fx import FX_TO_USD  # noqa: E402

DEFAULT_DATA_DIR = ROOT / "ingestion" / "data"
DATE_FMT = "%Y-%m-%d %I:%M:%S %p"  # same format Spark_consumer already parses

HOME = "Egypt"
FOREIGN = ["Jordan", "Germany", "Canada", "Italy", "UK", "Lebanon", "USA",
           "France", "Australia", "UAE", "Saudi Arabia", "Spain"]
FAR = ["USA", "Canada", "Australia", "UK", "Germany", "France", "Italy", "Spain"]
BANKS = ["Fransabank", "Arab Bank", "National Bank of Kuwait", "Riyad Bank",
         "Banque du Caire", "CIB", "QNB", "Banque Misr"]
REASONS = ["Transfer", "Salary", "Bill Payment", "Shopping"]
TYPES = ["Withdrawal", "Deposit", "Transfer"]
TYPE_W = [0.35, 0.30, 0.35]
HOURLY = [1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 8, 7, 5, 3, 2]
# Weights are per CAMPAIGN (velocity campaigns have ~11 events, others 1-6), tuned so
# every pattern has enough events to evaluate its rule separately.
PATTERN_W = {"VELOCITY": 8, "IMPOSSIBLE_TRAVEL": 22, "STRUCTURING": 18,
             "MULE": 18, "NEW_DEVICE_HIGH_VALUE": 20, "HIGH_AMOUNT": 14}
FRAUD_PATTERNS = list(PATTERN_W)


@dataclass
class Profile:
    clt_id: int
    cards: List[int]
    devices: List[int]
    currency: str
    scale_usd: float


@dataclass
class Event:
    ts: datetime
    rec: dict
    pattern: str = "NORMAL"

    @property
    def is_fraud(self) -> int:
        return 0 if self.pattern == "NORMAL" else 1


def _read_avro(path: Path) -> list:
    with open(path, "rb") as f:
        return list(fastavro.reader(f))


def load_profiles(data_dir, rng: random.Random):
    """Build customer profiles from the existing dimension files.

    Only clients that own at least one card are used, and every transaction uses
    a card owned by its client (the old data had 38% mismatches).
    """
    data_dir = Path(data_dir)
    cards = _read_avro(data_dir / "cards.avro")
    devices = _read_avro(data_dir / "devices.avro")
    clients = _read_avro(data_dir / "clients.avro")

    cards_by_client: Dict[int, List[int]] = {}
    for c in cards:
        cards_by_client.setdefault(c["Clt_id"], []).append(c["Card_id"])
    dev_ids = [d["Dev_id"] for d in devices]
    dev_loc = {d["Dev_id"]: d["Dev_Ip_Location"] for d in devices}

    profiles = []
    for cl in clients:
        cid = cl["Clt_id"]
        if cid not in cards_by_client:
            continue
        currency = rng.choices(["EGP", "USD", "EUR"], weights=[70, 20, 10])[0]
        profiles.append(Profile(
            clt_id=cid,
            cards=cards_by_client[cid],
            devices=rng.sample(dev_ids, rng.choice([1, 1, 2])),
            currency=currency,
            scale_usd=rng.lognormvariate(4.1, 0.6),  # median ~60 USD
        ))
    return profiles, dev_ids, dev_loc


class _Builder:
    def __init__(self, rng, dev_ids, dev_loc):
        self.rng, self.dev_ids, self.dev_loc = rng, dev_ids, dev_loc

    def record(self, p: Profile, usd, *, currency=None, ttype=None, country=None,
               dev=None, status=None) -> dict:
        r = self.rng
        currency = currency or p.currency
        dev = dev if dev is not None else r.choice(p.devices)
        return {
            "Trans_id": None,  # assigned after the global time-sort
            "Clt_id": p.clt_id,
            "Card_id": r.choice(p.cards),
            "Dev_id": dev,
            "Trans_amount": max(1, int(round(usd / FX_TO_USD[currency]))),
            "Trans_date": None,  # set from the event timestamp
            "Trans_type": ttype or r.choices(TYPES, weights=TYPE_W)[0],
            "Trans_status": status or ("Rejected" if r.random() < 0.04 else "Successful"),
            "Trans_destination": r.choice(BANKS),
            "Dev_Ip_Location": self.dev_loc[dev],
            "Trans_Ref_No": f"C{r.randrange(10**14, 10**15)}",
            "Currency": currency,
            "Trans_Reason": r.choice(REASONS),
            "Dest_account_No": r.randrange(100_000_000, 999_999_999),
            "Country_Dest": HOME if r.random() < 0.85 else r.choice(FOREIGN),
            "Country_Src": country or HOME,
        }

    # ---- normal behaviour -------------------------------------------------
    def normal(self, p: Profile, ts) -> Event:
        r = self.rng
        usd = p.scale_usd * r.lognormvariate(0, 0.7)
        if r.random() < 0.015:  # hard negative: big legit purchase
            usd = r.uniform(1000, 7000)
        country = r.choice(FOREIGN) if r.random() < 0.03 else HOME  # hard negative: travel
        return Event(ts, self.record(p, max(usd, 1.0), country=country))

    # ---- fraud campaigns ---------------------------------------------------
    def campaign(self, kind: str, p: Profile, t0: datetime) -> List[Event]:
        r = self.rng
        if kind == "VELOCITY":
            k, span = r.randint(8, 15), r.uniform(120, 480)
            offs = sorted(r.uniform(0, span) for _ in range(k))
            return [Event(t0 + timedelta(seconds=o),
                          self.record(p, r.uniform(5, 60), ttype=r.choice(["Transfer", "Withdrawal"])),
                          kind) for o in offs]
        if kind == "IMPOSSIBLE_TRAVEL":
            far = r.choice(FAR)
            evs = [Event(t0, self.record(p, p.scale_usd * r.lognormvariate(0, 0.5)), "NORMAL")]  # anchor
            for _ in range(r.randint(1, 2)):
                evs.append(Event(t0 + timedelta(seconds=r.uniform(300, 1800)),
                                 self.record(p, r.uniform(50, 400), country=far), kind))
            return evs
        if kind == "STRUCTURING":
            k, span = r.randint(4, 6), r.uniform(1200, 3000)
            offs = sorted(r.uniform(0, span) for _ in range(k))
            return [Event(t0 + timedelta(seconds=o),
                          self.record(p, r.uniform(8500, 9900), currency="USD",
                                      ttype=r.choice(["Deposit", "Transfer"]), status="Successful"),
                          kind) for o in offs]
        if kind == "MULE":
            usd_in = r.uniform(6000, 15000)
            return [
                Event(t0, self.record(p, usd_in, currency="USD", ttype="Deposit", status="Successful"), kind),
                Event(t0 + timedelta(seconds=r.uniform(300, 1200)),
                      self.record(p, usd_in * r.uniform(0.90, 0.98), currency="USD",
                                  ttype="Withdrawal", status="Successful"), kind),
            ]
        if kind == "NEW_DEVICE_HIGH_VALUE":
            unknown = r.choice([d for d in self.dev_ids if d not in p.devices])
            return [Event(t0, self.record(p, r.uniform(5000, 15000), currency="USD",
                                          ttype=r.choice(["Withdrawal", "Transfer"]),
                                          country=r.choice(FAR), dev=unknown, status="Successful"), kind)]
        if kind == "HIGH_AMOUNT":
            return [Event(t0, self.record(p, r.uniform(25000, 60000), currency="USD",
                                          ttype=r.choice(["Withdrawal", "Transfer"]),
                                          status="Successful"), kind)]
        raise ValueError(kind)


def _sample_ts(rng, start: datetime, hours: float) -> datetime:
    peak = max(HOURLY)
    while True:
        t = start + timedelta(seconds=rng.uniform(0, hours * 3600))
        if rng.random() < HOURLY[t.hour] / peak:
            return t


def generate(n: int, seed: int = 42, start: Optional[datetime] = None,
             fraud_rate: float = 0.025, events_per_hour: float = 250.0,
             start_id: int = 100_001, data_dir=DEFAULT_DATA_DIR) -> List[Event]:
    """Return ~n events sorted by time, with sequential Trans_id from start_id.

    Total is n plus a small overshoot (a campaign is never cut in half).
    """
    rng = random.Random(seed)
    profiles, dev_ids, dev_loc = load_profiles(data_dir, rng)
    b = _Builder(rng, dev_ids, dev_loc)

    hours = max(n / events_per_hour, 1.0)
    start = start or (datetime.now().replace(microsecond=0) - timedelta(hours=hours))
    n_fraud = round(n * fraud_rate)

    events: List[Event] = []
    for _ in range(n - n_fraud):
        events.append(b.normal(rng.choice(profiles), _sample_ts(rng, start, hours)))

    kinds, weights = list(PATTERN_W), list(PATTERN_W.values())
    fraud_count = 0
    while fraud_count < n_fraud:
        kind = rng.choices(kinds, weights=weights)[0]
        camp = b.campaign(kind, rng.choice(profiles), _sample_ts(rng, start, max(hours - 1, 1)))
        events.extend(camp)
        fraud_count += sum(e.is_fraud for e in camp)

    events.sort(key=lambda e: e.ts)  # stable sort -> deterministic
    for i, e in enumerate(events):
        e.rec["Trans_id"] = start_id + i
        e.rec["Trans_date"] = e.ts.strftime(DATE_FMT)
    return events


def write_labels(events: List[Event], path) -> None:
    """Ground truth for evaluation. Deliberately separate from the event stream."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Trans_id", "is_fraud", "pattern"])
        for e in events:
            w.writerow([e.rec["Trans_id"], e.is_fraud, e.pattern])


def summarize(events: List[Event]) -> str:
    total = len(events)
    fr = [e for e in events if e.is_fraud]
    by: Dict[str, int] = {}
    for e in fr:
        by[e.pattern] = by.get(e.pattern, 0) + 1
    lines = [f"events={total}  fraud={len(fr)} ({len(fr) / total:.2%})",
             f"time span: {events[0].ts} -> {events[-1].ts}"]
    lines += [f"  {k:24s} {v}" for k, v in sorted(by.items())]
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Generate labeled synthetic transactions (inspection tool).")
    ap.add_argument("--n", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", type=datetime.fromisoformat, default=None,
                    help="ISO datetime, e.g. 2026-09-01T00:00:00")
    ap.add_argument("--fraud-rate", type=float, default=0.025)
    ap.add_argument("--start-id", type=int, default=100_001)
    ap.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    ap.add_argument("--labels-out", default="labels/ground_truth.csv")
    a = ap.parse_args(argv)
    ev = generate(a.n, a.seed, a.start, a.fraud_rate, start_id=a.start_id, data_dir=a.data_dir)
    write_labels(ev, a.labels_out)
    print(summarize(ev))
    print(f"labels -> {a.labels_out}")


if __name__ == "__main__":
    main()
