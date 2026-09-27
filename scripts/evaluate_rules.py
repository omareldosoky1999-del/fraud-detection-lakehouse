"""Evaluate the fraud rules against the generator's ground truth.

Usage
-----
    python scripts/evaluate_rules.py \
        --decisions /path/to/fraud_decisions.parquet \
        --labels labels/ground_truth.csv

Works standalone with pandas + pyarrow (no Spark/cluster needed) so it can be
run straight from a laptop against a parquet export, or against files pulled
out of HDFS with `hdfs dfs -get`. This is deliberately separate from the
Spark jobs: evaluation is an analysis step, not part of the pipeline itself.

Ground truth (`Trans_id, is_fraud, pattern`) comes only from
ingestion/generator/transaction_generator.write_labels -- the pipeline never
sees it, so scoring against it is a fair test of what the rules actually
catch on their own.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


def read_decisions(path: str) -> pd.DataFrame:
    """Read a (possibly partitioned, possibly multi-file) decisions Parquet
    dataset into one DataFrame with a plain python list in `matched_rules`.
    """
    p = Path(path)
    if pq is not None:
        df = pq.ParquetDataset(str(p)).read().to_pandas()
    else:
        df = pd.read_parquet(p)
    df["matched_rules"] = df["matched_rules"].apply(
        lambda v: list(v) if v is not None else [])
    return df


def confusion_counts(merged: pd.DataFrame) -> dict:
    pred = merged["decision"] != "PASS"
    actual = merged["is_fraud"] == 1
    return {
        "tp": int((pred & actual).sum()),
        "fp": int((pred & ~actual).sum()),
        "fn": int((~pred & actual).sum()),
        "tn": int((~pred & ~actual).sum()),
    }


def precision_recall_f1(counts: dict) -> dict:
    tp, fp, fn = counts["tp"], counts["fp"], counts["fn"]
    precision = tp / (tp + fp) if (tp + fp) else float("nan")
    recall = tp / (tp + fn) if (tp + fn) else float("nan")
    f1 = (2 * precision * recall / (precision + recall)
         if (precision + recall) and precision == precision and recall == recall else float("nan"))
    return {"precision": precision, "recall": recall, "f1": f1}


def per_pattern_recall(merged: pd.DataFrame) -> pd.DataFrame:
    """For every injected fraud PATTERN (VELOCITY, MULE, ...): of the events
    the generator labeled with that pattern, what fraction did the pipeline
    flag (with ANY rule, not necessarily the "matching" one)?
    """
    fraud = merged[merged["is_fraud"] == 1].copy()
    fraud["caught"] = fraud["decision"] != "PASS"
    return (fraud.groupby("pattern")
           .agg(n=("caught", "size"), recall=("caught", "mean"))
           .sort_index())


def per_rule_precision(merged: pd.DataFrame) -> pd.DataFrame:
    """For every RULE: of the transactions that specific rule fired on, what
    fraction are actually fraud (of any pattern)? This is the number that
    tells you whether a rule is worth keeping as-is, tightening, or dropping.
    """
    all_rules = sorted({r for rules in merged["matched_rules"] for r in rules})
    rows = []
    for rule in all_rules:
        hit = merged[merged["matched_rules"].apply(lambda L, r=rule: r in L)]
        rows.append({"rule": rule, "fired": len(hit), "precision": hit["is_fraud"].mean()})
    return pd.DataFrame(rows).set_index("rule")


def evaluate(decisions: pd.DataFrame, labels: pd.DataFrame) -> dict:
    merged = decisions.merge(labels, left_on="transaction_id", right_on="Trans_id", how="inner")
    if len(merged) == 0:
        raise ValueError("No overlap between decisions.transaction_id and labels.Trans_id -- "
                         "are you pointing at the right files?")
    counts = confusion_counts(merged)
    metrics = precision_recall_f1(counts)
    return {
        "n_matched": len(merged),
        "n_decisions": len(decisions),
        "n_labels": len(labels),
        "counts": counts,
        "metrics": metrics,
        "per_pattern_recall": per_pattern_recall(merged),
        "per_rule_precision": per_rule_precision(merged),
    }


def print_report(result: dict, out=sys.stdout):
    c, m = result["counts"], result["metrics"]
    print(f"matched {result['n_matched']} transactions "
         f"({result['n_decisions']} decisions, {result['n_labels']} labels)", file=out)
    print(f"\nOVERALL  TP={c['tp']} FP={c['fp']} FN={c['fn']} TN={c['tn']}", file=out)
    print(f"         precision={m['precision']:.3f}  recall={m['recall']:.3f}  f1={m['f1']:.3f}", file=out)
    print("\nRecall by injected fraud pattern (caught by ANY rule):", file=out)
    print(result["per_pattern_recall"].to_string(float_format=lambda v: f"{v:.2f}"), file=out)
    print("\nPrecision by rule (of what THIS rule fired on, how much is fraud):", file=out)
    print(result["per_rule_precision"].to_string(float_format=lambda v: f"{v:.2f}"), file=out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--decisions", required=True, help="path to the fraud_decisions Parquet dataset (dir or file)")
    ap.add_argument("--labels", required=True, help="path to labels/ground_truth.csv")
    ap.add_argument("--out", default=None, help="optional: write the text report to this file too")
    a = ap.parse_args(argv)

    decisions = read_decisions(a.decisions)
    labels = pd.read_csv(a.labels)
    result = evaluate(decisions, labels)
    print_report(result)
    if a.out:
        with open(a.out, "w") as f:
            print_report(result, out=f)
        print(f"\nreport written -> {a.out}")


if __name__ == "__main__":
    main()
