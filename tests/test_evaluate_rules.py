import pandas as pd
import pytest

from scripts.evaluate_rules import (
    confusion_counts, evaluate, per_pattern_recall, per_rule_precision, precision_recall_f1)


def decisions_df():
    return pd.DataFrame([
        dict(transaction_id=1, matched_rules=["HIGH_AMOUNT"], risk_score=45, decision="BLOCK"),
        dict(transaction_id=2, matched_rules=[], risk_score=0, decision="PASS"),
        dict(transaction_id=3, matched_rules=["VELOCITY"], risk_score=20, decision="FLAG"),
        dict(transaction_id=4, matched_rules=[], risk_score=0, decision="PASS"),
        dict(transaction_id=5, matched_rules=["HIGH_AMOUNT", "MULE"], risk_score=90, decision="BLOCK"),
    ])


def labels_df():
    return pd.DataFrame([
        dict(Trans_id=1, is_fraud=1, pattern="HIGH_AMOUNT"),
        dict(Trans_id=2, is_fraud=0, pattern="NORMAL"),
        dict(Trans_id=3, is_fraud=0, pattern="NORMAL"),   # false positive
        dict(Trans_id=4, is_fraud=1, pattern="MULE"),      # false negative (missed)
        dict(Trans_id=5, is_fraud=1, pattern="MULE"),
    ])


def test_confusion_and_prf():
    merged = decisions_df().merge(labels_df(), left_on="transaction_id", right_on="Trans_id")
    counts = confusion_counts(merged)
    assert counts == {"tp": 2, "fp": 1, "fn": 1, "tn": 1}
    m = precision_recall_f1(counts)
    assert m["precision"] == pytest.approx(2 / 3)
    assert m["recall"] == pytest.approx(2 / 3)


def test_per_pattern_recall():
    merged = decisions_df().merge(labels_df(), left_on="transaction_id", right_on="Trans_id")
    out = per_pattern_recall(merged)
    assert out.loc["HIGH_AMOUNT", "recall"] == 1.0
    assert out.loc["MULE", "n"] == 2
    assert out.loc["MULE", "recall"] == pytest.approx(0.5)  # caught 5, missed 4


def test_per_rule_precision():
    merged = decisions_df().merge(labels_df(), left_on="transaction_id", right_on="Trans_id")
    out = per_rule_precision(merged)
    assert out.loc["VELOCITY", "precision"] == 0.0   # fired on txn 3, which is not fraud
    assert out.loc["HIGH_AMOUNT", "precision"] == 1.0  # fired on 1 and 5, both fraud
    assert out.loc["MULE", "fired"] == 1


def test_evaluate_raises_on_no_overlap():
    dec = decisions_df().assign(transaction_id=lambda d: d.transaction_id + 1000)
    with pytest.raises(ValueError, match="No overlap"):
        evaluate(dec, labels_df())
