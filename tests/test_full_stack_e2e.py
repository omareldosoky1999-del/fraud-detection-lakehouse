from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_full_stack_e2e_covers_core_banking_path():
    text = (ROOT / '.github' / 'workflows' / 'full-stack-e2e.yml').read_text(encoding='utf-8')
    for marker in [
        'transaction_producer.py',
        'train.py',
        'promote_ensemble.py',
        'bronze_silver_job.py',
        'polaris.gold.fraud_decisions',
        'alert-consumer',
        'v1/clients/',
    ]:
        assert marker in text
    assert '--promote-alias candidate' in text
    assert '--expected-run-id' in text
