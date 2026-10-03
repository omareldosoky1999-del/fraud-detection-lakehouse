"""Runtime paths must not depend on the process working directory.

`docker exec spark-master spark-submit /app/...` starts in the image's workdir,
not /app, which previously broke training with
FileNotFoundError: ingestion/data/cards.avro.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_defaults_resolve_from_any_directory(tmp_path):
    code = (
        "import os\n"
        "from pathlib import Path\n"
        "from processing.ml.registry import DEFAULT_CONFIG\n"
        "from processing.ml.train import DATA_DIR\n"
        "assert Path(DEFAULT_CONFIG).is_file(), DEFAULT_CONFIG\n"
        "for name in ['cards.avro', 'clients.avro', 'devices.avro']:\n"
        "    assert (Path(DATA_DIR) / name).is_file(), name\n"
        "print('ok', os.getcwd())\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path, text=True, capture_output=True,
        env={"PYTHONPATH": str(ROOT), "PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
    )
    assert proc.returncode == 0, proc.stderr[-1500:]
    assert proc.stdout.startswith("ok")


def test_small_training_sets_have_too_few_validation_positives():
    """Documents why train.py enforces a minimum number of validation positives:
    with seed 42 the temporal 80/20 split leaves no fraud rows in validation for
    small --n, which used to surface as an opaque 'AUC 0.0000'."""
    from datetime import datetime

    from ingestion.generator.transaction_generator import generate
    from processing.ml.train import DATA_DIR, MIN_VALIDATION_POSITIVES

    fmt = "%Y-%m-%d %I:%M:%S %p"
    events = generate(1000, seed=42, start=datetime(2026, 9, 1), data_dir=DATA_DIR)
    times = [datetime.strptime(e.rec["Trans_date"], fmt) for e in events]
    cutoff = sorted(times)[int(len(times) * 0.8)]
    val_pos = sum(e.is_fraud for e, t in zip(events, times) if t >= cutoff)
    assert val_pos < MIN_VALIDATION_POSITIVES
