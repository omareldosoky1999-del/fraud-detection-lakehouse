from pathlib import Path
import yaml

ROOT = Path(__file__).parents[1]


def test_storage_profiles_cover_all_targets():
    data = yaml.safe_load(
        (ROOT / "config" / "storage_profiles.yml").read_text(encoding="utf-8")
    )
    profiles = data["storage_profiles"]

    assert set(profiles) == {"local", "aws", "azure", "gcp"}
    assert profiles["local"]["fileio_impl"].endswith("S3FileIO")
    assert profiles["aws"]["fileio_impl"].endswith("S3FileIO")
    assert profiles["azure"]["fileio_impl"].endswith("ADLSFileIO")
    assert profiles["gcp"]["fileio_impl"].endswith("GCSFileIO")
