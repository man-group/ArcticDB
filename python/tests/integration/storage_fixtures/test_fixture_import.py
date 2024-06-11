from pathlib import Path
import subprocess

# Assuming arcticdb wheel has been pre-installed, which is the case in the pipeline
def test_fixture_import(monkeypatch):
    script = """
import sys
sys.path = [path for path in sys.path[1:]]
import arcticdb.storage_fixtures.s3
import arcticdb.storage_fixtures.azure
import arcticdb.storage_fixtures.mongo
import arcticdb.storage_fixtures.lmdb
    """
    p = subprocess.Popen(["python", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert p.wait() == 0, "Failed to import storage_fixtures modules"
