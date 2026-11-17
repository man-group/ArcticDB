"""
Copyright 2026 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

PYTHON_DIR = Path(__file__).resolve().parents[2]

HEAVY_TESTS = """
import os
import time
import pytest

@pytest.mark.xdist_group(name="stress_heavy")
@pytest.mark.parametrize("i", range(4))
def test_heavy(i):
    start = time.monotonic()
    time.sleep(0.5)
    with open(os.environ["INTERVAL_LOG"], "a") as f:
        f.write(f"{os.environ['PYTEST_XDIST_WORKER']} {start} {time.monotonic()}\\n")
"""


@pytest.mark.parametrize("dist", ["worksteal", "loadgroup"])
def test_stress_heavy_tests_never_overlap(tmp_path, dist):
    # Own ini file so the toy suite gets its own rootdir and none of the ArcticDB conftest.
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "conftest.py").write_text("from tests.util.stress_heavy_lock import serialise_stress_heavy_tests\n")
    (tmp_path / "test_toy.py").write_text(HEAVY_TESTS)
    # Drop this run's PYTEST_* variables, PYTEST_XDIST_WORKER above all, which the fixture checks.
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_")}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(PYTHON_DIR), env.get("PYTHONPATH")]))
    env["INTERVAL_LOG"] = str(tmp_path / "intervals")
    cmd = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-n", "2", "--dist", dist]
    cmd += [f"--basetemp={tmp_path / 'base'}", str(tmp_path)]
    result = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr

    rows = [line.split() for line in (tmp_path / "intervals").read_text().splitlines()]
    assert len(rows) == 4
    if dist == "worksteal":
        # Both workers got tests, so without the lock they would have run concurrently. Not checked for loadgroup,
        # which puts them all on one worker wherever xdist honours xdist_group.
        assert len({worker for worker, _, _ in rows}) == 2
    intervals = sorted((float(start), float(end)) for _, start, end in rows)
    assert all(later[0] >= earlier[1] for earlier, later in zip(intervals, intervals[1:]))
