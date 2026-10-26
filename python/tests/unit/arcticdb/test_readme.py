"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import ast
import re
from pathlib import Path

from pandas.testing import assert_frame_equal

README = Path(__file__).resolve().parents[4] / "README.md"
PYTHON_FENCES = re.findall(r"^```python\n(.*?)^```", README.read_text(encoding="utf-8"), re.S | re.M | re.I)


def test_readme_has_python_fences():
    assert PYTHON_FENCES


def test_readme_python_fences_parse():
    for fence in PYTHON_FENCES:
        ast.parse(fence)


def test_readme_lmdb_quickstart_runs(tmp_path):
    code = "\n".join(fence for fence in PYTHON_FENCES if "s3://" not in fence)
    namespace = {}
    exec(code.replace("lmdb:///<path>", f"lmdb://{tmp_path.as_posix()}"), namespace)
    assert_frame_equal(namespace["data"].data, namespace["df"], check_freq=False)
