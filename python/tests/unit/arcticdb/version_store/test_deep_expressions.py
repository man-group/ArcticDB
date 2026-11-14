"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import subprocess
import sys
import textwrap
import time

import psutil
import pytest

# Run in a subprocess: a stack overflow kills the process, and exponential evaluation never finishes or exhausts memory.
_MAX_RSS_BYTES = 4 * 1024**3
_SCRIPT = textwrap.dedent("""
    import sys
    import numpy as np
    import pandas as pd
    from arcticdb import Arctic, QueryBuilder

    path, kind, depth = sys.argv[1], sys.argv[2], int(sys.argv[3])
    lib = Arctic("lmdb://" + path).create_library("lib")
    df = pd.DataFrame({"i": np.arange(10, dtype=np.int64)})
    lib.write("sym", df)

    def read(q):
        return lib.read("sym", query_builder=q).data

    def assert_filtered(q, expected):
        pd.testing.assert_frame_equal(read(q).reset_index(drop=True), expected.reset_index(drop=True))

    q = QueryBuilder()
    if kind == "not_chain":
        e = q["i"] > 3
        for _ in range(depth):
            e = ~e
        expected = df[df["i"] > 3] if depth % 2 == 0 else df[df["i"] <= 3]
        assert_filtered(q[e], expected)
    elif kind == "and_or_chain":
        e = q["i"] > 3
        for k in range(depth):
            e = (e & (q["i"] >= 0)) if k % 2 else (e | (q["i"] < 0))
        assert_filtered(q[e], df[df["i"] > 3])
    elif kind == "projection_chain":
        e = q["i"]
        for _ in range(depth):
            e = e + 1
        q = q.apply("new", e)
        pd.testing.assert_series_equal(read(q)["new"], df["i"] + depth, check_names=False)
    elif kind == "shared_filter":
        base = q["i"] > 3
        e = base
        for k in range(depth):
            e = (e & e) if k % 2 else (e | e)
        pd.testing.assert_frame_equal(read(q[e]), read(QueryBuilder()[QueryBuilder()["i"] > 3]))
    elif kind == "shared_projection":
        e = q["i"]
        for _ in range(depth):
            e = e + e
        result = read(q.apply("new", e))["new"]
        equivalent = read(QueryBuilder().apply("new", QueryBuilder()["i"] * (2**depth)))["new"]
        pd.testing.assert_series_equal(result, equivalent)
    elif kind == "label_limit":
        def build(innermost):
            builder = QueryBuilder()
            e = builder["i"] > innermost
            for _ in range(depth):
                e = ~e
            return builder[e]

        assert build(1) == build(1)
        assert build(1) != build(2)
    else:
        raise ValueError(kind)
    """)


def _run(tmp_path, kind, depth, timeout):
    # A polled RSS limit rather than RLIMIT_AS, which macOS does not enforce and sanitizers cannot run under.
    # Output goes to a file because a pipe that nobody reads would block the child once full.
    output_path = tmp_path / "output.log"
    failure = None
    with open(output_path, "w") as output_file:
        process = subprocess.Popen(
            [sys.executable, "-c", _SCRIPT, str(tmp_path), kind, str(depth)],
            stdout=output_file,
            stderr=subprocess.STDOUT,
        )
        monitored = psutil.Process(process.pid)
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None and failure is None:
                try:
                    if monitored.memory_info().rss > _MAX_RSS_BYTES:
                        failure = f"exceeded {_MAX_RSS_BYTES} bytes of RSS"
                except psutil.NoSuchProcess:
                    break
                if time.monotonic() > deadline:
                    failure = f"timed out after {timeout}s"
                time.sleep(0.02)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
    assert failure is None, failure
    assert process.returncode == 0, f"exit code {process.returncode}\n{output_path.read_text()[-3000:]}"


@pytest.mark.parametrize("depth", [1_000, 10_000])
@pytest.mark.parametrize("kind", ["not_chain", "and_or_chain", "projection_chain"])
def test_deeply_nested_expression(tmp_path, kind, depth):
    _run(tmp_path, kind, depth, timeout=120)


@pytest.mark.parametrize("kind", ["shared_filter", "shared_projection"])
def test_shared_subexpressions_are_evaluated_once(tmp_path, kind):
    # Each level uses the level below twice, so evaluating per path costs 2**30 node evaluations
    _run(tmp_path, kind, 30, timeout=60)


def test_query_builders_differing_beyond_the_label_limit_are_not_equal(tmp_path):
    _run(tmp_path, "label_limit", 500, timeout=60)
