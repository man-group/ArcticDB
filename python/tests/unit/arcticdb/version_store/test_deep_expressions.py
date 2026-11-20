"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import subprocess
import sys
import textwrap
import time

import numpy as np
import pandas as pd
import psutil
import pytest

from arcticdb import QueryBuilder, where
from arcticdb.util.test import assert_frame_equal

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
        assert_filtered(q[e], df[df["i"] > 3])
    elif kind == "shared_projection":
        e = q["i"]
        for _ in range(depth):
            e = e + e
        result = read(q.apply("new", e))["new"]
        pd.testing.assert_series_equal(result, df["i"] * 2**depth, check_names=False)
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


def _deep_filter(q, kind):
    e = q["i"] > 3
    if kind == "not_chain":
        for _ in range(300):
            e = ~e
    elif kind == "shared_filter":
        for k in range(12):
            e = (e & e) if k % 2 else (e | e)
    elif kind == "shared_ternary":
        for _ in range(12):
            e = where(q["b"], e, e)
    else:
        raise ValueError(kind)
    return q[e]


# In process and shallower than the cases above, so that it can run over many segments and with column stats
@pytest.mark.parametrize("kind", ["not_chain", "shared_filter", "shared_ternary"])
def test_deep_and_shared_filters_over_many_segments(
    lmdb_version_store_tiny_segment, column_stats_filtering_enabled_and_disabled, kind
):
    lib = lmdb_version_store_tiny_segment
    index = pd.date_range("2026-01-01", periods=20)
    df = pd.DataFrame({"i": np.arange(20, dtype=np.int64), "b": np.arange(20) % 2 == 0}, index=index)
    lib.write("sym", df)
    lib.create_column_stats_experimental("sym")
    received = lib.read("sym", query_builder=_deep_filter(QueryBuilder(), kind)).data
    assert_frame_equal(received, df[df["i"] > 3])


def test_shared_filter_on_a_column_missing_from_some_segments(lmdb_version_store_tiny_segment_dynamic):
    lib = lmdb_version_store_tiny_segment_dynamic
    index = pd.date_range("2026-01-01", periods=12)
    with_column = pd.DataFrame({"i": np.arange(6, dtype=np.int64), "b": [True] * 6}, index=index[:6])
    lib.write("sym", with_column)
    lib.append("sym", pd.DataFrame({"b": [False] * 6}, index=index[6:]))
    received = lib.read("sym", query_builder=_deep_filter(QueryBuilder(), "shared_filter")).data
    assert_frame_equal(received, with_column[with_column["i"] > 3])


def test_repeated_shared_projection_with_nans_over_many_segments(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    values = np.arange(20, dtype=np.float64)
    values[::3] = np.nan
    df = pd.DataFrame({"f": values}, index=pd.date_range("2026-01-01", periods=20))
    lib.write("sym", df)
    q = QueryBuilder()

    def doubled(times):
        e = q["f"]
        for _ in range(times):
            e = e + e
        return e

    # Two structurally equal copies built separately, so the second reuses the first's result
    q = q.apply("new", doubled(12) + doubled(12))
    received = lib.read("sym", query_builder=q).data
    expected = df.assign(new=df["f"] * 2**13)
    assert_frame_equal(received, expected)


def test_repeated_string_sub_expression_over_many_segments(lmdb_version_store_tiny_segment_dynamic_strings):
    lib = lmdb_version_store_tiny_segment_dynamic_strings
    rng = np.random.default_rng(0)
    df = pd.DataFrame(
        {
            "s": rng.choice(["x", "y", "z"], 20).astype(object),
            "t": rng.choice(["x", "y", "z"], 20).astype(object),
            "b": rng.random(20) < 0.5,
            "c": rng.random(20) < 0.5,
        },
        index=pd.date_range("2026-01-01", periods=20),
    )
    lib.write("sym", df)
    q = QueryBuilder()

    def pick():
        return where(q["b"], q["s"], q["t"])

    # Two structurally equal copies producing a string column, at different operand positions
    q = q.apply("picked", where(q["c"], pick(), pick()))
    received = lib.read("sym", query_builder=q).data
    assert_frame_equal(received, df.assign(picked=np.where(df["b"], df["s"], df["t"])))
