"""
QueryBuilders are pickled to ship queries between processes (e.g. to distributed workers), which may run different
ArcticDB versions. Check that resample clauses survive a pickle round trip between an old release and this version.
"""

import pickle

import numpy as np
import pandas as pd
import pytest
from packaging.version import Version
from pandas.tseries.frequencies import to_offset

from arcticdb import QueryBuilder
from arcticdb.util.test import assert_frame_equal
from arcticdb.util.venv import CompatLibrary
from tests.util.mark import ARCTICDB_USING_CONDA, MACOS_WHEEL_BUILD, WINDOWS

if ARCTICDB_USING_CONDA:
    pytest.skip("These tests rely on pip based environments", allow_module_level=True)

if MACOS_WHEEL_BUILD:
    pytest.skip("We don't have previous versions of arcticdb pypi released for MacOS", allow_module_level=True)

SYM = "sym"
AGGREGATIONS = {"col": "sum", "col_max": ("col", "max"), "col_mean": ("col", "mean")}

# (first version supporting these arguments, resample kwargs)
# resample arrived by 4.5.1, offset in 5.0.0, origin in 5.2.0
RESAMPLE_VARIANTS = [
    ("4.5.1", {"rule": "10min"}),
    ("4.5.1", {"rule": "15s", "label": "right"}),
    ("4.5.1", {"rule": "1h", "closed": "right"}),
    ("4.5.1", {"rule": "1h", "closed": "right", "label": "right"}),
    ("4.5.1", {"rule": "1D", "closed": "left", "label": "left"}),
    ("5.0.0", {"rule": "1h", "offset": "7min"}),
    ("5.0.0", {"rule": "30min", "closed": "right", "label": "right", "offset": "5min"}),
    ("5.2.0", {"rule": "2h", "origin": "start"}),
    ("5.2.0", {"rule": "45min", "closed": "right", "origin": "start_day"}),
    ("5.2.0", {"rule": "1h", "closed": "right", "origin": pd.Timestamp("2024-01-01 00:13:00")}),
]


def _variants_or_skip(version):
    if Version(version) < Version("4.5.1"):
        pytest.skip(f"arcticdb {version} has no QueryBuilder.resample")
    return [kwargs for min_version, kwargs in RESAMPLE_VARIANTS if Version(version) >= Version(min_version)]


def _df():
    index = pd.date_range("2024-01-01 00:00:17", periods=1000, freq="37s")
    return pd.DataFrame({"col": np.arange(len(index), dtype=np.int64)}, index=index)


def _fresh_query(kwargs):
    return QueryBuilder().resample(**kwargs).agg(AGGREGATIONS)


def _old_venv_prelude(variants):
    return [
        "import pickle",
        "from arcticdb import QueryBuilder",
        # repr(variants) spells a pd.Timestamp origin as Timestamp(...)
        "from pandas import Timestamp",
        f"variants = {variants!r}",
        f"aggregations = {AGGREGATIONS!r}",
    ]


def test_resample_query_builder_pickled_by_old_version(old_venv, s3_ssl_disabled_storage, lib_name, tmp_path):
    variants = _variants_or_skip(old_venv.version)
    pickle_path = tmp_path / "query_builders.pkl"
    with CompatLibrary(old_venv, s3_ssl_disabled_storage.arctic_uri, lib_name) as compat:
        compat.old_lib.execute(
            _old_venv_prelude(variants)
            + [
                f"lib.write({SYM!r}, df)",
                "qbs = [QueryBuilder().resample(**kwargs).agg(aggregations) for kwargs in variants]",
                f"open({str(pickle_path)!r}, 'wb').write(pickle.dumps(qbs))",
            ],
            dfs={"df": _df()},
        )

        payload = pickle_path.read_bytes()
        # The old version knows nothing of rule_ns, so unpickling must take the fallback that parses the rule string
        assert b"rule_ns" not in payload
        query_builders = pickle.loads(payload)
        assert len(query_builders) == len(variants)

        with compat.current_version() as curr:
            for kwargs, unpickled in zip(variants, query_builders):
                assert unpickled._python_clauses[0].rule_ns == to_offset(kwargs["rule"]).nanos
                expected = curr.lib.read(SYM, query_builder=_fresh_query(kwargs)).data
                received = curr.lib.read(SYM, query_builder=unpickled).data
                assert not expected.empty
                assert_frame_equal(expected, received)


def test_resample_query_builder_pickled_by_current_version(old_venv, s3_ssl_disabled_storage, lib_name, tmp_path):
    variants = _variants_or_skip(old_venv.version)
    if WINDOWS and old_venv.version == "5.0.0":
        # The os._exit below hangs on Windows before 6.1.2: the TaskScheduler destructor waits for the killed
        # threads (fixed in 6.1.2 by #2582)
        pytest.skip("arcticdb 5.0.0 hangs in os._exit on Windows")
    pickle_path = tmp_path / "query_builders.pkl"
    payload = pickle.dumps([_fresh_query(kwargs) for kwargs in variants])
    assert b"rule_ns" in payload
    pickle_path.write_bytes(payload)

    with CompatLibrary(old_venv, s3_ssl_disabled_storage.arctic_uri, lib_name) as compat:
        compat.old_lib.execute(
            _old_venv_prelude(variants)
            + [
                f"lib.write({SYM!r}, df)",
                f"qbs = pickle.loads(open({str(pickle_path)!r}, 'rb').read())",
                "assert len(qbs) == len(variants)",
                # One command: the venv runner interleaves a print between commands, which would break a loop body.
                # Old versions' arcticdb.util.test differs, so compare with pandas directly.
                f"""
for kwargs, unpickled in zip(variants, qbs):
    expected = lib.read({SYM!r}, query_builder=QueryBuilder().resample(**kwargs).agg(aggregations)).data
    received = lib.read({SYM!r}, query_builder=unpickled).data
    assert not expected.empty
    pd.testing.assert_frame_equal(expected, received)
""",
            ]
            # arcticdb 5.0.0 segfaults at interpreter exit after any resample read, pickled or not; skip interpreter
            # teardown once the assertions above have passed
            + (["import os, sys", "sys.stdout.flush()", "os._exit(0)"] if old_venv.version == "5.0.0" else []),
            dfs={"df": _df()},
        )
