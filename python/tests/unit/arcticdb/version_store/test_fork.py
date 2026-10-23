"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import multiprocessing
import os
import subprocess
import sys
import textwrap
import time
from multiprocessing import Pool
import numpy as np
import pandas as pd
import pytest
from arcticdb import Arctic
from arcticdb_ext import set_config_int, unset_config_int

from arcticdb.util.test import assert_frame_equal

from tests.util.mark import SKIP_CONDA_MARK

FORK_SUPPORTED = pytest.mark.skipif(sys.platform == "win32", reason="fork/forkserver not available on Windows")

FORK_WARNING_SUPPORTED = pytest.mark.skipif(
    sys.version_info < (3, 12) or sys.platform == "darwin",
    reason="The fork warning is only compiled in for Python 3.12+, and not on macOS",
)

FORK_WARNING = "fork() called in a process using ArcticDB"


def df(symbol):
    return pd.DataFrame({symbol: np.arange(100)})


def write_symbol(args):
    store, symbol = args
    print("start {}".format(symbol))
    store.write(symbol, df(symbol))
    print("end {}".format(symbol))
    return symbol


def test_map(lmdb_version_store):
    symbols = ["XXX", "YYY"]
    p = Pool(1)
    p.map(write_symbol, [(lmdb_version_store, s) for s in symbols])
    for s in symbols:
        vit = lmdb_version_store.read(s)
        assert_frame_equal(vit.data, df(s))
    p.close()
    p.join()


def _read_and_assert_symbol(args):
    from arcticdb.exceptions import NoSuchVersionException

    lib, symbol, idx = args
    for attempt in range(1, 11):
        print("start {}_{} attempt {}".format(symbol, idx, attempt))
        try:
            ss = lib.read(symbol)
        except NoSuchVersionException:
            print("attempt {} fail (symbol not found yet)".format(attempt))
            time.sleep(0.5)
            continue
        if df("test1").equals(ss.data):
            assert_frame_equal(ss.data, df("test1"))
            print("end {}".format(idx))
            return
        print("attempt {} fail".format(attempt))
        time.sleep(0.5)  # Make sure the writes have finished, especially azurite.
    raise AssertionError(f"Symbol {symbol!r} not readable after 10 attempts")


@pytest.mark.parametrize(
    "store_factory",
    [
        "s3_store_factory",
        pytest.param(
            "azure_store_factory",
            marks=pytest.mark.skip(
                reason="Azure SDK's CurlConnectionPool is global and is not fork-safe. Monday ref 12128961896"
            ),
        ),
    ],
)
def test_parallel_reads(store_factory, request):
    lib = request.getfixturevalue(store_factory)()
    symbols = ["XXX"] * 20
    lib.write(symbols[0], df("test1"))
    time.sleep(0.1)  # Make sure the writes have finished, especially azurite.
    p = Pool(10)
    p.map(_read_and_assert_symbol, [(lib, s, idx) for idx, s in enumerate(symbols)])
    p.close()
    p.join()


@pytest.mark.parametrize("storage_name", ["s3_storage", "gcp_storage"])
@SKIP_CONDA_MARK
def test_parallel_reads_arctic(storage_name, request, lib_name):
    storage = request.getfixturevalue(storage_name)
    ac = Arctic(storage.arctic_uri)
    try:
        lib = ac.create_library(lib_name)
        symbols = [f"{i}" for i in range(10)]
        for s in symbols:
            lib.write(s, df("test1"))
        p = Pool(10)
        p.map(_read_and_assert_symbol, [(lib, s, idx) for idx, s in enumerate(symbols)])
        p.close()
        p.join()
    finally:
        ac.delete_library(lib_name)


def _check_config_in_child(args):
    """Worker function: verify ConfigsMap was propagated via pickle."""
    _obj, key, expected = args
    from arcticdb_ext import get_config_int

    actual = get_config_int(key)
    assert actual == expected, f"Config {key}: expected {expected}, got {actual}"


@pytest.mark.parametrize(
    "start_method",
    [
        "spawn",
        pytest.param("forkserver", marks=FORK_SUPPORTED),
    ],
)
@pytest.mark.parametrize("store_fixture", ["lmdb_version_store_v1", "lmdb_library"])
def test_configs_propagated_to_child_process(request, store_fixture, start_method):
    """ConfigsMap settings must survive spawn/forkserver process boundaries via pickle."""
    store = request.getfixturevalue(store_fixture)
    set_config_int("TestPropagation", 12345)
    try:
        ctx = multiprocessing.get_context(start_method)
        with ctx.Pool(1) as p:
            p.map(_check_config_in_child, [(store, "TestPropagation", 12345)])
    finally:
        unset_config_int("TestPropagation")


def _run_and_count(argv, env=None):
    child_env = dict(os.environ)
    child_env.update(env or {})
    proc = subprocess.run(argv, capture_output=True, text=True, env=child_env)
    assert proc.returncode == 0, proc.stderr
    return proc.stderr.count(FORK_WARNING)


def _count_fork_warnings(body, env=None):
    """Run body in a fresh interpreter and count the fork warnings it logs to stderr."""
    return _run_and_count([sys.executable, "-c", textwrap.dedent(body)], env)


def _count_fork_warnings_in_script(tmp_path, body, env=None):
    """As _count_fork_warnings, but from a real script file.

    The forkserver start method preloads __main__, which it cannot do for `python -c`. Only a
    script on disk makes the forkserver process import arcticdb.
    """
    script = tmp_path / "fork_body.py"
    script.write_text(textwrap.dedent(body))
    return _run_and_count([sys.executable, str(script)], env)


# The warning is only logged once ArcticDB has started IO threads
_IMPORT_AND_WRITE = """
    import os
    import sys
    import tempfile
    import pandas as pd
    import arcticdb

    _lib = arcticdb.Arctic("lmdb://" + tempfile.mkdtemp()).get_library("arm", create_if_missing=True)
    _lib.write("s", pd.DataFrame({"a": [1, 2, 3]}))
"""

_FORK_ONCE = _IMPORT_AND_WRITE + """
    pid = os.fork()
    if pid == 0:
        os._exit(0)
    os.waitpid(pid, 0)
"""


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_fork_warning_logged():
    assert _count_fork_warnings(_FORK_ONCE) == 1


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_fork_warning_logged_for_pool():
    assert _count_fork_warnings(_IMPORT_AND_WRITE + """
    import multiprocessing

    with multiprocessing.get_context("fork").Pool(3) as pool:
        assert pool.map(abs, [-1, -2, -3]) == [1, 2, 3]
    """) == 1


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_fork_warning_logged_once_per_process():
    assert _count_fork_warnings(_IMPORT_AND_WRITE + """
    for _ in range(3):
        pid = os.fork()
        if pid == 0:
            os._exit(0)
        os.waitpid(pid, 0)
    """) == 1


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_fork_warning_disabled_by_env_var():
    assert _count_fork_warnings(_FORK_ONCE, env={"ARCTICDB_Fork_WarnOnFork_int": "0"}) == 0


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_fork_warning_disabled_by_config():
    assert _count_fork_warnings(_IMPORT_AND_WRITE + """
    from arcticdb_ext import set_config_int

    set_config_int("Fork.WarnOnFork", 0)
    pid = os.fork()
    if pid == 0:
        os._exit(0)
    os.waitpid(pid, 0)
    """) == 0


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_no_fork_warning_without_arcticdb_io():
    """Creating a library and listing symbols starts no pool threads, so forking is safe."""
    assert _count_fork_warnings("""
    import os
    import sys
    import tempfile
    import arcticdb

    lib = arcticdb.Arctic("lmdb://" + tempfile.mkdtemp()).get_library("x", create_if_missing=True)
    assert lib.list_symbols() == []

    pid = os.fork()
    if pid == 0:
        os._exit(0)
    os.waitpid(pid, 0)
    """) == 0


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_no_fork_warning_for_spawn_pool():
    assert _count_fork_warnings(_IMPORT_AND_WRITE + """
    import multiprocessing

    with multiprocessing.get_context("spawn").Pool(2) as pool:
        assert pool.map(abs, [-1, -2]) == [1, 2]
    """) == 0


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_no_fork_warning_for_subprocess():
    assert _count_fork_warnings(_IMPORT_AND_WRITE + """
    import subprocess

    subprocess.run([sys.executable, "-c", ""], check=True)
    """) == 0


# A forkserver preloads __main__, so the work has to sit under the guard for the forkserver process
# not to run it. That is also how multiprocessing asks you to write a script.
_POOL_IN_SCRIPT = """
    import tempfile
    import multiprocessing
    import pandas as pd
    import arcticdb

    if __name__ == "__main__":
        lib = arcticdb.Arctic("lmdb://" + tempfile.mkdtemp()).get_library("x", create_if_missing=True)
        lib.write("s", pd.DataFrame({"a": [1, 2, 3]}))
        with multiprocessing.get_context("__START_METHOD__").Pool(2) as pool:
            assert pool.map(abs, [-1, -2]) == [1, 2]
"""


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_no_fork_warning_for_forkserver_pool(tmp_path):
    """The forkserver process forks every worker, but holds no ArcticDB threads of its own.

    It imports __main__ to preload it, so it registers the atfork handler, but the __main__ guard
    keeps the ArcticDB work out of it. The parent holds the threads and never forks.
    """
    body = _POOL_IN_SCRIPT.replace("__START_METHOD__", "forkserver")
    assert _count_fork_warnings_in_script(tmp_path, body) == 0


@FORK_SUPPORTED
@FORK_WARNING_SUPPORTED
def test_fork_warning_logged_for_fork_pool_in_script(tmp_path):
    """Positive control for test_no_fork_warning_for_forkserver_pool."""
    body = _POOL_IN_SCRIPT.replace("__START_METHOD__", "fork")
    assert _count_fork_warnings_in_script(tmp_path, body) == 1


def test_set_config_int_overrides_env_var_after_spawn(lmdb_version_store_v1, monkeypatch):
    """When a config key has both an env var (ARCTICDB_<KEY>_INT) and a
    set_config_int override, the child sees the set_config_int value.

    Env vars survive spawn naturally because the child re-runs
    set_config_from_env_vars on import. The pickle payload is then merged
    on top via __setstate__, so the explicit value takes precedence."""
    monkeypatch.setenv("ARCTICDB_SPAWNOVERRIDE_INT", "42")
    set_config_int("SPAWNOVERRIDE", 99)
    try:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(1) as p:
            # Child import sets SPAWNOVERRIDE=42 from env var,
            # then __setstate__ overwrites it to 99 from the pickle payload.
            p.map(_check_config_in_child, [(lmdb_version_store_v1, "SPAWNOVERRIDE", 99)])
    finally:
        unset_config_int("SPAWNOVERRIDE")
