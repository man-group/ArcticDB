"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import multiprocessing
import socket
import threading
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import pytest

from arcticc.pb2.s3_storage_pb2 import Config as S3Config
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticdb.config import Defaults
from arcticdb.exceptions import NoSuchVersionException
from arcticdb.version_store.helper import ArcticMemoryConfig

SYMBOL = "sym"
CHILD_TIMEOUT_S = 60


def _get_info_of_missing_symbol(lib):
    with pytest.raises(NoSuchVersionException):
        lib.get_info("missing")


CALLS = {
    "read_descriptor": lambda lib: lib.get_info(SYMBOL),
    "read_descriptor_missing_symbol": _get_info_of_missing_symbol,
    "batch_read_descriptor": lambda lib: lib.batch_get_info([SYMBOL]),
    "read_metadata": lambda lib: lib.read_metadata(SYMBOL),
    "batch_read_metadata": lambda lib: lib.batch_read_metadata([SYMBOL]),
}


def _pump(src, dst):
    try:
        while data := src.recv(65536):
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def _serve_proxy(listener, target):
    while True:
        client, _ = listener.accept()
        try:
            upstream = socket.create_connection(target)
        except OSError:
            client.close()
            continue
        for src, dst in ((client, upstream), (upstream, client)):
            threading.Thread(target=_pump, args=(src, dst), daemon=True).start()


def _route_s3_through_proxy(cfg):
    for storage in cfg.env_by_id[Defaults.ENV].storage_by_id.values():
        s3 = S3Config()
        storage.config.Unpack(s3)
        endpoint = urlsplit(s3.endpoint)
        assert endpoint.port is not None, f"needs an endpoint with an explicit port, got {s3.endpoint}"
        listener = socket.create_server((endpoint.hostname, 0))
        proxy_port = listener.getsockname()[1]
        s3.endpoint = urlunsplit(endpoint._replace(netloc=f"{endpoint.hostname}:{proxy_port}"))
        storage.config.Pack(s3, type_url_prefix="cxx.arctic.org")
        threading.Thread(target=_serve_proxy, args=(listener, (endpoint.hostname, endpoint.port)), daemon=True).start()


def _call_through_python_proxy(cfg_bytes, native_cfg, lib_name, call_name):
    # Every storage request goes through a proxy running in a Python thread of this process, so it can only be served
    # while the calling thread has released the GIL.
    cfg = EnvironmentConfigsMap()
    cfg.ParseFromString(cfg_bytes)
    _route_s3_through_proxy(cfg)
    lib = ArcticMemoryConfig(cfg, Defaults.ENV, native_cfg)[lib_name]
    CALLS[call_name](lib)


@pytest.mark.parametrize("call_name", list(CALLS))
def test_blocking_read_releases_gil(s3_storage, lib_name, call_name):
    lib = s3_storage.create_version_store_factory(lib_name)()
    lib.write(SYMBOL, pd.DataFrame({"col": [1, 2, 3]}), metadata={"meta": 1})
    cfg, native_cfg = s3_storage.create_test_cfg(lib_name)

    ctx = multiprocessing.get_context("spawn")
    p = ctx.Process(target=_call_through_python_proxy, args=(cfg.SerializeToString(), native_cfg, lib_name, call_name))
    p.start()
    p.join(CHILD_TIMEOUT_S)
    if p.is_alive():
        p.kill()
        p.join()
        pytest.fail(
            f"{call_name} did not complete within {CHILD_TIMEOUT_S}s: it waits on storage while holding the GIL"
        )
    assert p.exitcode == 0
