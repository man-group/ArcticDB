"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.

Temporary probe (not part of the normal suite): measures whether resolving "localhost" costs more than "127.0.0.1"
when talking to the moto fixture, which binds IPv4 only. On Windows "localhost" resolves to ::1 first.
"""

import socket
import time

import requests


def _time_requests(host, port, n=100):
    url = f"http://{host}:{port}/"
    session_start = time.time()
    for _ in range(n):
        try:
            requests.get(url, timeout=10)
        except Exception:
            pass
    return time.time() - session_start


def _time_connects(host, port, n=100):
    start = time.time()
    for _ in range(n):
        try:
            s = socket.create_connection((host, port), timeout=10)
            s.close()
        except Exception:
            pass
    return time.time() - start


def test_localhost_vs_ipv4_literal(s3_storage):
    port = s3_storage.factory.port
    for host in ("localhost", "127.0.0.1"):
        addrs = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        families = [a[0].name for a in addrs]
        conn = _time_connects(host, port)
        req = _time_requests(host, port)
        print(f"PROBE host={host:10} families={families} 100 connects={conn:6.2f}s 100 requests={req:6.2f}s", flush=True)
