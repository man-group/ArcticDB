"""Port allocation for the test storage fixtures (moto, azurite, mongod)."""

import time

from arcticdb.storage_fixtures.utils import get_ephemeral_port


def test_get_ephemeral_port_returns_promptly():
    # This used to hold the socket open for 20s on every call to detect collisions, which cost ~22% of the
    # integration suite and ~25% of the unit suite across every moto, azurite and mongod startup.
    start = time.monotonic()
    get_ephemeral_port()
    assert time.monotonic() - start < 5


def test_get_ephemeral_port_does_not_repeat():
    ports = [get_ephemeral_port(seed) for seed in (1, 2, 7, 10, 20) for _ in range(3)]
    assert len(set(ports)) == len(ports), f"duplicate ports handed out: {sorted(ports)}"
