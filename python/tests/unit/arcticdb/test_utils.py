"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import gc

import pytest

from unittest.mock import patch
from arcticdb.util.utils import deferred_gc


@pytest.fixture(autouse=True)
def restore_gc():
    # Every test here leaves the interpreter's collector however it found it, including when the
    # assertion under test is the thing that fails.
    was_enabled = gc.isenabled()
    try:
        yield
    finally:
        gc.enable() if was_enabled else gc.disable()


def test_deferred_gc_disables_inside_and_restores_after():
    gc.enable()

    with deferred_gc():
        assert not gc.isenabled()

    assert gc.isenabled()


def test_deferred_gc_restores_when_the_block_raises():
    gc.enable()

    with pytest.raises(ValueError, match="from inside the block"):
        with deferred_gc():
            assert not gc.isenabled()
            raise ValueError("from inside the block")

    assert gc.isenabled()


@pytest.mark.parametrize("raises", [True, False])
def test_deferred_gc_leaves_an_already_disabled_collector_alone(raises):
    # A caller running with collection deliberately off must not have it switched back on underneath them.
    gc.disable()

    if raises:
        with pytest.raises(ValueError):
            with deferred_gc():
                raise ValueError("from inside the block")
    else:
        with deferred_gc():
            assert not gc.isenabled()

    assert not gc.isenabled()


def test_deferred_gc_does_not_force_a_collection():
    # The point of the guard is to skip tracing a live set, so it must not pay that trace on the way out;
    # whatever the block allocated is still reachable and the next scheduled collection can deal with it.
    gc.enable()

    with patch("gc.collect") as collect:
        with deferred_gc():
            pass

    collect.assert_not_called()
