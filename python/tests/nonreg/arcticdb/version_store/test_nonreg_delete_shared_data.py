"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pandas as pd
import pytest

from arcticdb.util.test import assert_frame_equal
from arcticdb.exceptions import KeyNotFoundException

SYM = "sym"


def _df(value, start="2020-01-01"):
    return pd.DataFrame({"x": [value] * 3}, index=pd.date_range(start, periods=3))


def _write_version_sharing_data_across_a_snapshotted_gap(lib, share_via_append):
    """Leaves v0 live, v1 held only by snapshot "a", and v2 held only by snapshot "b".

    v2 shares v0's data key, because v0 was the latest live version when v2 was written.
    """
    lib.write(SYM, _df(1))
    lib.write(SYM, _df(2))
    lib.snapshot("a", versions={SYM: 1})
    lib.delete_version(SYM, 1)
    if share_via_append:
        lib.append(SYM, _df(3, start="2021-01-01"))
    else:
        lib.write(SYM, _df(1))
    lib.snapshot("b", versions={SYM: 2})
    lib.delete_version(SYM, 2)
    return lib.read(SYM, as_of="b").data


def _delete_version(lib):
    lib.delete_version(SYM, 0)


def _delete_last_snapshot_holding_version(lib):
    lib.snapshot("z", versions={SYM: 0})
    lib.delete_version(SYM, 0)
    lib.delete_snapshot("z")


@pytest.mark.xfail(
    reason="delete_trees_responsibly protects only the nearest live and snapshotted neighbours of a deleted version",
    raises=KeyNotFoundException,
    strict=True,
)
@pytest.mark.parametrize(
    "de_duplication, share_via_append",
    [(True, False), (True, True), (False, True)],
    ids=["dedup_write", "dedup_append", "append"],
)
@pytest.mark.parametrize("delete", [_delete_version, _delete_last_snapshot_holding_version])
def test_delete_keeps_data_shared_with_non_adjacent_snapshotted_version(
    version_store_factory, de_duplication, share_via_append, delete
):
    lib = version_store_factory(use_tombstones=True, de_duplication=de_duplication)
    expected = _write_version_sharing_data_across_a_snapshotted_gap(lib, share_via_append)

    delete(lib)

    assert_frame_equal(lib.read(SYM, as_of="b").data, expected)
