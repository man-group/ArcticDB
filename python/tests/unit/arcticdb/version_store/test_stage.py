"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pickle

import pandas as pd
import pytest

from arcticdb import LibraryOptions
from arcticdb_ext.version_store import StageResult
from arcticdb_ext import set_config_int
from arcticdb.version_store.library import StagedDataFinalizeMethod
from arcticdb.util.test import assert_frame_equal


@pytest.mark.parametrize("new_api_enabled", [True, False])
@pytest.mark.parametrize("finalize_mode", [StagedDataFinalizeMethod.WRITE, StagedDataFinalizeMethod.APPEND])
def test_stage(lmdb_storage, lib_name, new_api_enabled, finalize_mode):
    set_config_int("dev.stage_new_api_enabled", 1 if new_api_enabled else 0)
    sym = "sym"
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name, library_options=LibraryOptions(rows_per_segment=1))
    not_staged = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=pd.date_range("2025-01-01", periods=2))

    lib.write(sym, not_staged)
    data_to_stage = [
        pd.DataFrame({"col1": [5, 6], "col2": [7, 8]}, index=pd.date_range("2025-01-03", periods=2)),
        pd.DataFrame({"col1": [9, 10], "col2": [11, 12]}, index=pd.date_range("2025-01-05", periods=2)),
        pd.DataFrame({"col1": [13, 14], "col2": [15, 16]}, index=pd.date_range("2025-01-07", periods=2)),
    ]

    staged_results = [lib.stage(sym, df) for df in data_to_stage]

    if not new_api_enabled:
        assert all(x is None for x in staged_results)
    else:
        assert all(len(staged_result.staged_segments) == 2 and staged_result.version == 0 for staged_result in staged_results)

    assert_frame_equal(lib.read(sym).data, not_staged, check_freq=False)
    lib.finalize_staged_data(sym, mode=finalize_mode)
    expected_df = pd.concat([not_staged] + data_to_stage) if finalize_mode is StagedDataFinalizeMethod.APPEND else pd.concat(data_to_stage)
    assert_frame_equal(lib.read(sym).data, expected_df, check_freq=False)


def test_stage_result_pickle(lmdb_storage, lib_name):
    set_config_int("dev.stage_new_api_enabled", 1)
    sym = "sym"
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name, library_options=LibraryOptions(rows_per_segment=1))
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=pd.date_range("2025-01-01", periods=2))
    stage_result = lib.stage(sym, df)
    stage_result_after_pickling = pickle.loads(pickle.dumps(stage_result))
    segments = stage_result.staged_segments
    segments_after_pickling = stage_result_after_pickling.staged_segments

    assert segments == segments_after_pickling
