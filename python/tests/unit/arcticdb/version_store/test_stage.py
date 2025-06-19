"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from multiprocessing import SimpleQueue

import pandas as pd
import pytest

from arcticdb import LibraryOptions
from arcticdb_ext.version_store import StageResult
from arcticdb_ext import set_config_int
from arcticdb.version_store.library import StagedDataFinalizeMethod
from arcticdb.util.test import assert_frame_equal
from multiprocessing import Process


@pytest.mark.parametrize("v2_api_enabled", [True, False])
@pytest.mark.parametrize("finalize_mode", [StagedDataFinalizeMethod.WRITE, StagedDataFinalizeMethod.APPEND])
def test_stage(lmdb_storage, lib_name, v2_api_enabled, finalize_mode):
    set_config_int("Stage.IsV2APIEnabled", 1 if v2_api_enabled else 0)
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

    if not v2_api_enabled:
        assert all(x is None for x in staged_results)
    else:
        assert all(len(staged_result._staged_segments) == 2 and staged_result._version == 0 for staged_result in staged_results)

    assert_frame_equal(lib.read(sym).data, not_staged, check_freq=False)
    lib.finalize_staged_data(sym, mode=finalize_mode)
    expected_df = pd.concat([not_staged] + data_to_stage) if finalize_mode is StagedDataFinalizeMethod.APPEND else pd.concat(data_to_stage)
    assert_frame_equal(lib.read(sym).data, expected_df, check_freq=False)

def atom_key_equal(l, r):
    return (
        l.version_id == r.version_id and
        l.creation_ts == r.creation_ts and
        l.content_hash == r.content_hash and
        l.start_index == r.start_index and
        l.end_index == r.end_index and
        l.type == r.type and
        l.id == r.id
    )

def stage_result_worker(stage_result, result_queue):
    assert stage_result._version == 0
    assert len(stage_result._staged_segments) == 2
    result_queue.put(stage_result)

def test_stage_result_pickle(lmdb_storage, lib_name):
    set_config_int("Stage.IsV2APIEnabled", 1)
    sym = "sym"
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name, library_options=LibraryOptions(rows_per_segment=1))
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=pd.date_range("2025-01-01", periods=2))
    stage_result_in = lib.stage(sym, df)
    result_queue = SimpleQueue()
    p = Process(target=stage_result_worker, args=(stage_result_in, result_queue))
    p.start()
    p.join()
    assert p.exitcode == 0
    stage_result_out = result_queue.get()
    keys_in = stage_result_in._staged_segments
    keys_out = stage_result_out._staged_segments

    assert len(keys_in) == len(keys_out) and all(atom_key_equal(a, b) for a, b in zip(keys_in, keys_out))