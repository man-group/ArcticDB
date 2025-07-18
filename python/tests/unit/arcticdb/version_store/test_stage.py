"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import itertools
import pickle

import numpy as np
import pandas as pd
import pytest

from arcticdb import LibraryOptions
from arcticdb_ext.exceptions import UserInputException, UnsortedDataException, SortingException
from arcticdb_ext.storage import KeyType
from arcticdb_ext.version_store import StageResult, NoSuchVersionException, KeyNotFoundInTokenInfo, AtomKey, RefKey
from arcticdb.version_store.library import Library
from arcticdb.util.test import assert_frame_equal, config_context
from arcticdb.exceptions import MissingKeysInTokensError


@pytest.fixture
def new_staged_data_api_enabled():
    with config_context("dev.stage_new_api_enabled", 1):
        yield True


@pytest.fixture(scope="function", params=["v1", "v2-regular", "v2-sort"])
def arctic_api(request):
    return request.param


@pytest.fixture(scope="function", params=["regular", "sort"])
def flavour(request):
    """What sort of finalization to do - regular, or sort_and_finalize_staged_data."""
    return request.param


@pytest.fixture(scope="function", params=[True, False])
def prune_previous_versions(request):
    return request.param


@pytest.fixture(scope="function", params=[True, False])
def validate_index(request):
    return request.param


@pytest.fixture(scope="function", params=[True, False])
def should_enable_new_api(request):
    return request.param


def finalize(api_version, lib: Library, sym, mode="write", _stage_results=None, metadata=None,
             prune_previous_versions=False, validate_index=True, delete_staged_data_on_failure=False):
    if api_version == "v1":
        lib._nvs.compact_incomplete(sym, append=mode == "append", _stage_results=_stage_results,
                                    convert_int_to_float=False, metadata=metadata, prune_previous_version=prune_previous_versions,
                                    validate_index=validate_index, delete_staged_data_on_failure=delete_staged_data_on_failure)
    elif api_version == "v2-regular":
        lib.finalize_staged_data(sym, mode=mode, _stage_results=_stage_results, prune_previous_versions=prune_previous_versions,
                                 metadata=metadata, validate_index=validate_index, delete_staged_data_on_failure=delete_staged_data_on_failure)
    elif api_version == "v2-sort":
        lib.sort_and_finalize_staged_data(sym, mode=mode, _stage_results=_stage_results, prune_previous_versions=prune_previous_versions,
                                 metadata=metadata, delete_staged_data_on_failure=delete_staged_data_on_failure)
    else:
        raise RuntimeError(f"Unexpected api_version {api_version}")


@pytest.mark.parametrize("finalize_mode", ["write", "append"])
def test_stage(lmdb_library_factory, should_enable_new_api, finalize_mode, arctic_api):
    with config_context("dev.stage_new_api_enabled", 1 if should_enable_new_api else 0):
        sym = "sym"
        lib = lmdb_library_factory(LibraryOptions(rows_per_segment=1))
        not_staged = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=pd.date_range("2025-01-01", periods=2))

        lib.write(sym, not_staged)
        data_to_stage = [
            pd.DataFrame({"col1": [5, 6], "col2": [7, 8]}, index=pd.date_range("2025-01-03", periods=2)),
            pd.DataFrame({"col1": [9, 10], "col2": [11, 12]}, index=pd.date_range("2025-01-05", periods=2)),
            pd.DataFrame({"col1": [13, 14], "col2": [15, 16]}, index=pd.date_range("2025-01-07", periods=2)),
        ]

        staged_results = [lib.stage(sym, df) for df in data_to_stage]

    if should_enable_new_api:
        assert all(len(staged_result.staged_segments) == 2 for staged_result in staged_results)

        assert_frame_equal(lib.read(sym).data, not_staged, check_freq=False)
        finalize(arctic_api, lib, sym, mode=finalize_mode)
        expected_df = pd.concat([not_staged] + data_to_stage) if finalize_mode == "append" else pd.concat(data_to_stage)
        assert_frame_equal(lib.read(sym).data, expected_df, check_freq=False)
    else:
        assert all(x is None for x in staged_results)


def test_stage_result_pickle(lmdb_library_factory, new_staged_data_api_enabled):
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=1))
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=pd.date_range("2025-01-01", periods=2))
    stage_result = lib.stage(sym, df)
    assert len(stage_result.staged_segments) == 2
    stage_result_after_pickling = pickle.loads(pickle.dumps(stage_result))
    segments = stage_result.staged_segments
    segments_after_pickling = stage_result_after_pickling.staged_segments

    assert segments == segments_after_pickling


def test_stage_submit_tokens_for_wrong_symbol(lmdb_library_factory, new_staged_data_api_enabled, arctic_api):
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=1))

    data_to_stage = [
        pd.DataFrame({"col1": [5, 6], "col2": [7, 8]}, index=pd.date_range("2025-01-03", periods=2)),
        pd.DataFrame({"col1": [9, 10], "col2": [11, 12]}, index=pd.date_range("2025-01-05", periods=2)),
        pd.DataFrame({"col1": [13, 14], "col2": [15, 16]}, index=pd.date_range("2025-01-07", periods=2)),
    ]

    staged_results = [lib.stage("sym", data_to_stage[0]), lib.stage("other_sym", data_to_stage[1]), lib.stage("sym", data_to_stage[2])]

    with pytest.raises(UserInputException, match="E_TOKEN_WITH_INCORRECT_SYMBOL"):
        finalize(arctic_api, lib, "sym", _stage_results=staged_results)

    assert not lib.has_symbol("sym")
    staged_segments = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(staged_segments) == 6

    finalize(arctic_api, lib, "sym", _stage_results=[staged_results[0], staged_results[2]])
    assert_frame_equal(lib.read("sym").data, pd.concat([data_to_stage[0], data_to_stage[2]]), check_freq=False)

    staged_segments = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(staged_segments) == 2


# The tests below us rows_per_segment=2. Choose some index ranges that cover subsets of those indexes.
DATE_RANGE_INDEXES = [pd.date_range("2025-01-01", periods=3),
                      pd.date_range("2025-01-04", periods=2),
                      pd.date_range("2025-01-06", periods=1),
                      pd.date_range("2025-01-07", periods=5)
                      ]


ROWCOUNT_INDEXES = [np.arange(0, 3, dtype=np.int64), np.arange(4, 6, dtype=np.int64), np.arange(6, 7, dtype=np.int64), np.arange(7, 12, dtype=np.int64)]


STRING_INDEXES = [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h", "i", "j", "k"]]


@pytest.mark.parametrize("dynamic_schema", (True, False))
@pytest.mark.parametrize("indexes", [DATE_RANGE_INDEXES, ROWCOUNT_INDEXES, STRING_INDEXES])
def test_finalize_with_tokens_append_mode(lmdb_library_factory, new_staged_data_api_enabled, indexes, dynamic_schema, arctic_api):
    if arctic_api == "v2-sort" and indexes is not DATE_RANGE_INDEXES:
        pytest.skip("sort_and_finalize_staged_data only supports datetime indexed data")

    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2, dynamic_schema=dynamic_schema))
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0], dtype=np.int64)
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1], dtype=np.int64)
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2], dtype=np.int64)
    df_4 = pd.DataFrame({"col1": np.arange(11, 16), "col2": np.arange(12, 17)}, index=indexes[3], dtype=np.int64)

    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)

    other_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=pd.date_range("2025-01-01", periods=2))
    lib.stage("other_sym", other_df)  # stage an unrelated symbol, just to check we aren't finalizing an unasked for symbol!

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 4
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_1])
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 2

    result = lib.read(sym).data
    assert_frame_equal(df_1, result)

    finalize(arctic_api, lib, sym, _stage_results=[stage_result_2], mode="append")
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 1

    result = lib.read(sym).data
    assert_frame_equal(pd.concat([df_1, df_2]), result)

    stage_result_3 = lib.stage(sym, df_3)
    stage_result_4 = lib.stage(sym, df_4)
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 5
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_3, stage_result_4], mode="append")
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 1

    result = lib.read(sym).data
    assert_frame_equal(pd.concat([df_1, df_2, df_3, df_4]), result)

    # Check all this finalization hasn't touched the other symbol
    with pytest.raises(NoSuchVersionException):
        lib.read("other_sym")

    finalize(arctic_api, lib, "other_sym")
    assert_frame_equal(lib.read("other_sym").data, other_df)
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 0

    with pytest.raises(UserInputException, match="E_NO_STAGED_SEGMENTS"):
        finalize(arctic_api, lib, sym)


def test_finalize_with_tokens_write_mode(lmdb_library_factory, arctic_api, new_staged_data_api_enabled):
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0], dtype=np.int64)
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1], dtype=np.int64)
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2], dtype=np.int64)
    df_4 = pd.DataFrame({"col1": np.arange(11, 16), "col2": np.arange(12, 17)}, index=indexes[3], dtype=np.int64)

    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)
    stage_result_3 = lib.stage(sym, df_3)
    stage_result_4 = lib.stage(sym, df_4)

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 7
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_3], mode="write")
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 4

    result = lib.read(sym).data
    assert_frame_equal(pd.concat([df_1, df_3]), result)

    finalize(arctic_api, lib, sym, _stage_results=[stage_result_2, stage_result_4], mode="write")
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 0

    result = lib.read(sym).data
    assert_frame_equal(pd.concat([df_2, df_4]), result)

    # Check we can happily append after write
    df_5 = pd.DataFrame({"col1": [11, 12], "col2": [13, 14]}, index=pd.date_range("2025-12-01", periods=2))
    stage_result_5 = lib.stage(sym, df_5)

    finalize(arctic_api, lib, sym, _stage_results=[stage_result_5], mode="append")

    result = lib.read(sym).data
    assert_frame_equal(pd.concat([df_2, df_4, df_5]), result)


@pytest.mark.parametrize("mode", ("write", "append"))
def test_finalize_with_tokens_then_without(lmdb_library_factory, arctic_api, new_staged_data_api_enabled, mode):
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0], dtype=np.int64)
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1], dtype=np.int64)
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2], dtype=np.int64)
    df_4 = pd.DataFrame({"col1": np.arange(11, 16), "col2": np.arange(12, 17)}, index=indexes[3], dtype=np.int64)

    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)
    lib.stage(sym, df_3)
    lib.stage(sym, df_4)

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 7
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2], mode=mode)
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 4

    result = lib.read(sym).data
    assert_frame_equal(pd.concat([df_1, df_2]), result)

    # This should process any remaining APPEND_DATA keys
    finalize(arctic_api, lib, sym, mode=mode)
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 0

    result = lib.read(sym).data

    if mode == "append":
        assert_frame_equal(pd.concat([df_1, df_2, df_3, df_4]), result)
    elif mode == "write":
        assert_frame_equal(pd.concat([df_3, df_4]), result)
    else:
        raise RuntimeError(f"Unexpected mode {mode}")


def test_finalize_with_tokens_new_api_disabled(lmdb_library_factory, arctic_api):
    """We should raise if anyone attempts to use the new API without the feature flag."""
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    stage_result_1 = lib.stage(sym, df_1)

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 2

    # Any exception is fine, it's just a developer facing feature flag
    with config_context("dev.stage_new_api_enabled", 0):
        with pytest.raises(Exception):
            finalize(arctic_api, lib, sym, _stage_results=[stage_result_1])

    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 2
    assert not lib.has_symbol(sym)


def test_finalize_missing_keys(lmdb_library_factory, arctic_api, new_staged_data_api_enabled):
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1])
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2])
    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)
    stage_result_3 = lib.stage(sym, df_3)

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 4
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2], mode="write")
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 1

    # Do we raise if someone finalizes a key that no longer exists?
    try:
        finalize(arctic_api, lib, sym, _stage_results=[stage_result_3, stage_result_2, stage_result_1], mode="write")
    except MissingKeysInTokensError as e:
        bad_tokens = e.tokens_with_missing_keys
        assert len(bad_tokens) == 3
        first_bad_token = bad_tokens[0]
        assert first_bad_token.missing_key == stage_result_2.staged_segments[0]
        assert first_bad_token.token_index == 1
        second_bad_token = bad_tokens[1]
        assert second_bad_token.missing_key == stage_result_1.staged_segments[0]
        assert second_bad_token.token_index == 2
        third_bad_token = bad_tokens[2]
        assert third_bad_token.missing_key == stage_result_1.staged_segments[1]
        assert third_bad_token.token_index == 2

    assert lib.read(sym).version == 0
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 1
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_3], mode="write")
    assert_frame_equal(lib.read(sym).data, df_3)


def test_missing_keys_error():
    """Test the MissingKeysInTokensError type."""
    # Given
    missing_key_one = AtomKey(
        "key_one",
        42,
        1,
        0,
        1,
        2,
        KeyType.TABLE_DATA
    )

    missing_key_two = RefKey("key_two", KeyType.VERSION_REF)

    info_one = KeyNotFoundInTokenInfo(44, missing_key_one)
    info_two = KeyNotFoundInTokenInfo(3, missing_key_two)

    # When
    error = MissingKeysInTokensError("my message", [info_one, info_two])
    equal_error = MissingKeysInTokensError("my message", [info_one, info_two])
    non_equal_error = MissingKeysInTokensError("my message", [info_one])

    # Then - check the error object behaves correctly
    as_str = str(error)
    assert "msg=my message" in as_str
    assert "Tokens with missing keys" in as_str

    rep = repr(error)
    assert "MissingKeysInTokensError(" in rep
    assert "msg='my message'" in rep
    assert "tokens_with_missing_keys=" in rep

    assert error == equal_error
    assert error != non_equal_error

    assert error.msg == "my message"
    assert error.tokens_with_missing_keys == [info_one, info_two]


def test_finalize_noop_if_any_missing_keys(lmdb_library_factory, arctic_api, new_staged_data_api_enabled):
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1])
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2])
    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)
    stage_result_3 = lib.stage(sym, df_3)

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 4
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_3], mode="write")
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 3

    # Do we raise if someone finalizes a key that no longer exists?
    with pytest.raises(MissingKeysInTokensError):
        finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2, stage_result_3], mode="write")

    # Do we leave everything that was on disk alone?
    res = lib.read(sym)
    assert res.version == 0
    assert_frame_equal(res.data, df_3)
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 3

    # Can we still go ahead and finalize without the missing key?
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2], mode="write")

    res = lib.read(sym)
    assert res.version == 1
    assert_frame_equal(res.data, pd.concat([df_1, df_2]))
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 0


def test_finalize_with_tokens_and_prune_previous(lmdb_library_factory, arctic_api, new_staged_data_api_enabled, prune_previous_versions):
    """Do we respect pruning when we also have tokens? This test also checks that we support metadata with tokens."""
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1])
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2])
    lib.write(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)
    lib.stage(sym, df_3)

    lt = lib._dev_tools.library_tool()
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 2
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_2],
             prune_previous_versions=prune_previous_versions, metadata="blah")
    assert len(lt.find_keys(KeyType.APPEND_DATA)) == 1

    res = lib.read(sym)
    assert res.metadata == "blah"
    assert_frame_equal(res.data, df_2)

    if prune_previous_versions:
        with pytest.raises(NoSuchVersionException):
            lib.read(sym, as_of=0)
    else:
        res = lib.read(sym, as_of=0)
        assert_frame_equal(res.data, df_1)


def test_finalize_with_tokens_and_validate_index_all_ok(lmdb_library_factory, arctic_api, new_staged_data_api_enabled, validate_index):
    sym = "good_sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    existing = pd.date_range("2024-01-01", periods=1)
    df_0 = pd.DataFrame({"col1": [1], "col2": [2]}, index=existing)
    lib.write(sym, df_0)

    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1])
    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)

    # We should still get an ordered index regardless of the ordering of the tokens
    finalize(arctic_api, lib, sym, _stage_results=[stage_result_2, stage_result_1], validate_index=validate_index, mode="append")

    res = lib.read(sym)
    assert_frame_equal(res.data, pd.concat([df_0, df_1, df_2]))
    assert res.data.index.is_monotonic_increasing


@pytest.mark.parametrize("indexes", [DATE_RANGE_INDEXES, ROWCOUNT_INDEXES, STRING_INDEXES])
def test_ordering_of_tokens_should_not_matter(lmdb_library_factory, arctic_api, new_staged_data_api_enabled, indexes):
    """The order that users submit the tokens in should not matter for date range indexes. For rowcount and string
    indexes the result will be saved with an arbitrary order."""
    is_datetime = isinstance(indexes[0], pd.DatetimeIndex)
    if arctic_api == "v2-sort" and not is_datetime:
        pytest.skip("sort_and_finalize_staged_data only supports datetime indexed data")

    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))

    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1])
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2])

    for permutation in itertools.permutations(range(3)):
        stage_results = [lib.stage(sym, df_1), lib.stage(sym, df_2), lib.stage(sym, df_3)]
        tokens = [None, None, None]
        for i, p in enumerate(permutation):
            tokens[i] = stage_results[p]

        finalize(arctic_api, lib, sym, _stage_results=tokens)
        res = lib.read(sym)
        if is_datetime:
            assert_frame_equal(res.data, pd.concat([df_1, df_2, df_3]))
            assert res.data.index.is_monotonic_increasing
        else:
            # All bets are off for rowcount indexes...
            assert res.data.shape == (6, 2)
            assert set(res.data["col1"]) == {1, 2, 3, 4, 7}
            assert set(res.data["col2"]) == {3, 4, 5, 6, 9}


@pytest.mark.parametrize("indexes", [DATE_RANGE_INDEXES, ROWCOUNT_INDEXES, STRING_INDEXES])
def test_sorting_of_result_without_tokens(lmdb_library_factory, arctic_api, new_staged_data_api_enabled, indexes):
    """Same as the test above but without tokens, checking both paths through the API are interoperable when we finalize
    everything. """
    is_datetime = isinstance(indexes[0], pd.DatetimeIndex)
    if arctic_api == "v2-sort" and not is_datetime:
        pytest.skip("sort_and_finalize_staged_data only supports datetime indexed data")
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))

    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1])
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=indexes[2])
    for d in (df_1, df_2, df_3):
        lib.stage(sym, d)

    finalize(arctic_api, lib, sym, _stage_results=None)
    res = lib.read(sym)
    if is_datetime:
        assert_frame_equal(res.data, pd.concat([df_1, df_2, df_3]))
        assert res.data.index.is_monotonic_increasing
    else:
        # All bets are off for rowcount indexes...
        assert res.data.shape == (6, 2)
        assert set(res.data["col1"]) == {1, 2, 3, 4, 7}
        assert set(res.data["col2"]) == {3, 4, 5, 6, 9}


def test_finalize_with_tokens_and_validate_index_out_of_order(lmdb_library_factory, arctic_api, new_staged_data_api_enabled,
                                                 validate_index):
    # Given a symbol starting in 2026
    sym = "bad_sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))
    indexes = DATE_RANGE_INDEXES
    existing = pd.date_range("2026-01-01", periods=1)
    df_0 = pd.DataFrame({"col1": [1], "col2": [2]}, index=existing)
    lib.write(sym, df_0)

    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=indexes[0])
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=indexes[1])
    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)

    # When we finalize
    if validate_index or arctic_api == "v2-sort":
        with pytest.raises(UnsortedDataException):
            finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2], validate_index=validate_index, mode="append")
    else:
        finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2], validate_index=validate_index, mode="append")
        res = lib.read(sym)
        assert_frame_equal(res.data, pd.concat([df_0, df_1, df_2]))
        assert not res.data.index.is_monotonic_increasing


def test_compact_incomplete_with_tokens_without_via_iteration_not_ok(lmdb_library_factory, new_staged_data_api_enabled):
    """We validate against submitting with tokens and via_iteration False as this is not a required use case (and doesn't
    make a great deal of sense)."""
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))

    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=DATE_RANGE_INDEXES[0])
    stage_result_1 = lib.stage(sym, df_1)
    keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(keys) == 2

    with pytest.raises(UserInputException):
        lib._nvs.compact_incomplete(sym, append=False, _stage_results=[stage_result_1], convert_int_to_float=False, via_iteration=False)

    assert not lib.has_symbol(sym)
    keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(keys) == 2


def test_delete_staged_data_on_failure_with_tokens_overlap(lmdb_library_factory, arctic_api, new_staged_data_api_enabled, lib_name):
    """Check what happens to staged tokens when we fail due to an overlapping index in the staged segments."""
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))

    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=[pd.Timestamp(10_000), pd.Timestamp(11_000), pd.Timestamp(12_000)])

    # Index overlaps with df_1 so should fail when validate_index=True
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=[pd.Timestamp(11_500), pd.Timestamp(13_000)])
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=[pd.Timestamp(20_000)])

    stage_result_1 = lib.stage(sym, df_1)
    stage_result_2 = lib.stage(sym, df_2)
    stage_result_3 = lib.stage(sym, df_3)
    keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(keys) == 4

    if arctic_api == "v2-sort":
        finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2], delete_staged_data_on_failure=True)

        assert lib.has_symbol(sym)
        res = lib.read(sym)
        assert_frame_equal(res.data, pd.concat([df_1, df_2]).sort_index())
        assert res.version == 0
        keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
        assert len(keys) == 1
    else:
        with pytest.raises(SortingException):
            finalize(arctic_api, lib, sym, _stage_results=[stage_result_1, stage_result_2], validate_index=True, delete_staged_data_on_failure=True)

        assert not lib.has_symbol(sym)
        keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
        assert len(keys) == 1

        finalize(arctic_api, lib, sym, _stage_results=[stage_result_3])
        res = lib.read(sym)
        assert_frame_equal(res.data, df_3)
        assert res.version == 0


def test_delete_staged_data_on_failure_with_tokens_out_of_order_append(lmdb_library_factory, new_staged_data_api_enabled, arctic_api):
    """Check what happens to staged tokens when we fail due to an out of order append."""
    sym = "sym"
    lib = lmdb_library_factory(LibraryOptions(rows_per_segment=2))

    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5]}, index=pd.date_range("2025-01-01", periods=3))
    df_2 = pd.DataFrame({"col1": [3, 4], "col2": [5, 6]}, index=pd.date_range("2024-01-02", periods=2))
    df_3 = pd.DataFrame({"col1": [7], "col2": [9]}, index=pd.date_range("2025-01-04", periods=1))

    lib.write(sym, df_1)

    stage_result_2 = lib.stage(sym, df_2)
    stage_result_3 = lib.stage(sym, df_3)
    keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(keys) == 2

    with pytest.raises(UnsortedDataException):
        # Expect this to fail as df_2's index starts before df_1, which has already been written
        finalize(arctic_api, lib, sym, _stage_results=[stage_result_2], validate_index=True, mode="append", delete_staged_data_on_failure=True)

    # We shouldn't delete the token that wasn't submitted to the failed call
    keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(keys) == 1

    # We shouldn't touch the symbol
    res = lib.read(sym)
    assert_frame_equal(res.data, df_1)
    assert res.version == 0

    # When we finalize we should sweep up stage_result_3 only
    finalize(arctic_api, lib, sym, mode="write")
    keys = lib._dev_tools.library_tool().find_keys(KeyType.APPEND_DATA)
    assert len(keys) == 0
    res = lib.read(sym)
    assert_frame_equal(res.data, df_3)
    assert res.version == 1
