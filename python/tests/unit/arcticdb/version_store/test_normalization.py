"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
from collections import namedtuple

import numpy as np
import pandas as pd
import pytest
import pytz
from numpy.testing import assert_equal, assert_array_equal
from arcticdb.util.test import assert_frame_equal, assert_series_equal

from arcticdb.exceptions import ArcticNativeNotYetImplemented
from arcticdb.version_store._custom_normalizers import (
    register_normalizer,
    get_custom_normalizer,
    clear_registered_normalizers,
    CompositeCustomNormalizer,
)
from arcticdb.version_store._normalization import (
    FrameData,
    MsgPackNormalizer,
    normalize_metadata,
    denormalize_user_metadata,
    CompositeNormalizer,
    _to_tz_timestamp,
    _from_tz_timestamp,
    DataFrameNormalizer,
    NdArrayNormalizer,
    NPDDataFrame,
)
from arcticdb.version_store._common import TimeFrame
from arcticdb.util.test import param_dict, CustomThing, TestCustomNormalizer
from arcticdb.exceptions import ArcticNativeException

params = {
    "simple_dict": {"a": "1", "b": 2, "c": 3.0, "d": True},
    "pd_ts": {"a": pd.Timestamp("2018-01-12 09:15"), "b": pd.Timestamp("2017-01-31", tz="America/New_York")},
}

# Use a smaller memory mapped limit for this test, no point memor mapping 2g
MsgPackNormalizer.MMAP_DEFAULT_SIZE = 20 * (1 << 20)
test_msgpack_normalizer = MsgPackNormalizer()


@param_dict("d", params)
def test_msg_pack(d):
    norm = test_msgpack_normalizer

    df, norm_meta = norm.normalize(d)
    fd = FrameData.from_npd_df(df)
    D = norm.denormalize(fd, norm_meta)

    assert d == D


@param_dict("d", params)
def test_user_meta_and_msg_pack(d):
    n = normalize_metadata(d)
    D = denormalize_user_metadata(n)
    assert d == D


def test_fails_humongous_meta():
    with pytest.raises(ArcticNativeNotYetImplemented):
        from arcticdb.version_store._normalization import _MAX_USER_DEFINED_META as MAX

        meta = {"a": "x" * (MAX)}
        normalize_metadata(meta)


def test_fails_humongous_data():
    norm = test_msgpack_normalizer
    with pytest.raises(ArcticNativeNotYetImplemented):
        big = [1] * (norm.MMAP_DEFAULT_SIZE + 1)
        norm.normalize(big)


def create_df_params():
    params = dict()
    if pd.__version__.startswith("1"):
        df = pd.DataFrame(data={"C": []}, index=pd.MultiIndex(levels=[["A"], ["B"]], codes=[[], []], names=["X", "Y"]))
    else:
        df = pd.DataFrame(data={"C": []}, index=pd.MultiIndex(levels=[["A"], ["B"]], labels=[[], []], names=["X", "Y"]))
    params["empty_midx"] = df
    return params


@param_dict("d", create_df_params())
def test_store_df(d):
    norm = CompositeNormalizer()
    df, norm_meta = norm.normalize(d)
    fd = FrameData.from_npd_df(df)
    D = norm.denormalize(fd, norm_meta)
    if pd.__version__.startswith("1"):
        assert_equal(d.index.to_numpy(), D.index.to_numpy())
    else:
        assert_equal(d.index.get_values(), D.index.get_values())
    D.index = d.index
    assert_frame_equal(d, D)


def get_multiindex_df_with_tz():
    dt1 = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = datetime.datetime(2019, 4, 9, 10, 5, 2, 1)
    nytz = pytz.timezone("America/New_York")
    loc_dt1 = nytz.localize(dt1)
    loc_dt2 = nytz.localize(dt2)
    arr1 = [loc_dt1, loc_dt1, loc_dt2, loc_dt2]
    arr2 = [loc_dt1, loc_dt1, loc_dt2, loc_dt2]
    arr3 = [0, 1, 0, 1]
    return pd.DataFrame(
        data={"X": [10, 11, 12, 13]},
        index=pd.MultiIndex.from_arrays([arr1, arr2, arr3], names=["datetime", "level_1", "level_2"]),
    )


def test_multiindex_with_tz():
    d = get_multiindex_df_with_tz()
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=True, fallback_normalizer=test_msgpack_normalizer)
    df, norm_meta = norm.normalize(d)
    fd = FrameData.from_npd_df(df)
    denorm = norm.denormalize(fd, norm_meta)
    if pd.__version__.startswith("1"):
        assert_equal(d.index.to_numpy(), denorm.index.to_numpy())
    else:
        assert_equal(d.index.get_values(), denorm.index.get_values())
    denorm.index = d.index
    assert_frame_equal(d, denorm)


def test_empty_df_with_multiindex_with_tz():
    orig_df = get_multiindex_df_with_tz()
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=True, fallback_normalizer=test_msgpack_normalizer)
    norm_df, norm_meta = norm.normalize(orig_df)

    # Slice the normalized df to an empty df (this can happen after date range slicing)
    sliced_norm_df = NPDDataFrame(
        index_names=norm_df.index_names,
        column_names=norm_df.column_names,
        index_values=[norm_df.index_values[0][0:0]],
        columns_values=[col_vals[0:0] for col_vals in norm_df.columns_values],
    )

    fd = FrameData.from_npd_df(sliced_norm_df)
    sliced_denorm_df = norm.denormalize(fd, norm_meta)

    for index_level_num in [0, 1, 2]:
        assert isinstance(sliced_denorm_df.index.levels[index_level_num], type(orig_df.index.levels[index_level_num]))

    assert sliced_denorm_df.index.names == orig_df.index.names

    for index_level_num in [0, 1]:
        assert sliced_denorm_df.index.levels[index_level_num].tz.zone == orig_df.index.levels[index_level_num].tz.zone


def test_timestamp_without_tz():
    dt = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    ts, tz = _to_tz_timestamp(dt)

    assert ts == pd.Timestamp(dt).value
    assert tz is None

    rt_dt = _from_tz_timestamp(ts, tz)
    assert rt_dt == dt


def test_column_with_mixed_types():
    df = pd.DataFrame({"col": [1, "a"]})
    with pytest.raises(ArcticNativeNotYetImplemented):
        DataFrameNormalizer().normalize(df)


def test_timestamp_with_tz_pytz():
    dt = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    nytz = pytz.timezone("America/New_York")
    loc_dt = nytz.localize(dt)

    ts, tz = _to_tz_timestamp(loc_dt)

    assert ts == pd.Timestamp(dt).value
    assert tz == "America/New_York"

    rt_dt = _from_tz_timestamp(ts, tz)
    assert rt_dt == loc_dt


def test_custom_pack_datetime():
    dt = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    nytz = pytz.timezone("America/New_York")
    loc_dt = nytz.localize(dt)

    norm = test_msgpack_normalizer
    packed = norm._custom_pack(loc_dt)
    rt_dt = norm._ext_hook(packed.code, packed.data)

    assert rt_dt == loc_dt


def test_custom_pack_timedelta():
    td = datetime.timedelta(days=2, hours=5, minutes=30)

    norm = test_msgpack_normalizer
    packed = norm._custom_pack(td)
    rt_td = norm._ext_hook(packed.code, packed.data)

    assert rt_td == td


def test_msg_pack_normalizer_strict_mode():
    norm = test_msgpack_normalizer
    assert not norm.strict_mode


NT = namedtuple("NT", ["X", "Y"])


def test_namedtuple_inside_df():
    d = pd.DataFrame({"A": [NT(1, "b"), NT(2, "a")]})
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=True, fallback_normalizer=test_msgpack_normalizer)
    df, norm_meta = norm.normalize(d)
    fd = FrameData.from_npd_df(df)
    denorm = norm.denormalize(fd, norm_meta)
    if pd.__version__.startswith("1"):
        assert_equal(d.index.to_numpy(), denorm.index.to_numpy())
    else:
        assert_equal(d.index.get_values(), denorm.index.get_values())
    denorm.index = d.index
    assert_frame_equal(d, denorm)


def get_custom_thing():
    columns = ["a", "b"]
    index = [12, 13]
    values = [[2.0, 4.0], [3.0, 5.0]]
    return CustomThing(custom_columns=columns, custom_index=index, custom_values=values)


def test_single_custom_normalizer():
    dt = get_custom_thing()
    register_normalizer(TestCustomNormalizer())
    custom_normalizer = get_custom_normalizer(fail_on_missing=True)
    df, custom_norm_meta = custom_normalizer.normalize(dt)
    denormed = custom_normalizer.denormalize(df, custom_norm_meta)
    assert_equal(type(dt), type(denormed))
    assert_array_equal(dt.custom_index, denormed.custom_index)
    assert_array_equal(dt.custom_columns, denormed.custom_columns)
    assert_array_equal(dt.custom_values, denormed.custom_values)


def test_serialize_custom_normalizer():
    dt = get_custom_thing()
    clear_registered_normalizers()
    register_normalizer(TestCustomNormalizer())
    normalizer = get_custom_normalizer(fail_on_missing=True)
    state = normalizer.__getstate__()
    cloned_normalizer = CompositeCustomNormalizer([], False)
    cloned_normalizer.__setstate__(state)
    assert_equal(len(normalizer._normalizers), len(cloned_normalizer._normalizers))
    assert_equal(len(normalizer._normalizer_by_typename), len(cloned_normalizer._normalizer_by_typename))
    assert_equal(normalizer._normalizers[0].__class__, cloned_normalizer._normalizers[0].__class__)
    assert_equal(normalizer._fail_on_missing_type, cloned_normalizer._fail_on_missing_type)
    df, norm_meta = cloned_normalizer.normalize(dt)
    denormed = cloned_normalizer.denormalize(df, norm_meta)
    assert_array_equal(dt.custom_values, denormed.custom_values)


def test_force_pickle_on_norm_failure():
    normal_df = pd.DataFrame({"a": [1, 2, 3]})
    mixed_type_df = pd.DataFrame({"a": [1, 2, "a"]})
    # Turn off the global flag for the normalizer
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=False, fallback_normalizer=test_msgpack_normalizer)
    # This should work as before
    _d, _meta = norm.normalize(normal_df)

    # This should fail as the df has mixed type
    with pytest.raises(ArcticNativeNotYetImplemented):
        norm.normalize(mixed_type_df)

    # Explicitly passing in pickle settings without global pickle flag being set should work
    _d, _meta = norm.normalize(mixed_type_df, pickle_on_failure=True)

    norm = CompositeNormalizer(use_norm_failure_handler_known_types=True, fallback_normalizer=test_msgpack_normalizer)

    # Forcing it to pickle should work with the bool set.
    _d, _meta = norm.normalize(mixed_type_df)


def test_numpy_array_normalization_composite():
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=False, fallback_normalizer=test_msgpack_normalizer)
    arr = np.random.rand(10, 10, 10)
    df, norm_meta = norm.normalize(arr)
    fd = FrameData.from_npd_df(df)
    d = norm.denormalize(fd, norm_meta)
    assert np.array_equal(d, arr)


def test_numpy_array_normalizer():
    norm = NdArrayNormalizer()
    arr = np.random.rand(10, 10)
    df, norm_meta = norm.normalize(arr)
    fd = FrameData.from_npd_df(df)
    d = norm.denormalize(fd, norm_meta.np)
    assert np.array_equal(d, arr)


def test_ndarray_arbitrary_shape():
    norm = NdArrayNormalizer()
    shape = np.random.randint(1, 5, 5)
    arr = np.random.rand(*shape)
    df, norm_meta = norm.normalize(arr)
    fd = FrameData.from_npd_df(df)
    d = norm.denormalize(fd, norm_meta.np)
    assert np.array_equal(d, arr)


def test_dict_with_tuples():
    data = {(1, 2): [1, 24, 55]}
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=False, fallback_normalizer=test_msgpack_normalizer)
    df, norm_meta = norm.normalize(data)
    fd = FrameData.from_npd_df(df)
    denormalized_data = norm.denormalize(fd, norm_meta)
    assert denormalized_data == data


def test_will_item_be_pickled(lmdb_version_store, sym):
    df = pd.DataFrame(data=np.arange(10), index=pd.date_range(pd.Timestamp(0), periods=10))
    pickle = {"hello": "world"}
    ndarr = np.arange(100)
    not_so_bad_df = pd.DataFrame({"": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))

    assert not lmdb_version_store.will_item_be_pickled(df)
    assert lmdb_version_store.will_item_be_pickled(pickle)
    assert not lmdb_version_store.will_item_be_pickled(ndarr)
    assert not lmdb_version_store.will_item_be_pickled(not_so_bad_df)

    lmdb_version_store.write(sym, not_so_bad_df)
    assert_frame_equal(not_so_bad_df, lmdb_version_store.read(sym).data)


def test_numpy_ts_col_with_none(lmdb_version_store):
    df = pd.DataFrame(data={"a": [None, None]})
    df["a"][0] = pd.Timestamp(0)
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=False, fallback_normalizer=test_msgpack_normalizer)
    df, norm_meta = norm.normalize(df)
    fd = FrameData.from_npd_df(df)
    df_denormed = norm.denormalize(fd, norm_meta)
    assert df_denormed["a"][0] == pd.Timestamp(0)
    # None's should be converted to NaT's now.
    assert pd.isnull(df_denormed["a"][1])


def test_none_in_columns_names(lmdb_version_store, sym):
    df = pd.DataFrame(data={None: [1.2, 2.2], "None": [2.3, 3.5]}, index=[pd.Timestamp(0), pd.Timestamp(1)])
    lmdb_version_store.write(sym, df)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)
    df2 = pd.DataFrame(data={None: [5.2, 6.2], "None": [1.3, 5.5]}, index=[pd.Timestamp(2), pd.Timestamp(3)])
    lmdb_version_store.append(sym, df2)
    vit = lmdb_version_store.read(sym)
    assert_frame_equal(vit.data, df.append(df2))


def test_same_columns_names(lmdb_version_store, sym):
    df = pd.DataFrame(
        data={"test": [1.2, 2.2], "test2": [2.3, 3.5], "test3": [2.5, 8.5], "test4": [9.3, 1.5]},
        index=[pd.Timestamp(0), pd.Timestamp(1)],
    )
    df.columns = ["test", None, "test", None]
    lmdb_version_store.write(sym, df)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)
    df2 = pd.DataFrame(
        data={"test": [2.2, 5.2], "test2": [1.3, 8.5], "test3": [2.5, 11.5], "test4": [12.3, 51.5]},
        index=[pd.Timestamp(2), pd.Timestamp(3)],
    )
    df2.columns = ["test", None, "test", None]
    lmdb_version_store.append(sym, df2)
    vit = lmdb_version_store.read(sym)
    assert_frame_equal(vit.data, df.append(df2))

    x = pd.DataFrame([[1, 2], [3, 4]], columns=[0, 0])
    lmdb_version_store.write(sym, x)
    assert_frame_equal(lmdb_version_store.read(sym).data, x)


def test_columns_names_dynamic_schema(lmdb_version_store_dynamic_schema, sym):
    lmdb_version_store = lmdb_version_store_dynamic_schema
    df = pd.DataFrame(data={None: [1.2, 2.2], "None": [2.3, 3.5]}, index=[pd.Timestamp(0), pd.Timestamp(1)])
    lmdb_version_store.write(sym, df)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)

    df = pd.DataFrame(data={None: [1.2, 2.2], "none": [2.3, 3.5]}, index=[pd.Timestamp(0), pd.Timestamp(1)])
    df.index.name = "None"
    lmdb_version_store.write(sym, df)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)

    df2 = pd.DataFrame(data={"none": [22.4, 21.2], None: [25.3, 31.5]}, index=[pd.Timestamp(2), pd.Timestamp(3)])
    df2.index.name = "None"
    lmdb_version_store.append(sym, df2)
    df3 = df.append(df2)
    df4 = lmdb_version_store.read(sym).data
    df4 = df4[df3.columns.tolist()]
    assert_frame_equal(df4, df3)

    df = pd.DataFrame(data={"test": [1.2, 2.2], "test2": [2.3, 3.5], "test3": [2.5, 8.5], "test4": [9.3, 1.5]})
    df.columns = ["test", None, "test", None]
    with pytest.raises(ArcticNativeException) as e_info:
        lmdb_version_store.write(sym, df)


def test_columns_names_timeframe(lmdb_version_store, sym):
    tz = "America/New_York"
    dtidx = pd.date_range("2019-02-06 11:43", periods=6).tz_localize(tz)
    tf = TimeFrame(dtidx.values, columns_names=[None], columns_values=[np.asarray([1, 3, 42, 54, 23, 4])])
    lmdb_version_store.write(sym, tf)
    vit = lmdb_version_store.read(sym)

    assert tf == vit.data


def test_columns_names_series(lmdb_version_store, sym):
    dr = pd.date_range("2020-01-01", "2020-12-31", name="date")
    date_series = pd.Series(dr, index=dr)

    lmdb_version_store.write(sym, date_series)
    assert_series_equal(lmdb_version_store.read(sym).data, date_series)


def test_columns_names_series_dynamic(lmdb_version_store_dynamic_schema, sym):
    dr = pd.date_range("2020-01-01", "2020-12-31", name="date")
    date_series = pd.Series(dr, index=dr)

    lmdb_version_store_dynamic_schema.write(sym + "dynamic_schema", date_series)
    assert_series_equal(lmdb_version_store_dynamic_schema.read(sym + "dynamic_schema").data, date_series)
