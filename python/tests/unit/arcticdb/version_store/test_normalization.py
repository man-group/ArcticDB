"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
from collections import namedtuple

import numpy as np
import pandas as pd
import dateutil as du
import pytest
import pytz
from numpy.testing import assert_equal, assert_array_equal
from arcticdb_ext.version_store import SortedValue as _SortedValue

from arcticdb.exceptions import ArcticDbNotYetImplemented
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
from arcticdb.util.test import (
    param_dict,
    CustomThing,
    TestCustomNormalizer,
    assert_frame_equal,
    assert_series_equal,
)
from arcticdb.util._versions import IS_PANDAS_ZERO, IS_PANDAS_TWO
from arcticdb.exceptions import ArcticNativeException

params = {
    "simple_dict": {"a": "1", "b": 2, "c": 3.0, "d": True},
    "pd_ts": {"a": pd.Timestamp("2018-01-12 09:15"), "b": pd.Timestamp("2017-01-31", tz="America/New_York")},
}

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
    with pytest.raises(ArcticDbNotYetImplemented):
        from arcticdb.version_store._normalization import _MAX_USER_DEFINED_META as MAX

        meta = {"a": "x" * (MAX)}
        normalize_metadata(meta)


def test_fails_humongous_data():
    norm = test_msgpack_normalizer
    with pytest.raises(ArcticDbNotYetImplemented):
        big = [1] * (norm.MMAP_DEFAULT_SIZE + 1)
        norm.normalize(big)


def test_empty_df():
    if IS_PANDAS_ZERO:
        d = pd.DataFrame(data={"C": []}, index=pd.MultiIndex(levels=[["A"], ["B"]], labels=[[], []], names=["X", "Y"]))
    else:
        d = pd.DataFrame(data={"C": []}, index=pd.MultiIndex(levels=[["A"], ["B"]], codes=[[], []], names=["X", "Y"]))

    norm = CompositeNormalizer()
    # TODO: Remove coerce_columns (#224) and/or hard-coded index column name (#222)
    df, norm_meta = norm.normalize(d, coerce_columns={"__idx__Y": "O", "C": "float64"})
    fd = FrameData.from_npd_df(df)
    D = norm.denormalize(fd, norm_meta)
    if not IS_PANDAS_ZERO:
        assert_equal(d.index.to_numpy(), D.index.to_numpy())
    D.index = d.index
    assert_frame_equal(d, D)


@pytest.mark.parametrize(
    "tz", ["UTC", "Europe/Amsterdam", pytz.UTC, pytz.timezone("Europe/Amsterdam"), du.tz.gettz("UTC")]
)
def test_write_tz(lmdb_version_store, sym, tz):
    assert tz is not None
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10, tz=tz))
    lmdb_version_store.write(sym, df)
    result = lmdb_version_store.read(sym).data
    assert_frame_equal(df, result)
    df_tz = df.index.tzinfo
    assert str(df_tz) == str(tz)


def get_multiindex_df_with_tz(tz):
    dt1 = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    dt2 = datetime.datetime(2019, 4, 9, 10, 5, 2, 1)
    loc_dt1 = tz.localize(dt1)
    loc_dt2 = tz.localize(dt2)
    arr1 = [loc_dt1, loc_dt1, loc_dt2, loc_dt2]
    arr2 = [loc_dt1, loc_dt1, loc_dt2, loc_dt2]
    arr3 = [0, 1, 0, 1]
    return pd.DataFrame(
        data={"X": [10, 11, 12, 13]},
        index=pd.MultiIndex.from_arrays([arr1, arr2, arr3], names=["datetime", "level_1", "level_2"]),
    )


@pytest.mark.parametrize("tz", [pytz.timezone("America/New_York"), pytz.UTC])
def test_multiindex_with_tz(tz):
    d = get_multiindex_df_with_tz(tz)
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=True, fallback_normalizer=test_msgpack_normalizer)
    df, norm_meta = norm.normalize(d)
    fd = FrameData.from_npd_df(df)
    denorm = norm.denormalize(fd, norm_meta)
    if pd.__version__.startswith("0"):
        assert_equal(d.index.get_values(), denorm.index.get_values())
    else:
        assert_equal(d.index.to_numpy(), denorm.index.to_numpy())
    denorm.index = d.index
    assert_frame_equal(d, denorm)


@pytest.mark.parametrize("tz", [pytz.timezone("America/New_York"), pytz.UTC])
def test_empty_df_with_multiindex_with_tz(tz):
    orig_df = get_multiindex_df_with_tz(tz)
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=True, fallback_normalizer=test_msgpack_normalizer)
    norm_df, norm_meta = norm.normalize(orig_df)

    # Slice the normalized df to an empty df (this can happen after date range slicing)
    sliced_norm_df = NPDDataFrame(
        index_names=norm_df.index_names,
        column_names=norm_df.column_names,
        index_values=[norm_df.index_values[0][0:0]],
        columns_values=[col_vals[0:0] for col_vals in norm_df.columns_values],
        sorted=_SortedValue.UNKNOWN,
    )

    fd = FrameData.from_npd_df(sliced_norm_df)
    sliced_denorm_df = norm.denormalize(fd, norm_meta)

    for index_level_num in [0, 1, 2]:
        assert isinstance(sliced_denorm_df.index.levels[index_level_num], type(orig_df.index.levels[index_level_num]))

    assert sliced_denorm_df.index.names == orig_df.index.names

    for index_level_num in [0, 1]:
        sliced_denorm_df_index_level_num = sliced_denorm_df.index.levels[index_level_num]
        orig_df_index_level_num = orig_df.index.levels[index_level_num]
        if IS_PANDAS_TWO and tz is pytz.UTC:
            # Pandas 2.0.0 now uses `datetime.timezone.utc` instead of `pytz.UTC`.
            # See: https://github.com/pandas-dev/pandas/issues/34916
            # TODO: is there a better way to handle this edge case?
            assert sliced_denorm_df_index_level_num.tz == datetime.timezone.utc
            assert isinstance(orig_df_index_level_num.tz, pytz.BaseTzInfo)
            assert sliced_denorm_df_index_level_num.dtype == orig_df_index_level_num.dtype == "datetime64[ns, UTC]"
        else:
            assert isinstance(sliced_denorm_df_index_level_num.tz, pytz.BaseTzInfo)
            assert isinstance(orig_df_index_level_num.tz, pytz.BaseTzInfo)
            assert sliced_denorm_df_index_level_num.tz.zone == orig_df_index_level_num.tz.zone


def test_timestamp_without_tz():
    dt = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    ts, tz = _to_tz_timestamp(dt)

    assert ts == pd.Timestamp(dt).value
    assert tz is None

    rt_dt = _from_tz_timestamp(ts, tz)
    assert rt_dt == dt


def test_column_with_mixed_types():
    df = pd.DataFrame({"col": [1, "a"]})
    with pytest.raises(ArcticDbNotYetImplemented):
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
    if pd.__version__.startswith("0"):
        assert_equal(d.index.get_values(), denorm.index.get_values())
    else:
        assert_equal(d.index.to_numpy(), denorm.index.to_numpy())
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
    with pytest.raises(ArcticDbNotYetImplemented):
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
    assert_frame_equal(vit.data, pd.concat((df, df2)))


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
    assert_frame_equal(vit.data, pd.concat((df, df2)))

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
    df3 = pd.concat((df, df2))
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


@pytest.mark.skipif(not IS_PANDAS_TWO, reason="pandas 2.0-specific test")
@pytest.mark.parametrize("datetime64_dtype", ["datetime64[s]", "datetime64[ms]", "datetime64[us]", "datetime64[ns]"])
@pytest.mark.parametrize("PandasContainer", [pd.DataFrame, pd.Series])
def test_no_inplace_index_array_modification(lmdb_version_store, sym, datetime64_dtype, PandasContainer):
    # Normalization must not modify Series' or DataFrames' index array in-place.
    pandas_container = PandasContainer(
        data={"a": [1, 2, 3]},
        index=pd.DatetimeIndex(["2020-01-01", "2020-01-02", "2020-01-03"], dtype=datetime64_dtype),
    )
    original_index_array = pandas_container.index
    lmdb_version_store.write(sym, pandas_container)
    assert pandas_container.index is original_index_array
    assert pandas_container.index.dtype == datetime64_dtype


@pytest.mark.skipif(
    not IS_PANDAS_TWO, reason="The full-support of pyarrow-backed pandas objects is pandas 2.0-specific."
)
def test_pyarrow_error(lmdb_version_store):
    error_msg_intro = "PyArrow-backed pandas DataFrame and Series are not currently supported by ArcticDB."
    df = pd.DataFrame(data=[[1.0, 0.2], [0.2, 0.5]], dtype="float32[pyarrow]")
    with pytest.raises(ArcticDbNotYetImplemented, match=error_msg_intro):
        lmdb_version_store.write("test_pyarrow_error_df", df)

    series = pd.Series(data=[1.0, 0.2], dtype="float32[pyarrow]")
    with pytest.raises(ArcticDbNotYetImplemented, match=error_msg_intro):
        lmdb_version_store.write("test_pyarrow_error_series", series)

    # Mixed NumPy- and PyArrow-backed DataFrame.
    np_ser = pd.Series([0.1], dtype="float64")
    pa_ser = pd.Series([0.1], dtype="float64[pyarrow]")

    mixed_df = pd.DataFrame({"numpy": np_ser, "pyarrow": pa_ser})

    assert mixed_df["numpy"].dtype == "float64"
    assert mixed_df["pyarrow"].dtype == "float64[pyarrow]"

    with pytest.raises(ArcticDbNotYetImplemented, match=error_msg_intro):
        lmdb_version_store.write("test_pyarrow_error_mixed_df", mixed_df)
