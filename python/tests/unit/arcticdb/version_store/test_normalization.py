"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import datetime
from email import errors
import inspect
import itertools
import sys
from collections import namedtuple
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import dateutil as du
import pytest
import pytz
from arcticdb_ext.types import IndexKind

if sys.version_info >= (3, 9):
    import zoneinfo
from numpy.testing import assert_equal, assert_array_equal
from arcticdb_ext.version_store import SortedValue as _SortedValue

from arcticdb import QueryBuilder
from arcticdb.exceptions import ArcticDbNotYetImplemented, ArcticException
from arcticdb.version_store._custom_normalizers import (
    register_normalizer,
    get_custom_normalizer,
    clear_registered_normalizers,
    CompositeCustomNormalizer,
)
from arcticdb.version_store._normalization import (
    BlockManagerUnconsolidated,
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
    CustomThing,
    TestCustomNormalizer,
    assert_frame_equal,
    assert_series_equal,
)
from arcticdb.util._versions import IS_PANDAS_ZERO, IS_PANDAS_TWO
from arcticdb.exceptions import ArcticNativeException
import arcticc.pb2.descriptors_pb2 as descriptors_pb2

from tests.util.mark import param_dict, ZONE_INFO_MARK

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


def test_msg_pack_legacy_1():
    # test that we can read old data
    # serialised data created with Python 3.6, msgpack 0.6.2, pandas 0.25.3
    # this was before string and bytes types were seperated in msgpack
    norm = test_msgpack_normalizer
    packed = b"\x82\xa1a\xc7\x0b \x92\xcf\x15\t\x05:\xdfT\xc8\x00\xc0\xa1b\xc7\x1b \x92\xcf\x14\x9e\xc2\x84+~ \x00\xb0America/New_York"
    data = norm._msgpack_unpackb(packed)
    assert data == {
        "a": pd.Timestamp("2018-01-12 09:15:00"),
        "b": pd.Timestamp("2017-01-31 00:00:00-0500", tz="America/New_York"),
    }


def test_msg_pack_legacy_2():
    # test that we can read old data
    # serialised data created with Python 3.6, msgpack 0.6.2, pandas 0.25.3
    # this was before string and bytes types were seperated in msgpack
    norm = test_msgpack_normalizer
    packed = b"\xc7\x1b!\x92\xcf\x15\x93w\xb1\xd2\xa6\x8f\xe8\xb0America/New_York"
    dt = datetime.datetime(2019, 4, 8, 10, 5, 2, 1)
    nytz = pytz.timezone("America/New_York")
    loc_dt = nytz.localize(dt)
    data = norm._msgpack_unpackb(packed)
    assert data == loc_dt


def test_decode_python2_pickle_in_msgpack_dict():
    """See python2_pickles.py for the generation steps. This is the py2_dict.bin case.

    This is to check that we can still deserialize pickles that were written with Python 2 correctly.
    """
    norm = test_msgpack_normalizer
    packed = b"\x81\xa8dict_key\xc7:f\xda\x007cemail.errors\nBoundaryError\np0\n(S'bananas'\np1\ntp2\nRp3\n."
    data = norm._msgpack_unpackb(packed)
    assert list(data.keys()) == ["dict_key"]
    assert isinstance(data["dict_key"], errors.BoundaryError)
    assert data["dict_key"].args[0] == "bananas"


def test_decode_python2_pickle_in_msgpack_obj():
    """See python2_pickles.py for the generation steps. This is the py2_obj.bin case.

    This is to check that we can still deserialize pickles that were written with Python 2 correctly.
    """
    norm = test_msgpack_normalizer
    packed = b"\xc7:f\xda\x007cemail.errors\nBoundaryError\np0\n(S'bananas'\np1\ntp2\nRp3\n."
    data = norm._msgpack_unpackb(packed)
    assert isinstance(data, errors.BoundaryError)
    assert data.args[0] == "bananas"


def test_decode_python2_str_in_msgpack():
    """See python2_pickles.py for the generation steps. This is the py2_str.bin case.

    This is to check that we can still deserialize strings that were written with Python 2 correctly.
    """
    norm = test_msgpack_normalizer
    packed = b"\xa9my_string"
    data = norm._msgpack_unpackb(packed)
    assert data == "my_string"
    assert isinstance(data, str)


def test_decode_python2_bytes_in_old_msgpack():
    """See python2_pickles.py for the generation steps. This is the py2_str_bytes.bin case.

    This is to check that we can still deserialize bytes that were written with Python 2 correctly.
    """
    norm = test_msgpack_normalizer
    packed = b"\xa8my_bytes"
    data = norm._msgpack_unpackb(packed)

    # We claim it's `str` upon decoding because the `xa8` leading bytes tells us this is a fixed string type.
    assert data == "my_bytes"
    assert isinstance(data, str)


def test_decode_python2_bytes_in_newer_msgpack():
    """See python2_pickles.py for the generation steps. This is the py2_str_bytes.bin case.

    This was written with msgpack 1.0.5 not 0.6.2 like the other examples. In this version, msgpack has
    a dedicated type for bytes.

    This is to check that we can still deserialize bytes that were written with Python 2 correctly.
    """
    norm = test_msgpack_normalizer
    packed = b"\xc4\x08my_bytes"
    data = norm._msgpack_unpackb(packed)
    assert data == b"my_bytes"
    assert isinstance(data, bytes)


def test_decode_python3_pickle_in_msgpack_dict():
    norm = test_msgpack_normalizer
    obj = {"dict_key": errors.BoundaryError("bananas")}
    packed = norm._msgpack_packb(obj)

    data = norm._msgpack_unpackb(packed)
    assert list(data.keys()) == ["dict_key"]
    assert isinstance(data["dict_key"], errors.BoundaryError)
    assert data["dict_key"].args[0] == "bananas"


def test_decode_python3_pickle_in_msgpack_obj():
    norm = test_msgpack_normalizer
    obj = errors.BoundaryError("bananas")
    packed = norm._msgpack_packb(obj)

    data = norm._msgpack_unpackb(packed)
    assert isinstance(data, errors.BoundaryError)
    assert data.args[0] == "bananas"


def test_decode_python3_pickle_in_msgpack_str():
    norm = test_msgpack_normalizer
    obj = "bananas"
    packed = norm._msgpack_packb(obj)

    data = norm._msgpack_unpackb(packed)
    assert isinstance(data, str)
    assert data == "bananas"


def test_decode_python3_pickle_in_msgpack_bytes():
    norm = test_msgpack_normalizer
    obj = b"bananas"
    packed = norm._msgpack_packb(obj)

    data = norm._msgpack_unpackb(packed)
    assert isinstance(data, bytes)
    assert data == b"bananas"


@param_dict("d", params)
def test_user_meta_and_msg_pack(d):
    n = normalize_metadata(d)
    D = denormalize_user_metadata(n)
    assert d == D


@patch("arcticdb.version_store._normalization.log")
def test_warns_on_large_meta(log):
    meta = {"a": "x" * 9 * 1024 * 1024}  # larger than 8MB
    normalize_metadata(meta)
    assert "User defined metadata is above warning size" in log.warn.call_args[0][0]


def test_fails_humongous_meta():
    with pytest.raises(ArcticDbNotYetImplemented):
        from arcticdb.version_store._normalization import _MAX_USER_DEFINED_META as MAX

        meta = {"a": "x" * (MAX)}
        normalize_metadata(meta)


def test_empty_df():
    if IS_PANDAS_ZERO:
        d = pd.DataFrame(data={"C": []}, index=pd.MultiIndex(levels=[["A"], ["B"]], labels=[[], []], names=["X", "Y"]))
    else:
        d = pd.DataFrame(data={"C": []}, index=pd.MultiIndex(levels=[["A"], ["B"]], codes=[[], []], names=["X", "Y"]))

    norm = CompositeNormalizer()
    df, norm_meta = norm.normalize(d)
    fd = FrameData.from_npd_df(df)
    D = norm.denormalize(fd, norm_meta)
    if not IS_PANDAS_ZERO:
        assert_equal(d.index.to_numpy(), D.index.to_numpy())
    D.index = d.index
    assert_frame_equal(d, D)


timezone_params = [
    "UTC",
    "Europe/Amsterdam",
    pytz.UTC,
    pytz.timezone("Europe/Amsterdam"),
    du.tz.gettz("UTC"),
]
if sys.version_info > (3, 9):
    timezone_params += [
        zoneinfo.ZoneInfo("UTC"),
        zoneinfo.ZoneInfo("Pacific/Kiritimati"),
        zoneinfo.ZoneInfo("America/Los_Angeles"),
    ]


# See test_get_description_date_range_tz in test_arctic.py for the V2 API equivalent
@pytest.mark.parametrize("tz", timezone_params)
def test_write_tz(lmdb_version_store, sym, tz):
    assert tz is not None
    index = pd.date_range(pd.Timestamp(0), periods=10, tz=tz)
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=index)
    lmdb_version_store.write(sym, df)
    result = lmdb_version_store.read(sym).data
    df_tz = df.index.tzinfo
    assert str(df_tz) == str(tz)
    expected_df = df
    if sys.version_info >= (3, 9):
        if isinstance(tz, zoneinfo.ZoneInfo):
            # We convert all timezones to pytz, so we expect to convert to a pytz timezone.
            expected_df.index = expected_df.index.tz_convert(pytz.timezone(str(tz)))
    assert_frame_equal(expected_df, result)
    start_ts, end_ts = lmdb_version_store.get_timerange_for_symbol(sym)
    assert isinstance(start_ts, datetime.datetime)
    assert isinstance(end_ts, datetime.datetime)
    assert start_ts == index[0]
    assert end_ts == index[-1]


@pytest.mark.parametrize(
    "column_data", itertools.permutations([pd.Timestamp(0), pd.NaT, pd.Timestamp(0, tz="Europe/Amsterdam")])
)
def test_write_mixed_tz(lmdb_version_store_v1, column_data):
    lib = lmdb_version_store_v1
    sym = "test_write_mixed_tz"
    df = pd.DataFrame({"col": column_data})
    with pytest.raises(ArcticException):
        lib.write(sym, df)


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
    # This has to be pickled because msgpack doesn't differentiate between tuples and lists
    data = {(1, 2): [1, 24, 55]}
    norm = CompositeNormalizer(use_norm_failure_handler_known_types=False, fallback_normalizer=test_msgpack_normalizer)
    df, norm_meta = norm.normalize(data)
    fd = FrameData.from_npd_df(df)
    denormalized_data = norm.denormalize(fd, norm_meta)
    assert denormalized_data == data
    assert isinstance(denormalized_data, dict)
    assert isinstance(next(iter(denormalized_data)), tuple)
    assert isinstance(denormalized_data[(1, 2)], list)


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


@pytest.mark.parametrize(
    "data",
    [
        {"a": {"b": {"c": {"d": np.arange(24)}}}},
        {
            "a": [1, 2, 3],
            "b": {"c": np.arange(24)},
            "d": [TestCustomNormalizer()],
        },  # A random item that will be pickled
    ],
)
def test_will_item_be_pickled_recursive_normalizer(lmdb_version_store_v1, data):
    lib = lmdb_version_store_v1
    assert lib.will_item_be_pickled(data, recursive_normalizers=True) == True


def test_will_item_pickled_msgpack(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    assert lib.will_item_be_pickled({"hello": "there"}) == True


def test_numpy_ts_col_with_none(lmdb_version_store):
    df = pd.DataFrame(data={"a": [None, None]})
    df.loc[0, "a"] = pd.Timestamp(0)
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


@pytest.mark.parametrize("name", (None, "", "non_empty"))
def test_roundtrip_series_name(lmdb_version_store_v1, name):
    lib = lmdb_version_store_v1
    sym = "test_roundtrip_series_name"
    series = pd.Series(np.arange(1), name=name)
    lib.write(sym, series)
    assert_series_equal(series, lib.read(sym).data)


@pytest.mark.parametrize("name", (None, "", "non_empty"))
def test_roundtrip_index_name(lmdb_version_store_v1, name):
    lib = lmdb_version_store_v1
    sym = "test_roundtrip_index_name"
    df = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    df.index.name = name
    lib.write(sym, df)
    assert_frame_equal(df, lib.read(sym).data)


def test_series_read_column_names(lmdb_version_store, sym):
    lib = lmdb_version_store
    series = pd.Series(np.arange(366), name="test", index=pd.date_range("2020-01-01", "2020-12-31"))
    lib.write(sym, series)
    assert_series_equal(series, lib.read(sym, columns=["test"]).data)


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


@pytest.mark.parametrize("with_columns", [True, False])
@pytest.mark.parametrize("index_names", [None, ["level_0", "level_1"], ["date", "value"]])
def test_series_with_multiindex(lmdb_version_store, sym, index_names, with_columns):
    lib = lmdb_version_store
    dtidx = pd.date_range("2020-01-01", periods=10)
    vals = np.arange(10, dtype=np.uint32)[::-1]
    multiindex = pd.MultiIndex.from_arrays([dtidx, vals], names=index_names)
    series = pd.Series(np.arange(10, dtype=np.float64), name="data", index=multiindex)
    lib.write(sym, series)
    result = lib.read(sym, columns=["data"] if with_columns else None).data
    assert_series_equal(series, result)


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


def test_index_names_datetime_support(lmdb_version_store, sym):
    df = pd.DataFrame(data={"a": [1, 2, 3]}, index=pd.DatetimeIndex(["2020-01-01", "2020-01-02", "2020-01-03"]))
    new_index = pd.Timestamp("2020-01-01")
    df.index.rename(new_index, inplace=True)
    with pytest.raises(ArcticException):
        lmdb_version_store.write(sym, df)


def test_index_names_tuple_support(lmdb_version_store, sym):
    df = pd.DataFrame(data={"a": [1, 2, 3]}, index=pd.DatetimeIndex(["2020-01-01", "2020-01-02", "2020-01-03"]))
    new_index = tuple([1, 2, 3])
    df.index.rename(new_index, inplace=True)
    with pytest.raises(ArcticException):
        lmdb_version_store.write(sym, df)


def test_index_names_roundtrip_csv(lmdb_version_store, sym):
    import io

    buf = io.StringIO("2023-11-27 00:00:00,0.73260,0.73260,0.73260,0.73260,7")
    df = pd.read_csv(buf, parse_dates=[0], index_col=0, header=None)
    df.index = pd.to_datetime(df.index)

    lmdb_version_store.write(sym, df)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)
    assert df.index.names[0] == 0


def test_column_names_datetime_support(lmdb_version_store, sym):
    df = pd.DataFrame(data={"a": [1, 2, 3]}, index=pd.DatetimeIndex(["2020-01-01", "2020-01-02", "2020-01-03"]))
    new_index = pd.Timestamp("2020-01-01")
    df.rename(columns={"a": new_index}, inplace=True)
    with pytest.raises(ArcticException):
        lmdb_version_store.write(sym, df)


def test_column_names_mixed_types(lmdb_version_store, sym):
    import io

    buf = io.StringIO("2023-11-27 00:00:00,0.73260,0.73260,0.73260,0.73260,7")
    df = pd.read_csv(buf, parse_dates=[0], index_col=0, header=None)
    df_to_write = df.rename(columns={1: 1, 2: pd.Timestamp("2020-01-01"), 3: tuple([1, 2, 3]), 4: "test", 5: 5.5})
    with pytest.raises(ArcticException):
        lmdb_version_store.write(sym, df_to_write)


def test_column_names_roundtrip_csv(lmdb_version_store, sym):
    import io

    buf = io.StringIO("2023-11-27 00:00:00,0.73260,0.73260,0.73260,0.73260,7")
    df = pd.read_csv(buf, parse_dates=[0], index_col=0, header=None)
    lmdb_version_store.write(sym, df)
    assert_frame_equal(lmdb_version_store.read(sym).data, df)


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


# See test of same name in test_arctic.py for V2 API equivalent
def test_norm_failure_error_message(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_norm_failure_error_message"
    col_name = "My unnormalizable column"
    df = pd.DataFrame({col_name: [1, [1, 2]]})
    with pytest.raises(ArcticDbNotYetImplemented) as write_exception:
        lib.write(sym, df)
    with pytest.raises(ArcticDbNotYetImplemented) as batch_write_exception:
        lib.batch_write([sym], [df])
    with pytest.raises(ArcticDbNotYetImplemented) as append_exception:
        lib.append(sym, df)
    with pytest.raises(ArcticDbNotYetImplemented) as batch_append_exception:
        lib.batch_append([sym], [df])
    with pytest.raises(ArcticDbNotYetImplemented) as update_exception:
        lib.update(sym, df)

    assert all(
        col_name in str(e.value)
        for e in [write_exception, batch_write_exception, append_exception, batch_append_exception, update_exception]
    )
    assert all("pickle_on_failure" in str(e.value) for e in [write_exception, batch_write_exception])
    assert all(
        "pickle_on_failure" not in str(e.value) for e in [append_exception, batch_append_exception, update_exception]
    )


def test_writing_timedelta(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_writing_timedelta"
    df = pd.DataFrame({"col": [pd.Timedelta(1, unit="day")]})
    with pytest.raises(ArcticDbNotYetImplemented):
        lib.write(sym, df)


def test_bools_are_pickled(lmdb_version_store_allows_pickling):
    lib = lmdb_version_store_allows_pickling
    sym = "test_bools_are_pickled"

    df = pd.DataFrame({"a": [True, False]})
    lib.write(sym, df)
    lib.get_info(sym)["type"] == "pickled"
    assert_frame_equal(df, lib.read(sym).data)

    df = pd.DataFrame({"a": [True, False, np.nan]})
    lib.write(sym, df)
    lib.get_info(sym)["type"] == "pickled"
    assert_frame_equal(df, lib.read(sym).data)


def test_bools_with_nan_throw_without_pickling(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_bools_throw_without_pickling"

    df = pd.DataFrame({"a": [True, False, np.nan]})
    with pytest.raises(Exception):
        lib.write(sym, df)


def test_arrays_are_pickled(lmdb_version_store_allows_pickling):
    lib = lmdb_version_store_allows_pickling
    sym = "test_arrays_are_pickled"

    df = pd.DataFrame({"a": [np.array([1, 2])]})
    lib.write(sym, df)
    lib.get_info(sym)["type"] == "pickled"
    assert_frame_equal(df, lib.read(sym).data)

    df = pd.DataFrame({"a": [[1, 2]]})
    lib.write(sym, df)
    lib.get_info(sym)["type"] == "pickled"
    assert_frame_equal(df, lib.read(sym).data)


def test_arrays_throw_without_pickling(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_arrays_throw_without_pickling"

    df = pd.DataFrame({"a": [np.array([1, 2])]})

    with pytest.raises(Exception):
        lib.write(sym, df)


def test_series_zero_name(lmdb_version_store, sym):
    lib = lmdb_version_store
    series = pd.Series(
        [3.14, np.nan, 5.7, np.inf], pd.date_range("2020-01-01", periods=4, freq="D", name="date")
    ).rename(0)
    lib.write(sym, series)
    vit = lib.read(sym)
    assert vit.data.equals(series)


@pytest.mark.parametrize(
    "returns_expected",
    [
        {"returns": ArcticDbNotYetImplemented(), "expected": ArcticDbNotYetImplemented},
        {"returns": Exception(), "expected": ArcticNativeException},
        {"returns": (MagicMock(), None), "expected": ArcticNativeException},
    ],
)
@pytest.mark.parametrize("method_to_test", ["write", "update", "stage", "append"])
def test_throws_correct_exceptions(returns_expected, method_to_test, lmdb_version_store):
    mock_normalizer = MagicMock(name="mock_normalizer")
    returns = returns_expected["returns"]
    expected = returns_expected["expected"]
    normalize_method_mock = mock_normalizer.normalize
    if isinstance(returns, Exception):
        normalize_method_mock.side_effect = returns
    else:
        normalize_method_mock.return_value = returns
    lib = lmdb_version_store
    lib._normalizer = mock_normalizer

    method_to_test = getattr(lib, method_to_test)
    non_default_arg_count = (
        sum(1 for param in inspect.signature(method_to_test).parameters.values() if param.default is param.empty) - 1
    )
    args = [MagicMock()] * non_default_arg_count
    with pytest.raises(expected):
        method_to_test(*args)


def test_numpy_none_slice(lmdb_version_store):
    lib = lmdb_version_store

    dat = np.array([1.0, 2.0, 3.0, 4.0])
    idx = pd.DatetimeIndex(["2020-01-01"], name="date")
    columns_names = ["A", "B", "C", "D"]

    # This is a view, not a copy
    # it transposes the array, so the shape is (4,) instead of (1,4)
    sl = dat[None, :]
    df = pd.DataFrame(sl, index=idx, columns=columns_names)

    lib.write("df_none_slice", df)

    result = lib.read("df_none_slice").data
    pd.testing.assert_frame_equal(result, df)


def test_numpy_newaxis_slice(lmdb_version_store):
    lib = lmdb_version_store

    dat = np.array([1.0, 2.0, 3.0, 4.0])
    idx = pd.DatetimeIndex(["2020-01-01"], name="date")
    columns_names = ["A", "B", "C", "D"]

    # This is a view, not a copy
    # it transposes the array, so the shape is (4,) instead of (1,4)
    sl = dat[np.newaxis, :]
    df = pd.DataFrame(sl, index=idx, columns=columns_names)

    lib.write("df_none_slice", df)

    result = lib.read("df_none_slice").data
    pd.testing.assert_frame_equal(result, df)


def test_view_with_reshape(lmdb_version_store):
    lib = lmdb_version_store

    dat = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    reshaped = dat.reshape(2, 3)  # Creates a view with different strides
    idx = pd.DatetimeIndex(["2020-01-01", "2020-01-02"], name="date")
    columns_names = ["A", "B", "C"]
    df = pd.DataFrame(reshaped, index=idx, columns=columns_names)

    lib.write("df_reshaped", df)

    result = lib.read("df_reshaped").data
    pd.testing.assert_frame_equal(result, df)


def test_view_with_transpose(lmdb_version_store):
    lib = lmdb_version_store

    original = np.array([[1, 2, 3], [4, 5, 6]])
    transposed = original.T  # Shape changes from (2,3) to (3,2)
    idx = pd.DatetimeIndex(["2020-01-01", "2020-01-02", "2020-01-03"], name="date")
    columns_names = ["A", "B"]

    df = pd.DataFrame(transposed, index=idx, columns=columns_names)

    lib.write("df_transposed", df)

    result = lib.read("df_transposed").data
    pd.testing.assert_frame_equal(result, df)


def test_view_with_fancy_indexing(lmdb_version_store):
    lib = lmdb_version_store

    original = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])

    indices = np.array([0, 2])
    view = original[indices]  # Selects rows 0 and 2

    idx = pd.DatetimeIndex(["2020-01-01", "2020-01-02"], name="date")
    columns_names = ["A", "B", "C", "D"]

    df = pd.DataFrame(view, index=idx, columns=columns_names)

    lib.write("df_fancy_idx", df)

    result = lib.read("df_fancy_idx").data
    pd.testing.assert_frame_equal(result, df)


def test_view_with_boolean_masking(lmdb_version_store):
    lib = lmdb_version_store

    original = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])

    mask = np.array([True, False, True])
    view = original[mask]  # Selects rows 0 and 2

    idx = pd.DatetimeIndex(["2020-01-01", "2020-01-02"], name="date")
    columns_names = ["A", "B", "C", "D"]

    df = pd.DataFrame(view, index=idx, columns=columns_names)

    lib.write("df_bool_mask", df)

    result = lib.read("df_bool_mask").data
    pd.testing.assert_frame_equal(result, df)


def test_view_with_slice(lmdb_version_store):
    lib = lmdb_version_store

    # Create a 2D array
    original = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
    view = original[0:2, 1:3]  # Select rows 0-1 and columns 1-2
    idx = pd.DatetimeIndex(["2020-01-01", "2020-01-02"], name="date")
    columns_names = ["B", "C"]

    df = pd.DataFrame(view, index=idx, columns=columns_names)

    lib.write("df_slice", df)

    result = lib.read("df_slice").data
    pd.testing.assert_frame_equal(result, df)


def test_empty_dimension(lmdb_version_store):
    lib = lmdb_version_store

    # 0 rows, 3 columns
    zero_dim_array = np.zeros((0, 3))
    columns_names = ["A", "B", "C"]

    # Empty index
    # N.B. Make sure not to pass a name to the index
    # as we don't keep names for empty indices
    # and pandas does
    idx = pd.DatetimeIndex([])
    df = pd.DataFrame(zero_dim_array, index=idx, columns=columns_names)

    lib.write("df_zero_dim", df)

    result = lib.read("df_zero_dim").data
    pd.testing.assert_frame_equal(result, df)


@pytest.mark.parametrize("dynamic_schema", [True, False])
@pytest.mark.parametrize("segment_row_size", [2, 100_000])
@pytest.mark.parametrize("column_group_size", [2, 127])
@pytest.mark.parametrize("data_type", ["dataframe", "series"])
@pytest.mark.parametrize(
    "index",
    [
        None,
        pd.date_range("2025-01-01", periods=12),
        pd.MultiIndex.from_product([pd.date_range("2025-01-01", periods=6), ["hello", "goodbye"]]),
        pd.MultiIndex.from_product(
            [pd.date_range("2025-01-01", periods=3), ["hello", "goodbye"], ["bonjour", "au revoir"]]
        ),
    ],
)
def test_required_field_inclusion(
    version_store_factory, dynamic_schema, segment_row_size, column_group_size, data_type, index
):
    lib = version_store_factory(
        dynamic_schema=dynamic_schema, column_group_size=column_group_size, segment_row_size=segment_row_size
    )
    sym = "test_required_field_inclusion"
    num_rows = len(index) if index is not None else 12
    original_data = (
        pd.Series(np.arange(num_rows), index=index)
        if data_type == "series"
        else pd.DataFrame(
            {"col1": np.arange(num_rows), "col2": np.arange(num_rows), "col3": np.arange(num_rows)}, index=index
        )
    )
    lib.write(sym, original_data)
    received_data = lib.read(sym).data
    if data_type == "series":
        assert_series_equal(original_data, received_data)
    else:
        assert_frame_equal(original_data, received_data)
        # Also test with column selection and processing
        received_data = lib.read(sym, columns=["col3"]).data
        expected_df = original_data.drop(columns=["col1", "col2"])
        assert_frame_equal(expected_df, received_data)
        q = QueryBuilder()
        q = q[q["col1"] > 5]
        received_data = lib.read(sym, query_builder=q).data
        expected_df = original_data[original_data["col1"] > 5]
        if index is None:
            expected_df.index = pd.RangeIndex(0, 6)
        assert_frame_equal(expected_df, received_data)
        received_data = lib.read(sym, columns=["col3"], query_builder=q).data
        expected_df = expected_df.drop(columns=["col1", "col2"])
        assert_frame_equal(expected_df, received_data)


@pytest.mark.parametrize("env_var_set", [True, False])
def test_pandas_consolidation_v1(version_store_factory, monkeypatch, env_var_set):
    if env_var_set:
        monkeypatch.setenv("SKIP_DF_CONSOLIDATION", "true")
    lib = version_store_factory()
    assert lib._normalizer.df._skip_df_consolidation == (env_var_set and IS_PANDAS_TWO)
    sym = "test_pandas_consolidation_v1"
    lib.write(sym, pd.DataFrame({"col": [0]}))
    df = lib.read(sym).data
    if lib._normalizer.df._skip_df_consolidation:
        assert isinstance(df._mgr, BlockManagerUnconsolidated)
    else:
        assert isinstance(df._mgr, pd.core.internals.managers.BlockManager)


@pytest.mark.parametrize("env_var_set", [True, False])
def test_pandas_consolidation_v2(lmdb_library_factory, monkeypatch, env_var_set):
    if env_var_set:
        monkeypatch.setenv("SKIP_DF_CONSOLIDATION", "true")
    lib = lmdb_library_factory()
    assert lib._nvs._normalizer.df._skip_df_consolidation == IS_PANDAS_TWO
    sym = "test_pandas_consolidation_v2"
    lib.write(sym, pd.DataFrame({"col": [0]}))
    df = lib.read(sym).data
    if lib._nvs._normalizer.df._skip_df_consolidation:
        assert isinstance(df._mgr, BlockManagerUnconsolidated)
    else:
        assert isinstance(df._mgr, pd.core.internals.managers.BlockManager)


@pytest.mark.parametrize("use_col_name_for_index", (True, False))
def test_norm_meta_column_and_index_names_df(lmdb_version_store, use_col_name_for_index):
    lib = lmdb_version_store

    start = pd.Timestamp("2018-01-02")
    index = pd.date_range(start=start, periods=2)

    df = pd.DataFrame(
        index=index, data=[[1, 2, 3, 4, 5]], columns=["col_one", "col_two", "col_two", "col_one", "col_three"]
    )

    if use_col_name_for_index:
        index_name = "col_one"
        df.index.set_names([index_name], inplace=True)
    else:
        index_name = "index_name"
        df.index.set_names([index_name], inplace=True)

    lib.write("sym", df)

    res = lib.get_info("sym")
    assert list(df.columns) == res["col_names"]["columns"]
    assert res["col_names"]["index"] == [index_name]

    version_query = lib._get_version_query(None)
    descriptor = lib.version_store.read_descriptor("sym", version_query)

    norm_meta = descriptor.timeseries_descriptor.normalization
    assert norm_meta.df.common.index.name == index_name
    assert norm_meta.df.common.index.is_physically_stored
    assert dict(norm_meta.df.common.col_names) == {
        "__col_col_one__0": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_one"),
        "__col_col_two__1": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_two"),
        "__col_col_two__2": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_two"),
        "__col_col_one__3": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_one"),
        "col_three": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_three"),
    }

    assert_frame_equal(lib.read("sym").data, df)


@pytest.mark.parametrize("use_col_name_for_index", (True, False))
def test_norm_meta_column_and_index_names_series(lmdb_version_store_static_and_dynamic, use_col_name_for_index):
    lib = lmdb_version_store_static_and_dynamic

    start = pd.Timestamp("2018-01-02")
    index = pd.date_range(start=start, periods=2)

    series = pd.Series(index=index, data=[1, 2], name="col_one")

    if use_col_name_for_index:
        index_name = "col_one"
        series.index.set_names([index_name], inplace=True)
    else:
        index_name = "index_name"
        series.index.set_names([index_name], inplace=True)

    lib.write("sym", series)

    res = lib.get_info("sym")

    # For some reason the index name appears only under col_names
    assert [index_name, "col_one"] == res["col_names"]["columns"]
    assert res["col_names"]["index"] == []

    version_query = lib._get_version_query(None)
    descriptor = lib.version_store.read_descriptor("sym", version_query)

    norm_meta = descriptor.timeseries_descriptor.normalization
    assert norm_meta.series.common.index.name == index_name
    assert norm_meta.series.common.index.is_physically_stored
    if use_col_name_for_index:
        assert dict(norm_meta.series.common.col_names) == {
            "__col_col_one__0": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_one"),
        }
    else:
        assert dict(norm_meta.series.common.col_names) == {
            "col_one": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_one"),
        }

    assert_series_equal(lib.read("sym").data, series)


@pytest.mark.parametrize("use_col_name_for_index", (True, False))
def test_norm_meta_column_and_index_names_df_dynamic_schema(lmdb_version_store_dynamic_schema, use_col_name_for_index):
    lib = lmdb_version_store_dynamic_schema

    start = pd.Timestamp("2018-01-02")
    index = pd.date_range(start=start, periods=2)

    df = pd.DataFrame(index=index, data=[[1, 2]], columns=["col_one", "col_two"])

    if use_col_name_for_index:
        index_name = "col_one"
        df.index.set_names([index_name], inplace=True)
    else:
        index_name = "index_name"
        df.index.set_names([index_name], inplace=True)

    lib.write("sym", df)

    res = lib.get_info("sym")
    assert list(df.columns) == res["col_names"]["columns"]
    assert res["col_names"]["index"] == [index_name]

    version_query = lib._get_version_query(None)
    descriptor = lib.version_store.read_descriptor("sym", version_query)

    norm_meta = descriptor.timeseries_descriptor.normalization
    assert norm_meta.df.common.index.name == index_name
    assert norm_meta.df.common.index.is_physically_stored

    if use_col_name_for_index:
        assert dict(norm_meta.df.common.col_names) == {
            "__col_col_one__0": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_one"),
            "col_two": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_two"),
        }
    else:
        assert dict(norm_meta.df.common.col_names) == {
            "col_one": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_one"),
            "col_two": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_two"),
        }

    assert_frame_equal(lib.read("sym").data, df)


def test_norm_meta_column_and_index_names_df_multi_index(lmdb_version_store_static_and_dynamic):
    lib = lmdb_version_store_static_and_dynamic
    is_dynamic_schema = lib.lib_cfg().lib_desc.version.write_options.dynamic_schema

    start = pd.Timestamp("2018-01-02")
    num_rows = 4
    index = pd.MultiIndex.from_arrays(
        [[start + datetime.timedelta(days=i) for i in range(num_rows)], ["a", "b", "c", "d"]]
    )

    df = pd.DataFrame(index=index, data={"col_one": [1, 2, 3, 4], "col_two": [1, 2, 3, 4], "col_three": [5, 6, 7, 8]})
    df.index.set_names(["col_one", "col_two"], inplace=True)
    lib.write("sym", df)

    res = lib.get_info("sym")
    assert list(df.columns) == res["col_names"]["columns"]
    assert res["col_names"]["index"] == ["col_one", "col_two"]

    version_query = lib._get_version_query(None)
    descriptor = lib.version_store.read_descriptor("sym", version_query)

    norm_meta = descriptor.timeseries_descriptor.normalization
    index_meta = norm_meta.df.common.multi_index
    assert index_meta.name == "col_one"

    expected_col_one_name = "__col_col_one__0" if is_dynamic_schema else "__col_col_one__1"
    assert dict(norm_meta.df.common.col_names) == {
        "__idx__col_two": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="__idx__col_two"),
        expected_col_one_name: descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_one"),
        "col_two": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_two"),
        "col_three": descriptors_pb2.NormalizationMetadata.Pandas.ColumnName(original_name="col_three"),
    }

    stream_descriptor = descriptor.timeseries_descriptor.as_stream_descriptor
    field_names = [f.name for f in stream_descriptor.fields()]
    assert field_names == ["col_one", "__idx__col_two", expected_col_one_name, "col_two", "col_three"]
    assert stream_descriptor.index.field_count() == 1
    assert stream_descriptor.index.kind() == IndexKind.TIMESTAMP


@pytest.mark.skip(reason="Monday ref: 9714233101")
def test_multi_index_same_names(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(
        {"x": np.arange(10)},
        index=[
            [chr(ord("a") + i // 5) for i in range(10)],
            [i % 2 for i in range(10)],
            [i % 3 for i in range(10)],
            [i % 4 for i in range(10)],
        ],
    )
    df.index.names = ["index", "index", "index", "another_index", "another_index"]
    lib.write("sym", df)
    result_df = lib.read("sym").data
    assert_frame_equal(result_df, df)


@pytest.mark.skip(reason="Monday ref: 9715738171")
def test_digit_columns(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    df = pd.DataFrame(np.arange(20).resize(10, 2), columns=[1, "1"])
    lib.write("sym", df)
    result_df = lib.read("sym").data
    # Both column contents and column names are broken
    assert_frame_equal(result_df, df)


@pytest.mark.skip(reason="Monday ref: 18197986461")
def test_groupby_timeseries_column_with_timezone(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "sym"
    timezone_times = pd.date_range(pd.Timestamp(2025, 1, 1, tz="America/New_York"), periods=5)
    non_timezone_times = pd.date_range(pd.Timestamp(2025, 1, 1), periods=5)
    df = pd.DataFrame({"time": non_timezone_times, "ints": np.arange(5)}, index=timezone_times)
    lib.write(sym, df)
    q = QueryBuilder().groupby("time").agg({"ints": "sum"})
    result = lib.read(sym, query_builder=q).data
    expected = df
    expected.reset_index(drop=True, inplace=True)
    expected.set_index("time", inplace=True)
    assert_frame_equal(result, expected)
