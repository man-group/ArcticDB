"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import numpy as np
import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone

from arcticdb.version_store import TimeFrame
from numpy.testing import assert_array_equal

from arcticdb.version_store._custom_normalizers import (
    CustomNormalizer,
    register_normalizer,
    clear_registered_normalizers,
)
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata


def test_update_date_range_dataframe(lmdb_version_store):
    """Restrictive update - when date_range is specified ensure that we only touch values in that range."""
    # given
    dtidx = pd.date_range("2022-06-01", "2022-06-05")
    df = pd.DataFrame(index=dtidx, data={"a": [1, 2, 3, 4, 5]}, dtype=np.int64)
    lmdb_version_store.write("sym_1", df)

    dtidx = pd.date_range("2022-05-01", "2022-06-10")
    a = np.arange(dtidx.shape[0])
    update_df = pd.DataFrame(index=dtidx, data={"a": a}, dtype=np.int64)

    # when
    lmdb_version_store.update("sym_1", update_df, date_range=(datetime(2022, 6, 2), datetime(2022, 6, 4)))

    # then
    result = lmdb_version_store.read("sym_1").data
    np.testing.assert_array_equal(result["a"].values, [1, 32, 33, 34, 5])


class CustomIndex:
    def __init__(self, index: pd.Index):
        self.wrapped = index

    def __len__(self):
        return len(self.wrapped)

    def __getitem__(self, item):
        return self.wrapped[item]

    def __getattr__(self, name):
        if name in ("is_monotonic_increasing", "is_monotonic_decreasing"):
            raise AttributeError("These Pandas settings are not implemented on this non-Pandas timeseries index")
        return getattr(self.wrapped, name)


class CustomTimeseries:
    """Simulation of a non-Pandas DataFrame-like object, with some behaviour similar to a legacy one used in Man."""

    def __init__(self, wrapped: pd.DataFrame, *args, custom_index: bool, with_timezone_attr: bool, timezone):
        if timezone and not with_timezone_attr:
            raise RuntimeError("Meaningless test case - set with_timezone_attr=True")
        if with_timezone_attr:
            self.timezone = timezone
        if custom_index:
            self.index = CustomIndex(wrapped.index)
        self.wrapped = wrapped
        self.with_timezone_attr = with_timezone_attr
        self.custom_index = custom_index

    def __getitem__(self, item):
        if isinstance(item, slice):
            open_ended = slice(item.start + timedelta(microseconds=1), item.stop - timedelta(microseconds=1), item.step)
            return CustomTimeseries(
                self.wrapped[open_ended],
                custom_index=self.custom_index,
                with_timezone_attr=self.with_timezone_attr,
                timezone=self.timezone,
            )
        else:
            return CustomTimeseries(
                self.wrapped[item],
                custom_index=self.custom_index,
                with_timezone_attr=self.with_timezone_attr,
                timezone=self.timezone,
            )

    def __getattr__(self, name):
        if name == "loc":
            raise AttributeError("loc is not implemented on this non-Pandas timeseries")
        return getattr(self.wrapped, name)


class CustomTimeseriesNormalizer(CustomNormalizer):
    def normalize(self, item, **kwargs):
        if isinstance(item, CustomTimeseries):
            df = TimeFrame(
                times=item.wrapped.index.values,
                columns_names=list(item.wrapped.columns),
                columns_values=[item.wrapped[c].values for c in item.columns],
            )
            return df, NormalizationMetadata.CustomNormalizerMeta()

    def denormalize(self, item, norm_meta):
        df = pd.DataFrame(index=item.times, data=dict(zip(item.columns_names, item.columns_values)))
        return CustomTimeseries(df, custom_index=False, with_timezone_attr=True, timezone=None)


@pytest.fixture
def lmdb_version_store_custom_norm(version_store_factory):
    try:
        register_normalizer(CustomTimeseriesNormalizer())
        yield version_store_factory()
    finally:
        clear_registered_normalizers()


@pytest.mark.parametrize(
    "with_timezone_attr,timezone_", [(True, None), (True, None), (True, timezone.utc), (False, None)]
)
@pytest.mark.skip("These fail due to too strict unsorted data checks, PR #388 should resolve")
def test_update_date_range_non_pandas_dataframe(lmdb_version_store_custom_norm, with_timezone_attr, timezone_):
    """Check that updates with a daterange work for a simple non-Pandas timeseries.

    This simulates a legacy DataFrame equivalent still used occasionally in Man.
    """
    version_store = lmdb_version_store_custom_norm

    # given
    dtidx = pd.date_range("2022-06-01", "2022-06-05")
    df = pd.DataFrame(index=dtidx, data={"a": [1, 2, 3, 4, 5]}, dtype=np.int64)
    version_store.write("sym_1", CustomTimeseries(df, with_timezone_attr=with_timezone_attr, timezone=timezone_))

    dtidx = pd.date_range("2022-05-01", "2022-06-10")
    a = np.arange(dtidx.shape[0])
    update_df = pd.DataFrame(index=dtidx, data={"a": a}, dtype=np.int64)

    # when
    version_store.update(
        "sym_1",
        CustomTimeseries(update_df, with_timezone_attr=with_timezone_attr, timezone=timezone_),
        date_range=(datetime(2022, 6, 2), datetime(2022, 6, 4)),
    )

    # then
    result = version_store.read("sym_1").data
    np.testing.assert_array_equal(result["a"].values, [1, 32, 33, 34, 5])


def test_update_date_range_dataframe_multiindex(lmdb_version_store):
    """Similar to the test_update_date_range_dataframe, but with a multiindex."""
    # given
    dtidx = pd.date_range("2022-06-01", "2022-06-05")
    second_level = np.arange(7, 12)
    a = np.arange(1, 6)
    multi_df = pd.DataFrame({"a": a}, index=pd.MultiIndex.from_arrays([dtidx, second_level]))
    sym = "update_date_range_dataframe_multiindex"
    lmdb_version_store.write(sym, multi_df)

    dtidx = pd.date_range("2022-05-01", "2022-06-10")
    second_level = np.arange(dtidx.shape[0])
    a = np.arange(dtidx.shape[0])
    update_df = pd.DataFrame(index=pd.MultiIndex.from_arrays([dtidx, second_level]), data={"a": a}, dtype=np.int64)

    # when
    lmdb_version_store.update(sym, update_df, date_range=(datetime(2022, 6, 2), datetime(2022, 6, 4)))

    # then
    result = lmdb_version_store.read(sym).data
    np.testing.assert_array_equal(result["a"].values, [1, 32, 33, 34, 5])
