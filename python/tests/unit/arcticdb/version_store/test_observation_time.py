"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pandas as pd

from arcticdb.version_store.processing import QueryBuilder


def test_observation_time(lmdb_version_store):
    # Test case will assume data that ticks hourly, and that this data is provided by the vendor daily at 23:59:00
    # The test will cover 5 days worth of data: 2000-01-01 00:00:00 through to 2000-01-05 23:00:00.
    # To begin with, data will be collected from 2000-01-03 i.e. missing two vendor-provided sets.
    # A backfill will then be performed to fill in the missing data from 2000-01-01 and 2000-01-02.
    # Finally, we will pretend that the data provided on 2000-01-04 was missing 12:00:00-16:00:00 (inclusive),
    # so these will be added in as well.
    # At all stages, we should be able to use the query_builder kwarg of read to extract data as it was at a
    # specified observation time

    lib = lmdb_version_store
    sym = "test_observation_time"
    data_col_name = "data"
    observed_col_name = "observed"
    hours_per_day = 24

    # Setup the data we will receive from the vendor
    # Observed 2000-01-01
    df_1 = pd.DataFrame(
        {
            data_col_name: np.arange(0 * hours_per_day, 1 * hours_per_day),
            observed_col_name: hours_per_day * [pd.Timestamp(year=2000, month=1, day=1, hour=23, minute=59)],
        },
        index=pd.date_range(pd.Timestamp(year=2000, month=1, day=1), periods=hours_per_day, freq="H"),
    )
    # Observed 2000-01-02
    df_2 = pd.DataFrame(
        {
            data_col_name: np.arange(1 * hours_per_day, 2 * hours_per_day),
            observed_col_name: hours_per_day * [pd.Timestamp(year=2000, month=1, day=2, hour=23, minute=59)],
        },
        index=pd.date_range(pd.Timestamp(year=2000, month=1, day=2), periods=hours_per_day, freq="H"),
    )
    # Observed 2000-01-03
    df_3 = pd.DataFrame(
        {
            data_col_name: np.arange(2 * hours_per_day, 3 * hours_per_day),
            observed_col_name: hours_per_day * [pd.Timestamp(year=2000, month=1, day=3, hour=23, minute=59)],
        },
        index=pd.date_range(pd.Timestamp(year=2000, month=1, day=3), periods=hours_per_day, freq="H"),
    )
    # Observed 2000-01-04, with rows missing 12:00:00-16:00:00 (inclusive) initially
    df_4 = pd.DataFrame(
        {
            data_col_name: np.arange(3 * hours_per_day, 4 * hours_per_day),
            observed_col_name: hours_per_day * [pd.Timestamp(year=2000, month=1, day=4, hour=23, minute=59)],
        },
        index=pd.date_range(pd.Timestamp(year=2000, month=1, day=4), periods=hours_per_day, freq="H"),
    )
    df_4_initial = df_4.loc[(df_4[data_col_name] <= 84) | (df_4[data_col_name] >= 90)]
    df_4_patch = df_4.loc[(df_4[data_col_name] > 84) & (df_4[data_col_name] < 90)]
    # Observed 2000-01-05
    df_5 = pd.DataFrame(
        {
            data_col_name: np.arange(4 * hours_per_day, 5 * hours_per_day),
            observed_col_name: hours_per_day * [pd.Timestamp(year=2000, month=1, day=5, hour=23, minute=59)],
        },
        index=pd.date_range(pd.Timestamp(year=2000, month=1, day=5), periods=hours_per_day, freq="H"),
    )
    df_total = pd.concat((df_1, df_2, df_3, df_4, df_5))

    # Write the data for 2000-01-03
    lib.write(sym, df_3)
    # Append 2000-01-04 with missing data
    lib.append(sym, df_4_initial)
    # Append 2000-01-05
    lib.append(sym, df_5)

    # Reading with observation times should only pick up data available at that time
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=2)]
    received = lib.read(sym, query_builder=q).data
    assert received.empty
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=3)]
    received = lib.read(sym, query_builder=q).data
    assert received.empty
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=4)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(df_3, received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=5)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_3, df_4_initial)), received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=6)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_3, df_4_initial, df_5)), received)

    # Now backfill data for 2000-01-01 and 2000-01-02
    lib.update(sym, df_1)
    lib.update(sym, df_2)

    # Reading with observation times should only pick up data available at that time
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=2)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(df_1, received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=3)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2)), received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=4)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2, df_3)), received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=5)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2, df_3, df_4_initial)), received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=6)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2, df_3, df_4_initial, df_5)), received)

    # Finally, update with the rows missing on 2000-01-04
    lib.update(sym, df_4_patch)

    # Reading with observation times should only pick up data available at that time
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=2)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(df_1, received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=3)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2)), received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=4)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2, df_3)), received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=5)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2, df_3, df_4)), received)
    q = QueryBuilder()
    q = q[q[observed_col_name] < pd.Timestamp(year=2000, month=1, day=6)]
    received = lib.read(sym, query_builder=q).data
    assert np.array_equal(pd.concat((df_1, df_2, df_3, df_4, df_5)), received)
