"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import random
import string
import pandas as pd
from pandas.tseries.offsets import MonthBegin
import pytest

from arcticdb.util.test import assert_frame_equal


def id_generator(size=75, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


def dict_merge(d1, d2):
    if not d1:
        return d2 or {}
    elif not d2:
        return d1 or {}

    d = d1.copy()
    d.update(d2)
    return d


def make_periods(start_date, end_date, freq, range_type="b"):
    min_time = pd.to_datetime(start_date)
    max_time = pd.to_datetime(end_date)
    periods = pd.period_range(start_date, end_date, freq=freq)
    range_func = pd.bdate_range if range_type == "b" else pd.date_range
    ranges = [range_func(max(p.start_time, min_time), min(p.end_time, max_time), name="date") for p in periods]
    return [r for r in ranges if len(r) > 0]


@pytest.mark.parametrize("lib_type", ["lmdb_version_store_big_map", "s3_version_store", "s3_version_store"])
def test_stress_multicolumn(lib_type, request):
    lib = request.getfixturevalue(lib_type)
    start = (pd.Timestamp("now") - MonthBegin(10)).strftime("%Y%m%d")
    end = pd.Timestamp("now").strftime("%Y%m%d")
    # total securities - too big for build pipeline
    # securities = 63814
    securities = 6000  # total securities
    securities = range(100000, 100000 + securities)
    sec_data = map(lambda sec_id: dict(id=sec_id, val=id_generator()), securities)
    dataframes = []
    for idx, p in enumerate(make_periods(start, end, freq="Y", range_type=None)):
        test_data = pd.concat(
            [pd.DataFrame([dict_merge(dict(date=date), sd) for sd in sec_data]) for date in p], ignore_index=True
        )

        dataframes.append(test_data)

    print("Starting arctic write")

    for count, test_data in enumerate(dataframes):
        name = "test" + str(count)
        now = pd.Timestamp("now")
        lib.write(name, test_data)
        print("saving to arctic native: {}".format(pd.Timestamp("now") - now))

        now = pd.Timestamp("now")
        output_df = lib.read(name).data
        print("reading from arctic native: {}".format(pd.Timestamp("now") - now))

        assert_frame_equal(test_data, output_df)
