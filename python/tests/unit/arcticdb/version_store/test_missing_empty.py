"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import pytest

pytest.skip("", allow_module_level=True)

import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass
from typing import List, Optional, Union

log = logging.getLogger("missing_empty_tests")
log.setLevel(logging.INFO)


@dataclass
class TestCase:
    name: str
    base_name: Optional[str]
    type: Optional[str]
    data: Union[List, np.array]
    index: Optional[Union[List, pd.Index]]
    mark: pytest.mark = None

    @property
    def symbol(self) -> str:
        return f"missing_empty/{self.name}"

    @property
    def base_symbol(self) -> str:
        return f"missing_empty/{self.base_name}"

    @property
    def pytest_marks(self):
        return self.mark if self.mark else ()

    @property
    def pytest_param(self):
        return pytest.param(self, id=self.name, marks=self.pytest_marks)

    @staticmethod
    def pytest_param_list(test_cases: List):
        return [test_case.pytest_param for test_case in test_cases]


@dataclass
class TestResult:
    result: bool
    message: str = None

    @property
    def pass_fail(self):
        return "PASS" if self.result else "FAIL"


def pd_mask_index_range(df, index_range, keep_range):
    if not index_range:
        return df
    # either keep or remove data in the index range, according to keep_range
    keep_mask = (
        (df.index >= index_range[0]) & (df.index <= index_range[1])
        if keep_range
        else (df.index < index_range[0]) | (df.index > index_range[1])
    )
    return df[keep_mask]


def pd_remove_secondary_index_levels(df):
    # not a multi-index
    if len(df.index.names) == 1:
        return df
    return df.reset_index(level=df.index.names[1:])


def pd_restore_secondary_index_levels(df, original_level_names):
    # not a multi-index
    if len(original_level_names) == 1:
        return df
    return df.set_index(original_level_names[1:], append=True)


def pd_mask_index_range_restore_index(df, index_range, keep_range, original_level_names):
    df_masked = pd_mask_index_range(df, index_range, keep_range)
    return pd_restore_secondary_index_levels(df_masked, original_level_names)


def pd_delete_replace(df1, df2, date_range=None):
    # this is intended to be equivalent to arcticdb update
    df1_primary = pd_remove_secondary_index_levels(df1)
    df2_primary = pd_remove_secondary_index_levels(df2)
    # df1 empty -> use df2
    if len(df1) == 0:
        return pd_mask_index_range_restore_index(
            df2_primary, date_range, keep_range=True, original_level_names=df2.index.names
        )
    # df2 empty -> use df1
    if len(df2) == 0:
        return pd_mask_index_range_restore_index(
            df1_primary, date_range, keep_range=False, original_level_names=df2.index.names
        )
    df2_use = df2_primary
    date_range_delete = None
    if date_range:
        date_range_delete = date_range
        # date_range is the data to keep in df2, which needs to be applied if the date_range is specified
        df2_use = pd_mask_index_range(df2_primary, date_range_delete, keep_range=True)
    elif len(df2_use) > 0:
        # special case when df2 contains only 1 row (0 rows handled above)
        last_row_idx = -1 if len(df2_use) > 1 else 0
        date_range_delete = (df2_use.index[0], df2_use.index[last_row_idx])
    df1_before = df1_primary
    df1_after = None
    if date_range_delete:
        df1_before = df1_primary[df1_primary.index < date_range_delete[0]]
        df1_after = df1_primary[df1_primary.index > date_range_delete[1]]
        pd_mask_index_range(df1_primary, date_range_delete, keep_range=False)
    to_concat = (df1_before, df2_use, df1_after) if df1_after is not None else (df1_before, df2_use)
    # concat preserves types over other methods eg int -> float (due to temp NaN creation)
    return pd_restore_secondary_index_levels(pd.concat(to_concat), df1.index.names)


def infer_type(s: pd.Series):
    t = None
    for v in s:
        if not pd.isnull(v):
            tv = type(v)
            if t and tv != t:
                raise ValueError(f"Mixed non-None types in column {s.name}: {t} and {tv}")
            t = tv
    return t


def fill_value(t):
    fill_values = {bool: False, int: 0, np.int64: 0}
    if t in fill_values:
        return fill_values[t]
    return None


def pd_arcticdb_read_sim(df):
    read_df = df.copy(True)
    for c in df.columns:
        t = infer_type(df[c])
        fv = fill_value(t)
        if fv is not None:
            read_df[c] = df[c].fillna(fv)
    return read_df


def create_df(dtype, data, index):
    if dtype is None:
        return pd.DataFrame({"col": data}, index=index)
    return pd.DataFrame({"col": data}, dtype=dtype, index=index)


def compare_dfs(df1: pd.DataFrame, df2: pd.DataFrame):
    if df1.equals(df2):
        return "match"
    if len(df1) != len(df2):
        return f"no match: len differs {len(df1)} vs {len(df2)}"
    if len(df1.columns) != len(df2.columns):
        return (
            f"no match: number of columns differs {len(df1.columns)}:{df1.columns} vs {len(df2.columns)}:{df2.columns}"
        )
    if (df1.columns != df2.columns).any():
        return f"no match: columns differ {df1.columns} vs {df2.columns}"
    if type(df1.index) != type(df2.index):
        return f"no match: index types differ {type(df1.index)} vs {type(df2.index)}"
    if (df1.index != df2.index).any():
        return f"no match: index values differ {df1.index} vs {df2.index}"
    for c in df1.columns:
        if (df1[c] != df2[c]).any():
            return f"no match: data values differ for column {c}: {df1[c].values} vs {df2[c].values}"
        if df1[c].dtype != df2[c].dtype:
            return f"no match: data types differ for column {c}: {df1[c].dtype} vs {df2[c].dtype}"
    return f"no match: undetermined reason"


def write_df(lib, df: pd.DataFrame, test: TestCase):
    try:
        lib.write(test.symbol, df)
    except Exception as e:
        return TestResult(False, f"Write error: {e}")
    return TestResult(True)


def round_trip(lib, df: pd.DataFrame, test: TestCase):
    write_res = write_df(lib, df, test)
    if not write_res.result:
        return write_res
    try:
        df_db = lib.read(test.symbol).data
    except Exception as e:
        return TestResult(False, f"Read error: {e}")
    match = df.equals(df_db)
    message = "match" if match else compare_dfs(df, df_db)
    return TestResult(match, message)


def append_update(lib, df, test, verb, verb_name, pd_mod_func):
    try:
        base_df = lib.read(test.base_symbol).data
    except Exception as e:
        return TestResult(False, f"Read base error: {e}")
    try:
        lib.write(test.symbol, base_df)
    except Exception as e:
        return TestResult(False, f"Write base error: {e}")
    try:
        verb(test.symbol, df)
    except Exception as e:
        return TestResult(False, f"{verb_name} error: {e}")
    try:
        df_db = lib.read(test.symbol).data
    except Exception as e:
        return TestResult(False, f"Read error: {e}")
    try:
        df_mod_pd_raw = pd_mod_func(base_df, df, test.index is None)
    except Exception as e:
        return TestResult(False, f"Error running Pandas repro function for Arctic {verb_name}: {e}")
    try:
        df_mod_pd = pd_arcticdb_read_sim(df_mod_pd_raw)
    except Exception as e:
        return TestResult(False, f"Error running Pandas read simulation function: {e}")

    match = df_mod_pd.equals(df_db)
    message = "match" if match else compare_dfs(df_mod_pd, df_db)
    return TestResult(match, message)


def pd_append(base_df, df, ignore_index):
    return pd.concat([base_df, df], ignore_index=ignore_index)


def pd_update(base_df, df, ignore_index):
    return pd_delete_replace(base_df, df)


def append(lib, df, test):
    return append_update(lib, df, test, lib.append, "Append", pd_append)


def update(lib, df, test):
    return append_update(lib, df, test, lib.update, "Update", pd_update)


def run_test(lib, test: TestCase, action, base_test: TestCase = None):
    if base_test:
        base_df = create_df(base_test.type, base_test.data, base_test.index)
        write_res = write_df(lib, base_df, base_test)
        if not write_res.result:
            return write_res
    try:
        df = create_df(test.type, test.data, test.index)
    except Exception as e:
        return TestResult(False, f"Dataframe create error: {e}")
    res = action(lib, df, test)
    if not res.result:
        pytest.fail(res.message)
    assert res.result is True


_datetime_index1 = pd.date_range("20231201", "20231203")
_datetime_overlap_index1 = pd.date_range("20231202", "20231204")
_datetime_no_overlap_index1 = pd.date_range("20231203", "20231205")
_datetime_data1 = pd.date_range("20220601", "20220603").values
_datetime_data2 = pd.date_range("20220603", "20220605").values
_datetime_none_data1 = np.array([_datetime_data1[0], np.datetime64("NaT"), _datetime_data1[2]])
_int_index1 = [5, 6, 7]
_int_index2 = [6, 7, 8]
_empty_int_index = pd.Index(data=[], dtype="int")
_empty_datetime_index = pd.DatetimeIndex(data=[])

_ROUND_TRIP_TESTS_RAW = [
    # no index
    TestCase("no_index/bool_all", None, "bool", [False, True, False], None),
    TestCase("no_index/int_all", None, "int", [1, 2, 3], None),
    TestCase("no_index/float_all", None, "float", [1.1, 2.1, 3.1], None),
    TestCase("no_index/str_all", None, "str", ["a1", "a2", "a3"], None),
    TestCase("no_index/datetime_all", None, "datetime64[ns]", _datetime_data1, None),
    TestCase(
        "no_index/none_all",
        None,
        None,
        [None, None, None],
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/bool_single_none",
        None,
        None,
        [False, None, True],
        None,
        mark=pytest.mark.xfail(reason="needs nullable bool"),
    ),
    TestCase(
        "no_index/int_single_none", None, "int", [1, None, 3], None, mark=pytest.mark.xfail(reason="needs nullable int")
    ),
    TestCase("no_index/float_single_none", None, None, [1.1, None, 3.1], None),
    TestCase("no_index/float_single_nan", None, None, [1.1, np.nan, 3.1], None),
    TestCase("no_index/datetime_single_nat", None, "datetime64[ns]", _datetime_none_data1, None),
    TestCase("no_index/str_single_none", None, None, ["a1", None, "a3"], None),
    TestCase("no_index/bool_empty", None, "bool", [], None, mark=pytest.mark.xfail(reason="must be fixed for 4.4.0")),
    TestCase("no_index/int_empty", None, "int", [], None, mark=pytest.mark.xfail(reason="must be fixed for 4.4.0")),
    TestCase("no_index/float_empty", None, "float", [], None, mark=pytest.mark.xfail(reason="must be fixed for 4.4.0")),
    TestCase("no_index/str_empty", None, "str", [], None, mark=pytest.mark.xfail(reason="must be fixed for 4.4.0")),
    TestCase(
        "no_index/datetime_empty",
        None,
        "datetime64[ns]",
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase("no_index/no_type_empty", None, None, [], None, mark=pytest.mark.xfail(reason="must be fixed for 4.4.0")),
    # int index
    TestCase("int_index/bool_all", None, "bool", [False, True, False], _int_index1),
    TestCase("int_index/int_all", None, "int", [1, 2, 3], _int_index1),
    TestCase("int_index/float_all", None, "float", [1.1, 2.1, 3.1], _int_index1),
    TestCase("int_index/str_all", None, "str", ["a1", "a2", "a3"], _int_index1),
    TestCase("int_index/datetime_all", None, "datetime64[ns]", _datetime_data1, _int_index1),
    TestCase(
        "int_index/none_all",
        None,
        None,
        [None, None, None],
        _int_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/bool_single_none",
        None,
        None,
        [False, None, True],
        _int_index1,
        mark=pytest.mark.xfail(reason="needs nullable bool"),
    ),
    TestCase(
        "int_index/int_single_none",
        None,
        "int",
        [1, None, 3],
        _int_index1,
        mark=pytest.mark.xfail(reason="needs nullable int"),
    ),
    TestCase("int_index/float_single_none", None, None, [1.1, None, 3.1], _int_index1),
    TestCase("int_index/float_single_nan", None, None, [1.1, np.nan, 3.1], _int_index1),
    TestCase("int_index/datetime_single_nat", None, "datetime64[ns]", _datetime_none_data1, _int_index1),
    TestCase("int_index/str_single_none", None, None, ["a1", None, "a3"], _int_index1),
    TestCase("int_index/bool_empty", None, "bool", [], _empty_int_index),
    TestCase(
        "int_index/int_empty",
        None,
        "int",
        [],
        _empty_int_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/float_empty",
        None,
        "float",
        [],
        _empty_int_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/str_empty",
        None,
        "str",
        [],
        _empty_int_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/datetime_empty",
        None,
        "datetime64[ns]",
        [],
        _empty_int_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/no_type_empty",
        None,
        None,
        [],
        _empty_int_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    # datetime index
    TestCase("ts_index/bool_all", None, "bool", [False, True, False], _datetime_index1),
    TestCase("ts_index/int_all", None, "int", [1, 2, 3], _datetime_index1),
    TestCase("ts_index/float_all", None, "float", [1.1, 2.1, 3.1], _datetime_index1),
    TestCase("ts_index/str_all", None, "str", ["a1", "a2", "a3"], _datetime_index1),
    TestCase("ts_index/datetime_all", None, "datetime64[ns]", _datetime_data1, _datetime_index1),
    TestCase(
        "ts_index/none_all",
        None,
        None,
        [None, None, None],
        _datetime_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/bool_single_none",
        None,
        None,
        [False, None, True],
        _datetime_index1,
        mark=pytest.mark.xfail(reason="needs nullable bool"),
    ),
    TestCase(
        "ts_index/int_single_none",
        None,
        "int",
        [1, None, 3],
        _datetime_index1,
        mark=pytest.mark.xfail(reason="needs nullable bool"),
    ),
    TestCase("ts_index/float_single_none", None, None, [1.1, None, 3.1], _datetime_index1),
    TestCase("ts_index/float_single_nan", None, None, [1.1, np.nan, 3.1], _datetime_index1),
    TestCase("ts_index/datetime_single_nat", None, "datetime64[ns]", _datetime_none_data1, _datetime_index1),
    TestCase("ts_index/str_single_none", None, None, ["a1", None, "a3"], _datetime_index1),
    TestCase("ts_index/bool_empty", None, "bool", [], _empty_datetime_index),
    TestCase(
        "ts_index/int_empty",
        None,
        "int",
        [],
        _empty_datetime_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/float_empty",
        None,
        "float",
        [],
        _empty_datetime_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/str_empty",
        None,
        "str",
        [],
        _empty_datetime_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/datetime_empty",
        None,
        "datetime64[ns]",
        [],
        _empty_datetime_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/no_type_empty",
        None,
        None,
        [],
        _empty_datetime_index,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
]

_ROUND_TRIP_TESTS = TestCase.pytest_param_list(_ROUND_TRIP_TESTS_RAW)

_APPEND_TESTS_RAW = [
    # no index
    TestCase("no_index/bool_all_append", "no_index/bool_all", "bool", [False, True, False], None),
    TestCase("no_index/int_all_append", "no_index/int_all", "int", [11, 12, 13], None),
    TestCase("no_index/float_all_append", "no_index/float_all", "float", [11.1, 12.1, 13.1], None),
    TestCase("no_index/str_all_append", "no_index/str_all", "str", ["b1", "b2", "b3"], None),
    TestCase("no_index/datetime_all_append", "no_index/datetime_all", "datetime64[ns]", _datetime_data2, None),
    TestCase(
        "no_index/none_all_append",
        "no_index/none_all",
        None,
        [None, None, None],
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/bool_all_append_none",
        "no_index/bool_all",
        None,
        [None, None, None],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/int_all_append_none",
        "no_index/int_all",
        None,
        [None, None, None],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase("no_index/float_all_append_none", "no_index/float_all", "float", [None, None, None], None),
    TestCase(
        "no_index/str_all_append_none",
        "no_index/str_all",
        "str",
        [None, None, None],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase("no_index/datetime_all_append_none", "no_index/datetime_all", "datetime64[ns]", [None, None, None], None),
    TestCase(
        "no_index/none_all_append_bool",
        "no_index/none_all",
        "bool",
        [False, True, False],
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/none_all_append_int",
        "no_index/none_all",
        "int",
        [11, 12, 13],
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/none_all_append_float",
        "no_index/none_all",
        "float",
        [11.1, 12.1, 13.1],
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/none_all_append_str",
        "no_index/none_all",
        "str",
        ["b1", "b2", "b3"],
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/none_all_append_datetime",
        "no_index/none_all",
        "datetime64[ns]",
        _datetime_data2,
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/bool_empty_append",
        "no_index/bool_empty",
        "bool",
        [False, True, False],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/int_empty_append",
        "no_index/int_empty",
        "int",
        [11, 12, 13],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/float_empty_append",
        "no_index/float_empty",
        "float",
        [11.1, 12.1, 13.1],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/str_empty_append",
        "no_index/str_empty",
        "str",
        ["b1", "b2", "b3"],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/datetime_empty_append",
        "no_index/datetime_empty",
        "datetime64[ns]",
        _datetime_data2,
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/bool_empty_append_none",
        "no_index/bool_empty",
        None,
        [None, None, None],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/int_empty_append_none",
        "no_index/int_empty",
        None,
        [None, None, None],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/float_empty_append_none",
        "no_index/float_empty",
        "float",
        [None, None, None],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/str_empty_append_none",
        "no_index/str_empty",
        "str",
        [None, None, None],
        None,
        mark=pytest.mark.skip(reason="fails due to a bug, fixed in 4.3.1+"),
    ),
    TestCase(
        "no_index/datetime_empty_append_none",
        "no_index/datetime_empty",
        "datetime64[ns]",
        [None, None, None],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "no_index/no_type_empty_append_none",
        "no_index/no_type_empty",
        None,
        [None, None, None],
        None,
        mark=pytest.mark.skip(reason="must be fixed for 4.4.0"),
    ),
    # int index
    TestCase("int_index/bool_all_append", "int_index/bool_all", "bool", [False, True, False], _int_index2),
    TestCase("int_index/int_all_append", "int_index/int_all", "int", [11, 12, 13], _int_index2),
    TestCase("int_index/float_all_append", "int_index/float_all", "float", [11.1, 12.1, 13.1], _int_index2),
    TestCase("int_index/str_all_append", "int_index/str_all", "str", ["b1", "b2", "b3"], _int_index2),
    TestCase("int_index/datetime_all_append", "int_index/datetime_all", "datetime64[ns]", _datetime_data2, _int_index2),
    TestCase(
        "int_index/none_all_append",
        "int_index/none_all",
        None,
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/bool_all_append_none",
        "int_index/bool_all",
        None,
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/int_all_append_none",
        "int_index/int_all",
        None,
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase("int_index/float_all_append_none", "int_index/float_all", "float", [None, None, None], _int_index2),
    TestCase(
        "int_index/str_all_append_none",
        "int_index/str_all",
        "str",
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/datetime_all_append_none",
        "int_index/datetime_all",
        "datetime64[ns]",
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/none_all_append_bool",
        "int_index/none_all",
        "bool",
        [False, True, False],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/none_all_append_int",
        "int_index/none_all",
        "int",
        [11, 12, 13],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/none_all_append_float",
        "int_index/none_all",
        "float",
        [11.1, 12.1, 13.1],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/none_all_append_str",
        "int_index/none_all",
        "str",
        ["b1", "b2", "b3"],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/none_all_append_datetime",
        "int_index/none_all",
        "datetime64[ns]",
        _datetime_data2,
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/bool_empty_append",
        "int_index/bool_empty",
        "bool",
        [False, True, False],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/int_empty_append",
        "int_index/int_empty",
        "int",
        [11, 12, 13],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/float_empty_append",
        "int_index/float_empty",
        "float",
        [11.1, 12.1, 13.1],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/str_empty_append",
        "int_index/str_empty",
        "str",
        ["b1", "b2", "b3"],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/datetime_empty_append",
        "int_index/datetime_empty",
        "datetime64[ns]",
        _datetime_data2,
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/no_type_empty_append",
        "int_index/no_type_empty",
        None,
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/bool_empty_append_none",
        "int_index/bool_empty",
        None,
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/int_empty_append_none",
        "int_index/int_empty",
        None,
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/float_empty_append_none",
        "int_index/float_empty",
        "float",
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/str_empty_append_none",
        "int_index/str_empty",
        "str",
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/datetime_empty_append_none",
        "int_index/datetime_empty",
        "datetime64[ns]",
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "int_index/no_type_empty_append_none",
        "int_index/no_type_empty",
        None,
        [None, None, None],
        _int_index2,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    # datetime index
    TestCase(
        "ts_index/bool_all_append", "ts_index/bool_all", "bool", [False, True, False], _datetime_no_overlap_index1
    ),
    TestCase("ts_index/int_all_append", "ts_index/int_all", "int", [11, 12, 13], _datetime_no_overlap_index1),
    TestCase(
        "ts_index/float_all_append", "ts_index/float_all", "float", [11.1, 12.1, 13.1], _datetime_no_overlap_index1
    ),
    TestCase("ts_index/str_all_append", "ts_index/str_all", "str", ["b1", "b2", "b3"], _datetime_no_overlap_index1),
    TestCase(
        "ts_index/datetime_all_append",
        "ts_index/datetime_all",
        "datetime64[ns]",
        _datetime_data2,
        _datetime_no_overlap_index1,
    ),
    TestCase(
        "ts_index/none_all_append",
        "ts_index/none_all",
        None,
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/bool_all_append_none",
        "ts_index/bool_all",
        None,
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/int_all_append_none",
        "ts_index/int_all",
        None,
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/float_all_append_none", "ts_index/float_all", "float", [None, None, None], _datetime_no_overlap_index1
    ),
    TestCase(
        "ts_index/str_all_append_none",
        "ts_index/str_all",
        "str",
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/datetime_all_append_none",
        "ts_index/datetime_all",
        "datetime64[ns]",
        [None, None, None],
        _datetime_no_overlap_index1,
    ),
    TestCase(
        "ts_index/none_all_append_bool",
        "ts_index/none_all",
        "bool",
        [False, True, False],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_append_int",
        "ts_index/none_all",
        "int",
        [11, 12, 13],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_append_float",
        "ts_index/none_all",
        "float",
        [11.1, 12.1, 13.1],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_append_str",
        "ts_index/none_all",
        "str",
        ["b1", "b2", "b3"],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_append_datetime",
        "ts_index/none_all",
        "datetime64[ns]",
        _datetime_data2,
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/bool_empty_append", "ts_index/bool_empty", "bool", [False, True, False], _datetime_no_overlap_index1
    ),
    TestCase("ts_index/int_empty_append", "ts_index/int_empty", "int", [11, 12, 13], _datetime_no_overlap_index1),
    TestCase(
        "ts_index/float_empty_append", "ts_index/float_empty", "float", [11.1, 12.1, 13.1], _datetime_no_overlap_index1
    ),
    TestCase("ts_index/str_empty_append", "ts_index/str_empty", "str", ["b1", "b2", "b3"], _datetime_no_overlap_index1),
    TestCase(
        "ts_index/datetime_empty_append",
        "ts_index/datetime_empty",
        "datetime64[ns]",
        _datetime_data2,
        _datetime_no_overlap_index1,
    ),
    TestCase(
        "ts_index/no_type_empty_append",
        "ts_index/no_type_empty",
        None,
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/bool_empty_append_none",
        "ts_index/bool_empty",
        None,
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/int_empty_append_none",
        "ts_index/int_empty",
        None,
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/float_empty_append_none",
        "ts_index/float_empty",
        "float",
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/str_empty_append_none",
        "ts_index/str_empty",
        "str",
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/datetime_empty_append_none",
        "ts_index/datetime_empty",
        "datetime64[ns]",
        [None, None, None],
        _datetime_no_overlap_index1,
    ),
    TestCase(
        "ts_index/no_type_empty_append_none",
        "ts_index/no_type_empty",
        None,
        [None, None, None],
        _datetime_no_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/bool_all_append_empty",
        "ts_index/bool_all",
        "bool",
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/int_all_append_empty",
        "ts_index/int_all",
        "int",
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/float_all_append_empty",
        "ts_index/float_all",
        "float",
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/str_all_append_empty",
        "ts_index/str_all",
        "str",
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/datetime_all_append_empty",
        "ts_index/datetime_all",
        "datetime64[ns]",
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_append_empty",
        "ts_index/none_all",
        None,
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
]

_APPEND_TESTS = TestCase.pytest_param_list(_APPEND_TESTS_RAW)

_UPDATE_TESTS_RAW = [
    TestCase(
        "ts_index/bool_all_update",
        "ts_index/bool_all",
        "bool",
        [False, True, False],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/int_all_update",
        "ts_index/int_all",
        "int",
        [11, 12, 13],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase("ts_index/float_all_update", "ts_index/float_all", "float", [11.1, 12.1, 13.1], _datetime_overlap_index1),
    TestCase("ts_index/str_all_update", "ts_index/str_all", "str", ["b1", "b2", "b3"], _datetime_overlap_index1),
    TestCase(
        "ts_index/datetime_all_update",
        "ts_index/datetime_all",
        "datetime64[ns]",
        _datetime_data2,
        _datetime_overlap_index1,
    ),
    TestCase(
        "ts_index/none_all_update",
        "ts_index/none_all",
        None,
        [None, None, None],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/bool_all_update_none",
        "ts_index/bool_all",
        None,
        [None, None, None],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/int_all_update_none",
        "ts_index/int_all",
        None,
        [None, None, None],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/float_all_update_none", "ts_index/float_all", "float", [None, None, None], _datetime_overlap_index1
    ),
    TestCase(
        "ts_index/datetime_all_update_none",
        "ts_index/datetime_all",
        "datetime64[ns]",
        [None, None, None],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/str_all_update_none",
        "ts_index/str_all",
        "str",
        [None, None, None],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_update_bool",
        "ts_index/none_all",
        "bool",
        [False, True, False],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_update_int",
        "ts_index/none_all",
        "int",
        [11, 12, 13],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_update_float",
        "ts_index/none_all",
        "float",
        [11.1, 12.1, 13.1],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_update_datetime",
        "ts_index/datetime_all",
        "float",
        _datetime_data2,
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase(
        "ts_index/none_all_update_str",
        "ts_index/none_all",
        "str",
        ["b1", "b2", "b3"],
        _datetime_overlap_index1,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
    TestCase("ts_index/bool_all_update_empty", "ts_index/bool_all", "bool", [], None),
    TestCase("ts_index/int_all_update_empty", "ts_index/int_all", "int", [], None),
    TestCase("ts_index/float_all_update_empty", "ts_index/float_all", "float", [], None),
    TestCase("ts_index/str_all_update_empty", "ts_index/str_all", "str", [], None),
    TestCase("ts_index/datetime_all_update_empty", "ts_index/datetime_all", "datetime64[ns]", [], None),
    TestCase(
        "ts_index/none_all_update_empty",
        "ts_index/none_all",
        None,
        [],
        None,
        mark=pytest.mark.xfail(reason="must be fixed for 4.4.0"),
    ),
]

_UPDATE_TESTS = TestCase.pytest_param_list(_UPDATE_TESTS_RAW)

_ALL_TESTS_RAW = _ROUND_TRIP_TESTS_RAW + _APPEND_TESTS_RAW + _UPDATE_TESTS_RAW

_TEST_LOOKUP = {test.name: test for test in _ALL_TESTS_RAW}


@pytest.mark.parametrize("test_case", _ROUND_TRIP_TESTS)
def test_empty_missing_round_trip_lmdb(lmdb_version_store_empty_types, test_case: TestCase):
    run_test(lmdb_version_store_empty_types, test_case, round_trip)


@pytest.mark.parametrize("test_case", _ROUND_TRIP_TESTS)
def test_empty_missing_round_trip_lmdb_dynamic_schema(
    lmdb_version_store_empty_types_dynamic_schema, test_case: TestCase
):
    run_test(lmdb_version_store_empty_types_dynamic_schema, test_case, round_trip)


@pytest.mark.parametrize("test_case", _APPEND_TESTS)
def test_empty_missing_append_lmdb(lmdb_version_store_empty_type, test_case: TestCase):
    if test_case.base_name not in _TEST_LOOKUP:
        pytest.fail(f"Base test case {test_case.base_name} not found")
    run_test(lmdb_version_store_empty_type, test_case, append, _TEST_LOOKUP[test_case.base_name])


@pytest.mark.parametrize("test_case", _APPEND_TESTS)
def test_empty_missing_append_lmdb_dynamic_schema(lmdb_version_store_empty_type_dynamic_schema, test_case: TestCase):
    if test_case.base_name not in _TEST_LOOKUP:
        pytest.fail(f"Base test case {test_case.base_name} not found")
    run_test(lmdb_version_store_empty_type_dynamic_schema, test_case, append, _TEST_LOOKUP[test_case.base_name])


@pytest.mark.parametrize("test_case", _UPDATE_TESTS)
def test_empty_missing_update_lmdb(lmdb_version_store_empty_type, test_case: TestCase):
    if test_case.base_name not in _TEST_LOOKUP:
        pytest.fail(f"Base test case {test_case.base_name} not found")
    run_test(lmdb_version_store_empty_type, test_case, update, _TEST_LOOKUP[test_case.base_name])


@pytest.mark.parametrize("test_case", _UPDATE_TESTS)
def test_empty_missing_update_lmdb_dynamic_schema(lmdb_version_store_empty_type_dynamic_schema, test_case: TestCase):
    if test_case.base_name not in _TEST_LOOKUP:
        pytest.fail(f"Base test case {test_case.base_name} not found")
    run_test(lmdb_version_store_empty_type_dynamic_schema, test_case, update, _TEST_LOOKUP[test_case.base_name])


# to run a single test, edit the following 2 lines to contain the test and action you want to test,
# then run one of the two unit tests below
_SINGLE_TEST = [None]  # [_TEST_LOOKUP["ts_index/float_all_update_none"]]
_SINGLE_TEST_ACTION = update  # round_trip | append | update


@pytest.mark.parametrize("test_case", _SINGLE_TEST)
def test_empty_missing_single_lmdb(lmdb_version_store_empty_type, test_case: TestCase):
    if test_case:
        if test_case.base_name and test_case.base_name not in _TEST_LOOKUP:
            pytest.fail(f"Base test case {test_case.base_name} not found")
        run_test(lmdb_version_store_empty_type, test_case, _SINGLE_TEST_ACTION, _TEST_LOOKUP[test_case.base_name])


@pytest.mark.parametrize("test_case", _SINGLE_TEST)
def test_empty_missing_single_lmdb_dynamic_schema(lmdb_version_store_empty_type_dynamic_schema, test_case: TestCase):
    if test_case:
        if test_case.base_name and test_case.base_name not in _TEST_LOOKUP:
            pytest.fail(f"Base test case {test_case.base_name} not found")
        run_test(
            lmdb_version_store_empty_type_dynamic_schema,
            test_case,
            _SINGLE_TEST_ACTION,
            _TEST_LOOKUP[test_case.base_name],
        )
