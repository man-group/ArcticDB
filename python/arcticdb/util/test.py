"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
from contextlib import contextmanager
from typing import Mapping, Any, Optional, Iterable, NamedTuple, List, AnyStr
import numpy as np
import pandas as pd
import pytest
import string
import random
import time
import attr
from copy import deepcopy
from functools import wraps

from arcticdb.config import Defaults
from arcticdb.log import configure, logger_by_name
from arcticdb.util._versions import PANDAS_VERSION, CHECK_FREQ_VERSION
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store._custom_normalizers import CustomNormalizer
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata
from arcticc.pb2.logger_pb2 import LoggerConfig, LoggersConfig
from arcticc.pb2.storage_pb2 import LibraryDescriptor, VersionStoreConfig
from arcticdb.version_store.helper import ArcticFileConfig
from arcticdb.config import _DEFAULT_ENVS_PATH
from arcticdb_ext import set_config_int, get_config_int, unset_config_int


def create_df(start=0, columns=1) -> pd.DataFrame:
    data = {}
    for i in range(columns):
        col_name = chr(ord("x") + i)  # Generates column names like 'x', 'y', 'z', etc.
        data[col_name] = np.arange(start + i * 10, start + (i + 1) * 10, dtype=np.int64)

    index = np.arange(start, start + 10, dtype=np.int64)
    return pd.DataFrame(data, index=index)


def maybe_not_check_freq(f):
    """Ignore frequency when pandas is newer as starts to check frequency which it did not previously do."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        if PANDAS_VERSION >= CHECK_FREQ_VERSION and "check_freq" not in kwargs:
            kwargs["check_freq"] = False
        return f(*args, **kwargs)

    return wrapper


assert_frame_equal = maybe_not_check_freq(pd.testing.assert_frame_equal)
assert_series_equal = maybe_not_check_freq(pd.testing.assert_series_equal)


def random_string(length: int):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def get_sample_dataframe(size=1000, seed=0):
    np.random.seed(seed)
    df = pd.DataFrame(
        {
            "uint8": random_integers(size, np.uint8),
            "strings": [random_string(10) for _ in range(size)],
            "uint16": random_integers(size, np.uint16),
            "uint32": random_integers(size, np.uint32),
            "uint64": random_integers(size, np.uint64),
            "int8": random_integers(size, np.int8),
            "int16": random_integers(size, np.int16),
            "int32": random_integers(size, np.int32),
            "int64": random_integers(size, np.int64),
            "float32": np.random.randn(size).astype(np.float32),
            "float64": np.arange(size, dtype=np.float64),
            "bool": np.random.randn(size) > 0,
        }
    )
    return df


def get_sample_dataframe_only_strings(size=1000, seed=0, num_cols=1):
    np.random.seed(seed)
    df = pd.DataFrame({"strings" + str(idx): [random_string(10) for _ in range(size)] for idx in range(num_cols)})
    return df


def get_sample_dataframe_no_strings(size=1000, seed=0):
    np.random.seed(seed)
    df = pd.DataFrame(
        {
            "uint8": random_integers(size, np.uint8),
            "uint16": random_integers(size, np.uint16),
            "uint32": random_integers(size, np.uint32),
            "uint64": random_integers(size, np.uint64),
            "int8": random_integers(size, np.int8),
            "int16": random_integers(size, np.int16),
            "int32": random_integers(size, np.int32),
            "int64": random_integers(size, np.int64),
            "float32": np.random.randn(size).astype(np.float32),
            "float64": np.arange(size, dtype=np.float64),
            "bool": np.random.randn(size) > 0,
        }
    )
    return df


def get_lib_by_name(lib_name, env, conf_path=_DEFAULT_ENVS_PATH):
    local_conf = ArcticFileConfig(env, conf_path)
    lib = local_conf[lib_name]
    return lib


@contextmanager
def config_context(name, value):
    try:
        initial = get_config_int(name)
        set_config_int(name, value)
        yield
    finally:
        if initial is not None:
            set_config_int(name, initial)
        else:
            unset_config_int(name)


def param_dict(fields, cases=None, xfail=None):
    _cases = deepcopy(cases) if cases else dict()
    if _cases:
        ids, params = zip(*list(sorted(_cases.items())))
    else:
        ids, params = [], []

    if xfail is not None:
        xfail_ids, xfail_params = zip(*list(sorted(xfail.items())))
        xfail_marker = tuple(pytest.mark.xfail()(p) for p in xfail_params)
        params = params + xfail_marker
        ids = ids + xfail_ids

    return pytest.mark.parametrize(fields, params, ids=ids)


CustomThing = NamedTuple(
    "CustomThing",
    [
        ("custom_index", np.ndarray),
        ("custom_columns", List[AnyStr]),
        ("custom_values", List[np.ndarray]),
    ],
)


class TestCustomNormalizer(CustomNormalizer):
    def normalize(self, item, **kwargs):
        if isinstance(item, CustomThing):
            norm_meta = NormalizationMetadata.CustomNormalizerMeta()
            df = pd.DataFrame(index=item.custom_index, columns=item.custom_columns, data=item.custom_values)
            return df, norm_meta

    def denormalize(self, item, norm_meta):
        # type: (Any, CustomNormalizerMeta)->Any
        return CustomThing(custom_index=item.index, custom_columns=item.columns, custom_values=item.values)


def sample_dataframe(size=1000, seed=0):
    return get_sample_dataframe(size, seed)


def sample_dataframe_only_strings(size=1000, seed=0, num_cols=1):
    return get_sample_dataframe_only_strings(size, seed, num_cols)


def sample_dataframe_without_strings(size=1000, seed=0):
    return get_sample_dataframe_no_strings(size, seed)


def populate_db(version_store):
    df = sample_dataframe()
    version_store.write("symbol", df)
    version_store.write("pickled", {"a": 1}, pickle_on_failure=True)
    version_store.snapshot("mysnap")
    version_store.write("rec_norm", data={"a": np.arange(5), "b": np.arange(8), "c": None}, recursive_normalizers=True)


def random_integers(size, dtype):
    # We do not generate integers outside the int64 range
    platform_int_info = np.iinfo("int_")
    iinfo = np.iinfo(dtype)
    return np.random.randint(
        max(iinfo.min, platform_int_info.min), min(iinfo.max, platform_int_info.max), size=size
    ).astype(dtype)


def get_wide_dataframe(size=10000, seed=0):
    np.random.seed(seed)
    return pd.DataFrame(
        {
            "uint8": random_integers(size, np.uint8),
            "strings": [random_string(100) for _ in range(size)],
            "uint16": random_integers(size, np.uint16),
            "uint32": random_integers(size, np.uint32),
            "uint64": random_integers(size, np.uint64),
            "int8": random_integers(size, np.int8),
            "int16": random_integers(size, np.int16),
            "int32": random_integers(size, np.int32),
            "int64": random_integers(size, np.int64),
            "float32": np.random.randn(size).astype(np.float32),
            "float64": np.arange(size, dtype=np.float64),
            "bool": np.random.randn(size) > 0,
        }
    )


def get_pickle():
    return (
        list(random_integers(10000, np.uint32)),
        str(random_string(100)),
        {"a": list(random_integers(100000, np.int32))},
    )[np.random.randint(0, 2)]


def random_strings_of_length(num, length, unique):
    out = []
    for i in range(num):
        out.append(random_string(length))

    if unique:
        return list(set(out))
    else:
        return out


def random_strings_of_length_with_nan(num, length):
    out = []
    for i in range(num):
        if i % 3 == 1:
            out.append(np.nan)
        else:
            out.append("".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length)))

    return out


def random_floats(num):
    return np.random.uniform(low=0.5, high=20.0, size=(num,))


def random_dates(num):
    base_date = np.datetime64("2017-01-01")
    return np.array([base_date + random.randint(0, 100) for _ in range(num)])


def dataframe_for_date(dt, identifiers):
    length = len(identifiers)
    index = pd.MultiIndex.from_arrays([[dt] * length, identifiers])
    return pd.DataFrame(random_floats(length), index=index)


def get_symbols(lib):
    return sorted(lib.list_symbols(all_symbols=True))


def get_versions(lib):
    symbols = get_symbols(lib)
    versions_dict = dict()
    for sym in symbols:
        versions_list = lib.list_versions(sym)
        symbol_versions_dict = dict()
        for version in versions_list:
            symbol_versions_dict[version["version"]] = {k: version[k] for k in ["deleted", "snapshots"]}
        versions_dict[sym] = symbol_versions_dict
    return versions_dict


def get_snapshots(lib):
    return lib.list_snapshots()


def get_snapshot_versions(lib):
    snapshots = get_snapshots(lib)
    versions_dict = dict()
    for snapshot in snapshots.keys():
        snapshot_versions_list = lib.list_versions(snapshot=snapshot)
        snapshot_versions_dict = dict()
        for snapshot_version in snapshot_versions_list:
            snapshot_versions_dict[snapshot_version["symbol"]] = {
                k: snapshot_version[k] for k in ["version", "snapshots"]
            }
        versions_dict[snapshot] = snapshot_versions_dict
    return versions_dict


_WriteOpts = VersionStoreConfig.WriteOptions.DESCRIPTOR


def _calc_write_opt_one_ofs():
    for oneof in _WriteOpts.oneofs:
        for option in oneof.fields:
            if set(option.message_type.fields_by_name.keys()) == {"enabled"}:
                yield option.name


_WRITE_OPTION_ONE_OFS = tuple(_calc_write_opt_one_ofs())


def apply_lib_cfg(lib_cfg: LibraryDescriptor, cfg_dict: Mapping[str, Any]):
    """Used by library factory functions to apply configuration supplied as kwargs"""
    write_opts = lib_cfg.version.write_options
    for k, v in cfg_dict.items():
        if k in _WRITE_OPTION_ONE_OFS:
            setattr(getattr(write_opts, k), "enabled", v)
        elif k in _WriteOpts.fields_by_name:
            setattr(write_opts, k, v)
        elif k in VersionStoreConfig.DESCRIPTOR.fields_by_name:
            setattr(lib_cfg.version, k, v)
        elif k in VersionStoreConfig.MsgPack.DESCRIPTOR.fields_by_name:
            setattr(lib_cfg.version.msg_pack, k, v)
        elif k in LibraryDescriptor.DESCRIPTOR.fields_by_name:
            setattr(lib_cfg, k, v)
        else:
            raise NotImplementedError(k + " is not in the protobuf definitions")


def compare_version_data(source_lib, target_libs, versions):
    for symbol, symbol_versions in versions.items():
        for version, version_info in symbol_versions.items():
            source_vit = source_lib.read(symbol, as_of=version)
            for target_lib in target_libs:
                target_vit = target_lib.read(symbol, as_of=version)
                try:
                    compare_data(
                        source_vit.data,
                        source_vit.metadata,
                        target_vit.data,
                        target_vit.metadata,
                    )
                except AssertionError as e:
                    print("Version {} of symbol {} differs".format(version, symbol))
                    print("Source:\n{}".format(source_vit.data))
                    print("Target:\n{}".format(target_vit.data))
                    raise e


def compare_snapshot_data(source_lib, target_libs, snapshot_versions):
    for snapshot, symbol_versions in snapshot_versions.items():
        for symbol, version_info in symbol_versions.items():
            source_vit = source_lib.read(symbol, as_of=snapshot)
            for target_lib in target_libs:
                target_vit = target_lib.read(symbol, as_of=snapshot)
                try:
                    compare_data(
                        source_vit.data,
                        source_vit.metadata,
                        target_vit.data,
                        target_vit.metadata,
                    )
                except AssertionError as e:
                    print("Snapshot {} of symbol {} differs".format(snapshot, symbol))
                    print("Source:\n{}".format(source_vit.data))
                    print("Target:\n{}".format(target_vit.data))
                    raise e


def compare_data(source_data, source_metadata, target_data, target_metadata):
    if isinstance(source_data, pd.DataFrame):
        assert_frame_equal(source_data, target_data)
    else:
        # Recursive normalised symbol is a tuple of ndarrays
        assert len(source_data) == len(target_data)
        for idx in range(len(source_data)):
            assert np.allclose(source_data[idx], target_data[idx])
    assert source_metadata == target_metadata


def libraries_identical(source_lib, target_libs):
    # Assume if target_libs is not a list then it is a single library
    if not isinstance(target_libs, list):
        target_libs = [target_libs]
    with config_context("VersionMap.ReloadInterval", 0):
        symbols_source = get_symbols(source_lib)
        versions_source = get_versions(source_lib)
        snapshots_source = get_snapshots(source_lib)
        snapshot_versions_source = get_snapshot_versions(source_lib)

        for target_lib in target_libs:
            symbols_target = get_symbols(target_lib)
            if symbols_source != symbols_target:
                print("symbols_source != symbols_target")
                print("symbols_source: {}".format(symbols_source))
                print("symbols_target: {}".format(symbols_target))
                return False

            versions_target = get_versions(target_lib)
            if versions_source != versions_target:
                print("versions_source != versions_target")
                print("versions_source: {}".format(versions_source))
                print("versions_target: {}".format(versions_target))
                return False

            snapshots_target = get_snapshots(target_lib)
            if snapshots_source != snapshots_target:
                print("snapshots_source != snapshots_target")
                print("snapshots_source: {}".format(snapshots_source))
                print("snapshots_target: {}".format(snapshots_target))
                return False

            snapshot_versions_target = get_snapshot_versions(target_lib)
            if snapshot_versions_source != snapshot_versions_target:
                print("snapshot_versions_source != snapshots_target")
                print("snapshot_versions_source: {}".format(snapshot_versions_source))
                print("snapshot_versions_target: {}".format(snapshot_versions_target))
                return False

        compare_version_data(source_lib, target_libs, versions_source)
        compare_snapshot_data(source_lib, target_libs, snapshot_versions_source)
        return True


def make_dynamic(df, num_slices=10):
    cols = df.columns
    num_cols = len(cols)
    num_rows = len(df)
    rows_per_slice = int(num_rows / num_slices)
    rows_per_slice = 1 if rows_per_slice == 0 else rows_per_slice

    slices = []
    column_index = 0

    for step in range(0, num_rows, rows_per_slice):
        df_slice = df.iloc[step : step + rows_per_slice]
        col_to_drop_i = (column_index + 1) % num_cols
        if col_to_drop_i != 0:
            col_to_drop = cols[col_to_drop_i]
            df_slice = df_slice.drop(columns=[col_to_drop])
        column_index += 1
        slices.append(df_slice)

    expected = pd.concat(slices)
    return expected, slices


def regularize_dataframe(df):
    output = df.copy(deep=True)
    for col in output.select_dtypes(include=["object"]).columns:
        output[col] = output[col].fillna("")

    # TODO remove this when filtering code returns NaN
    output.fillna(0, inplace=True)
    output = output.reindex(sorted(output.columns), axis=1)
    output = output.reset_index(drop=True)
    output = output.astype("float", errors="ignore")
    return output


@attr.s(slots=True, auto_attribs=True)
class BeforeAfterTimestamp:
    before: pd.Timestamp
    after: Optional[pd.Timestamp]


@contextmanager
def distinct_timestamps(lib: NativeVersionStore):
    """Ensures the timestamp used by ArcticDB operations before, during and leaving the context are all different.

    Yields
    ------
    BeforeAfterTimestamp
    """
    get_ts = lib.version_store.get_store_current_timestamp_for_tests
    before = get_ts()
    while get_ts() == before:
        time.sleep(0.000001)  # 1us - The worst resolution in our clock implementations
    out = BeforeAfterTimestamp(pd.Timestamp(before, unit="ns"), None)
    try:
        yield out
    finally:
        right_after = get_ts()
        while get_ts() == right_after:
            time.sleep(0.000001)
        out.after = pd.Timestamp(get_ts(), unit="ns")


@contextmanager
def random_seed_context():
    seed = os.getenv("ARCTICDB_RAND_SEED")
    state = random.getstate()
    random.seed(int(seed) if seed is not None else state)
    try:
        yield
    finally:
        random.setstate(state)
