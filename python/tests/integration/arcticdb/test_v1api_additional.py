"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import os
import pytest
import uuid
import random
import string
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from arcticdb.util.utils import DFGenerator, generate_random_sparse_numpy_array, set_seed
from arcticdb.util.test import assert_frame_equal, assert_series_equal
from arcticdb.version_store._store import NativeVersionStore
from arcticdb_ext.exceptions import InternalException



def generate_test_metadata(
    with_extra_fields: bool = True,
    seed: int = None
) -> dict:
    """
    Create a sample metadata dictionary for use in store write tests.

    Parameters
    ----------
    with_extra_fields : bool, optional
        Whether to include non-required/custom fields for robustness testing.
    seed : int, optional
        Random seed for reproducibility.

    Returns
    -------
    dict
        Metadata dictionary with reproducible values if seed is set.
    """
    if seed is not None:
        random.seed(seed)

    metadata = {
        "id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "owner": random.choice(["alice", "bob", "charlie", "diana"]),
        "source": random.choice(["unit-test", "integration-test", "manual-run"]),
        "tags": random.sample(["finance", "etl", "prod", "archival"], k=2)
    }

    if with_extra_fields:
        metadata.update({
            "description": "Synthetic metadata for write/read test",
            "random_label": "".join(random.choices(string.ascii_lowercase, k=6)),
            "expires_at": (datetime.now(timezone.utc) +
                           timedelta(days=random.randint(1, 90))
                          ).isoformat()
        })

    return metadata


@pytest.mark.parametrize("pickle_on_failure", [True, False])
@pytest.mark.parametrize("validate_index", [True, False])
def test_write_sparse_data_all_types(basic_store, pickle_on_failure, validate_index):
    """
    Test writing and reading a variety of sparse data types to a NativeVersionStore.

    This test ensures that the store correctly handles multiple data formats,
    index validation settings, and version pruning behavior. It parametrizes over:

    Overall, this test provides broad coverage of the NativeVersionStore write/read
    pipeline for heterogeneous, sparse, and non-tabular payloads under different
    configuration flags.
    """

    nvs: NativeVersionStore = basic_store
    max_length = 100
    sym = "__qwerty124"

    # Data definitions to be written to symbol include different types
    # of 
    arr_float = generate_random_sparse_numpy_array(max_length, np.float64)
    arr_str = generate_random_sparse_numpy_array(max_length, str)
    arr_datetime = generate_random_sparse_numpy_array(max_length, np.datetime64)
    arr_empty = np.empty((3))
    arr_multi = np.random.rand(2, 3, 4, 5, 6)
    ser_float = pd.Series(arr_float, name="float")
    ser_str = pd.Series(arr_str, name="str")
    ser_date = pd.Series(arr_datetime, name="date")
    df = (DFGenerator(size=max_length, density=0.19)
        .add_float_col("float")
        .add_int_col("int")
        .add_bool_col("bool")
        .add_timestamp_col("ts")
        .add_string_col("str", str_size=18)
        .add_string_col("str2", num_unique_values=44, str_size=6)
        .generate_dataframe())

    nvs.write(sym, arr_str, pickle_on_failure=pickle_on_failure, validate_index=validate_index)
    np.testing.assert_array_equal(arr_str, nvs.read(sym).data)

    meta = generate_test_metadata()
    nvs.write(sym, arr_datetime, pickle_on_failure=pickle_on_failure, validate_index=validate_index,
              metadata=meta)
    np.testing.assert_array_equal(arr_datetime, nvs.read(sym).data)
    assert 2 == len(nvs.list_versions()) # Assert it grows
    assert meta == nvs.read(sym).metadata # Metadata on arrays

    nvs.write(sym, ser_float, pickle_on_failure=pickle_on_failure, validate_index=validate_index)
    assert_series_equal(ser_float, nvs.read(sym).data)

    nvs.write(sym, None, pickle_on_failure=pickle_on_failure, validate_index=validate_index)
    assert nvs.read(sym).data is None

    nvs.write(sym, ser_str, pickle_on_failure=pickle_on_failure, validate_index=validate_index,
              prune_previous_version=False)
    assert_series_equal(ser_str, nvs.read(sym).data)
    assert 2 < len(nvs.list_versions()) # Assert prune_previous_version false has no effect

    nvs.write(sym, arr_float, pickle_on_failure=pickle_on_failure, validate_index=validate_index)
    np.testing.assert_array_equal(arr_float, nvs.read(sym).data)

    meta = generate_test_metadata(True)
    nvs.write(sym, ser_date, pickle_on_failure=pickle_on_failure, validate_index=validate_index, 
              prune_previous_version=True, metadata=meta)
    assert_series_equal(ser_date, nvs.read(sym).data)
    assert meta == nvs.read(sym).metadata # Metadata on Series
    assert 1 == len(nvs.list_versions())

    nvs.write(sym, os.urandom(128), pickle_on_failure=pickle_on_failure, validate_index=validate_index)
    with pytest.raises(InternalException):
        nvs.append(sym, os.urandom(128))

    nvs.write(sym, arr_empty , pickle_on_failure=pickle_on_failure, validate_index=validate_index)
    np.testing.assert_array_equal(arr_empty , nvs.read(sym).data)

    nvs.write(sym, arr_multi, pickle_on_failure=pickle_on_failure, validate_index=validate_index,
              prune_previous_version=True)
    np.testing.assert_array_equal(arr_multi, nvs.read(sym).data)
    assert 1 == len(nvs.list_versions()) # Assert prune_previous_version have desired effect

    nvs.write(sym, df, pickle_on_failure=pickle_on_failure, validate_index=validate_index)
    assert_frame_equal(df, nvs.read(sym).data)


