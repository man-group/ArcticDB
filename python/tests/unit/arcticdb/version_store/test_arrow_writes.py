"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import numpy as np
import pyarrow as pa
import pytest

from arcticdb.version_store._normalization import ArrowTableNormalizer


def test_record_batch_roundtrip():
    table = pa.table({"col": pa.array([0, 1], pa.int64())})
    normalizer = ArrowTableNormalizer()
    arcticdb_record_batches, _ = normalizer.normalize(table)
    pa_record_batches = []
    for record_batch in arcticdb_record_batches:
        pa_record_batches.append(pa.RecordBatch._import_from_c(record_batch.array(), record_batch.schema()))
    returned_table = pa.Table.from_batches(pa_record_batches)
    assert table.equals(returned_table)


def test_multiple_record_batches_roundtrip():
    t0 = pa.table({"col": pa.array([0, 1], pa.int64())})
    t1 = pa.table({"col": pa.array([2, 3, 4], pa.int64())})
    table = pa.concat_tables([t0, t1])
    normalizer = ArrowTableNormalizer()
    arcticdb_record_batches, _ = normalizer.normalize(table)
    pa_record_batches = []
    for record_batch in arcticdb_record_batches:
        pa_record_batches.append(pa.RecordBatch._import_from_c(record_batch.array(), record_batch.schema()))
    returned_table = pa.Table.from_batches(pa_record_batches)
    assert table.equals(returned_table)


def test_basic_write(lmdb_version_store_arrow):
    lib = lmdb_version_store_arrow
    sym = "test_basic_write"
    table = pa.table({"col": pa.array([0, 1], pa.int64())})
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)


@pytest.mark.parametrize("num_rows", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("num_cols", [1, 2, 3, 4, 5])
def test_write_sliced(lmdb_version_store_tiny_segment, num_rows, num_cols):
    lib = lmdb_version_store_tiny_segment
    lib.set_output_format("experimental_arrow")
    sym = "test_write_sliced"
    table = pa.table(
        {
            f"col{idx}": pa.array(np.arange(idx * num_rows, (idx + 1) * num_rows), pa.uint32()) for idx in range(num_cols)
        }
    )
    lib.write(sym, table)
    received = lib.read(sym).data
    assert table.equals(received)

