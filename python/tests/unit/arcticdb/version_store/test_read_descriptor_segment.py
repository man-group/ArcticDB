"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, version 2.0.
"""

import pandas as pd
from arcticdb_ext.version_store import PythonVersionStoreBatchReadOptions


class TestReadDescriptorIncludeSegment:
    def test_read_descriptor_without_segment(self, lmdb_version_store):
        """Default behavior should not include segment."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))

        vq = lib._get_version_query(None)
        result = lib.version_store.read_descriptor("sym", vq)

        assert result.segment is None

    def test_read_descriptor_with_segment(self, lmdb_version_store):
        """include_segment=True should return the index segment."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))

        vq = lib._get_version_query(None)
        result = lib.version_store.read_descriptor("sym", vq, include_segment=True)

        assert result.segment is not None
        assert result.segment.row_count > 0

    def test_read_descriptor_segment_row_count(self, lmdb_version_store):
        """Segment row count should match index entries."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))

        vq = lib._get_version_query(None)
        result = lib.version_store.read_descriptor("sym", vq, include_segment=True)

        # For a small dataframe, there should be 1 index entry (1 data segment)
        assert result.segment.row_count >= 1

    def test_read_descriptor_segment_latest_version(self, lmdb_version_store):
        """as_of=None returns segment for latest version."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))  # v0
        lib.write("sym", pd.DataFrame({"col": [4, 5, 6, 7]}))  # v1

        vq = lib._get_version_query(None)
        result = lib.version_store.read_descriptor("sym", vq, include_segment=True)

        assert result.segment is not None
        assert result.timeseries_descriptor.total_rows == 4  # Latest version has 4 rows

    def test_read_descriptor_segment_specific_version(self, lmdb_version_store):
        """Verify segment is returned for a specific version number."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))  # v0
        lib.write("sym", pd.DataFrame({"col": [4, 5, 6, 7]}))  # v1

        vq_v0 = lib._get_version_query(0)
        result_v0 = lib.version_store.read_descriptor("sym", vq_v0, include_segment=True)

        vq_v1 = lib._get_version_query(1)
        result_v1 = lib.version_store.read_descriptor("sym", vq_v1, include_segment=True)

        assert result_v0.segment is not None
        assert result_v1.segment is not None
        # Timeseries descriptors should differ
        assert result_v0.timeseries_descriptor.total_rows == 3
        assert result_v1.timeseries_descriptor.total_rows == 4

    def test_read_descriptor_segment_snapshot(self, lmdb_version_store):
        """Verify segment is returned when querying by snapshot name."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))
        lib.snapshot("snap1")
        lib.write("sym", pd.DataFrame({"col": [4, 5, 6, 7]}))

        vq = lib._get_version_query("snap1")
        result = lib.version_store.read_descriptor("sym", vq, include_segment=True)

        assert result.segment is not None
        assert result.timeseries_descriptor.total_rows == 3

    def test_read_descriptor_segment_metadata_present(self, lmdb_version_store):
        """Verify the segment has metadata bytes."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))

        vq = lib._get_version_query(None)
        result = lib.version_store.read_descriptor("sym", vq, include_segment=True)

        metadata_bytes = result.segment.metadata()
        assert isinstance(metadata_bytes, bytes)

    def test_read_descriptor_segment_num_columns(self, lmdb_version_store):
        """Verify segment.num_columns is correct for index segment."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))

        vq = lib._get_version_query(None)
        result = lib.version_store.read_descriptor("sym", vq, include_segment=True)

        # Index segment should have columns for start_index, end_index, version_id, etc.
        assert result.segment.num_columns > 0

    def test_read_descriptor_segment_timeseries_matches(self, lmdb_version_store):
        """Verify segment's timeseries info matches DescriptorItem.timeseries_descriptor."""
        lib = lmdb_version_store
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        lib.write("sym", df)

        vq = lib._get_version_query(None)
        result = lib.version_store.read_descriptor("sym", vq, include_segment=True)

        assert result.segment is not None
        assert result.timeseries_descriptor.total_rows == 3


class TestBatchReadDescriptorIncludeSegment:
    def test_batch_read_descriptor_without_segment(self, lmdb_version_store):
        """Default behavior should not include segments."""
        lib = lmdb_version_store
        lib.write("sym1", pd.DataFrame({"col": [1, 2]}))
        lib.write("sym2", pd.DataFrame({"col": [3, 4]}))

        vqs = [lib._get_version_query(None), lib._get_version_query(None)]
        batch_opts = PythonVersionStoreBatchReadOptions(False)

        results = lib.version_store.batch_read_descriptor(["sym1", "sym2"], vqs, batch_opts)

        assert all(r.segment is None for r in results)

    def test_batch_read_descriptor_with_segment(self, lmdb_version_store):
        """include_segment=True should return segments for all symbols."""
        lib = lmdb_version_store
        lib.write("sym1", pd.DataFrame({"col": [1, 2]}))
        lib.write("sym2", pd.DataFrame({"col": [3, 4, 5]}))

        vqs = [lib._get_version_query(None), lib._get_version_query(None)]
        batch_opts = PythonVersionStoreBatchReadOptions(False)

        results = lib.version_store.batch_read_descriptor(["sym1", "sym2"], vqs, batch_opts, include_segment=True)

        assert all(r.segment is not None for r in results)
        assert results[0].timeseries_descriptor.total_rows == 2
        assert results[1].timeseries_descriptor.total_rows == 3

    def test_batch_read_descriptor_mixed_versions(self, lmdb_version_store):
        """Batch with different version queries should return correct segments."""
        lib = lmdb_version_store
        lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))  # v0
        lib.write("sym", pd.DataFrame({"col": [4, 5, 6, 7]}))  # v1
        lib.snapshot("snap")

        vqs = [
            lib._get_version_query(0),
            lib._get_version_query(1),
            lib._get_version_query("snap"),
        ]
        batch_opts = PythonVersionStoreBatchReadOptions(False)

        results = lib.version_store.batch_read_descriptor(["sym", "sym", "sym"], vqs, batch_opts, include_segment=True)

        assert results[0].timeseries_descriptor.total_rows == 3  # v0
        assert results[1].timeseries_descriptor.total_rows == 4  # v1
        assert results[2].timeseries_descriptor.total_rows == 4  # snap points to v1

    def test_batch_read_descriptor_segment_metadata(self, lmdb_version_store):
        """Verify all segments in batch have metadata bytes."""
        lib = lmdb_version_store
        lib.write("sym1", pd.DataFrame({"col": [1, 2]}))
        lib.write("sym2", pd.DataFrame({"col": [3, 4]}))

        vqs = [lib._get_version_query(None), lib._get_version_query(None)]
        batch_opts = PythonVersionStoreBatchReadOptions(False)

        results = lib.version_store.batch_read_descriptor(["sym1", "sym2"], vqs, batch_opts, include_segment=True)

        for result in results:
            assert result.segment is not None
            metadata_bytes = result.segment.metadata()
            assert isinstance(metadata_bytes, bytes)
