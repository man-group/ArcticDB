"""Regression test: a string column with only None values has no string pool on disk, and must
still read back as None through both the pandas and the arrow output paths. This guards the
null-string-pool corruption check added on the decode path from rejecting valid data.
"""

import pandas as pd
import pytest

from arcticdb.options import OutputFormat


@pytest.mark.parametrize("output_format", [OutputFormat.PANDAS, OutputFormat.PYARROW])
def test_all_none_string_column_reads(lmdb_version_store_v1, output_format):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": [None, None, None], "b": [1, 2, 3]})
    lib.write("sym", df)
    result = lib.read("sym", output_format=output_format).data
    if output_format == OutputFormat.PYARROW:
        result = result.to_pandas()
    assert result["a"].isna().all()
    assert list(result["b"]) == [1, 2, 3]


@pytest.mark.parametrize("output_format", [OutputFormat.PANDAS, OutputFormat.PYARROW])
def test_mixed_none_and_string_column_reads(lmdb_version_store_v1, output_format):
    lib = lmdb_version_store_v1
    df = pd.DataFrame({"a": ["x", None, "y"]})
    lib.write("sym", df)
    result = lib.read("sym", output_format=output_format).data
    if output_format == OutputFormat.PYARROW:
        result = result.to_pandas()
    assert result["a"].iloc[0] == "x"
    assert result["a"].iloc[1] is None or pd.isna(result["a"].iloc[1])
    assert result["a"].iloc[2] == "y"
