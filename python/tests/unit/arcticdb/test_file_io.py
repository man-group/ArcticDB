import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

# Import the file I/O wrappers.
# Adjust the import if necessary to match your module’s structure.
from arcticdb.file import to_file, from_file


def test_roundtrip_dataframe(tmp_path):
    # Create a simple DataFrame.
    df_original = pd.DataFrame({
        "A": [1, 2, 3],
        "B": ["x", "y", "z"]
    }, index=pd.date_range("2020-01-01", periods=3))

    tmp_path = "/tmp/"

    # Construct a file path in the temporary directory.
    file_path = tmp_path + "testfile.dat"

    # Write the DataFrame to the file using our to_file wrapper.
    vi = to_file("test_symbol", df_original, str(file_path))
    # Note: vi is a dummy VersionedItem since the C++ write returns void.

    # Read the data back from the file.
    vi_roundtrip = from_file("test_symbol", str(file_path))
    df_roundtrip = vi_roundtrip.data  # Denormalized DataFrame

    # Assert that the round-tripped DataFrame equals the original.
    assert_frame_equal(df_roundtrip, df_original)



