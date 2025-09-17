import pandas as pd
from pandas.testing import assert_frame_equal
from arcticdb.file import _to_file, _from_file
from arcticdb.util.test import get_sample_dataframe


def test_roundtrip_dataframe(tmp_path):
    df_original = pd.DataFrame(
        {"A": [1, 2, 3], "B": ["x", "y", "z"]}, index=pd.date_range(start="2020-01-01", end="2020-01-03")
    )

    file_path = str(tmp_path) + "testfile.dat"
    _to_file("test_symbol", df_original, str(file_path))
    vi_roundtrip = _from_file("test_symbol", str(file_path))
    df_roundtrip = vi_roundtrip.data

    assert_frame_equal(df_roundtrip, df_original, check_like=True)


def test_roundtrip_mixed_dataframe(tmp_path):
    df_original = get_sample_dataframe(1000)

    file_path = str(tmp_path) + "testfile.dat"
    _to_file("test_symbol", df_original, str(file_path))
    vi_roundtrip = _from_file("test_symbol", str(file_path))
    df_roundtrip = vi_roundtrip.data

    assert_frame_equal(df_roundtrip, df_original, check_like=True)


def test_roundtrip_large_dataframe(tmp_path):
    df_original = get_sample_dataframe(1000000)

    file_path = str(tmp_path) + "testfile.dat"
    _to_file("test_symbol", df_original, str(file_path))
    vi_roundtrip = _from_file("test_symbol", str(file_path))
    df_roundtrip = vi_roundtrip.data

    assert_frame_equal(df_roundtrip, df_original, check_like=True)


def test_roundtrip_metadata(tmp_path):
    df_original = get_sample_dataframe(1000)

    file_path = str(tmp_path) + "testfile.dat"
    _to_file("test_symbol", df_original, str(file_path), metadata={"hello": "world"})
    vi_roundtrip = _from_file("test_symbol", str(file_path))
    df_roundtrip = vi_roundtrip.data

    assert_frame_equal(df_roundtrip, df_original, check_like=True)
    assert vi_roundtrip.metadata == {"hello": "world"}
