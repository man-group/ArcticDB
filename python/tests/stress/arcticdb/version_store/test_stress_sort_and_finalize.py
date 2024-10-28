import numpy as np
import pandas as pd
from typing import Union, List, Optional
import pytest
from arcticdb.version_store.library import StagedDataFinalizeMethod
from itertools import tee

_DEFAULT_SEED = 102323
time_column = "timestamp"

freq_type_to_ns = {
    "N": 1,  # nanoseconds
    "U": 1e3,  # microseconds
    "L": 1e6,  # milliseconds
    "S": 1e9,  # seconds
    "T": 60e9,  # minutes
    "H": 36e11,  # hours
    "D": 864e11,  # days
    "W": 6048e11,  # weeks
}


def parse_freq_str(freq: str) -> float:
    # this currently assumes single letter frequency types
    if freq is None or len(freq) < 1:
        raise ValueError(f"Invalid freq string {freq}: must be at least 1 character")
    freq_type = freq[-1].upper()
    if not freq_type in freq_type_to_ns:
        raise ValueError(
            f"Invalid freq string {freq}: last character must be one of {list(freq_type_to_ns.keys())} (Pandas offset aliases)"
        )
    mul = 1 if len(freq) == 1 else int(freq[:-1])
    return int(mul * freq_type_to_ns[freq_type])


def unique_random_integers(top_of_range: int, size: int):
    rng = np.random.default_rng()
    numbers = rng.choice(top_of_range, size=size, replace=False)
    return numbers


def copy_set_random_nans(to_copy: Union[np.array, List[float]], fraction_nans: float):
    result = np.copy(to_copy)
    num_nans = int(fraction_nans * len(result))
    if num_nans == 0:
        return result
    random_index = unique_random_integers(len(result), size=num_nans)
    result[random_index] = np.nan
    return result


def create_synthetic_tick_data(
    start: pd.Timestamp,
    end: pd.Timestamp,
    tick_freq: str,
    time_col: str = "timestamp",
    price_col: str = "last",
    volume_col: str = "volume",
    initial_price: Optional[float] = 100.0,
    volume_daily_max: int = 100_000_000,
    annual_vol=0.1,
    seed: int = 57,
    decimals: int = -1,
    fraction_nans: float = 0.0,
) -> pd.DataFrame:
    index = pd.date_range(start=start, end=end, freq=tick_freq, name=time_col)
    np.random.seed(seed)
    rtn_scale = annual_vol * np.sqrt(parse_freq_str(tick_freq) / (252 * parse_freq_str("1D")))
    rtn = rtn_scale * (np.random.randn(len(index)) + 0.0001)
    cum_rtn = np.cumprod(1 + rtn)
    data = np.empty(len(index))
    price_data = initial_price * cum_rtn
    nans_data = copy_set_random_nans(price_data, fraction_nans)
    volume_scale = parse_freq_str(tick_freq) / parse_freq_str("1D")
    volume_data = np.random.randint(low=1, high=int(volume_daily_max * volume_scale), size=len(index))
    volume_data[np.isnan(price_data)] = 0
    df = pd.DataFrame(index=index, data={price_col: price_data, volume_col: volume_data})
    # df = pd.DataFrame(index=index, data={price_col: price_data})
    return df if decimals < 0 else df.round(decimals)


def generate_overlapping_dataframes(
    n_dataframes: int,
    min_rows: int = 2000,
    max_rows: int = 300000,
    overlap_pct: float = 0.01,
    start_time: pd.Timestamp = pd.Timestamp("2023-01-01"),
) -> List[pd.DataFrame]:
    dataframes = []

    for i in range(n_dataframes):
        n_rows = np.random.randint(min_rows, max_rows + 1)
        end_time = start_time + pd.Timedelta(seconds=n_rows)

        df = create_synthetic_tick_data(
            start=start_time,
            end=end_time,
            tick_freq="1S",
            time_col="timestamp",
            price_col="price",
            volume_col="volume",
            initial_price=100 + i * 10,  # Vary initial price for each dataframe
            seed=i,  # Use different seed for each dataframe
        )

        # Add 8 more random columns
        for j in range(8):
            col_name = f"feature_{j + 1}"
            df[col_name] = np.random.randn(len(df))

        dataframes.append(df)

        # Calculate overlap
        overlap_rows = int(n_rows * overlap_pct)
        start_time = end_time - pd.Timedelta(seconds=overlap_rows)

    return dataframes


def assert_sorted_frames_with_repeated_index_equal(left, right):
    assert set(left.columns) == set(
        right.columns
    ), f"Column sets are different {set(left.columns)} != {set(right.columns)}"
    assert left.shape == right.shape, f"Shapes are different {left.shape} != {right.shape}"

    used = np.full(len(left), False, dtype="bool")
    bucket_start = 0
    current_index = left.index[0]
    row_count = len(left)
    right_bucket_start = right.itertuples()
    right_iterator = right.itertuples()
    for row_index, row in enumerate(left.itertuples()):
        if row[0] != current_index:
            bucket_start = row_index
            current_index = row[0]
            right_bucket_start, right_iterator = tee(right_iterator)
        right_row = next(right_iterator)
        if row == right_row:
            used[row_index] = True
        else:
            right_bucket_start, tmp = tee(right_bucket_start)
            found_match = False
            for i, right_row in enumerate(tmp, bucket_start):
                if right_row[0] != current_index:
                    break
                if not used[i] and row == right_row:
                    found_match = True
                    used[i] = True
                    break
                i += 1
            assert (
                found_match
            ), f"DataFrames are different could row {row_index} = {row} from left cannot be found in right"
    assert all(used)


def test_sort_and_finalize_write_stress(lmdb_library):
    lib = lmdb_library
    dataframes = generate_overlapping_dataframes(5)
    for dataframe in dataframes:
        lib.write("sym", dataframe, staged=True)
    lib.sort_and_finalize_staged_data("sym", StagedDataFinalizeMethod.WRITE)
    sorted_input = pd.concat(dataframes).sort_index()
    assert_sorted_frames_with_repeated_index_equal(lib.read("sym").data, sorted_input)


def test_sort_and_finalize_append_stress(lmdb_library):
    lib = lmdb_library
    start_time = pd.Timestamp("2023-01-01")
    n_rows = np.random.randint(2000, 300000)
    end_time = start_time + pd.Timedelta(seconds=n_rows)

    df = create_synthetic_tick_data(
        start=start_time,
        end=end_time,
        tick_freq="1S",
        time_col="timestamp",
        price_col="price",
        volume_col="volume",
        initial_price=100,  # Vary initial price for each dataframe
        seed=42,  # Use different seed for each dataframe
    )

    # Add 8 more random columns
    for j in range(8):
        col_name = f"feature_{j + 1}"
        df[col_name] = np.random.randn(len(df))

    lib.write("sym", df)

    dataframes = generate_overlapping_dataframes(5, start_time=df.index[-1])
    for dataframe in dataframes:
        lib.write("sym", dataframe, staged=True)
    lib.sort_and_finalize_staged_data("sym", StagedDataFinalizeMethod.APPEND)
    sorted_input = pd.concat([df] + dataframes).sort_index()
    assert_sorted_frames_with_repeated_index_equal(lib.read("sym").data, sorted_input)
