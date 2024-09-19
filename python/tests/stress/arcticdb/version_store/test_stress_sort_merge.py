import numpy as np
import pandas as pd
from typing import Union, List, Optional

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
            f"Invalid freq string {freq}: last character must be one of {list(freq_type_to_ns.keys())} (Pandas offset aliases)")
    mul = 1 if len(freq) == 1 else int(freq[:-1])
    return int(mul * freq_type_to_ns[freq_type])


def copy_set_nans(d, mask):
    d1 = np.copy(d)
    d1[mask] = np.nan
    return d1


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


def create_synthetic_tick_data(start: pd.Timestamp,
                               end: pd.Timestamp,
                               tick_freq: str,
                               time_col: str = 'timestamp',
                               price_col: str = 'last',
                               volume_col: str = 'volume',
                               initial_price: Optional[float] = 100.,
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


def generate_overlapping_dataframes(n_dataframes: int, min_rows: int = 2000, max_rows: int = 300000,
                                    overlap_pct: float = 0.01) -> List[pd.DataFrame]:
    dataframes = []
    start_time = pd.Timestamp('2023-01-01')

    for i in range(n_dataframes):
        n_rows = np.random.randint(min_rows, max_rows + 1)
        end_time = start_time + pd.Timedelta(seconds=n_rows)

        df = create_synthetic_tick_data(
            start=start_time,
            end=end_time,
            tick_freq='1S',
            time_col='timestamp',
            price_col='price',
            volume_col='volume',
            initial_price=100 + i * 10,  # Vary initial price for each dataframe
            seed=i  # Use different seed for each dataframe
        )

        # Add 8 more random columns
        for j in range(8):
            col_name = f'feature_{j + 1}'
            df[col_name] = np.random.randn(len(df))

        dataframes.append(df)

        # Calculate overlap
        overlap_rows = int(n_rows * overlap_pct)
        start_time = end_time - pd.Timedelta(seconds=overlap_rows)

    return dataframes


# Generate 5 overlapping dataframes
result = generate_overlapping_dataframes(5)
