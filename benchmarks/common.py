import pandas as pd
import numpy as np

def generate_pseudo_random_dataframe(n, freq="S", end_timestamp="1/1/2023"):
    """
    Generates a Data Frame with 2 columns (timestamp and value) and N rows
    - timestamp contains timestamps with a given frequency that end at end_timestamp
    - value contains random floats that sum up to approximately N, for easier testing/verifying
    """
    # Generate random values such that their sum is equal to N
    values = np.random.uniform(0, 2, size=n)
    # Generate timestamps
    timestamps = pd.date_range(end=end_timestamp, periods=n, freq="S")
    # Create dataframe
    df = pd.DataFrame({"timestamp": timestamps, "value": values[0]})
    return df

def get_prewritten_lib_name(rows):
    return f"prewritten_{rows}"
