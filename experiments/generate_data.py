import arcticdb as adb
from arcticdb import Arctic
from arcticdb.storage_fixtures.s3 import real_s3_from_environment_variables
import pandas as pd
import numpy as np
import os
import io
import time
from contextlib import contextmanager, redirect_stdout

n = os.getenv("ARCTICDB_1TRC_N", 1000)
lib_name = os.getenv("ARCTICDB_GPETROV_LIB_NAME", "gpetrov_exp_2")
std = 10.0
lookup_df = pd.read_csv("lookup.csv")


@contextmanager
def calc_exec_time(func):
    print(f"Running {func.__name__}")
    start = time.time()
    yield
    end = time.time()
    print(f"{func.__name__} took {end - start:.2f} seconds")


def generate_df_data():
    rng = np.random.default_rng(0)
    df = pd.DataFrame(
        {
            # Choose a random station from the lookup table for each row in our output
            "station": rng.integers(0, len(lookup_df) - 1, int(n)),
            # Generate a normal distibution around zero for each row in our output
            # Because the std is the same for every station we can adjust the mean for each row afterwards
            "measure": rng.normal(0, std, int(n)),
        }
    )

    # Offset each measurement by the station's mean value
    df.measure += df.station.map(lookup_df.mean_temp)
    # Round the temprature to one decimal place
    df.measure = df.measure.round(decimals=1)
    # Convert the station index to the station name
    df.station = df.station.map(lookup_df.station)

    return df


print("Generating data")
df = generate_df_data()
print("Data generated")
print(df)
mem_usage = df.memory_usage().sum() / 1024
print(f"The data has {len(df)} rows and a total of {mem_usage:.2f} KB")

print("Connecting to ArcticDB")
ac = (
    real_s3_from_environment_variables(shared_path=True)
    .create_fixture()
    .create_arctic()
)
lib = ac.get_library(lib_name, create_if_missing=True)

sym_name = f"{n}_rows"
output = io.StringIO()
with calc_exec_time(lib.write):
    with redirect_stdout(output):
        lib.write(sym_name, df)

with calc_exec_time(lib.read):
    data = lib.read(sym_name).data

print("Filter data")
q = adb.QueryBuilder()
q = (
    q.apply("min_col", q["measure"] * 1)
    .apply("max_col", q["measure"] * 1)
    .apply("mean_col", q["measure"] * 1)
    .groupby("station")
    .agg({"min_col": "min", "max_col": "max", "mean_col": "mean"})
)

with calc_exec_time(lib.read):
    data = lib.read(sym_name, query_builder=q).data
