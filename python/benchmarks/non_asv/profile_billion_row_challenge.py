"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import time

import numpy as np
import pandas as pd

from arcticdb import QueryBuilder, Arctic

rows_per_segment = 100_000
rng = np.random.default_rng()

ac = Arctic("lmdb:///tmp/arcticdb")
lib = ac.get_library("billion_row_challenge_profiling", create_if_missing=True)
lib = lib._nvs
sym = f"billion_row_challenge"


def test_write_data():
    lib.delete(sym)
    num_rows = 1_000_000_000
    num_cities = 10_000
    cities = [f"City {idx :04}" for idx in range(num_cities)]

    num_segments = num_rows // rows_per_segment
    for idx in range(num_segments):
        print(f"{idx + 1}/{num_segments}")
        temperature_column = 100_000 * rng.random(rows_per_segment)
        cities_column = np.random.choice(cities, rows_per_segment)
        df = pd.DataFrame({"City": cities_column, "Temperature": temperature_column})
        lib.append(sym, df, write_if_missing=True)


def test_read_data():
    start = time.time()
    df = lib.read(sym).data
    end = time.time()
    print(f"Reading {len(df)} rows took {end - start}s")


def test_groupby():
    q = QueryBuilder()
    q = q.groupby("City").agg(
        {"Min": ("Temperature", "min"), "Max": ("Temperature", "max"), "Mean": ("Temperature", "mean")}
    )
    start = time.time()
    df = lib.read(sym, query_builder=q).data
    end = time.time()
    print(f"Grouping to {len(df)} rows took {end - start}s")


def test_project_then_filter():
    q = QueryBuilder()
    q = q.apply("new col", q["Temperature"] * 1.5)
    q = q[q["City"] == "City 9999"]
    start = time.time()
    df = lib.read(sym, query_builder=q).data
    end = time.time()
    print(f"Projecting then filtering to {len(df)} rows took {end - start}s")
