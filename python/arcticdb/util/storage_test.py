from arcticdb import Arctic
import pandas as pd
import os
import numpy as np
import argparse


def normalize_lib_name(lib_name):
    lib_name = lib_name.replace(".", "_")
    lib_name = lib_name.replace("-", "_")

    return lib_name


def real_s3_credentials(shared_path: bool = True):
    endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    region = os.getenv("ARCTICDB_REAL_S3_REGION")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    if shared_path:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX")

    clear = True if str(os.getenv("ARCTICDB_REAL_S3_CLEAR")).lower() in ["true", "1"] else False

    return endpoint, bucket, region, access_key, secret_key, path_prefix, clear


def get_real_s3_uri(shared_path: bool = True):
    endpoint, bucket, region, access_key, secret_key, path_prefix, _ = real_s3_credentials(shared_path)
    uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix={path_prefix}"
    return uri


def get_seed_libraries(ac=Arctic(get_real_s3_uri())):
    return [lib for lib in ac.list_libraries() if lib.startswith("seed_")]


def get_test_libraries(ac=Arctic(get_real_s3_uri())):
    return [lib for lib in ac.list_libraries() if lib.startswith("test_")]


def test_df_3_cols(start=0):
    return pd.DataFrame(
        {
            "x": np.arange(start, start + 10, dtype=np.int64),
            "y": np.arange(start + 10, start + 20, dtype=np.int64),
            "z": np.arange(start + 20, start + 30, dtype=np.int64),
        },
        index=np.arange(start, start + 10, dtype=np.int64),
    )


def verify_library(ac):
    libraries = get_test_libraries(ac)
    for lib_name in libraries:
        lib = ac[lib_name]

        symbols = lib.list_symbols()
        assert len(symbols) == 3
        for sym in ["one", "two", "three"]:
            assert sym in symbols
        for sym in symbols:
            df = lib.read(sym).data
            column_names = df.columns.values.tolist()
            assert column_names == ["x", "y", "z"]


import re


def is_strategy_branch_valid_format(input_string):
    pattern = r"^(linux|windows)_cp3(6|7|8|9|10|11).*$"
    match = re.match(pattern, input_string)
    return bool(match)


def seed_library(ac, version: str = ""):
    strategy_branch = os.getenv("ARCTICDB_PERSISTENT_STORAGE_STRATEGY_BRANCH")

    if not is_strategy_branch_valid_format(strategy_branch):
        raise ValueError(f"The strategy_branch: {strategy_branch} is not formatted correctly")

    lib_name = f"seed_{version}{strategy_branch}"
    lib_name = normalize_lib_name(lib_name)

    # Each branch should create its own seed and it should be fresh on each run
    # so delete the library, if it exists / wasn't cleaned on a previous run
    ac.delete_library(lib_name)
    ac.create_library(lib_name)

    library = ac[lib_name]

    one_df = test_df_3_cols()
    library.write("one", one_df)

    two_df = test_df_3_cols(1)
    library.write("two", two_df)

    two_df = test_df_3_cols(2)
    library.append("two", two_df)

    three_df = test_df_3_cols(3)
    library.append("three", three_df)


def cleanup_libraries(ac):
    for lib in get_seed_libraries(ac):
        ac.delete_library(lib)

    for lib in get_test_libraries(ac):
        ac.delete_library(lib)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="storage_test",
        description="wrapper for the functionalities that are needed for persistent storage testing",
    )

    parser.add_argument("-t", "--type", required=True)
    parser.add_argument(
        "-v",
        "--version",
        required=False,
        help=(
            "The version of ArcticDB that is used for seed, this is to make the name of the library that is seeded"
            " unique"
        ),
    )
    args = parser.parse_args()
    job_type = str(args.type).lower()
    # TODO: Add support for other storages
    uri = get_real_s3_uri()

    ac = Arctic(uri)

    if "seed" == job_type:
        seed_library(ac, args.version)
    elif "verify" == job_type:
        verify_library(ac)
    elif "cleanup" == job_type:
        cleanup_libraries(ac)
    else:
        raise ValueError(f"The argument {job_type} is an unsupported job type")
