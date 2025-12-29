"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import subprocess
from collections import defaultdict

import pandas as pd

# Importing inf and nan so they can be evaluated correctly during extraction
from numpy import inf, nan
from arcticdb.storage_fixtures.s3 import real_s3_from_environment_variables
import json
from pathlib import Path
from arcticdb import Arctic
from argparse import ArgumentParser


def setup_machine_folder(json_data, machine_path):
    machine_path.mkdir()
    with open(machine_path / "machine.json", "w") as out:
        machine_data = json_data["params"]
        machine_data["version"] = 1
        json.dump(machine_data, out, indent=4, default=str)


def df_to_asv_json(results_df: pd.DataFrame):
    """
        Convert the results dataframe to the format that asv expects

        Input:
            results_df: pd.DataFrame
            Example:
                                                test_name                                            results  ...                                          durations version
    0            basic_functions.BasicFunctions.time_read  [[0.8234815306001111, 1.685872498299932, 0.855...  ...  {'<build>': 515.5997927188873, '<setup_cache b...       2
    1      basic_functions.BasicFunctions.time_read_batch  [[0.2775141046000044, 0.597266279600126, 0.379...  ...  {'<build>': 515.5997927188873, '<setup_cache b...       2
    """
    new_df = results_df.copy()
    new_df["date"] = (pd.to_datetime(results_df["date"]).astype(int) // 1000000).astype(object)
    new_df["version"] = results_df["version"].astype(object)

    metadata = {
        "commit_hash": new_df["commit_hash"].iloc[0],
        "env_name": new_df["env_name"].iloc[0],
        "date": new_df["date"].iloc[0],
        "params": eval(new_df["params"].iloc[0]),
        "python": new_df["python"].iloc[0],
        "requirements": eval(new_df["requirements"].iloc[0]),
        "env_vars": eval(new_df["env_vars"].iloc[0]),
        "result_columns": eval(new_df["result_columns"].iloc[0]),
        "durations": eval(new_df["durations"].iloc[0]),
        "version": new_df["version"].iloc[0],
    }

    json_data = {**metadata, "results": {}}
    for _, row in new_df.iterrows():
        test_name = row["test_name"]
        res_data = eval(row["results"])

        json_data["results"][test_name] = res_data

    return json_data


def asv_json_to_df(full_path: str) -> pd.DataFrame:
    with open(full_path, "r") as f:
        data = json.load(f)

    results_list = []
    for test_name, test_results in data["results"].items():
        flattened_data = pd.json_normalize({"test_name": test_name, "results": str(test_results)})
        flattened_data["commit_hash"] = data["commit_hash"]
        flattened_data["env_name"] = data["env_name"]
        flattened_data["date"] = data["date"]
        flattened_data["params"] = str(data["params"])
        flattened_data["python"] = data["python"]
        flattened_data["requirements"] = str(data["requirements"])
        flattened_data["env_vars"] = str(data["env_vars"])
        flattened_data["result_columns"] = str(data["result_columns"])
        flattened_data["durations"] = str(data["durations"])
        flattened_data["version"] = data["version"]
        results_list.append(flattened_data)

    results = pd.concat(results_list, ignore_index=True)
    results["date"] = pd.to_datetime(data["date"], unit="ms")
    return results


def get_results_lib(arcticdb_client_override, arcticdb_library):
    if arcticdb_client_override:
        ac = Arctic(arcticdb_client_override)
    else:
        factory = real_s3_from_environment_variables(shared_path=True)
        factory.default_prefix = "asv_results"
        ac = factory.create_fixture().create_arctic()

    lib = ac.get_library(arcticdb_library, create_if_missing=True)
    return lib


def save_asv_results(lib, json_path):
    for file_path in json_path.glob("**/*.json"):
        if "benchmark" in file_path.name or "machine" in file_path.name:
            continue
        full_path = str(file_path)
        commit_hash = file_path.name.split("-")[0]
        print(f"Processing {full_path}")
        df = asv_json_to_df(full_path)
        lib.write(commit_hash, df)


def is_tag_commit(commit_id):
    is_tag = subprocess.run(
        ["git", "describe", "--contains", commit_id],
        check=False,
        capture_output=True
    )

    return is_tag.returncode == 0


def is_master_or_tag_commit(commit_id, branch="master"):
    is_master = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit_id, branch],
        check=False,
        capture_output=False
    )

    if is_master.returncode == 0:
        return True, "master"
    elif is_master.returncode == 1:
        return is_tag_commit(commit_id), "tag"
    else:
        return False, "error"


def extract_asv_results(lib, json_path):
    syms = lib.list_symbols()
    interesting_commits = []
    counts = defaultdict(int)
    for s in syms:
        is_interesting, commit_type = is_master_or_tag_commit(s)
        if is_interesting:
            interesting_commits.append(s)
        counts[commit_type] += 1

    print(f"Loaded {len(syms)} results of types {json.dumps(counts)}. Those on master and tags will be published, the rest skipped.")

    for sym in interesting_commits:
        print(f"Processing {sym}...")

        results_df = lib.read(sym).data
        json_data = df_to_asv_json(results_df)
        full_json_path = get_result_json_path(json_path, sym, json_data)

        print(f"Writing {full_json_path}...")
        with open(full_json_path, "w") as out:
            json.dump(json_data, out, indent=4, default=str)


def analyze_asv_results(lib, hash):
    """This function is designed to analyze the performance of our ASV benchmarks, so we can keep their runtime under control.

    TODO We could also track these results inside ASV itself.

    You can test this function against our real results database by running with,

    python transform_asv_results.py \
--mode=analyze --arcticdb_client_override="s3://s3.eu-west-1.amazonaws.com:arcticdb-ci-benchmark-results?aws_auth=true&path_prefix=asv_results" --hash=abaaa08b

    or using the "analyze ASV results" run configuration stored in .idea/.
    """
    assert lib.has_symbol(hash), f"Results for hash {hash} not found in {lib}"
    benchmark_results = lib.read(hash).data
    assert benchmark_results.shape > (0, 0)
    benchmark_results = benchmark_results[["test_name", "results", "result_columns", "durations"]]
    """
    Example of the stored data:
    
    test_name
        bi_benchmarks.BIBenchmarks.time_query_groupby_city_count_all

    results
        i-th entry corresponds to the i-th entry of result_columns
        [[0.02064474750000045, 0.22353983374998165], [['1', '10']], 'bf5e390b01e356685500d464be897fe7cb51531dcd92fccedec980f97f361e3c', 1765845676840, 5.7363, [0.018747, 0.21073], [0.069667, 0.29402], [0.020253, 0.21876], [0.025704, 0.23415], [2, 2], [10, 10]]

    result_columns
        ['result', 'params', 'version', 'started_at', 'duration', 'stats_ci_99_a', 'stats_ci_99_b', 'stats_q_25', 'stats_q_75', 'stats_number', 'stats_repeat', 'samples', 'profile']

    durations
        This is the same in each row.
        
        {'<setup_cache bi_benchmarks:68>': 11.555371, '<setup_cache basic_functions:49>': 8.773198, '<setup_cache basic_functions:182>': 34.551478, '<setup_cache basic_functions:341>': 29.068089, '<setup_cache comparison_benchmarks:44>': 133.676543, '<setup_cache finalize_staged_data:47>': 19.000038, '<setup_cache finalize_staged_data:100>': 0.980301, '<setup_cache list_snapshots:46>': 40.629586, '<setup_cache list_symbols:29>': 23.854835, '<setup_cache list_versions:44>': 418.163515, '<setup_cache local_query_builder:33>': 8.492626, '<setup_cache real_batch_functions:59>': 3.201688, '<setup_cache real_comparison_benchmarks:76>': 91.077791, '<setup_cache real_finalize_staged_data:42>': 3.188584, '<setup_cache real_list_operations:58>': 3.157586, '<setup_cache real_list_operations:138>': 3.393059, '<setup_cache real_modification_functions:215>': 50.894361, '<setup_cache real_modification_functions:261>': 3.848478, '<setup_cache real_modification_functions:73>': 192.683119, '<setup_cache real_query_builder:76>': 3.191312, '<setup_cache real_read_write:87>': 3.191894, '<setup_cache real_read_write:243>': 3.177817, '<setup_cache real_read_write:212>': 4.189118, '<setup_cache recursive_normalizer:47>': 6.11023, '<setup_cache resample:134>': 4.986128, '<setup_cache version_chain:42>': 585.851281}

    """
    cache_setup_str = benchmark_results.loc[0].durations
    cache_setup_dict = json.loads(cache_setup_str.replace("'", '"'))
    cache_setup_df = pd.DataFrame.from_dict(cache_setup_dict, orient="index", columns=["Duration (s)"])
    cache_setup_df = cache_setup_df.reset_index().rename(columns={'index': 'Step'})
    cache_setup_df = cache_setup_df.sort_values(by="Duration (s)", ascending=False)

    print("Time spent outside of benchmarks (excluding build):\n")
    with pd.option_context('display.max_rows', None, 'display.max_colwidth', None):
        print(cache_setup_df)
    print("\n")

    def extract_time(r):
        """r looks like the "results" mentioned in the docstring above. Using eval as the results can contain nan, inf etc which json.loads cannot parse"""
        as_list = eval(r)
        return as_list[4]

    benchmark_results["Duration (s)"] = benchmark_results.results.map(extract_time)
    benchmark_results = benchmark_results[["test_name", "Duration (s)"]]
    benchmark_results = benchmark_results.sort_values(by="Duration (s)", ascending=False)

    print("Time spent in benchmarks:\n")
    with pd.option_context('display.max_rows', None, 'display.max_colwidth', None):
        print(benchmark_results)

    print(f"\nSummary:")
    cache_setup_time = cache_setup_df["Duration (s)"].sum() / 60
    print(f"Total time outside benchmarks (mins): {cache_setup_time}")
    benchmarks_run_time = benchmark_results["Duration (s)"].sum() / 60
    print(f"Total time running benchmarks (mins): {benchmarks_run_time}")


def get_result_json_path(json_path, sym, json_data):
    env_name = json_data["env_name"]
    machine = json_data["params"]["machine"]
    machine_path = json_path / machine
    if not machine_path.exists():
        setup_machine_folder(json_data, machine_path)

    result_json_name = f"{sym}-{env_name}.json"
    full_json_path = json_path / machine / result_json_name
    return full_json_path


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--results_path",
        help="Path to the asv json files",
        default="python/.asv/results",
    )
    parser.add_argument(
        "--arcticdb_client_override",
        help="Override the path to the arcticdb client to use for writing the results, used for testing",
        default=None,
    )
    parser.add_argument(
        "--arcticdb_library",
        help="Override the name of the library to use for writing the results, used for testing/getting results for branches other than master",
        default="asv_results",
    )
    parser.add_argument(
        "--hash",
        help="Only used for the 'analyze' mode. The 8 character (git rev-parse --short=8 REF) hash of the commit to analyze. Eg --hash=abaaa08b will analyze the benchmarks for that commit.",
        default=None
    )
    parser.add_argument(
        "--mode",
        help="Mode to run the script in, 'save', 'extract' or 'analyze'. Analyze generates a report about the time taken to run benchmarks, to help us to keep the CI at a reasonable speed.",
    )

    args = parser.parse_args()
    json_path = Path(args.results_path)
    results_lib = get_results_lib(args.arcticdb_client_override, args.arcticdb_library)

    if args.mode == "save":
        save_asv_results(results_lib, json_path)
    elif args.mode == "extract":
        extract_asv_results(results_lib, json_path)
    elif args.mode == "analyze":
        assert args.hash, "--hash must be present for the analyze mode"
        analyze_asv_results(results_lib, args.hash)
    else:
        raise ValueError(f"Invalid mode {args.mode}, must be 'save', 'extract' or 'analyze'")
