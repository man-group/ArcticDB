"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import json
import os
import glob
import sys
from argparse import ArgumentParser


def main(commit_hash: str):
    assert commit_hash, "commit_hash must be provided but was blank"
    shortened_hash = commit_hash[:8]
    failures = []
    # Search for all result files in the asv results directory
    results_pattern = f"*/.asv/results/*/{shortened_hash}*-*.json"
    result_files = glob.glob(results_pattern, recursive=True)
    assert len(result_files) == 1, f"Expected one result file matching pattern {results_pattern} but found {result_files}"
    result_file = result_files[0]
    with open(result_file, 'r') as f:
        data = json.load(f)
        # Results are stored in a dictionary; failed ones are null
        for bench_name, result in data.get('results', {}).items():
            if result[0] is None:
                failures.append(bench_name)

    # Write to GitHub Step Summary
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
        summary.write("### 🚀 ASV Benchmark Report\n")
        if failures:
            summary.write("❌ **The following benchmarks failed:**\n\n")
            summary.write("| Benchmark Name |\n")
            summary.write("| :--- |\n")
            for f in sorted(set(failures)):
                summary.write(f"| `{f}` |\n")
            summary.write("\n> Check the 'Benchmark given commit' step logs for specific tracebacks.")
        else:
            summary.write("✅ All benchmarks passed successfully!\n")

    if failures:
        print("Check the workflow Summary page for a report on ASV failures.")
        sys.exit(1)
    else:
        print("There were no ASV failures to report.")
        sys.exit(0)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--commit-hash",
        help="Commit hash to summarize results for",
        required=True
    )
    args = parser.parse_args()
    main(args.commit_hash)
