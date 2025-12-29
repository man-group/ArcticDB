"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import json
import os
import glob
import sys

failures = []
# Search for all result files in the asv results directory
for result_file in glob.glob("*/.asv/results/*/*.json"):
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
