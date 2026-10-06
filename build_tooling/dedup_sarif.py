"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import argparse
import json


def _result_key(result):
    physical = result["locations"][0]["physicalLocation"]
    region = physical.get("region", {})
    return (
        physical["artifactLocation"]["uri"],
        region.get("startLine"),
        region.get("startColumn"),
        result.get("level"),
        result.get("ruleId"),
        result["message"]["text"],
    )


def dedup_results(results):
    unique = {}
    for result in results:
        unique.setdefault(_result_key(result), result)
    return list(unique.values())


def main():
    parser = argparse.ArgumentParser(
        description="Drop clang-tidy SARIF results repeated by every translation unit that includes the same header. "
        "The file is rewritten in place."
    )
    parser.add_argument("sarif_file")
    args = parser.parse_args()

    with open(args.sarif_file) as f:
        sarif = json.load(f)

    for run in sarif["runs"]:
        results = dedup_results(run["results"])
        print(f"{len(run['results'])} results, {len(results)} after deduplication")
        run["results"] = results

    with open(args.sarif_file, "w") as f:
        json.dump(sarif, f)


if __name__ == "__main__":
    main()
