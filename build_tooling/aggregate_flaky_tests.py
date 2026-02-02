"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import re
import sys
from datetime import datetime
import pandas as pd
from arcticdb import Arctic
from arcticdb.storage_fixtures.s3 import real_s3_from_environment_variables

from arcticdb.version_store.processing import QueryBuilder

from process_pytest_artifacts import get_results_lib

VERSION_TAG_PATTERN = re.compile(r"^v\d+\.\d+\.\d+(\+man\d+)?\|")
def is_master_or_release(symbol: str) -> bool:
    """Check if symbol is from master branch or a release version tag (vX.Y.Z or vX.Y.Z+manN)."""
    return symbol.startswith("master|") or VERSION_TAG_PATTERN.match(symbol) is not None


def parse_date_from_symbol(symbol: str) -> datetime | None:
    """
    Parse date from symbol format.
    Symbol format: "2853/merge|31f651a6ff800653f911354fb41f8b7812503405|2026-01-14_12-27-10|20991831321"
    """
    parts = symbol.split("|")
    if len(parts) < 3:
        return None
    date_str = parts[2]  
    try:
        return datetime.strptime(date_str, "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return None


def aggregate_failures(
    lib,
    symbols: list,
    since_date: datetime | None = None,
    include_message: str | None = None,
    exclude_message: str | None = None,
) -> tuple[pd.DataFrame, int]:
    """
    Aggregate all test data from the given symbols, optionally filtering by date and message regex.
    Returns (combined_data, total_runs).
    """

    if since_date:
        symbols = [sym for sym in symbols
                   if not (sym_date := parse_date_from_symbol(sym)) or sym_date >= since_date]

    q = QueryBuilder()
    q = q[(q["status"] == "failed") | (q["status"] == "error")]
    read_results = lib.read_batch(symbols, query_builder=q)
    for result in read_results:
        result.data["symbol"] = result.symbol
        result.data["failure_date"] = parse_date_from_symbol(result.symbol)
        result.data["is_master"] = is_master_or_release(result.symbol)
    combined = pd.concat([result.data for result in read_results], ignore_index=True)

    # Apply message regex filters
    if include_message:
        pattern = re.compile(include_message, re.IGNORECASE)
        combined = combined[combined["message"].fillna("").str.contains(pattern, regex=True)]
    if exclude_message:
        pattern = re.compile(exclude_message, re.IGNORECASE)
        combined = combined[~combined["message"].fillna("").str.contains(pattern, regex=True)]

    return combined, len(symbols)


def truncate_string(s: str, limit: int) -> str:
    """Truncate string to limit characters, adding ellipsis if needed."""
    if len(s) > limit:
        return s[:limit] + " ..."
    return s


def aggregate_failure_counts(combined_data: pd.DataFrame, truncate: bool = True) -> pd.DataFrame:
    """Aggregate failure data by test name and return a DataFrame."""
    if combined_data.empty:
        return pd.DataFrame()

    MESSAGE_LENGTH_LIMIT = 100

    def master_failure_count(group):
        return group[group["is_master"]].shape[0]

    def last_master_failure(group):
        master_rows = group[group["is_master"]]
        if master_rows.empty:
            return None
        return master_rows["failure_date"].max()

    failure_counts = combined_data.groupby("test_name").apply(
        lambda g: pd.Series({
            "master_failure_count": master_failure_count(g),
            "last_master_failure": last_master_failure(g),
            "failure_count": len(g),
            "last_failure": g["failure_date"].max(),
            "python_versions": ", ".join(sorted(str(v) for v in g["python_version"].unique() if pd.notna(v))),
            "test_type": g["test_type"].iloc[0] if not g["test_type"].empty else "",
            "error_messages": "; ".join(sorted(set(str(v).strip() for v in g["message"].unique() if pd.notna(v) and str(v).strip()))),
        })
    ).reset_index()

    failure_counts = failure_counts[failure_counts["master_failure_count"] > 0]
    failure_counts = failure_counts.sort_values(
        ["master_failure_count", "failure_count"], ascending=[False, False]
    )

    if truncate:
        failure_counts["test_name"] = failure_counts["test_name"].apply(
            lambda x: truncate_string(x, MESSAGE_LENGTH_LIMIT)
        )
        failure_counts["error_messages"] = failure_counts["error_messages"].apply(
            lambda x: truncate_string(x, MESSAGE_LENGTH_LIMIT)
        )

    return failure_counts


def generate_summary(combined_data: pd.DataFrame, total_runs: int, since_date: datetime | None = None) -> str:
    if combined_data.empty:
        return "# Flaky Tests Summary\n\nNo test data found for the specified date range."

    failure_counts = aggregate_failure_counts(combined_data)

    date_range = f"since {since_date.date()}" if since_date else "All data (since 2025-08-20)"

    summary = f"""# Flaky Tests Summary ({date_range})

**Total runs analyzed:** {total_runs}
\n*Showing {len(failure_counts)} tests with failures on master or release builds.*
## Top Failing Tests

| Test Name | Master/Release Failures | Last Master/Release Failure | All Failures | Last Failure | Python Versions | Test Type | Error Messages |
|-----------|-------------------------|-----------------------------|--------------|--------------|-----------------|-----------|----------------|
"""

    for _, row in failure_counts.head(1000).iterrows():
        test_name = row["test_name"].replace("\n", " ").replace("|", "\\|")
        error_messages = row["error_messages"].replace("\n", " ").replace("|", "\\|")
        python_versions = row["python_versions"].replace("\n", " ").replace("|", "\\|")
        test_type = str(row["test_type"]).replace("|", "\\|") if pd.notna(row["test_type"]) else ""
        last_failure = row["last_failure"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["last_failure"]) else "N/A"
        master_failures = int(row["master_failure_count"])
        last_master_failure = row["last_master_failure"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["last_master_failure"]) else "N/A"

        summary += f"| `{test_name}` | {master_failures} | {last_master_failure} | {int(row['failure_count'])} | {last_failure} | {python_versions} | {test_type} | {error_messages} |\n"

    if len(failure_counts) > 1000:
        summary += f"\n*Showing top 1000 of {len(failure_counts)} failing tests*\n"

    return summary


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Aggregate flaky test data from ArcticDB")
    parser.add_argument(
        "--since",
        type=str,
        required=False,
        default=None,
        help="Start date (inclusive) in YYYY-MM-DD format. If not provided, all data is included.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path (defaults to stdout, use $GITHUB_STEP_SUMMARY for CI)",
    )
    parser.add_argument(
        "--include-message",
        type=str,
        default=None,
        help="Regex pattern to include only failures with matching messages (case-insensitive)",
    )
    parser.add_argument(
        "--exclude-message",
        type=str,
        default=None,
        help="Regex pattern to exclude failures with matching messages (case-insensitive)",
    )
    format_group = parser.add_mutually_exclusive_group(required=True)
    format_group.add_argument(
        "--csv",
        action="store_true",
        help="Output as CSV",
    )
    format_group.add_argument(
        "--md",
        action="store_true",
        help="Output as markdown",
    )

    args = parser.parse_args()

    since_date = None
    if args.since:
        try:
            since_date = datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            print(f"Error: Invalid date format '{args.since}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)
        print(f"Fetching test data since {since_date.date()}...", file=sys.stderr)
    else:
        print("Fetching all test data...", file=sys.stderr)

    lib = get_results_lib("pytest_results")

    all_symbols = lib.list_symbols()
    print(f"Total symbols in library: {len(all_symbols)}", file=sys.stderr)

    combined_data, total_runs = aggregate_failures(
        lib, all_symbols, since_date, args.include_message, args.exclude_message
    )
    print(f"Runs processed: {total_runs}", file=sys.stderr)

    if total_runs == 0:
        date_info = f" since {since_date.date()}" if since_date else ""
        if args.csv:
            output = ""
        else:
            output = f"# Flaky Tests Summary\n\nNo test runs found{date_info}."
    elif args.csv:
        failure_counts = aggregate_failure_counts(combined_data)
        output = failure_counts.to_csv(index=False)
    else:
        output = generate_summary(combined_data, total_runs, since_date)

    # Output
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Summary written to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
