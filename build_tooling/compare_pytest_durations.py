"""Compare pytest durations from the current run against the latest master run.

Both runs are read from ArcticDB. Reports tests whose duration changed by more
than a configurable threshold (default 10%).
"""

import sys
from pathlib import Path

import click
import polars as pl

from process_pytest_artifacts import get_results_lib


def find_symbol(lib, branch=None, run_id=None):
    """Find a symbol by branch (latest) or exact run_id."""
    all_symbols = lib.list_symbols()

    if run_id:
        matches = [s for s in all_symbols if s.endswith(f"|{run_id}")]
        return matches[0] if matches else None

    if branch:
        matches = [s for s in all_symbols if s.startswith(f"{branch}|")]
        if not matches:
            return None
        # Symbol format: branch|commit|YYYY-MM-DD_HH-MM-SS|run_id
        matches.sort(key=lambda s: s.split("|")[2], reverse=True)
        return matches[0]

    return None


def load_df(lib, symbol):
    """Load run data from ArcticDB as a polars DataFrame."""
    pdf = lib.read(symbol).data
    return pl.from_pandas(pdf)


def build_full_comparison(current_df, master_df):
    """Join current and master on test identity and compute pct_change for every test."""
    join_keys = ["test_name", "python_version", "test_type", "cache_type"]

    cur = current_df.filter(pl.col("status") == "passed").select(join_keys + ["time"])
    mst = master_df.filter(pl.col("status") == "passed").select(join_keys + ["time"])

    return (
        cur.join(mst, on=join_keys, suffix="_master")
        .filter(pl.col("time_master") > 0)
        .with_columns(
            ((pl.col("time") - pl.col("time_master")) / pl.col("time_master") * 100).alias("pct_change")
        )
        .sort("pct_change", descending=True)
    )


def filter_diverged(full_comparison, threshold_pct):
    """Return only the tests whose duration diverged by more than threshold_pct."""
    return full_comparison.filter(pl.col("pct_change").abs() > threshold_pct)


def format_report(diverged, master_symbol, threshold_pct):
    """Format the comparison results as a Markdown string suitable for a PR comment."""
    lines = []
    lines.append("## Pytest Duration Report")
    lines.append("")
    lines.append(f"Threshold: **{threshold_pct}%** | Baseline: `{master_symbol}`")
    lines.append("")

    if diverged.is_empty():
        lines.append("No tests exceeded the threshold - durations are stable. :white_check_mark:")
        return "\n".join(lines)

    slower = diverged.filter(pl.col("pct_change") > 0)
    faster = diverged.filter(pl.col("pct_change") < 0)

    if not slower.is_empty():
        lines.append(f"### :warning: Slower ({len(slower)} tests)")
        lines.append("")
        lines.append("| Test | Config | Master | Current | Change |")
        lines.append("|------|--------|-------:|--------:|-------:|")
        for row in slower.iter_rows(named=True):
            config = f"{row['python_version']}/{row['test_type']}/{row['cache_type']}"
            lines.append(
                f"| `{row['test_name']}` | {config} "
                f"| {row['time_master']:.3f}s | {row['time']:.3f}s | {row['pct_change']:+.1f}% |"
            )
        lines.append("")

    if not faster.is_empty():
        lines.append(f"### :rocket: Faster ({len(faster)} tests)")
        lines.append("")
        lines.append("| Test | Config | Master | Current | Change |")
        lines.append("|------|--------|-------:|--------:|-------:|")
        for row in faster.iter_rows(named=True):
            config = f"{row['python_version']}/{row['test_type']}/{row['cache_type']}"
            lines.append(
                f"| `{row['test_name']}` | {config} "
                f"| {row['time_master']:.3f}s | {row['time']:.3f}s | {row['pct_change']:+.1f}% |"
            )

    lines.append("")
    lines.append(f"**Total: {len(slower)} slower, {len(faster)} faster**")
    return "\n".join(lines)


@click.command()
@click.option("--run-id", type=str, required=True, help="Run ID of the current build")
@click.option("--threshold", type=float, default=10.0, help="Percentage change threshold (default: 10)")
@click.option("--output", type=str, default=None, help="Write report to this file (for PR comments)")
def main(run_id, threshold, output):
    lib = get_results_lib("pytest_results")

    current_symbol = find_symbol(lib, run_id=run_id)
    if current_symbol is None:
        print(f"Error: No data found for run_id={run_id}", file=sys.stderr)
        sys.exit(1)

    master_symbol = find_symbol(lib, branch="master")
    if master_symbol is None:
        print("No master runs found in ArcticDB - skipping comparison.")
        sys.exit(0)

    # Don't compare master against itself.
    if current_symbol == master_symbol:
        print("Current run is the latest master run - skipping self-comparison.")
        sys.exit(0)

    print(f"Current run : {current_symbol}")
    print(f"Master baseline: {master_symbol}")
    print()

    current_df = load_df(lib, current_symbol)
    master_df = load_df(lib, master_symbol)

    full = build_full_comparison(current_df, master_df)

    print("=" * 80)
    print("Full duration comparison (all tests)")
    print("=" * 80)
    with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=80):
        print(full)
    print()

    diverged = filter_diverged(full, threshold)
    report = format_report(diverged, master_symbol, threshold)
    print(report)

    if output:
        Path(output).write_text(report)
        print(f"\nReport written to {output}")

    slower = diverged.filter(pl.col("pct_change") > 0)
    if not slower.is_empty():
        sys.exit(1)


if __name__ == "__main__":
    main()
