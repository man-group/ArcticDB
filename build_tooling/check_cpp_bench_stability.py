"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this
software will be governed by the Apache License, version 2.0.

Run each Google microbenchmark N times and report which ones have enough
run-to-run variance to risk a false-positive `asv compare -f 1.15` regression
on PRs.

Why this exists
---------------
On PRs the CI does ``asv compare -s -f 1.15 <master> HEAD``: any benchmark
whose HEAD time is more than 1.15x the master time is reported as a
regression and fails the build. For ``track_time_ms`` benchmarks (which is
what cpp_microbenchmarks.py registers) ASV records a single value per run,
so the comparison is a pure ratio — there is no statistical-significance
filter to absorb noise. A benchmark whose natural variance is greater than
~15% will therefore flap.

What it does
------------
For each benchmark name discovered from the binary, invoke the binary once
per simulated run (the same way cpp_microbenchmarks.py does in CI) and
collect the real_time. Then report:

  - max/min ratio  — worst-case pair of runs; if > threshold, two ASV runs
                     (e.g. master vs PR HEAD) could trip the regression check.
  - CV %           — coefficient of variation (stddev / mean), useful for
                     spotting steady jitter even when max/min looks OK.

Usage
-----
    python build_tooling/check_cpp_bench_stability.py --runs 10
    python build_tooling/check_cpp_bench_stability.py --filter '^BM_arrow_'
    python build_tooling/check_cpp_bench_stability.py --runs 20 --output stability.json
"""

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = REPO_ROOT / "cpp/out/linux-release-build/arcticdb/benchmarks"


def list_benchmark_names(binary: Path, filter_regex: str | None) -> list[str]:
    cmd = [str(binary), "--benchmark_list_tests=true"]
    if filter_regex:
        cmd.append(f"--benchmark_filter={filter_regex}")
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def escape_for_benchmark_filter(name: str) -> str:
    # Match cpp_microbenchmarks.py: do not escape '-' (std::regex rejects '\-').
    return re.sub(r"([\\^$.|?*+()\[\]{}])", r"\\\1", name)


def to_milliseconds(real_time: float, unit: str) -> float:
    if unit == "ns":
        return real_time / 1e6
    if unit == "us":
        return real_time / 1e3
    if unit == "ms":
        return real_time
    if unit == "s":
        return real_time * 1e3
    raise RuntimeError(f"Unexpected time_unit {unit!r}")


def run_one(binary: Path, full_name: str) -> float:
    """Same shell-out shape as cpp_microbenchmarks.run_one — one process per run.

    Mirrors the bridge: prefer the ``median`` aggregate row when the C++ side sets
    ``->Repetitions(N)->ReportAggregatesOnly(true)``; otherwise fall back to the
    single iteration value. Scoring matches what CI will see.
    """
    result = subprocess.run(
        [
            str(binary),
            f"--benchmark_filter=^{escape_for_benchmark_filter(full_name)}$",
            "--benchmark_format=json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)
    benchmarks = data.get("benchmarks", [])
    median_row = next(
        (b for b in benchmarks if b.get("run_type") == "aggregate" and b.get("aggregate_name") == "median"),
        None,
    )
    if median_row is not None:
        return to_milliseconds(median_row["real_time"], median_row.get("time_unit", "ms"))
    iterations = [b for b in benchmarks if b.get("run_type", "iteration") == "iteration"]
    if not iterations:
        raise RuntimeError(f"No iteration or aggregate result returned for {full_name!r}")
    bm = iterations[0]
    return to_milliseconds(bm["real_time"], bm.get("time_unit", "ms"))


def summarize(times: list[float]) -> dict:
    tmin = min(times)
    tmax = max(times)
    mean = statistics.mean(times)
    stddev = statistics.pstdev(times) if len(times) > 1 else 0.0
    return {
        "runs_ms": times,
        "min_ms": tmin,
        "max_ms": tmax,
        "mean_ms": mean,
        "median_ms": statistics.median(times),
        "stddev_ms": stddev,
        "max_over_min": tmax / tmin if tmin > 0 else float("inf"),
        "cv_pct": (stddev / mean * 100.0) if mean > 0 else 0.0,
    }


def print_table(rows: list[tuple[str, dict]]) -> None:
    if not rows:
        print("  (none)")
        return
    print(f"  {'max/min':>8}  {'CV %':>6}  {'min (ms)':>12}  {'max (ms)':>12}  {'median (ms)':>12}  name")
    for name, r in rows:
        print(
            f"  {r['max_over_min']:>8.3f}"
            f"  {r['cv_pct']:>6.2f}"
            f"  {r['min_ms']:>12.4f}"
            f"  {r['max_ms']:>12.4f}"
            f"  {r['median_ms']:>12.4f}"
            f"  {name}"
        )

def validate_args(args):
    if not args.binary.exists():
        raise ValueError(
            f"Benchmark binary not found at {args.binary}.\n"
            f"Build it with:\n"
            f"  cmake --preset linux-release -DTEST=ON cpp\n"
            f"  cmake --build cpp/out/linux-release-build --target benchmarks"
        )

    if args.runs < 2:
        raise ValueError("--runs must be >= 2 to compute variance")

    names = list_benchmark_names(args.binary, args.filter)
    if not names:
        raise ValueError("No benchmarks discovered (check --filter).")


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--binary", type=Path, default=DEFAULT_BINARY,
        help=f"Path to the benchmarks binary (default: {DEFAULT_BINARY})",
    )
    parser.add_argument(
        "--runs", type=int, default=10,
        help="Number of times to invoke each benchmark (default: 10). Each invocation is a separate process, "
             "matching how cpp_microbenchmarks.py drives the binary in CI.",
    )
    parser.add_argument(
        "--threshold", type=float, default=1.15,
        help="Ratio above which a benchmark is flagged. Default 1.15 matches the CI `asv compare -f 1.15`.",
    )
    parser.add_argument(
        "--filter", type=str, default=None,
        help="Google Benchmark filter regex. Use to scope the run, e.g. '^BM_arrow_'.",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Optional JSON file to dump full per-benchmark results.",
    )
    parser.add_argument(
        "--fail-on-flagged", action="store_true",
        help="Exit non-zero if any benchmark exceeds the threshold (useful in CI).",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    validate_args(args)
    names = list_benchmark_names(args.binary, args.filter)
    total_invocations = len(names) * args.runs
    print(
        f"Running {len(names)} benchmark(s) x {args.runs} run(s) = {total_invocations} invocation(s). "
        f"Threshold = {args.threshold}x.",
        flush=True,
    )

    results: dict[str, dict] = {}
    failed: list[str] = []
    started = time.monotonic()

    for i, name in enumerate(names, 1):
        times: list[float] = []
        for r in range(args.runs):
            try:
                times.append(run_one(args.binary, name))
            except Exception as e:
                print(f"  [{i}/{len(names)}] {name}  run {r + 1} FAILED: {e}", flush=True)
        if times:
            stats = summarize(times)
            results[name] = stats
            flag = " [FLAG]" if stats["max_over_min"] > args.threshold else ""
            print(
                f"  [{i}/{len(names)}] {name}  "
                f"min={stats['min_ms']:.4f}ms max={stats['max_ms']:.4f}ms "
                f"max/min={stats['max_over_min']:.3f} cv={stats['cv_pct']:.2f}%{flag}",
                flush=True,
            )
        else:
            failed.append(name)

    elapsed = time.monotonic() - started
    print(f"\nDone in {elapsed:.1f}s.")

    flagged = sorted(
        ((n, r) for n, r in results.items() if r["max_over_min"] > args.threshold),
        key=lambda kv: kv[1]["max_over_min"],
        reverse=True,
    )
    safe = [(n, r) for n, r in results.items() if r["max_over_min"] <= args.threshold]

    print()
    print("=" * 100)
    print(f"At risk (max/min > {args.threshold}x) — {len(flagged)} of {len(results)} benchmark(s):")
    print_table(flagged)
    print()
    print(f"Stable — {len(safe)} benchmark(s).")
    if failed:
        print()
        print(f"Failed to run — {len(failed)} benchmark(s):")
        for n in failed:
            print(f"  {n}")

    if args.output:
        args.output.write_text(json.dumps(
            {"threshold": args.threshold, "runs": args.runs, "results": results, "failed": failed},
            indent=2,
        ))
        print(f"\nFull results written to {args.output}")

    if args.fail_on_flagged and flagged:
        return 1
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
