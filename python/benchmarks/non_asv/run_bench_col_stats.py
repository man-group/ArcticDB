import json
import shutil
import statistics
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from arcticdb import Arctic

LMDB_PATH = "/tmp/arcticdb_bench_col_stats"
SYMBOL_NAME = "test_symbol"
CREATE_STATS_WARMUP_RUNS = 2
CREATE_STATS_RUNS = 10
WORKER_SCRIPT = Path(__file__).parent / "bench_col_stats.py"

SCENARIOS = [
    (500, 500),
    (500, 100),
    (600, 600),
    (700, 700),
    (800, 500),
    (900, 800),
    (1_000, 100),
]

# SCENARIOS = [
#     (5_000, 5_000),
#     (5_000, 10_000),
#     (6_000, 6_000),
#     (7_000, 7_000),
#     (8_000, 5_000),
#     (9_000, 8_000),
#     (10_000, 10_000),
# ]


@dataclass
class ScenarioResult:
    rows: int = 0
    cols: int = 0
    symbol_write_time: float = 0.0
    stats_elapsed_time: list = field(default_factory=list)
    stats_memory_delta_mb: list = field(default_factory=list)


def print_results(results):
    column_width = 16
    header = (
        f"{'rows':>12}  {'cols':>6}"
        f"  {'write_time_s':>{column_width}}"
        f"  {'stats_time_mean':>{column_width}}  {'stats_time_median':>{column_width}}  {'stats_time_max':>{column_width}}"
        f"  {'stats_rss_mean_mb':>{column_width}}  {'stats_rss_median_mb':>{column_width}}  {'stats_rss_max_mb':>{column_width}}"
    )
    print()
    print(header)
    print("-" * len(header))

    for result in results:
        elapsed_times = result.stats_elapsed_time
        memory_values = result.stats_memory_delta_mb
        print(
            f"{result.rows:>12,}  {result.cols:>6,}"
            f"  {result.symbol_write_time:>{column_width}.2f}"
            f"  {statistics.mean(elapsed_times):>{column_width}.2f}  {statistics.median(elapsed_times):>{column_width}.2f}  {max(elapsed_times):>{column_width}.2f}"
            f"  {statistics.mean(memory_values):>{column_width}.1f}  {statistics.median(memory_values):>{column_width}.1f}  {max(memory_values):>{column_width}.1f}"
        )


def run_subprocess(operation, rows, cols):
    try:
        completed = subprocess.run(
            [sys.executable, str(WORKER_SCRIPT), "--scenario", f"{rows}x{cols}", "--operation", operation],
            capture_output=True, text=True, check=True,
        )
        return json.loads(completed.stdout)
    except subprocess.CalledProcessError as e:
        shutil.rmtree(LMDB_PATH, ignore_errors=True)
        killed_by_signal = e.returncode < 0
        reason = f"killed by signal {-e.returncode}" if killed_by_signal else f"exit code {e.returncode}"
        raise RuntimeError(f"[{operation}] subprocess failed ({reason}):\n{e.stderr}") from None


def measure(scenario, index, results):
    rows, cols = scenario
    results[index].rows = rows
    results[index].cols = cols

    print(f"  [write_symbol] {rows}x{cols}", file=sys.stderr)
    write_result = run_subprocess("write_symbol", rows, cols)
    results[index].symbol_write_time = write_result["elapsed_seconds"]

    for warmup_number in range(1, CREATE_STATS_WARMUP_RUNS + 1):
        print(f"  [create_stats] warmup {warmup_number}/{CREATE_STATS_WARMUP_RUNS}", file=sys.stderr)
        run_subprocess("create_stats", rows, cols)

    for run_number in range(1, CREATE_STATS_RUNS + 1):
        print(f"  [create_stats] run {run_number}/{CREATE_STATS_RUNS}", file=sys.stderr)
        stats_result = run_subprocess("create_stats", rows, cols)
        results[index].stats_elapsed_time.append(stats_result["elapsed_seconds"])
        results[index].stats_memory_delta_mb.append(stats_result["peak_rss_delta_mb"])

    ac = Arctic(f"lmdb://{LMDB_PATH}")
    ac.get_library("bench").delete(SYMBOL_NAME)


def cleanup():
    shutil.rmtree(LMDB_PATH, ignore_errors=True)


def run():
    cleanup()
    results = [ScenarioResult() for _ in SCENARIOS]
    for index, scenario in enumerate(SCENARIOS):
        print(f"\n=== scenario {scenario[0]}x{scenario[1]} ===", file=sys.stderr)
        measure(scenario, index, results)
    print_results(results)


if __name__ == "__main__":
    try:
        run()
    finally:
        cleanup()
