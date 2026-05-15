import json
import statistics
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from ahl.mongo import NativeMongoose


WARMUP_RUNS = 2
RUNS = 10
WRITE_SYMBOL_SCRIPT = Path(__file__).parent / "col_stats_bench_write_symbol.py"
CREATE_STATS_SCRIPT = Path(__file__).parent / "col_stats_bench_create_stats.py"

SCENARIOS = [
      (400, 400),
      (400, 1_000),
      (500, 500),
      (600, 800),
      (800, 600),
      (800, 1_000),
      (1_000, 1_000),
  ]

# SCENARIOS = [
#     (10, 10),
#     (1_000, 1_000),
#     (100_000, 1_000),
#     (100_000, 10_000),
#     (1_000_000, 1_000),
#     (1_000_000, 5_000),
#     (10_000_000, 1_000),
# ]

@dataclass
class Result:
    rows: int = 0
    cols: int = 0
    symbol_write_time: float = 0.0
    stats_create_times: list = field(default_factory=list)
    stats_rss_use: list = field(default_factory=list)


results = [Result() for _ in SCENARIOS]


def run_subprocess(script, args, label):
    try:
        completed = subprocess.run(
            [sys.executable, str(script), *map(str, args)],
            stdout=subprocess.PIPE, stderr=sys.stderr, text=True, check=True,
        )
        return json.loads(completed.stdout)
    except subprocess.CalledProcessError as e:
        killed_by_signal = e.returncode < 0
        reason = f"killed by signal {-e.returncode}" if killed_by_signal else f"exit code {e.returncode}"
        raise RuntimeError(f"[{label}] subprocess failed ({reason})") from None


def measure(scenario, index):
    rows, cols = scenario
    results[index].rows = rows
    results[index].cols = cols

    print(f"  [write_symbol] {rows}x{cols}", file=sys.stderr)
    results[index].symbol_write_time = run_subprocess(
        WRITE_SYMBOL_SCRIPT, [rows, cols], "write_symbol"
    )["elapsed_seconds"]

    for i in range(1, WARMUP_RUNS + 1):
        print(f"  [create_stats] warmup {i}/{WARMUP_RUNS}", file=sys.stderr)
        run_subprocess(CREATE_STATS_SCRIPT, [cols], "create_stats")

    for i in range(1, RUNS + 1):
        print(f"  [create_stats] run {i}/{RUNS}", file=sys.stderr)
        r = run_subprocess(CREATE_STATS_SCRIPT, [cols], "create_stats")

        results[index].stats_create_times.append(r["elapsed_seconds"])
        results[index].stats_rss_use.append(r["peak_rss_mb"])

    cleanup()


def print_results():
    cw = 14
    header = (
        f"{'rows':>12}  {'cols':>8}"
        f"  {'write_s':>{cw}}"
        f"  {'time_mean':>{cw}}  {'time_median':>{cw}}  {'time_max':>{cw}}"
        f"  {'rss_mean_mb':>{cw}}  {'rss_median_mb':>{cw}}  {'rss_max_mb':>{cw}}"
    )
    print()
    print(header)
    print("-" * len(header))

    for r in results:
        t = r.stats_create_times
        m = r.stats_rss_use
        print(
            f"{r.rows:>12,}  {r.cols:>8,}"
            f"  {r.symbol_write_time:>{cw}.2f}"
            f"  {statistics.mean(t):>{cw}.2f}  {statistics.median(t):>{cw}.2f}  {max(t):>{cw}.2f}"
            f"  {statistics.mean(m):>{cw}.1f}  {statistics.median(m):>{cw}.1f}  {max(m):>{cw}.1f}"
        )


def cleanup():
    lib = NativeMongoose("mktdatad").get_library("pmarkovski.columns_stats", api="v2")
    try:
        lib.delete("test_symbol")
    except Exception:
        pass


if __name__ == "__main__":
    cleanup()
    try:
        for i, scenario in enumerate(SCENARIOS):
            print(f"\n=== scenario {scenario[0]}x{scenario[1]} ===", file=sys.stderr)
            measure(scenario, i)
    finally:
        cleanup()
    print_results()
