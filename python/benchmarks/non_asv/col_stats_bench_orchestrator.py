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

BASE_ROWS = 100_000
BASE_COLS = 127
MULTIPLIER = 3

@dataclass
class Result:
    rows: int = 0
    cols: int = 0
    symbol_write_time: float = 0.0
    stats_create_times: list = field(default_factory=list)
    stats_rss_use: list = field(default_factory=list)


results = []


def run_subprocess(script, args, label):
    try:
        completed = subprocess.run(
            [sys.executable, str(script), *map(str, args)],
            stdout=subprocess.PIPE, stderr=sys.stderr, text=True, check=True,
        )
        return json.loads(completed.stdout)
    except subprocess.CalledProcessError as e:
        raise MemoryError(f"[{label}] exited with code {e.returncode}") from None


def measure(rows, cols):
    print(f"  [write_symbol] {rows}x{cols}", file=sys.stderr)
    write_time = run_subprocess(
        WRITE_SYMBOL_SCRIPT, [rows, cols], "write_symbol"
    )["elapsed_seconds"]

    result = Result(rows=rows, cols=cols, symbol_write_time=write_time)
    results.append(result)

    try:
        for i in range(1, WARMUP_RUNS + 1):
            print(f"  [create_stats] warmup {i}/{WARMUP_RUNS}", file=sys.stderr)
            run_subprocess(CREATE_STATS_SCRIPT, [cols], "create_stats")

        for i in range(1, RUNS + 1):
            print(f"  [create_stats] run {i}/{RUNS}", file=sys.stderr)
            r = run_subprocess(CREATE_STATS_SCRIPT, [cols], "create_stats")
            result.stats_create_times.append(r["elapsed_seconds"])
            result.stats_rss_use.append(r["peak_rss_mb"])
    except MemoryError:
        print(f"  OOM during stats collection after {len(result.stats_create_times)} run(s), stopping phase", file=sys.stderr)
        raise
    finally:
        cleanup()


def run_phase(scenario_gen):
    for rows, cols in scenario_gen:
        print(f"\n=== scenario {rows:,}x{cols:,} ===", file=sys.stderr)
        try:
            measure(rows, cols)
        except MemoryError as e:
            print(f"  OOM: {e}, stopping phase", file=sys.stderr)
            break


def row_scenarios():
    rows = BASE_ROWS
    while True:
        yield rows, BASE_COLS
        rows *= MULTIPLIER


def col_scenarios():
    cols = BASE_COLS * 10
    while True:
        yield BASE_ROWS, cols
        cols *= MULTIPLIER


def print_results():
    cw = 20
    header = (
        f"{'rows':>12}  {'cols':>8}"
        f"  {'write_s':>{cw}}"
        f"  {'col_stats_mean_s':>{cw}}  {'col_stats_max_s':>{cw}}"
        f"  {'col_stats_mean_rss_mb':>{cw}}  {'col_stats_max_rss_mb':>{cw}}"
    )
    print()
    print(header)
    print("-" * len(header))

    for r in results:
        if not r.stats_create_times:
            continue
        t = r.stats_create_times
        m = r.stats_rss_use
        print(
            f"{r.rows:>12,}  {r.cols:>8,}"
            f"  {r.symbol_write_time:>{cw}.2f}"
            f"  {statistics.mean(t):>{cw}.2f}  {max(t):>{cw}.2f}"
            f"  {statistics.mean(m):>{cw}.1f}  {max(m):>{cw}.1f}"
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
        run_phase(row_scenarios())
        run_phase(col_scenarios())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
    except Exception as e:
        print(f"Benchmark aborted unexpectedly: {e}", file=sys.stderr)
    finally:
        cleanup()
    print_results()
