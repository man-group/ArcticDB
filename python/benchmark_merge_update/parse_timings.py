import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict


class Run:
    def __init__(self, idx):
        self.idx = idx
        self.wall = None
        self.fn_times = defaultdict(list)
        self.column_times = defaultdict(list)
        self.pairs = []


def parse(lines):
    runs = {}
    current = None

    def get_run(idx):
        if idx not in runs:
            runs[idx] = Run(idx)
        return runs[idx]

    for line in lines:
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if not isinstance(record, dict):
            continue
        tag = record.get("tag")
        if tag == "merge_run":
            run = get_run(record["run"])
            if record["event"] == "start":
                current = run
            else:
                run.wall = record["wall_ns"]
                current = None
        elif tag == "merge_timer":
            run = current if current is not None else get_run(0)
            fn, dur_ns = record["fn"], record["dur_ns"]
            run.fn_times[fn].append(dur_ns)
            if fn == "merge":
                rows = ",".join(f"{start}-{end}" for start, end in record["rows"])
                run.column_times[record["column"]].append(dur_ns)
                run.pairs.append((rows, record["column"], dur_ns))
    return [runs[k] for k in sorted(runs)]


def to_s(ns):
    return ns / 1e9


def describe(values):
    return (
        f"count={len(values):>7}  sum={to_s(sum(values)):>12.6f}  mean={to_s(statistics.mean(values)):>10.6f}  "
        f"max={to_s(max(values)):>10.6f}"
    )


def across(values):
    return (
        f"mean={to_s(statistics.mean(values)):>12.6f}  median={to_s(statistics.median(values)):>12.6f}  "
        f"min={to_s(min(values)):>12.6f}  max={to_s(max(values)):>12.6f}"
    )


def print_run(run, top):
    print(f"=== Run {run.idx} ===")
    print(f"wall: {to_s(run.wall):.6f}s" if run.wall is not None else "wall: n/a")
    print("Per function, seconds (sums add up time across threads and can exceed wall time):")
    for fn in sorted(run.fn_times):
        print(f"  {fn:<20} {describe(run.fn_times[fn])}")
    if "WriteToSegmentTask" not in run.fn_times:
        print(f"  {'WriteToSegmentTask':<20} absent (only timed for Arrow sources)")
    print("Merge time by column:")
    for column, values in sorted(run.column_times.items(), key=lambda kv: -sum(kv[1])):
        print(f"  {column:<20} {describe(values)}")
    pairs = sorted(run.pairs, key=lambda p: -p[2])
    if top is not None:
        pairs = pairs[:top]
    print(f"Merge time by (row_ranges, column), slowest first ({len(pairs)} of {len(run.pairs)}):")
    for row_ranges, column, dur_ns in pairs:
        print(f"  {to_s(dur_ns):>10.6f}  {column:<20} {row_ranges}")
    print()


def print_across_runs(runs):
    print(f"=== Across {len(runs)} runs (per-run totals) ===")
    walls = [r.wall for r in runs if r.wall is not None]
    if walls:
        print(f"  {'wall':<28} {across(walls)}")
    for fn in sorted({fn for r in runs for fn in r.fn_times}):
        print(f"  {'fn ' + fn:<28} {across([sum(r.fn_times.get(fn, [])) for r in runs])}")
    for column in sorted({c for r in runs for c in r.column_times}):
        print(f"  {'column ' + column:<28} {across([sum(r.column_times.get(column, [])) for r in runs])}")


def write_pairs_csv(runs, path):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["run", "row_ranges", "column", "dur_ns"])
        for run in runs:
            for row_ranges, column, dur_ns in run.pairs:
                writer.writerow([run.idx, row_ranges, column, dur_ns])


def main():
    parser = argparse.ArgumentParser(description="Summarise the merge_timer JSON lines printed by benchmark.py")
    parser.add_argument("log", help="Captured stdout of benchmark.py, '-' for stdin")
    parser.add_argument("--top", type=int, default=None, help="Print only the N slowest (row_ranges, column) pairs")
    parser.add_argument("--pairs-csv", help="Write every (run, row_ranges, column, dur_ns) to this CSV")
    args = parser.parse_args()

    if args.log == "-":
        runs = parse(sys.stdin)
    else:
        with open(args.log) as f:
            runs = parse(f)
    if not runs:
        sys.exit("No merge_timer lines found")
    for run in runs:
        print_run(run, args.top)
    print_across_runs(runs)
    if args.pairs_csv:
        write_pairs_csv(runs, args.pairs_csv)


if __name__ == "__main__":
    main()
