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
        self.records = []


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
            run.records.append((record["thread"], record["start_ns"], dur_ns, fn))
            if fn == "merge":
                rows = ",".join(f"{start}-{end}" for start, end in record["rows"])
                run.column_times[record["column"]].append(dur_ns)
                run.pairs.append((rows, record["column"], dur_ns))
    return [runs[k] for k in sorted(runs)]


def nested_totals(records):
    """Total duration per call path, e.g. ("process", "update_and_insert", "merge").

    A timer is nested in another when it ran on the same thread inside the other's time interval, so this does not
    depend on the order lines were printed in.
    """
    totals = defaultdict(int)
    by_thread = defaultdict(list)
    for thread, start, dur, fn in records:
        by_thread[thread].append((start, start + dur, dur, fn))
    for intervals in by_thread.values():
        # Parents sort before children: earlier start, and the longer interval first on equal starts
        intervals.sort(key=lambda iv: (iv[0], -iv[1]))
        stack = []
        for start, end, dur, fn in intervals:
            while stack and stack[-1][0] < end:
                stack.pop()
            path = (stack[-1][1] if stack else ()) + (fn,)
            totals[path] += dur
            stack.append((end, path))
    return totals


def process_shares(run):
    """Per path under process: (depth, fn, total_ns, % of process, % of parent). Untimed remainders are included."""
    totals = nested_totals(run.records)
    root = ("process",)
    if root not in totals:
        return []
    process_total = totals[root]
    children = defaultdict(list)
    for path in totals:
        if len(path) > 1 and path[0] == "process":
            children[path[:-1]].append(path)
    res = []

    def visit(path):
        total = totals[path]
        parent_total = totals[path[:-1]] if len(path) > 1 else total
        res.append((len(path) - 1, path[-1], total, 100 * total / process_total, 100 * total / parent_total))
        kids = sorted(children[path], key=lambda k: -totals[k])
        for kid in kids:
            visit(kid)
        if kids:
            untimed = total - sum(totals[k] for k in kids)
            res.append((len(path), "(untimed)", untimed, 100 * untimed / process_total, 100 * untimed / total))

    visit(root)
    return res


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
    shares = process_shares(run)
    if shares:
        print("Share of process (summed over all process calls):")
        for depth, fn, total, of_process, of_parent in shares:
            label = "  " * depth + fn
            print(f"  {label:<28} {to_s(total):>12.6f}s  {of_process:>6.2f}% of process  {of_parent:>6.2f}% of parent")
    process_total = sum(run.fn_times.get("process", []))
    print("Merge time by column:")
    for column, values in sorted(run.column_times.items(), key=lambda kv: -sum(kv[1])):
        share = f"  {100 * sum(values) / process_total:>6.2f}% of process" if process_total else ""
        print(f"  {column:<20} {describe(values)}{share}")
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
    per_run_shares = [{(depth, fn): of_process for depth, fn, _, of_process, _ in process_shares(r)} for r in runs]
    paths = []
    for shares in per_run_shares:
        paths += [p for p in shares if p not in paths]
    if paths:
        print("  % of process:")
        for depth, fn in paths:
            values = [shares.get((depth, fn), 0.0) for shares in per_run_shares]
            label = "  " * depth + fn
            print(
                f"    {label:<26} mean={statistics.mean(values):>6.2f}%  median={statistics.median(values):>6.2f}%  "
                f"min={min(values):>6.2f}%  max={max(values):>6.2f}%"
            )


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
