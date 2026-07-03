#!/usr/bin/env python3
"""Post-hoc analysis of ArcticDB task-scheduler queue behaviour.

Reads a captured log file (or stdin) containing ``task_stats`` records emitted by
``TaskStatsLoggingObserver``. Each record is one completed task:

    task_stats pool=IO thread=IOPool7 task_id=42 enqueue_ns=1000 wait_ns=250 run_ns=9000 \
        expired=false priority=3

Enable the logging with two environment variables, both read at ``import arcticdb``:

    ARCTICDB_TaskScheduler_LogTaskStats_int=1   # add the observer to both pools
    ARCTICDB_schedule_loglevel=DEBUG            # emit the records (schedule logger to debug)

The records go to stderr by default, so capture them with a redirect and feed the file here:

    ARCTICDB_TaskScheduler_LogTaskStats_int=1 ARCTICDB_schedule_loglevel=DEBUG \
        python your_app.py 2> app.log
    scripts/analyze_task_scheduler_queues.py app.log

Times are ``steady_clock`` nanoseconds (monotonic, arbitrary origin); the script normalises
them to the earliest enqueue seen.

From one line per task it reconstructs, per pool:
  * wait-time and run-time distributions,
  * the queue-depth (tasks enqueued but not yet started) timeline and its peak,
and per IO worker thread it shows the round-robin load imbalance that the pool-wide
``getPendingTaskCount`` cannot reveal.

Usage:
    scripts/analyze_task_scheduler_queues.py app.log
    grep task_stats app.log | scripts/analyze_task_scheduler_queues.py -

See ``analyze_task_scheduler_queues.example_output.txt`` (alongside this script) for a sample
report: a QueryBuilder filter over an S3 backend throttled to 4 I/O threads, where the I/O
pool queues heavily and the wait histogram shifts into the 10-100ms bucket.
"""

import argparse
import re
import sys
from collections import defaultdict

_FIELD_RE = re.compile(r"(\w+)=(\S+)")
_TRAILING_INT_RE = re.compile(r"^(.*?)(\d*)$")
_SPARK = "▁▂▃▄▅▆▇█"

_LATENCY_EDGES_NS = (1e3, 1e4, 1e5, 1e6, 1e7, 1e8)
_LATENCY_LABELS = ("<1us", "1-10us", "10-100us", "100us-1ms", "1-10ms", "10-100ms", ">100ms")


def thread_sort_key(name):
    prefix, digits = _TRAILING_INT_RE.match(name).groups()
    return prefix, int(digits) if digits else -1


def histogram(values, edges, labels, width=40):
    counts = [0] * len(labels)
    for v in values:
        b = 0
        while b < len(edges) and v >= edges[b]:
            b += 1
        counts[b] += 1
    peak = max(counts) if counts else 0
    lines = []
    for label, count in zip(labels, counts):
        bar = "█" * round(count / peak * width) if peak and count else ""
        lines.append(f"    {label:>10} {count:>6} {bar}")
    return "\n".join(lines)


class Record:
    __slots__ = ("pool", "thread", "enqueue_ns", "wait_ns", "run_ns", "expired")

    def __init__(self, fields):
        self.pool = fields["pool"]
        self.thread = fields["thread"]
        self.enqueue_ns = int(fields["enqueue_ns"])
        self.wait_ns = int(fields["wait_ns"])
        self.run_ns = int(fields["run_ns"])
        self.expired = fields.get("expired") == "true"

    @property
    def start_ns(self):
        return self.enqueue_ns + self.wait_ns

    @property
    def end_ns(self):
        return self.start_ns + self.run_ns


def parse(lines):
    records = []
    for line in lines:
        idx = line.find("task_stats ")
        if idx == -1:
            continue
        fields = dict(_FIELD_RE.findall(line[idx:]))
        if "pool" not in fields or "enqueue_ns" not in fields:
            continue
        try:
            records.append(Record(fields))
        except (KeyError, ValueError):
            continue
    return records


def percentile(sorted_values, q):
    if not sorted_values:
        return 0
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = q / 100.0 * (len(sorted_values) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def fmt_ns(ns):
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("us", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.2f}{unit}"
    return f"{ns:.0f}ns"


def bar(value, peak, width=40):
    if peak <= 0:
        return ""
    return "█" * max(1, round(value / peak * width)) if value > 0 else ""


def sparkline(counts):
    peak = max(counts) if counts else 0
    if peak <= 0:
        return " " * len(counts)
    return "".join(_SPARK[min(len(_SPARK) - 1, round(c / peak * (len(_SPARK) - 1)))] for c in counts)


def max_queue_depth(records):
    events = []
    for r in records:
        events.append((r.enqueue_ns, 1))
        events.append((r.start_ns, -1))
    events.sort()
    depth = 0
    peak = 0
    for _, delta in events:
        depth += delta
        peak = max(peak, depth)
    return peak


def depth_timeline(records, t0, buckets):
    if not records:
        return [0] * buckets
    span = max(r.start_ns for r in records) - t0
    if span <= 0:
        return [0] * buckets
    events = []
    for r in records:
        events.append((r.enqueue_ns, 1))
        events.append((r.start_ns, -1))
    events.sort()
    peak_per_bucket = [0] * buckets
    depth = 0
    for t, delta in events:
        depth += delta
        b = min(buckets - 1, int((t - t0) / span * buckets))
        peak_per_bucket[b] = max(peak_per_bucket[b], depth)
    return peak_per_bucket


def report_pool(pool, records, t0, buckets):
    waits = sorted(r.wait_ns for r in records)
    runs = sorted(r.run_ns for r in records)
    expired = sum(1 for r in records if r.expired)

    print(f"\n=== {pool} pool ===")
    print(f"  tasks:        {len(records)}" + (f"  (expired: {expired})" if expired else ""))
    print(f"  peak depth:   {max_queue_depth(records)}  (tasks waiting, not yet started)")
    print(
        "  wait:         "
        f"p50 {fmt_ns(percentile(waits, 50))}  p90 {fmt_ns(percentile(waits, 90))}  "
        f"p99 {fmt_ns(percentile(waits, 99))}  max {fmt_ns(waits[-1])}"
    )
    print(
        "  run:          "
        f"p50 {fmt_ns(percentile(runs, 50))}  p90 {fmt_ns(percentile(runs, 90))}  "
        f"p99 {fmt_ns(percentile(runs, 99))}  max {fmt_ns(runs[-1])}"
    )
    print(f"  depth over time: {sparkline(depth_timeline(records, t0, buckets))}")
    print("  wait histogram:")
    print(histogram(waits, _LATENCY_EDGES_NS, _LATENCY_LABELS))
    print("  run histogram:")
    print(histogram(runs, _LATENCY_EDGES_NS, _LATENCY_LABELS))


def report_io_threads(records):
    by_thread = defaultdict(list)
    for r in records:
        by_thread[r.thread].append(r)
    if len(by_thread) <= 1:
        return

    busy = {t: sum(r.run_ns for r in rs) for t, rs in by_thread.items()}
    peak_busy = max(busy.values())
    print("\n  per-thread load (bar = busy time, round-robin imbalance shows here):")
    for thread in sorted(by_thread, key=thread_sort_key):
        rs = by_thread[thread]
        waits = sorted(r.wait_ns for r in rs)
        print(
            f"    {thread:<12} {len(rs):>6} tasks  busy {fmt_ns(busy[thread]):>9}  "
            f"wait p99 {fmt_ns(percentile(waits, 99)):>9}  {bar(busy[thread], peak_busy)}"
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logfile", help="log file to read, or - for stdin")
    ap.add_argument("--buckets", type=int, default=60, help="time buckets for the depth sparkline (default 60)")
    args = ap.parse_args()

    stream = sys.stdin if args.logfile == "-" else open(args.logfile)
    with stream:
        records = parse(stream)

    if not records:
        print("No task_stats records found.", file=sys.stderr)
        return 1

    t0 = min(r.enqueue_ns for r in records)
    by_pool = defaultdict(list)
    for r in records:
        by_pool[r.pool].append(r)

    for pool in sorted(by_pool):
        report_pool(pool, by_pool[pool], t0, args.buckets)
        if pool == "IO":
            report_io_threads(by_pool[pool])
    return 0


if __name__ == "__main__":
    sys.exit(main())
