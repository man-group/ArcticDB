#!/usr/bin/env python3
# ===========================================================================
# EXPERIMENT — NOT PART OF THE PIPELINE. Delete this file and the branch it
# lives on (`benchmark-winds-real-ab`) when the experiment is over.
# ===========================================================================
"""The encode kernel's concurrency curve, in BOTH units, for arctic-winds gha 120.

gha 114 measured per-encode cost on the fleet as a monotone function of
concurrent encodes (2.74 concurrent -> 10.12 ms, 7.99 -> 24.4-25.2 ms) and
convicted memory contention. gha 117 rebuilt the curve on a laptop and refuted
that at the scale that matters (1.073x from K=1 to K=6), leaving two suspects
that are both properties of the FLEET measurement and invisible from inside a
Mac:

  (a) SMT. `m6i.2xlarge` is 4 physical cores x 2 hyperthreads, so "8 concurrent
      encodes on 8 vCPUs" is two LZ4 streams per physical core.
  (b) UNITS. gha 114's figure is `run_ns` — task-body WALL, which includes time
      the task was runnable but descheduled. gha 117's is thread CPU. An 8-vCPU
      box running an 8-thread CPU pool AND a 48-thread IO pool has plenty of
      that.

The two are told apart by measuring both clocks at once, which is all this does.
K worker PROCESSES (not threads: no GIL, and `rusage` is then per worker) each
encode their own private corpus in a loop, released together by a barrier; each
reports its own wall and its own CPU time.

  * where the wall inflates and the CPU does NOT, the workers were waiting for a
    CPU — suspect (b), descheduling;
  * where BOTH inflate together, the workers were running slower while running —
    suspect (a), a shared physical core (or a shared memory system).

On an SMT-on lane K=8 is 8 runnable workers on 4 physical cores via SMT; on an
SMT-off lane (`threads_per_core: 1`) K=4 is full occupancy and K=8 is 8 runnable
workers on 4 physical cores via the scheduler. Same silicon, same demand, two
mechanisms — which is the discrimination gha 120 is buying.

The kernel is gha 117's: `encode_v1` LZ4s each column block (`lz4.hpp:34-39`)
and then hashes the SAME uncompressed bytes again with XXH64 (`lz4.hpp:55`).
The shape is `winds_span_workload.py`'s segment — seven 800,000-byte blocks
(100,000 rows x 8 bytes), five of them incompressible random float64 — so a
"segment" here is the same 5.6 MB of uncompressed input the fleet's encodes read.

Prints one `SMTKERNEL` line per K (parseable), then a markdown table. Writes
nothing, reads nothing, needs no storage and no ArcticDB: it is a CPU probe that
happens to run on the lane that also took the span capture.
"""

import json
import multiprocessing as mp
import os
import platform
import resource
import statistics
import sys
import time

BLOCK = 800_000          # one column block: 100,000 rows x 8 bytes
BLOCKS = 7               # five float64 + index + string, the workload's segment
ITERATIONS = int(os.environ.get("SMT_KERNEL_ITERATIONS", "30"))
WARMUP = int(os.environ.get("SMT_KERNEL_WARMUP", "5"))
LEVELS = [int(k) for k in os.environ.get("SMT_KERNEL_K", "1,2,4,8").split(",")]


def corpus(seed):
    """One segment's worth of uncompressed bytes, private to this worker.

    Private rather than shared on purpose: `fork` would give every worker a
    copy-on-write view of ONE corpus, and K workers reading the same 5.6 MB is
    kinder to the last-level cache than K encodes of K different segments, which
    is what the fleet actually runs.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    blocks = [rng.random(BLOCK // 8).tobytes() for _ in range(5)]          # float64, incompressible
    blocks.append(np.arange(BLOCK // 8, dtype="int64").tobytes())          # the index
    blocks.append((b"abc\x00\x00\x00\x00\x00" * (BLOCK // 8)))             # the string column
    return blocks


def encode(blocks, compress, digest):
    """One segment encode, gha 117's shape: LZ4 the block, then hash the input."""
    total = 0
    for block in blocks:
        total += len(compress(block, store_size=False))
        total += digest(block).intdigest() & 0xFF
    return total


def worker(seed, iterations, barrier, out):
    import lz4.block
    import xxhash

    blocks = corpus(seed)
    compress, digest = lz4.block.compress, xxhash.xxh64
    for _ in range(WARMUP):
        encode(blocks, compress, digest)
    barrier.wait()
    wall0 = time.perf_counter()
    cpu0 = resource.getrusage(resource.RUSAGE_SELF)
    for _ in range(iterations):
        encode(blocks, compress, digest)
    wall = time.perf_counter() - wall0
    cpu1 = resource.getrusage(resource.RUSAGE_SELF)
    cpu = (cpu1.ru_utime - cpu0.ru_utime) + (cpu1.ru_stime - cpu0.ru_stime)
    out.put((wall / iterations * 1e3, cpu / iterations * 1e3))


def sweep(k):
    barrier = mp.Barrier(k)
    out = mp.Queue()
    children = [
        mp.Process(target=worker, args=(1000 + i, ITERATIONS, barrier, out))
        for i in range(k)
    ]
    for child in children:
        child.start()
    results = [out.get() for _ in children]
    for child in children:
        child.join()
    walls = [r[0] for r in results]
    cpus = [r[1] for r in results]
    return {
        "k": k,
        "wall_ms": statistics.median(walls),
        "wall_max_ms": max(walls),
        "cpu_ms": statistics.median(cpus),
        "cpu_max_ms": max(cpus),
        "segments_per_s": k / (statistics.median(walls) / 1e3),
    }


def topology():
    logical = physical = 0
    try:
        with open("/proc/cpuinfo") as handle:
            socket, cores = None, set()
            for line in handle:
                if line.startswith("processor"):
                    logical += 1
                elif line.startswith("physical id"):
                    socket = line.split(":")[1].strip()
                elif line.startswith("core id"):
                    cores.add((socket, line.split(":")[1].strip()))
            physical = len(cores)
    except OSError:
        pass
    return {
        "online_cpus": os.cpu_count(),
        "logical_from_cpuinfo": logical,
        "physical_cores": physical,
        "machine": platform.machine(),
    }


def main():
    try:
        import lz4.block  # noqa: F401
        import numpy  # noqa: F401
        import xxhash  # noqa: F401
    except ImportError as exc:
        print(f"SMTKERNEL unavailable: {exc!r}", flush=True)
        return 0

    box = topology()
    print(f"SMTKERNEL topology {json.dumps(box)}", flush=True)
    print(
        f"SMTKERNEL shape blocks={BLOCKS}x{BLOCK}B iterations={ITERATIONS} warmup={WARMUP}",
        flush=True,
    )
    rows = []
    for k in LEVELS:
        row = sweep(k)
        rows.append(row)
        print("SMTKERNEL " + json.dumps(row), flush=True)

    base_wall = rows[0]["wall_ms"] if rows else 0.0
    base_cpu = rows[0]["cpu_ms"] if rows else 0.0
    print()
    print(f"| K | per-segment WALL ms | x K=1 | per-segment CPU ms | x K=1 | segments/s |")
    print("| ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in rows:
        print(
            f"| {row['k']} | {row['wall_ms']:.3f} | {row['wall_ms'] / base_wall:.3f} | "
            f"{row['cpu_ms']:.3f} | {row['cpu_ms'] / base_cpu:.3f} | {row['segments_per_s']:.1f} |"
        )
    print()
    print(
        "WALL inflating while CPU stays flat is descheduling; both inflating together "
        "is a shared core or a shared memory system."
    )
    return 0


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    sys.exit(main())
