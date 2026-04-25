"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this
software will be governed by the Apache License, version 2.0.

C++ micro-benchmark tracking.

Exposes each Google Benchmark function as a separate ASV class, mirroring the
Google Benchmark structure.  Parameter variants (e.g.
``BM_arrow_string_handler/10000/1/0/0/0``) become ASV parameters of the
corresponding class.

Execution model
---------------
The module is self-sufficient: it owns both discovery and execution. At
import time it invokes the benchmark binary with
``--benchmark_list_tests=true`` to enumerate benchmark names and registers
one ASV class per top-level Google Benchmark function. Each ``track_time_ms``
call shells out to the binary with
``--benchmark_filter=^<name>$ --benchmark_format=json`` and returns
``real_time`` converted to milliseconds.

Binary path is taken from ``ARCTICDB_CPP_BENCHMARKS_BINARY`` or defaults to
``cpp/out/linux-release-build/arcticdb/benchmarks`` relative to the repo
root. If the binary is missing the module raises ``FileNotFoundError`` at
import with instructions for building it — a silent fallback would cause
asv_checks.py to regenerate ``benchmarks.json`` without the C++ entries
and produce a confusing hash mismatch on PRs.
"""

import json
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

CPP_BENCHMARKS_BINARY = Path(
    os.environ.get(
        "ARCTICDB_CPP_BENCHMARKS_BINARY",
        REPO_ROOT / "cpp/out/linux-release-build/arcticdb/benchmarks",
    )
)

TIMEOUT_S = 3600


def to_identifier(name: str) -> str:
    """Replace non-identifier characters with '_', prepending '_' if the name starts with a digit."""
    ident = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if ident and ident[0].isdigit():
        ident = "_" + ident
    return ident


def to_milliseconds(real_time: float, unit: str, full_name: str) -> float:
    """Convert a Google Benchmark real_time value to milliseconds."""
    if unit == "ns":
        return real_time / 1e6
    elif unit == "us":
        return real_time / 1e3
    elif unit == "ms":
        return real_time
    elif unit == "s":
        return real_time * 1e3
    else:
        raise RuntimeError(f"Unexpected time_unit {unit!r} in benchmark output for {full_name!r}")


def list_benchmark_names(binary: Path) -> list[str]:
    """Enumerate all benchmark names (including param variants) from the binary."""
    result = subprocess.run(
        [str(binary), "--benchmark_list_tests=true"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def run_one(binary: Path, full_name: str) -> float:
    """Run a single benchmark by exact-match filter, return its real_time in milliseconds."""
    result = subprocess.run(
        [
            str(binary),
            f"--benchmark_filter=^{re.escape(full_name)}$",
            "--benchmark_format=json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)
    # Skip aggregate rows (mean, stddev, …) when --benchmark_repetitions is active.
    iterations = [b for b in data.get("benchmarks", []) if b.get("run_type", "iteration") == "iteration"]
    if not iterations:
        raise RuntimeError(f"No iteration result returned for benchmark {full_name!r}")
    bm = iterations[0]
    return to_milliseconds(bm["real_time"], bm.get("time_unit", "ms"), full_name)


def group_benchmarks(names: list[str]) -> dict[str, list[str | None]]:
    """Split each name on the first '/' into function name and variant (None if no '/')."""
    groups: dict[str, list[str | None]] = defaultdict(list)
    for name in names:
        if "/" in name:
            func, variant = name.split("/", 1)
        else:
            func, variant = name, None
        groups[func].append(variant)
    return dict(groups)


def make_benchmark_class(binary: Path, func_name: str, variants: list[str | None]) -> type:
    """Return a dynamically created ASV benchmark class for one C++ benchmark function."""

    def track_time_ms(self, params):
        full_name = f"{func_name}/{params}" if params is not None else func_name
        return run_one(binary, full_name)

    return type(
        to_identifier(func_name),
        (),
        {
            "timeout": TIMEOUT_S,
            # variants is [None] for benchmarks with no parameters (from group_benchmarks).
            # ASV requires params to be a non-empty list of lists, hence the outer wrap.
            "params": [variants],
            "param_names": ["params"],
            "unit": "ms",
            "track_time_ms": track_time_ms,
        },
    )


if not CPP_BENCHMARKS_BINARY.exists():
    raise FileNotFoundError(
        f"C++ benchmark binary not found at {CPP_BENCHMARKS_BINARY}.\n"
        f"\n"
        f"The ASV C++ microbenchmark tracking module needs this binary to\n"
        f"enumerate benchmark names at import time. Build it with:\n"
        f"\n"
        f"    cmake --preset linux-release -DTEST=ON cpp\n"
        f"    cmake --build cpp/out/linux-release-build --target benchmarks\n"
        f"\n"
        f"Or set ARCTICDB_CPP_BENCHMARKS_BINARY to an existing binary path."
    )

_NAMES = list_benchmark_names(CPP_BENCHMARKS_BINARY)

# Register one ASV class per C++ benchmark function in this module's namespace.
# to_identifier() is lossy (e.g. 'BM_foo-bar' and 'BM_foo_bar' both become
# 'BM_foo_bar'), so refuse to register colliding names rather than silently
# dropping one from ASV tracking.
_registered: dict[str, str] = {}
for func_name, variants in group_benchmarks(_NAMES).items():
    ident = to_identifier(func_name)
    if ident in _registered:
        raise RuntimeError(
            f"C++ benchmark name collision: {func_name!r} and {_registered[ident]!r} "
            f"both map to Python identifier {ident!r}. Rename one of the C++ benchmarks."
        )
    _registered[ident] = func_name
    globals()[ident] = make_benchmark_class(CPP_BENCHMARKS_BINARY, func_name, variants)
