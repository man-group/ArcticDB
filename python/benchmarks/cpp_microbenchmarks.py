"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this
software will be governed by the Apache License, version 2.0.

C++ micro-benchmark tracking.

Runs the C++ Google Benchmark binary and exposes each benchmark function as a
separate ASV class, mirroring the Google Benchmark structure.  Parameter
variants (e.g. ``BM_arrow_string_handler/10000/1/0/0/0``) become ASV
parameters of the corresponding class.

The binary path defaults to ``cpp/out/linux-release-build/arcticdb/benchmarks``
relative to the repository root (resolved from this file's location) and can be
overridden via the ``ARCTICDB_CPP_BENCHMARKS_BINARY`` environment variable.

If the binary is not present, no benchmark classes are registered and the ASV
run produces no C++ results.

Per-benchmark flags
-------------------
``BENCHMARK_FLAGS`` maps C++ benchmark function names to lists of CLI flags.
The key is the function name only (e.g. ``"BM_arrow_string_handler"``), which
is also the Google Benchmark name before the first ``/``.
Benchmarks not matched use ``DEFAULT_BENCHMARK_FLAGS``.
"""

import json
import os
import re
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

# Resolve relative to the repository root (three levels up from this file:
# python/benchmarks/cpp_microbenchmarks.py → python/benchmarks → python → repo root).
REPO_ROOT = Path(__file__).resolve().parents[2]

CPP_BENCHMARKS_BINARY = Path(
    os.environ.get(
        "ARCTICDB_CPP_BENCHMARKS_BINARY",
        REPO_ROOT / "cpp/out/linux-release-build/arcticdb/benchmarks",
    )
)

# Shared timeout value (seconds) used both as the ASV class-level timeout and
# as the subprocess timeout inside run_benchmark.
TIMEOUT_S = 3600

# Per-benchmark-function flag overrides.
#
# Keys are C++ benchmark function names (the part before the first ``/``).
# Values replace DEFAULT_BENCHMARK_FLAGS entirely for that function, so include
# every flag you need.
#
# Example — run arrow benchmarks with more iterations:
#   "BM_arrow_string_handler": ["--benchmark_min_time=100x", "--benchmark_time_unit=ms"],
BENCHMARK_FLAGS: dict[str, list[str]] = {}

DEFAULT_BENCHMARK_FLAGS = [
    "--benchmark_min_time=20x",
    "--benchmark_time_unit=ms",
]


def gbench_escape(name: str) -> str:
    # Google Benchmark passes filters to std::regex (ECMAScript). We cannot use
    # re.escape because it escapes '-' as '\-', which ECMAScript regex rejects
    # outside a character class.
    return re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", name)


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


def run_benchmark(full_name: str, func_name: str) -> float:
    """Run a single C++ benchmark and return its real-time duration in milliseconds."""
    fd, out_file = tempfile.mkstemp(suffix=".json")
    os.close(fd)

    try:
        result = subprocess.run(
            [
                str(CPP_BENCHMARKS_BINARY),
                f"--benchmark_filter=^{gbench_escape(full_name)}$",
                f"--benchmark_out={out_file}",
                "--benchmark_out_format=json",
                *BENCHMARK_FLAGS.get(func_name, DEFAULT_BENCHMARK_FLAGS),
            ],
            check=True,
            capture_output=True,
            timeout=TIMEOUT_S,
        )

        with open(out_file) as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Benchmark binary produced invalid JSON for {full_name!r}.\n"
                    f"stderr: {result.stderr.decode(errors='replace')}"
                ) from exc
    finally:
        os.unlink(out_file)

    for bm in data.get("benchmarks", []):
        # When --benchmark_repetitions is used Google Benchmark emits
        # aggregate rows (mean, stddev, …) alongside raw iteration rows.
        # Only keep iteration rows to avoid double-counting.
        if bm.get("run_type", "iteration") != "iteration":
            continue
        real_time = bm.get("real_time")
        if real_time is None:
            continue
        unit = bm.get("time_unit", "ms")
        return to_milliseconds(real_time, unit, full_name)

    raise RuntimeError(
        f"Benchmark binary ran successfully but produced no iteration results for {full_name!r}.\n"
        f"Check that --benchmark_filter matched exactly one benchmark."
    )


def discover_benchmark_names() -> list[str]:
    """Return every benchmark name from ``--benchmark_list_tests``, or [] if the binary is absent."""
    if not CPP_BENCHMARKS_BINARY.exists():
        return []
    result = subprocess.run(
        [str(CPP_BENCHMARKS_BINARY), "--benchmark_list_tests"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


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


def make_benchmark_class(func_name: str, variants: list[str | None]) -> type:
    """Return a dynamically created ASV benchmark class for one C++ benchmark function."""

    def track_time_ms(self, params):
        if not CPP_BENCHMARKS_BINARY.exists():
            raise FileNotFoundError(
                f"C++ benchmark binary not found: {CPP_BENCHMARKS_BINARY}\n"
                f"Build it with:\n"
                f"  cmake --preset linux-release -DTEST=ON cpp\n"
                f"  cmake --build cpp/out/linux-release-build --target benchmarks\n"
                f"or set ARCTICDB_CPP_BENCHMARKS_BINARY to the correct path."
            )
        full_name = f"{func_name}/{params}" if params is not None else func_name
        return run_benchmark(full_name, func_name)

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


# Register one ASV class per C++ benchmark function in this module's namespace.
for func_name, variants in group_benchmarks(discover_benchmark_names()).items():
    globals()[to_identifier(func_name)] = make_benchmark_class(func_name, variants)
