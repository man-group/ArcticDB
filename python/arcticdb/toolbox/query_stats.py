"""
Query Statistics API for ArcticDB.

This module provides scoped and global utilities for collecting storage, HTTP,
and per-library / per-symbol query statistics.

Notes
----------
!!! warning
    This API is unstable and not governed by ArcticDB's semantic versioning.
    It may change or be removed in future versions without notice.
"""

from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd

import arcticdb_ext.tools.query_stats as qs
from arcticdb.exceptions import UserInputException


def _diff_latency(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    count = max(0, int(after.get("count", 0)) - int(before.get("count", 0)))
    total_ms = max(0.0, float(after.get("total_ms", 0.0)) - float(before.get("total_ms", 0.0)))
    mean_ms = total_ms / count if count > 0 else 0.0
    if count == 0:
        return {
            "count": 0,
            "total_ms": 0.0,
            "mean_ms": 0.0,
            "min_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
        }
    # When subtracting cumulative snapshots from Python dicts, individual percentiles
    # cannot be reconstructed exactly without raw histograms; report `after` bounds if
    # `before` was empty, otherwise preserve count/total_ms/mean_ms cleanly.
    if int(before.get("count", 0)) == 0:
        return dict(after)
    return {
        "count": count,
        "total_ms": total_ms,
        "mean_ms": mean_ms,
        "min_ms": float(after.get("min_ms", 0.0)),
        "p50_ms": mean_ms,
        "p95_ms": float(after.get("p95_ms", 0.0)),
        "p99_ms": float(after.get("p99_ms", 0.0)),
        "max_ms": float(after.get("max_ms", 0.0)),
    }


def _diff_storage_op(after: Dict[str, Any], before: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    keys = (
        "ok",
        "not_found",
        "conflict",
        "unsupported",
        "error",
        "count",
        "bytes",
        "items",
        "pages",
        "partial_delete_retries",
    )
    out: Dict[str, Any] = {k: max(0, int(after.get(k, 0)) - int(before.get(k, 0))) for k in keys}
    if all(out[k] == 0 for k in keys):
        return None
    out["latency"] = _diff_latency(after.get("latency", {}), before.get("latency", {}))
    out["error_latency"] = _diff_latency(after.get("error_latency", {}), before.get("error_latency", {}))
    return out


def _diff_storage_map(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for backend, ops in after.items():
        b_ops = before.get(backend, {})
        ops_out: Dict[str, Any] = {}
        for op, kts in ops.items():
            b_kts = b_ops.get(op, {})
            kt_out: Dict[str, Any] = {}
            for kt, stats in kts.items():
                delta = _diff_storage_op(stats, b_kts.get(kt, {}))
                if delta is not None:
                    kt_out[kt] = delta
            if kt_out:
                ops_out[op] = kt_out
        if ops_out:
            out[backend] = ops_out
    return out


def _diff_http_op(after: Dict[str, Any], before: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    int_keys = (
        "requests",
        "attempts",
        "retries",
        "status_2xx",
        "status_3xx",
        "status_4xx",
        "status_5xx",
        "throttled",
        "network_errors",
        "checksum_mismatch_retries",
        "new_connections",
    )
    out: Dict[str, Any] = {k: max(0, int(after.get(k, 0)) - int(before.get(k, 0))) for k in int_keys}
    backoff_ms = max(0.0, float(after.get("backoff_time_ms", 0.0)) - float(before.get("backoff_time_ms", 0.0)))
    if all(out[k] == 0 for k in int_keys) and backoff_ms == 0.0:
        return None
    out["backoff_time_ms"] = backoff_ms
    for lat_key in (
        "request_latency",
        "attempt_latency",
        "dns_latency",
        "tcp_connect_latency",
        "tls_handshake_latency",
        "ttfb_latency",
        "transfer_latency",
    ):
        out[lat_key] = _diff_latency(after.get(lat_key, {}), before.get(lat_key, {}))
    return out


def _diff_http_map(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for svc, ops in after.items():
        b_ops = before.get(svc, {})
        ops_out: Dict[str, Any] = {}
        for op, stats in ops.items():
            delta = _diff_http_op(stats, b_ops.get(op, {}))
            if delta is not None:
                ops_out[op] = delta
        if ops_out:
            out[svc] = ops_out
    return out


def _diff_api_op(after: Dict[str, Any], before: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    keys = ("ok", "error", "count")
    out: Dict[str, Any] = {k: max(0, int(after.get(k, 0)) - int(before.get(k, 0))) for k in keys}
    if all(out[k] == 0 for k in keys):
        return None
    out["latency"] = _diff_latency(after.get("latency", {}), before.get("latency", {}))
    out["error_latency"] = _diff_latency(after.get("error_latency", {}), before.get("error_latency", {}))
    return out


def _diff_api_map(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for op, stats in after.items():
        delta = _diff_api_op(stats, before.get(op, {}))
        if delta is not None:
            out[op] = delta
    return out


def _diff_symbol_map(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    int_keys = (
        "reads",
        "writes",
        "deletes",
        "exists",
        "lists",
        "errors",
        "total_ops",
        "bytes_read",
        "bytes_written",
        "http_requests",
        "http_retries",
    )
    out: Dict[str, Any] = {}
    for sym, stats in after.items():
        b_stats = before.get(sym, {})
        row: Dict[str, Any] = {k: max(0, int(stats.get(k, 0)) - int(b_stats.get(k, 0))) for k in int_keys}
        if all(row[k] == 0 for k in int_keys):
            continue
        row["storage_latency"] = _diff_latency(stats.get("storage_latency", {}), b_stats.get("storage_latency", {}))
        out[sym] = row
    return out


def _empty_latency() -> Dict[str, Any]:
    return {
        "count": 0,
        "total_ms": 0.0,
        "mean_ms": 0.0,
        "min_ms": 0.0,
        "p50_ms": 0.0,
        "p95_ms": 0.0,
        "p99_ms": 0.0,
        "max_ms": 0.0,
    }


def _empty_thread_pools() -> Dict[str, Any]:
    return {
        "cpu": {"tasks": 0, "expired": 0, "wait_latency": _empty_latency(), "run_latency": _empty_latency()},
        "io": {"tasks": 0, "expired": 0, "wait_latency": _empty_latency(), "run_latency": _empty_latency()},
        "semaphore_waits": 0,
        "semaphore_wait_latency": _empty_latency(),
    }


def _empty_caches() -> Dict[str, Any]:
    return {
        "version_map": {"hits": 0, "misses": 0, "reloads": 0, "ref_retries": 0, "chain_hops": 0},
        "dedup_map": {"hits": 0, "misses": 0, "bytes_saved": 0},
        "config_cache": {"hits": 0, "misses": 0},
    }


def _empty_codec() -> Dict[str, Any]:
    return {
        "encodes": 0,
        "decodes": 0,
        "encode_uncompressed_bytes": 0,
        "encode_compressed_bytes": 0,
        "decode_compressed_bytes": 0,
        "decode_uncompressed_bytes": 0,
        "encode_compression_ratio": 0.0,
        "decode_compression_ratio": 0.0,
        "encode_latency": _empty_latency(),
        "decode_latency": _empty_latency(),
    }


def _empty_query() -> Dict[str, Any]:
    return {
        "queries": 0,
        "segments_total": 0,
        "segments_pruned_index": 0,
        "segments_pruned_column_stats": 0,
        "segments_read": 0,
        "columns_total": 0,
        "columns_projected": 0,
        "rows_total": 0,
        "rows_selected": 0,
    }


def _empty_processing() -> Dict[str, Any]:
    return {
        "clauses": {},
        "admission": {
            "units_admitted": 0,
            "residency_waits": 0,
            "segments_dispatched": 0,
            "read_window_waits": 0,
            "residency_wait_latency": _empty_latency(),
            "read_window_wait_latency": _empty_latency(),
        },
    }


def _empty_row_count_histogram() -> Dict[str, Any]:
    return {
        "count": 0,
        "sum": 0,
        "mean": 0.0,
        "min": 0,
        "p50": 0,
        "p95": 0,
        "p99": 0,
        "max": 0,
    }


def _empty_stream() -> Dict[str, Any]:
    return {
        "data_segments_written": 0,
        "staged_segments_written": 0,
        "small_segments_written": 0,
        "rows_per_segment": _empty_row_count_histogram(),
        "incomplete_chain_loads": 0,
        "incomplete_chain_hops": 0,
        "incomplete_iteration_loads": 0,
        "incomplete_iteration_segments": 0,
        "compactions": {},
        "conflicts": {},
    }


def _empty_pool_gauge() -> Dict[str, int]:
    return {
        "pending_tasks": 0,
        "active_threads": 0,
        "total_threads": 0,
        "max_threads": 0,
    }


def _empty_resources() -> Dict[str, Any]:
    return {
        "cpu_pool": _empty_pool_gauge(),
        "io_pool": _empty_pool_gauge(),
        "write_semaphore_in_flight": 0,
        "write_semaphore_limit": 0,
        "version_map_entries": 0,
        "config_cache_libraries": 0,
        "config_cache_storages": 0,
        "lmdb": {
            "open_envs": 0,
            "map_size_bytes": 0,
            "used_bytes": 0,
            "active_readers": 0,
            "map_full_errors": 0,
        },
    }


def _diff_row_count_histogram(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    count = max(0, int(after.get("count", 0)) - int(before.get("count", 0)))
    total = max(0, int(after.get("sum", 0)) - int(before.get("sum", 0)))
    if count == 0:
        return _empty_row_count_histogram()
    if int(before.get("count", 0)) == 0:
        return dict(after)
    mean = float(total) / float(count)
    return {
        "count": count,
        "sum": total,
        "mean": mean,
        "min": int(after.get("min", 0)),
        "p50": int(round(mean)),
        "p95": int(after.get("p95", 0)),
        "p99": int(after.get("p99", 0)),
        "max": int(after.get("max", 0)),
    }


def _diff_thread_pools(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for pool in ("cpu", "io"):
        ap = after.get(pool, {})
        bp = before.get(pool, {})
        out[pool] = {
            "tasks": max(0, int(ap.get("tasks", 0)) - int(bp.get("tasks", 0))),
            "expired": max(0, int(ap.get("expired", 0)) - int(bp.get("expired", 0))),
            "wait_latency": _diff_latency(ap.get("wait_latency", {}), bp.get("wait_latency", {})),
            "run_latency": _diff_latency(ap.get("run_latency", {}), bp.get("run_latency", {})),
        }
    out["semaphore_waits"] = max(0, int(after.get("semaphore_waits", 0)) - int(before.get("semaphore_waits", 0)))
    out["semaphore_wait_latency"] = _diff_latency(
        after.get("semaphore_wait_latency", {}), before.get("semaphore_wait_latency", {})
    )
    return out


def _diff_caches(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    def _diff_flat(a: Dict[str, Any], b: Dict[str, Any], keys: Tuple[str, ...]) -> Dict[str, int]:
        return {k: max(0, int(a.get(k, 0)) - int(b.get(k, 0))) for k in keys}

    return {
        "version_map": _diff_flat(
            after.get("version_map", {}),
            before.get("version_map", {}),
            ("hits", "misses", "reloads", "ref_retries", "chain_hops"),
        ),
        "dedup_map": _diff_flat(
            after.get("dedup_map", {}),
            before.get("dedup_map", {}),
            ("hits", "misses", "bytes_saved"),
        ),
        "config_cache": _diff_flat(
            after.get("config_cache", {}),
            before.get("config_cache", {}),
            ("hits", "misses"),
        ),
    }


def _diff_codec(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    int_keys = (
        "encodes",
        "decodes",
        "encode_uncompressed_bytes",
        "encode_compressed_bytes",
        "decode_compressed_bytes",
        "decode_uncompressed_bytes",
    )
    out: Dict[str, Any] = {k: max(0, int(after.get(k, 0)) - int(before.get(k, 0))) for k in int_keys}
    out["encode_compression_ratio"] = (
        float(out["encode_uncompressed_bytes"]) / float(out["encode_compressed_bytes"])
        if out["encode_compressed_bytes"] > 0
        else 0.0
    )
    out["decode_compression_ratio"] = (
        float(out["decode_uncompressed_bytes"]) / float(out["decode_compressed_bytes"])
        if out["decode_compressed_bytes"] > 0
        else 0.0
    )
    out["encode_latency"] = _diff_latency(after.get("encode_latency", {}), before.get("encode_latency", {}))
    out["decode_latency"] = _diff_latency(after.get("decode_latency", {}), before.get("decode_latency", {}))
    return out


def _diff_query(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "queries",
        "segments_total",
        "segments_pruned_index",
        "segments_pruned_column_stats",
        "segments_read",
        "columns_total",
        "columns_projected",
        "rows_total",
        "rows_selected",
    )
    return {k: max(0, int(after.get(k, 0)) - int(before.get(k, 0))) for k in keys}


def _diff_locks(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    int_keys = ("acquires", "conflicts", "retries", "timeouts")
    out: Dict[str, Any] = {}
    for kind, st in after.items():
        bst = before.get(kind, {})
        row: Dict[str, Any] = {k: max(0, int(st.get(k, 0)) - int(bst.get(k, 0))) for k in int_keys}
        lat = _diff_latency(st.get("wait_latency", {}), bst.get("wait_latency", {}))
        if any(row[k] > 0 for k in int_keys) or lat["count"] > 0:
            row["wait_latency"] = lat
            out[kind] = row
    return out


def _diff_boundary(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for phase, st in after.items():
        bst = before.get(phase, {})
        count = max(0, int(st.get("count", 0)) - int(bst.get("count", 0)))
        if count > 0:
            out[phase] = {
                "count": count,
                "latency": _diff_latency(st.get("latency", {}), bst.get("latency", {})),
            }
    return out


def _diff_processing(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    clauses_out: Dict[str, Any] = {}
    b_clauses = before.get("clauses", {})
    for clause, st in after.get("clauses", {}).items():
        bst = b_clauses.get(clause, {})
        execs = max(0, int(st.get("executions", 0)) - int(bst.get("executions", 0)))
        if execs > 0:
            clauses_out[clause] = {
                "executions": execs,
                "rows_in": max(0, int(st.get("rows_in", 0)) - int(bst.get("rows_in", 0))),
                "rows_out": max(0, int(st.get("rows_out", 0)) - int(bst.get("rows_out", 0))),
                "latency": _diff_latency(st.get("latency", {}), bst.get("latency", {})),
            }
    aa = after.get("admission", {})
    ba = before.get("admission", {})
    admission_out = {
        "units_admitted": max(0, int(aa.get("units_admitted", 0)) - int(ba.get("units_admitted", 0))),
        "residency_waits": max(0, int(aa.get("residency_waits", 0)) - int(ba.get("residency_waits", 0))),
        "segments_dispatched": max(0, int(aa.get("segments_dispatched", 0)) - int(ba.get("segments_dispatched", 0))),
        "read_window_waits": max(0, int(aa.get("read_window_waits", 0)) - int(ba.get("read_window_waits", 0))),
        "residency_wait_latency": _diff_latency(
            aa.get("residency_wait_latency", {}), ba.get("residency_wait_latency", {})
        ),
        "read_window_wait_latency": _diff_latency(
            aa.get("read_window_wait_latency", {}), ba.get("read_window_wait_latency", {})
        ),
    }
    return {"clauses": clauses_out, "admission": admission_out}


def _diff_stream(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    comp_out: Dict[str, Any] = {}
    b_comp = before.get("compactions", {})
    for kind, st in after.get("compactions", {}).items():
        bst = b_comp.get(kind, {})
        cnt = max(0, int(st.get("count", 0)) - int(bst.get("count", 0)))
        if cnt > 0:
            comp_out[kind] = {
                "count": cnt,
                "input_segments": max(0, int(st.get("input_segments", 0)) - int(bst.get("input_segments", 0))),
                "output_segments": max(0, int(st.get("output_segments", 0)) - int(bst.get("output_segments", 0))),
                "latency": _diff_latency(st.get("latency", {}), bst.get("latency", {})),
            }
    conf_out: Dict[str, int] = {}
    b_conf = before.get("conflicts", {})
    for kind, val in after.get("conflicts", {}).items():
        delta = max(0, int(val) - int(b_conf.get(kind, 0)))
        if delta > 0:
            conf_out[kind] = delta
    return {
        "data_segments_written": max(
            0, int(after.get("data_segments_written", 0)) - int(before.get("data_segments_written", 0))
        ),
        "staged_segments_written": max(
            0, int(after.get("staged_segments_written", 0)) - int(before.get("staged_segments_written", 0))
        ),
        "small_segments_written": max(
            0, int(after.get("small_segments_written", 0)) - int(before.get("small_segments_written", 0))
        ),
        "rows_per_segment": _diff_row_count_histogram(
            after.get("rows_per_segment", {}), before.get("rows_per_segment", {})
        ),
        "incomplete_chain_loads": max(
            0, int(after.get("incomplete_chain_loads", 0)) - int(before.get("incomplete_chain_loads", 0))
        ),
        "incomplete_chain_hops": max(
            0, int(after.get("incomplete_chain_hops", 0)) - int(before.get("incomplete_chain_hops", 0))
        ),
        "incomplete_iteration_loads": max(
            0, int(after.get("incomplete_iteration_loads", 0)) - int(before.get("incomplete_iteration_loads", 0))
        ),
        "incomplete_iteration_segments": max(
            0, int(after.get("incomplete_iteration_segments", 0)) - int(before.get("incomplete_iteration_segments", 0))
        ),
        "compactions": comp_out,
        "conflicts": conf_out,
    }


def _diff_resources(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    al = after.get("lmdb", {})
    bl = before.get("lmdb", {})
    return {
        "cpu_pool": dict(after.get("cpu_pool", _empty_pool_gauge())),
        "io_pool": dict(after.get("io_pool", _empty_pool_gauge())),
        "write_semaphore_in_flight": int(after.get("write_semaphore_in_flight", 0)),
        "write_semaphore_limit": int(after.get("write_semaphore_limit", 0)),
        "version_map_entries": int(after.get("version_map_entries", 0)),
        "config_cache_libraries": int(after.get("config_cache_libraries", 0)),
        "config_cache_storages": int(after.get("config_cache_storages", 0)),
        "lmdb": {
            "open_envs": int(al.get("open_envs", 0)),
            "map_size_bytes": int(al.get("map_size_bytes", 0)),
            "used_bytes": int(al.get("used_bytes", 0)),
            "active_readers": int(al.get("active_readers", 0)),
            "map_full_errors": max(0, int(al.get("map_full_errors", 0)) - int(bl.get("map_full_errors", 0))),
        },
    }


class QueryStatsReport:
    """
    Structured snapshot of collected query statistics.

    Populated live while inside a :func:`collect` block (via :meth:`snapshot`)
    and frozen automatically when the ``with collect() as report:`` block exits.

    Attributes
    ----------
    wall_time_ms : float
        Wall-clock duration of the collection scope in milliseconds.
    storage : Dict[str, Any]
        Storage operations nested as ``backend -> op -> key_type -> stats``.
    http : Dict[str, Any]
        HTTP transport metrics nested as ``service -> operation -> stats``.
    api : Dict[str, Any]
        Top-level Library API RED metrics nested as ``op -> {ok, error, count, latency, error_latency}``.
    by_library : Dict[str, Any]
        Per-library breakdown of ``storage``, ``http``, ``api``, ``by_symbol``, and ``dropped_symbols``.
    by_symbol : Dict[str, Any]
        Per-symbol rollup (populated only when ``by_symbol=True``).
    dropped_symbols : int
        Estimated count of distinct symbols spilled into ``"__other__"`` after reaching ``max_symbols``.
    thread_pools : Dict[str, Any]
        CPU/IO thread pool task counts, queue-wait and execution latencies, and semaphore backpressure waits.
    caches : Dict[str, Any]
        Hit/miss counters for ``version_map``, ``dedup_map``, and ``config_cache``.
    codec : Dict[str, Any]
        Encode/decode counts, compressed vs uncompressed byte totals, compression ratios, and latencies.
    query : Dict[str, Any]
        Index/column-stats segment pruning, column projection, and row selection statistics.
    locks : Dict[str, Any]
        Acquisition, conflict, retry, timeout, and wait latency statistics per lock kind.
    boundary : Dict[str, Any]
        Python/Arrow boundary and GIL wait overhead nested as ``phase -> {count, latency}``.
    processing : Dict[str, Any]
        QueryBuilder per-clause execution/rows/latency and read-admission residency/window backpressure stats.
    stream : Dict[str, Any]
        Write/append pathologies: segment row-size histogram, small segments, incomplete chain hops,
        compactions (incomplete/sort_merge/defragment/symbol_list), and write conflicts.
    resources : Dict[str, Any]
        Live point-in-time resource gauges (CPU/IO pool queues & threads, compaction write semaphore,
        VersionMap & ConfigCache sizes, LMDB map utilization & readers) and scoped LMDB map-full errors.
    """

    def __init__(self, data: Optional[Dict[str, Any]] = None, session: Optional[Any] = None) -> None:
        self._session = session
        self._data: Dict[str, Any] = (
            data
            if data is not None
            else {
                "wall_time_ms": 0.0,
                "storage": {},
                "http": {},
                "api": {},
                "by_library": {},
                "by_symbol": {},
                "dropped_symbols": 0,
                "thread_pools": _empty_thread_pools(),
                "caches": _empty_caches(),
                "codec": _empty_codec(),
                "query": _empty_query(),
                "locks": {},
                "boundary": {},
                "processing": _empty_processing(),
                "stream": _empty_stream(),
                "resources": _empty_resources(),
            }
        )

    def _freeze(self) -> None:
        if self._session is not None:
            self._data = self._session.snapshot()
            self._session = None

    def snapshot(self) -> "QueryStatsReport":
        """
        Return a frozen :class:`QueryStatsReport` of statistics collected so far.

        Can be called mid-scope while inside ``with collect() as report:`` to take
        incremental checkpoints for :meth:`diff`.
        """
        if self._session is not None:
            return QueryStatsReport(data=self._session.snapshot())
        return QueryStatsReport(data=self.to_dict())

    def _current(self) -> Dict[str, Any]:
        if self._session is not None:
            return self._session.snapshot()
        return self._data

    @property
    def wall_time_ms(self) -> float:
        return float(self._current().get("wall_time_ms", 0.0))

    @property
    def storage(self) -> Dict[str, Any]:
        return self._current().get("storage", {})

    @property
    def http(self) -> Dict[str, Any]:
        return self._current().get("http", {})

    @property
    def api(self) -> Dict[str, Any]:
        return self._current().get("api", {})

    @property
    def by_library(self) -> Dict[str, Any]:
        return self._current().get("by_library", {})

    @property
    def by_symbol(self) -> Dict[str, Any]:
        return self._current().get("by_symbol", {})

    @property
    def dropped_symbols(self) -> int:
        return int(self._current().get("dropped_symbols", 0))

    @property
    def thread_pools(self) -> Dict[str, Any]:
        return self._current().get("thread_pools", _empty_thread_pools())

    @property
    def caches(self) -> Dict[str, Any]:
        return self._current().get("caches", _empty_caches())

    @property
    def codec(self) -> Dict[str, Any]:
        return self._current().get("codec", _empty_codec())

    @property
    def query(self) -> Dict[str, Any]:
        return self._current().get("query", _empty_query())

    @property
    def locks(self) -> Dict[str, Any]:
        return self._current().get("locks", {})

    @property
    def boundary(self) -> Dict[str, Any]:
        return self._current().get("boundary", {})

    @property
    def processing(self) -> Dict[str, Any]:
        return self._current().get("processing", _empty_processing())

    @property
    def stream(self) -> Dict[str, Any]:
        return self._current().get("stream", _empty_stream())

    @property
    def resources(self) -> Dict[str, Any]:
        return self._current().get("resources", _empty_resources())

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain dictionary representation of the report."""
        cur = self._current()
        return {
            "wall_time_ms": float(cur.get("wall_time_ms", 0.0)),
            "storage": cur.get("storage", {}),
            "http": cur.get("http", {}),
            "api": cur.get("api", {}),
            "by_library": cur.get("by_library", {}),
            "by_symbol": cur.get("by_symbol", {}),
            "dropped_symbols": int(cur.get("dropped_symbols", 0)),
            "thread_pools": cur.get("thread_pools", _empty_thread_pools()),
            "caches": cur.get("caches", _empty_caches()),
            "codec": cur.get("codec", _empty_codec()),
            "query": cur.get("query", _empty_query()),
            "locks": cur.get("locks", {}),
            "boundary": cur.get("boundary", {}),
            "processing": cur.get("processing", _empty_processing()),
            "stream": cur.get("stream", _empty_stream()),
            "resources": cur.get("resources", _empty_resources()),
        }

    def diff(self, earlier: "QueryStatsReport") -> "QueryStatsReport":
        """
        Return a new :class:`QueryStatsReport` representing ``self - earlier``.

        Parameters
        ----------
        earlier : QueryStatsReport
            An earlier snapshot from the same collector scope.

        Returns
        -------
        QueryStatsReport
            Delta statistics between ``earlier`` and ``self``.
        """
        a = self._current()
        b = earlier._current()
        by_lib_out: Dict[str, Any] = {}
        b_libs = b.get("by_library", {})
        for lib, ldata in a.get("by_library", {}).items():
            b_ldata = b_libs.get(lib, {})
            l_storage = _diff_storage_map(ldata.get("storage", {}), b_ldata.get("storage", {}))
            l_http = _diff_http_map(ldata.get("http", {}), b_ldata.get("http", {}))
            l_api = _diff_api_map(ldata.get("api", {}), b_ldata.get("api", {}))
            l_sym = _diff_symbol_map(ldata.get("by_symbol", {}), b_ldata.get("by_symbol", {}))
            l_drop = max(0, int(ldata.get("dropped_symbols", 0)) - int(b_ldata.get("dropped_symbols", 0)))
            if l_storage or l_http or l_api or l_sym:
                by_lib_out[lib] = {
                    "storage": l_storage,
                    "http": l_http,
                    "api": l_api,
                    "by_symbol": l_sym,
                    "dropped_symbols": l_drop,
                }
        return QueryStatsReport(
            data={
                "wall_time_ms": max(0.0, float(a.get("wall_time_ms", 0.0)) - float(b.get("wall_time_ms", 0.0))),
                "storage": _diff_storage_map(a.get("storage", {}), b.get("storage", {})),
                "http": _diff_http_map(a.get("http", {}), b.get("http", {})),
                "api": _diff_api_map(a.get("api", {}), b.get("api", {})),
                "by_library": by_lib_out,
                "by_symbol": _diff_symbol_map(a.get("by_symbol", {}), b.get("by_symbol", {})),
                "dropped_symbols": max(0, int(a.get("dropped_symbols", 0)) - int(b.get("dropped_symbols", 0))),
                "thread_pools": _diff_thread_pools(a.get("thread_pools", {}), b.get("thread_pools", {})),
                "caches": _diff_caches(a.get("caches", {}), b.get("caches", {})),
                "codec": _diff_codec(a.get("codec", {}), b.get("codec", {})),
                "query": _diff_query(a.get("query", {}), b.get("query", {})),
                "locks": _diff_locks(a.get("locks", {}), b.get("locks", {})),
                "boundary": _diff_boundary(a.get("boundary", {}), b.get("boundary", {})),
                "processing": _diff_processing(a.get("processing", {}), b.get("processing", {})),
                "stream": _diff_stream(a.get("stream", {}), b.get("stream", {})),
                "resources": _diff_resources(a.get("resources", {}), b.get("resources", {})),
            }
        )

    def top_symbols(
        self,
        by: str = "storage_time_ms",
        n: int = 10,
        library: Optional[str] = None,
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Return the top ``n`` symbols sorted descending by ``by``.

        Parameters
        ----------
        by : str, default="storage_time_ms"
            Metric to sort by. Supported values: ``"storage_time_ms"``, ``"bytes_read"``,
            ``"bytes_written"``, ``"reads"``, ``"writes"``, ``"deletes"``, ``"errors"``,
            ``"total_ops"``, ``"http_requests"``, ``"http_retries"``.
        n : int, default=10
            Maximum number of entries to return.
        library : str, optional
            If given, rank symbols within that specific library.

        Returns
        -------
        List[Tuple[str, Dict[str, Any]]]
            Sorted ``(symbol, stats_dict)`` pairs.
        """
        cur = self._current()
        if library is not None:
            sym_map = cur.get("by_library", {}).get(library, {}).get("by_symbol", {})
        else:
            sym_map = cur.get("by_symbol", {})

        def _sort_key(item: Tuple[str, Dict[str, Any]]) -> Tuple[float, str]:
            sym, st = item
            if by == "storage_time_ms":
                val = float(st.get("storage_latency", {}).get("total_ms", 0.0))
            elif by in st:
                val = float(st[by])
            else:
                raise UserInputException(f"Unsupported top_symbols sort key: {by!r}")
            return (-val, sym)

        return sorted(sym_map.items(), key=_sort_key)[: max(0, n)]

    def to_dataframe(self, section: str = "storage") -> pd.DataFrame:
        """
        Convert a section of the report into a flat :class:`pandas.DataFrame`.

        Parameters
        ----------
        section : str, default="storage"
            One of ``"storage"``, ``"http"``, ``"api"``, ``"boundary"``, ``"processing"``,
            ``"by_symbol"``, ``"thread_pools"``, or ``"locks"``.

        Returns
        -------
        pandas.DataFrame
            Tabular view of the requested section.
        """
        cur = self._current()
        rows: List[Dict[str, Any]] = []
        if section == "storage":
            for backend, ops in cur.get("storage", {}).items():
                for op, kts in ops.items():
                    for kt, st in kts.items():
                        lat = st.get("latency", {})
                        rows.append(
                            {
                                "backend": backend,
                                "op": op,
                                "key_type": kt,
                                "ok": st["ok"],
                                "not_found": st["not_found"],
                                "conflict": st["conflict"],
                                "unsupported": st["unsupported"],
                                "error": st["error"],
                                "bytes": st["bytes"],
                                "items": st["items"],
                                "pages": st["pages"],
                                "total_ms": lat.get("total_ms", 0.0),
                                "mean_ms": lat.get("mean_ms", 0.0),
                                "p50_ms": lat.get("p50_ms", 0.0),
                                "p95_ms": lat.get("p95_ms", 0.0),
                                "p99_ms": lat.get("p99_ms", 0.0),
                                "max_ms": lat.get("max_ms", 0.0),
                            }
                        )
        elif section == "http":
            for svc, ops in cur.get("http", {}).items():
                for op, st in ops.items():
                    req_lat = st.get("request_latency", {})
                    att_lat = st.get("attempt_latency", {})
                    rows.append(
                        {
                            "service": svc,
                            "operation": op,
                            "requests": st["requests"],
                            "attempts": st["attempts"],
                            "retries": st["retries"],
                            "status_2xx": st["status_2xx"],
                            "status_3xx": st["status_3xx"],
                            "status_4xx": st["status_4xx"],
                            "status_5xx": st["status_5xx"],
                            "throttled": st.get("throttled", 0),
                            "network_errors": st["network_errors"],
                            "checksum_mismatch_retries": st["checksum_mismatch_retries"],
                            "new_connections": st["new_connections"],
                            "backoff_time_ms": st["backoff_time_ms"],
                            "request_total_ms": req_lat.get("total_ms", 0.0),
                            "request_p95_ms": req_lat.get("p95_ms", 0.0),
                            "attempt_p95_ms": att_lat.get("p95_ms", 0.0),
                            "dns_p95_ms": st.get("dns_latency", {}).get("p95_ms", 0.0),
                            "tcp_p95_ms": st.get("tcp_connect_latency", {}).get("p95_ms", 0.0),
                            "tls_p95_ms": st.get("tls_handshake_latency", {}).get("p95_ms", 0.0),
                            "ttfb_p95_ms": st.get("ttfb_latency", {}).get("p95_ms", 0.0),
                            "transfer_p95_ms": st.get("transfer_latency", {}).get("p95_ms", 0.0),
                        }
                    )
        elif section == "api":
            for op, st in cur.get("api", {}).items():
                lat = st.get("latency", {})
                err_lat = st.get("error_latency", {})
                rows.append(
                    {
                        "op": op,
                        "ok": st.get("ok", 0),
                        "error": st.get("error", 0),
                        "count": st.get("count", 0),
                        "total_ms": lat.get("total_ms", 0.0),
                        "mean_ms": lat.get("mean_ms", 0.0),
                        "p50_ms": lat.get("p50_ms", 0.0),
                        "p95_ms": lat.get("p95_ms", 0.0),
                        "p99_ms": lat.get("p99_ms", 0.0),
                        "max_ms": lat.get("max_ms", 0.0),
                        "error_p95_ms": err_lat.get("p95_ms", 0.0),
                    }
                )
        elif section == "by_symbol":
            for sym, st in cur.get("by_symbol", {}).items():
                lat = st.get("storage_latency", {})
                rows.append(
                    {
                        "symbol": sym,
                        "reads": st["reads"],
                        "writes": st["writes"],
                        "deletes": st["deletes"],
                        "exists": st["exists"],
                        "lists": st["lists"],
                        "errors": st["errors"],
                        "total_ops": st["total_ops"],
                        "bytes_read": st["bytes_read"],
                        "bytes_written": st["bytes_written"],
                        "http_requests": st["http_requests"],
                        "http_retries": st["http_retries"],
                        "storage_total_ms": lat.get("total_ms", 0.0),
                        "storage_p95_ms": lat.get("p95_ms", 0.0),
                    }
                )
        elif section == "thread_pools":
            tp = cur.get("thread_pools", {})
            for pool in ("cpu", "io"):
                pst = tp.get(pool, {})
                if pst.get("tasks", 0) or pst.get("expired", 0):
                    wlat = pst.get("wait_latency", {})
                    rlat = pst.get("run_latency", {})
                    rows.append(
                        {
                            "pool": pool,
                            "tasks": pst.get("tasks", 0),
                            "expired": pst.get("expired", 0),
                            "wait_total_ms": wlat.get("total_ms", 0.0),
                            "wait_p95_ms": wlat.get("p95_ms", 0.0),
                            "run_total_ms": rlat.get("total_ms", 0.0),
                            "run_p95_ms": rlat.get("p95_ms", 0.0),
                        }
                    )
        elif section == "locks":
            for kind, st in cur.get("locks", {}).items():
                lat = st.get("wait_latency", {})
                rows.append(
                    {
                        "lock": kind,
                        "acquires": st.get("acquires", 0),
                        "conflicts": st.get("conflicts", 0),
                        "retries": st.get("retries", 0),
                        "timeouts": st.get("timeouts", 0),
                        "wait_total_ms": lat.get("total_ms", 0.0),
                        "wait_p95_ms": lat.get("p95_ms", 0.0),
                    }
                )
        elif section == "boundary":
            for phase, st in cur.get("boundary", {}).items():
                lat = st.get("latency", {})
                rows.append(
                    {
                        "phase": phase,
                        "count": st.get("count", 0),
                        "total_ms": lat.get("total_ms", 0.0),
                        "mean_ms": lat.get("mean_ms", 0.0),
                        "p50_ms": lat.get("p50_ms", 0.0),
                        "p95_ms": lat.get("p95_ms", 0.0),
                        "p99_ms": lat.get("p99_ms", 0.0),
                        "max_ms": lat.get("max_ms", 0.0),
                    }
                )
        elif section == "processing":
            for clause, st in cur.get("processing", {}).get("clauses", {}).items():
                lat = st.get("latency", {})
                rows.append(
                    {
                        "clause": clause,
                        "executions": st.get("executions", 0),
                        "rows_in": st.get("rows_in", 0),
                        "rows_out": st.get("rows_out", 0),
                        "total_ms": lat.get("total_ms", 0.0),
                        "mean_ms": lat.get("mean_ms", 0.0),
                        "p50_ms": lat.get("p50_ms", 0.0),
                        "p95_ms": lat.get("p95_ms", 0.0),
                        "p99_ms": lat.get("p99_ms", 0.0),
                        "max_ms": lat.get("max_ms", 0.0),
                    }
                )
        elif section == "stream":
            stream = cur.get("stream", _empty_stream())
            for kind, st in stream.get("compactions", {}).items():
                lat = st.get("latency", {})
                rows.append(
                    {
                        "compaction": kind,
                        "count": st.get("count", 0),
                        "input_segments": st.get("input_segments", 0),
                        "output_segments": st.get("output_segments", 0),
                        "total_ms": lat.get("total_ms", 0.0),
                        "mean_ms": lat.get("mean_ms", 0.0),
                        "p50_ms": lat.get("p50_ms", 0.0),
                        "p95_ms": lat.get("p95_ms", 0.0),
                        "p99_ms": lat.get("p99_ms", 0.0),
                        "max_ms": lat.get("max_ms", 0.0),
                    }
                )
        else:
            raise UserInputException(
                f"Unsupported section: {section!r}. "
                "Expected 'storage', 'http', 'api', 'boundary', 'processing', 'stream', "
                "'by_symbol', 'thread_pools', or 'locks'."
            )
        return pd.DataFrame(rows)

    def summary(self) -> str:
        """Return a concise human-readable multi-line summary of the report."""
        cur = self._current()
        lines: List[str] = [f"QueryStatsReport (wall={cur.get('wall_time_ms', 0.0):.2f} ms)"]

        api = cur.get("api", {})
        if api:
            lines.append("API:")
            for op, st in sorted(api.items()):
                lat = st.get("latency", {})
                err_str = f" error={st['error']}" if st.get("error", 0) else ""
                lines.append(
                    f"  {op}: ok={st['ok']}{err_str} total={lat.get('total_ms', 0.0):.2f}ms "
                    f"p95={lat.get('p95_ms', 0.0):.2f}ms"
                )

        storage = cur.get("storage", {})
        if storage:
            lines.append("Storage:")
            for backend, ops in sorted(storage.items()):
                for op, kts in sorted(ops.items()):
                    for kt, st in sorted(kts.items()):
                        lat = st.get("latency", {})
                        parts = [
                            f"ok={st['ok']}",
                            f"not_found={st['not_found']}" if st["not_found"] else "",
                            f"conflict={st['conflict']}" if st["conflict"] else "",
                            f"unsupported={st['unsupported']}" if st["unsupported"] else "",
                            f"error={st['error']}" if st["error"] else "",
                            f"bytes={st['bytes']}" if st["bytes"] else "",
                            f"pages={st['pages']}" if st["pages"] else "",
                            f"total={lat.get('total_ms', 0.0):.2f}ms",
                            f"p95={lat.get('p95_ms', 0.0):.2f}ms",
                        ]
                        detail = " ".join(p for p in parts if p)
                        lines.append(f"  {backend}.{op}[{kt}]: {detail}")

        http = cur.get("http", {})
        if http:
            lines.append("HTTP:")
            for svc, ops in sorted(http.items()):
                for op, st in sorted(ops.items()):
                    req_lat = st.get("request_latency", {})
                    ttfb_lat = st.get("ttfb_latency", {})
                    throttled_str = f" throttled={st['throttled']}" if st.get("throttled", 0) else ""
                    ttfb_str = f" ttfb_p95={ttfb_lat.get('p95_ms', 0.0):.2f}ms" if ttfb_lat.get("count", 0) else ""
                    lines.append(
                        f"  {svc}.{op}: req={st['requests']} att={st['attempts']} "
                        f"retries={st['retries']} 2xx={st['status_2xx']} 4xx={st['status_4xx']} "
                        f"5xx={st['status_5xx']}{throttled_str} net_err={st['network_errors']} "
                        f"new_conn={st['new_connections']} backoff={st['backoff_time_ms']:.2f}ms "
                        f"p95={req_lat.get('p95_ms', 0.0):.2f}ms{ttfb_str}"
                    )

        by_lib = cur.get("by_library", {})
        if by_lib:
            lines.append(f"Libraries ({len(by_lib)}): {', '.join(sorted(by_lib.keys()))}")

        by_sym = cur.get("by_symbol", {})
        if by_sym:
            lines.append(f"Top symbols ({len(by_sym)}, dropped={cur.get('dropped_symbols', 0)}):")
            for sym, st in self.top_symbols(by="storage_time_ms", n=5):
                lat = st.get("storage_latency", {})
                lines.append(
                    f"  {sym}: ops={st['total_ops']} r={st['reads']} w={st['writes']} "
                    f"bytes_r={st['bytes_read']} bytes_w={st['bytes_written']} "
                    f"time={lat.get('total_ms', 0.0):.2f}ms"
                )

        tp = cur.get("thread_pools", {})
        tp_lines: List[str] = []
        for pool in ("cpu", "io"):
            pst = tp.get(pool, {})
            if pst.get("tasks", 0) or pst.get("expired", 0):
                wlat = pst.get("wait_latency", {})
                rlat = pst.get("run_latency", {})
                tp_lines.append(
                    f"  {pool}: tasks={pst['tasks']} expired={pst['expired']} "
                    f"wait_p95={wlat.get('p95_ms', 0.0):.2f}ms run_total={rlat.get('total_ms', 0.0):.2f}ms"
                )
        if tp.get("semaphore_waits", 0):
            slat = tp.get("semaphore_wait_latency", {})
            tp_lines.append(
                f"  semaphore: waits={tp['semaphore_waits']} total={slat.get('total_ms', 0.0):.2f}ms "
                f"p95={slat.get('p95_ms', 0.0):.2f}ms"
            )
        if tp_lines:
            lines.append("Thread Pools:")
            lines.extend(tp_lines)

        caches = cur.get("caches", {})
        cache_lines: List[str] = []
        vm = caches.get("version_map", {})
        if any(vm.get(k, 0) for k in ("hits", "misses", "reloads", "ref_retries", "chain_hops")):
            cache_lines.append(
                f"  version_map: hits={vm.get('hits', 0)} misses={vm.get('misses', 0)} "
                f"reloads={vm.get('reloads', 0)} chain_hops={vm.get('chain_hops', 0)} "
                f"ref_retries={vm.get('ref_retries', 0)}"
            )
        dd = caches.get("dedup_map", {})
        if any(dd.get(k, 0) for k in ("hits", "misses", "bytes_saved")):
            cache_lines.append(
                f"  dedup_map: hits={dd.get('hits', 0)} misses={dd.get('misses', 0)} "
                f"bytes_saved={dd.get('bytes_saved', 0)}"
            )
        cc = caches.get("config_cache", {})
        if cc.get("hits", 0) or cc.get("misses", 0):
            cache_lines.append(f"  config_cache: hits={cc.get('hits', 0)} misses={cc.get('misses', 0)}")
        if cache_lines:
            lines.append("Caches:")
            lines.extend(cache_lines)

        codec = cur.get("codec", {})
        if codec.get("encodes", 0) or codec.get("decodes", 0):
            lines.append("Codec:")
            if codec.get("encodes", 0):
                elat = codec.get("encode_latency", {})
                lines.append(
                    f"  encode: count={codec['encodes']} raw={codec['encode_uncompressed_bytes']}B "
                    f"compressed={codec['encode_compressed_bytes']}B "
                    f"ratio={codec.get('encode_compression_ratio', 0.0):.2f}x "
                    f"total={elat.get('total_ms', 0.0):.2f}ms"
                )
            if codec.get("decodes", 0):
                dlat = codec.get("decode_latency", {})
                lines.append(
                    f"  decode: count={codec['decodes']} compressed={codec['decode_compressed_bytes']}B "
                    f"raw={codec['decode_uncompressed_bytes']}B "
                    f"ratio={codec.get('decode_compression_ratio', 0.0):.2f}x "
                    f"total={dlat.get('total_ms', 0.0):.2f}ms"
                )

        query = cur.get("query", {})
        if query.get("queries", 0):
            lines.append(
                f"Query: queries={query['queries']} "
                f"segments={query['segments_read']}/{query['segments_total']} "
                f"(pruned_index={query['segments_pruned_index']} pruned_col_stats={query['segments_pruned_column_stats']}) "
                f"columns={query['columns_projected']}/{query['columns_total']} "
                f"rows={query['rows_selected']}/{query['rows_total']}"
            )

        locks = cur.get("locks", {})
        if locks:
            lines.append("Locks:")
            for kind, st in sorted(locks.items()):
                lat = st.get("wait_latency", {})
                lines.append(
                    f"  {kind}: acquires={st['acquires']} conflicts={st['conflicts']} "
                    f"retries={st['retries']} timeouts={st['timeouts']} "
                    f"wait_total={lat.get('total_ms', 0.0):.2f}ms"
                )

        boundary = cur.get("boundary", {})
        if boundary:
            lines.append("Boundary:")
            for phase, st in sorted(boundary.items()):
                lat = st.get("latency", {})
                lines.append(
                    f"  {phase}: count={st['count']} total={lat.get('total_ms', 0.0):.2f}ms "
                    f"p95={lat.get('p95_ms', 0.0):.2f}ms"
                )

        proc = cur.get("processing", {})
        proc_clauses = proc.get("clauses", {})
        adm = proc.get("admission", {})
        if proc_clauses or adm.get("units_admitted", 0) or adm.get("segments_dispatched", 0):
            lines.append("Processing:")
            for clause, st in sorted(proc_clauses.items()):
                lat = st.get("latency", {})
                lines.append(
                    f"  clause.{clause}: execs={st['executions']} rows={st['rows_in']}->{st['rows_out']} "
                    f"total={lat.get('total_ms', 0.0):.2f}ms p95={lat.get('p95_ms', 0.0):.2f}ms"
                )
            if adm.get("units_admitted", 0) or adm.get("segments_dispatched", 0):
                rlat = adm.get("residency_wait_latency", {})
                wlat = adm.get("read_window_wait_latency", {})
                lines.append(
                    f"  admission: units={adm.get('units_admitted', 0)} "
                    f"residency_waits={adm.get('residency_waits', 0)} (p95={rlat.get('p95_ms', 0.0):.2f}ms) "
                    f"segments={adm.get('segments_dispatched', 0)} "
                    f"read_window_waits={adm.get('read_window_waits', 0)} (p95={wlat.get('p95_ms', 0.0):.2f}ms)"
                )

        stream = cur.get("stream", {})
        rps = stream.get("rows_per_segment", {})
        compactions = stream.get("compactions", {})
        conflicts = stream.get("conflicts", {})
        if (
            stream.get("data_segments_written", 0)
            or stream.get("staged_segments_written", 0)
            or stream.get("incomplete_chain_loads", 0)
            or stream.get("incomplete_iteration_loads", 0)
            or compactions
            or conflicts
        ):
            lines.append("Stream:")
            if stream.get("data_segments_written", 0) or stream.get("staged_segments_written", 0):
                lines.append(
                    f"  segments: data={stream.get('data_segments_written', 0)} "
                    f"staged={stream.get('staged_segments_written', 0)} "
                    f"small={stream.get('small_segments_written', 0)} "
                    f"rows_p50={rps.get('p50', 0)} rows_p95={rps.get('p95', 0)} rows_max={rps.get('max', 0)}"
                )
            if stream.get("incomplete_chain_loads", 0) or stream.get("incomplete_iteration_loads", 0):
                lines.append(
                    f"  incomplete_loads: list={stream.get('incomplete_chain_loads', 0)} "
                    f"(hops={stream.get('incomplete_chain_hops', 0)}) "
                    f"iter={stream.get('incomplete_iteration_loads', 0)} "
                    f"(segs={stream.get('incomplete_iteration_segments', 0)})"
                )
            for kind, st in sorted(compactions.items()):
                lat = st.get("latency", {})
                lines.append(
                    f"  compaction.{kind}: count={st['count']} segs={st['input_segments']}->{st['output_segments']} "
                    f"total={lat.get('total_ms', 0.0):.2f}ms"
                )
            if conflicts:
                conf_str = " ".join(f"{k}={v}" for k, v in sorted(conflicts.items()))
                lines.append(f"  conflicts: {conf_str}")

        res = cur.get("resources", {})
        cpu_g = res.get("cpu_pool", {})
        io_g = res.get("io_pool", {})
        lmdb_g = res.get("lmdb", {})
        if (
            cpu_g.get("max_threads", 0)
            or io_g.get("max_threads", 0)
            or res.get("version_map_entries", 0)
            or res.get("config_cache_libraries", 0)
            or lmdb_g.get("open_envs", 0)
            or lmdb_g.get("map_full_errors", 0)
        ):
            lines.append("Resources:")
            if cpu_g.get("max_threads", 0) or io_g.get("max_threads", 0):
                lines.append(
                    f"  pools: cpu(pending={cpu_g.get('pending_tasks', 0)} "
                    f"threads={cpu_g.get('active_threads', 0)}/{cpu_g.get('max_threads', 0)}) "
                    f"io(pending={io_g.get('pending_tasks', 0)} "
                    f"threads={io_g.get('active_threads', 0)}/{io_g.get('max_threads', 0)})"
                )
            lines.append(
                f"  caches: version_map={res.get('version_map_entries', 0)} "
                f"config_libs={res.get('config_cache_libraries', 0)} "
                f"config_storages={res.get('config_cache_storages', 0)} "
                f"write_sem={res.get('write_semaphore_in_flight', 0)}/{res.get('write_semaphore_limit', 0)}"
            )
            if lmdb_g.get("open_envs", 0) or lmdb_g.get("map_full_errors", 0):
                lines.append(
                    f"  lmdb: open_envs={lmdb_g.get('open_envs', 0)} "
                    f"used={lmdb_g.get('used_bytes', 0)}/{lmdb_g.get('map_size_bytes', 0)}B "
                    f"readers={lmdb_g.get('active_readers', 0)} map_full_errors={lmdb_g.get('map_full_errors', 0)}"
                )

        return "\n".join(lines)

    def __repr__(self) -> str:
        return self.summary()


@contextmanager
def collect(
    *,
    by_library: bool = True,
    by_symbol: bool = False,
    max_symbols: int = 1000,
    isolate: bool = False,
) -> Iterator[QueryStatsReport]:
    """
    Scoped, thread-isolated context manager for collecting structured query statistics.

    Stats are scoped to the calling thread and any ArcticDB CPU/IO/async worker tasks
    spawned from within this block. Concurrent operations on unrelated threads are excluded.
    Nested ``collect()`` blocks are supported; pass ``isolate=True`` to prevent an inner
    block from also contributing to enclosing collectors.

    Parameters
    ----------
    by_library : bool, default=True
        Attribute storage and HTTP stats per library name.
    by_symbol : bool, default=False
        Attribute storage and HTTP stats per symbol name (opt-in to bound memory on
        high-cardinality workloads).
    max_symbols : int, default=1000
        Maximum distinct symbol entries tracked when ``by_symbol=True`` before spilling
        additional symbols into ``"__other__"`` and incrementing ``dropped_symbols``.
    isolate : bool, default=False
        When ``True``, operations inside this scope do not propagate into enclosing
        ``collect()`` blocks or the legacy global ``query_stats`` collector.

    Yields
    ------
    QueryStatsReport
        Live report handle that is automatically frozen upon exiting the context.

    Examples
    --------
    >>> with collect(by_symbol=True) as report:
    ...     lib.read("AAPL")
    >>> print(report.summary())

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
        It may change or be removed in future versions without notice.
    """
    session = qs.CollectorSession(
        by_library=by_library,
        by_symbol=by_symbol,
        max_symbols=max_symbols,
        isolate=isolate,
    )
    report = QueryStatsReport(session=session)
    session.enter()
    try:
        yield report
    finally:
        try:
            report._freeze()
        finally:
            session.exit()


@contextmanager
def query_stats() -> Iterator[None]:
    """
    Context manager for enabling global query statistics collection within a specific scope.

    When entering the context, global query statistics collection is enabled.
    When exiting the context, it is automatically disabled.
    For thread-isolated, nestable, structured collection with latency percentiles and
    HTTP / library / symbol attribution, prefer :func:`collect`.

    Raises
    ------
    UserInputException
        If global query stats is already enabled.

    Examples
    --------
    >>> with query_stats():
    ...     store.list_symbols()


    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    if qs.is_enabled():
        # This will prohibit the unsupported nested context managers usage
        raise UserInputException("Query Stats is already enabled")
    enable()
    try:
        yield
    finally:
        disable()


def get_query_stats() -> Dict[str, Any]:
    """
    Get collected global query statistics in the legacy dictionary format.

    Returns
    -------
    Dict[str, Any]:
        A dictionary containing statistics organized by operation group, task type,
        and key type.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    return qs.get_stats()


def reset_stats() -> None:
    """
    Reset all collected global query statistics.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    qs.reset_stats()


def enable() -> None:
    """
    Enable global query statistics collection.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    qs.enable()


def disable() -> None:
    """
    Disable global query statistics collection.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    qs.disable()


def configure_prometheus(
    host: str = "127.0.0.1",
    port: int = 9108,
    *,
    model: str = "pull",
    job_name: str = "arcticdb",
    instance: str = "local",
    prometheus_env: str = "local",
) -> None:
    """
    Configure ArcticDB's global ``PrometheusInstance`` in pull (HTTP ``/metrics``) or push mode.

    Parameters
    ----------
    host : str, default="127.0.0.1"
        Bind address for ``model="pull"`` (e.g. ``"0.0.0.0"`` to allow container scraping)
        or Pushgateway host for ``model="push"``.
    port : int, default=9108
        HTTP port for ``/metrics`` (pull) or Pushgateway port (push).
    model : str, default="pull"
        Either ``"pull"`` (starts an embedded HTTP ``/metrics`` exposer) or ``"push"``
        (pushes to a Prometheus Pushgateway via :func:`push_prometheus_metrics`).
    job_name : str, default="arcticdb"
        Prometheus job label (used in push mode).
    instance : str, default="local"
        Instance label (used in push mode).
    prometheus_env : str, default="local"
        Environment label (used in push mode).

    Raises
    ------
    UserInputException
        If ``model`` is not ``"pull"`` or ``"push"``.

    Examples
    --------
    >>> import arcticdb.toolbox.query_stats as qs
    >>> qs.configure_prometheus(host="127.0.0.1", port=9108, model="pull")
    >>> qs.enable_prometheus_sink(include_library=True)

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    qs.configure_prometheus(
        host=host,
        port=str(port),
        job_name=job_name,
        instance=instance,
        prometheus_env=prometheus_env,
        model=model,
    )


def push_prometheus_metrics() -> int:
    """
    Push collected Prometheus metrics to the configured Pushgateway (when ``model="push"``).

    Returns
    -------
    int
        HTTP status code returned by Pushgateway, or ``0`` if not configured in push mode.

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    return int(qs.push_prometheus_metrics())


def enable_prometheus_sink(*, include_library: bool = False) -> None:
    """
    Forward aggregate storage and HTTP metrics to ArcticDB's ``PrometheusInstance``.

    Metrics are exported with low-cardinality labels (``backend``, ``op``, ``outcome``
    for storage; ``service``, ``operation``, ``status_class`` for HTTP; plus ``library``
    when ``include_library=True``). Symbol names are never emitted as Prometheus labels.

    Parameters
    ----------
    include_library : bool, default=False
        Include a ``library`` label on exported Prometheus metrics.

    Examples
    --------
    >>> import arcticdb.toolbox.query_stats as qs
    >>> qs.enable_prometheus_sink(include_library=True)
    >>> qs.disable_prometheus_sink()

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    qs.enable_prometheus_sink(include_library)


def disable_prometheus_sink() -> None:
    """
    Stop forwarding query statistics metrics to ``PrometheusInstance``.

    Examples
    --------
    >>> import arcticdb.toolbox.query_stats as qs
    >>> qs.disable_prometheus_sink()

    Notes
    ----------
    !!! warning
        This API is unstable and not governed by ArcticDB's semantic versioning.
    """
    qs.disable_prometheus_sink()
