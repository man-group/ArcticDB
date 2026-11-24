# Query Statistics

> **Warning**: The Query Statistics API is unstable and not governed by ArcticDB's semantic versioning. It may change or be removed in future versions without notice.

ArcticDB provides a Query Statistics API (`arcticdb.toolbox.query_stats`) for measuring storage, HTTP transport, thread pool, cache, codec, query pruning, and lock performance across all storage backends (`s3`, `gcp_xml`, `nfs_backed`, `azure`, `lmdb`, `mongo`, `memory`, and `mapped_file`).

## When to Use `collect()` vs Legacy `query_stats()`

- **Prefer `qs.collect()`** for all new code. It is **thread-isolated** (tracks the calling thread and ArcticDB CPU/IO/async tasks spawned from it without picking up unrelated threads), **nestable** (with optional `isolate=True`), works across **every storage backend**, records **nanosecond latency histograms** (`p50`/`p95`/`p99`/`max`), classifies **5 storage outcomes** (`ok`, `not_found`, `conflict`, `unsupported`, `error`), tracks **HTTP transport**, **library/symbol attribution**, **thread pools**, **caches**, **codec**, **query pruning**, and **locks**.
- **Legacy `qs.query_stats()` / `qs.enable()` / `qs.get_query_stats()`** remains supported for backwards compatibility with existing scripts and tests, returning the legacy `storage_operations` dictionary for S3 and in-memory backends.

## Scoped Collection with `collect()`

```python
import arcticdb as adb
import arcticdb.toolbox.query_stats as qs

arctic = adb.Arctic(f"s3://{HOST}:{BUCKET}?access={ACCESS}&secret={SECRET}")
lib = arctic["library_name"]

with qs.collect() as report:
    df = lib.read("AAPL").data

print(report.summary())
```

### Per-Library and Per-Symbol Attribution

Library attribution is enabled by default (`by_library=True`). Symbol attribution is **opt-in** (`by_symbol=False` by default) to bound memory on high-cardinality workloads, and caps distinct symbols at `max_symbols` (default `1000`), spilling additional symbols into `"__other__"` and tracking the distinct overflow estimate in `report.dropped_symbols`:

```python
with qs.collect(by_library=True, by_symbol=True, max_symbols=500) as report:
    for sym in ["AAPL", "MSFT", "GOOG"]:
        lib.read(sym)

# Inspect top symbols by total storage time, bytes read, HTTP retries, etc.
for sym, stats in report.top_symbols(by="bytes_read", n=5):
    print(sym, stats["bytes_read"], stats["storage_latency"]["p95_ms"])

# Restrict ranking to one library
top_in_lib = report.top_symbols(by="storage_time_ms", n=5, library="library_name")
```

### Nested Scopes and Isolation

`collect()` blocks can be freely nested. By default (`isolate=False`), operations inside an inner block contribute to both the inner and enclosing reports. Pass `isolate=True` to keep an inner block's measurements strictly private (excluded from enclosing `collect()` blocks and the legacy global collector):

```python
with qs.collect() as outer:
    lib.read("A")
    with qs.collect(isolate=True) as warmup:
        lib.read("WARMUP_ONLY")
    lib.read("B")
```

### Incremental Checkpoints with `snapshot()` and `diff()`

Call `report.snapshot()` mid-scope to take immutable checkpoints and compute exact deltas with `.diff()`:

```python
with qs.collect(by_symbol=True) as live:
    lib.write("AAPL", df)
    after_write = live.snapshot()

    lib.read("AAPL")
    after_read = live.snapshot()

read_only = after_read.diff(after_write)
print(read_only.summary())
```

## Report Structure

`QueryStatsReport` exposes structured properties as well as `.to_dict()`, `.to_dataframe(section=...)`, and `.summary()`:

| Property | Structure / Key Fields |
|---|---|
| `wall_time_ms` | Wall-clock duration of the `collect()` block in milliseconds. |
| `storage` | `backend -> op -> key_type -> {ok, not_found, conflict, unsupported, error, count, bytes, items, pages, partial_delete_retries, latency, error_latency}`. Visitor callback time is excluded from `list`/`scan`/`visit_sizes` latency. |
| `http` | `service -> operation -> {requests, attempts, retries, status_2xx, status_3xx, status_4xx, status_5xx, throttled, network_errors, checksum_mismatch_retries, new_connections, backoff_time_ms, request_latency, attempt_latency, dns_latency, tcp_connect_latency, tls_handshake_latency, ttfb_latency, transfer_latency}`. Captures AWS S3/STS and Azure Blob HTTP traffic (including 429/503 throttling and libcurl phase timings). |
| `api` | `op ("read" \| "write" \| "append" \| "update" \| "delete" \| "*_batch" \| "list_symbols" \| "list_versions" \| "snapshot" \| "prune_previous_versions" \| "merge" \| "metadata" \| "compact") -> {ok, error, count, latency, error_latency}`. Top-level Library API RED metrics (Rate, Errors, Duration) with automatic nesting protection. |
| `by_library` | `library_name -> {storage, http, api, by_symbol, dropped_symbols}`. |
| `by_symbol` | `symbol_name -> {reads, writes, deletes, exists, lists, errors, total_ops, bytes_read, bytes_written, http_requests, http_retries, storage_latency}` (populated only when `by_symbol=True`). |
| `dropped_symbols` | Approximate count of distinct symbols spilled into `"__other__"` once `max_symbols` is exceeded. |
| `thread_pools` | `{cpu: {tasks, expired, wait_latency, run_latency}, io: {...}, semaphore_waits, semaphore_wait_latency}`. |
| `caches` | `{version_map: {hits, misses, reloads, ref_retries, chain_hops}, dedup_map: {hits, misses, bytes_saved}, config_cache: {hits, misses}}`. |
| `codec` | `{encodes, decodes, encode_uncompressed_bytes, encode_compressed_bytes, decode_compressed_bytes, decode_uncompressed_bytes, encode_compression_ratio, decode_compression_ratio, encode_latency, decode_latency}`. |
| `query` | `{queries, segments_total, segments_pruned_index, segments_pruned_column_stats, segments_read, columns_total, columns_projected, rows_total, rows_selected}`. |
| `locks` | `lock_kind ("storage_lock" \| "reliable_storage_lock") -> {acquires, conflicts, retries, timeouts, wait_latency}`. |
| `boundary` | `phase ("normalize" \| "denormalize" \| "cpp_input" \| "cpp_output" \| "gil_wait") -> {count, latency}`. Measures Python/Arrow normalization, denormalization, C++ frame conversion, and GIL acquisition wait time. |
| `processing` | `{clauses: {clause_kind -> {executions, rows_in, rows_out, latency}}, admission: {units_admitted, residency_waits, segments_dispatched, read_window_waits, residency_wait_latency, read_window_wait_latency}}`. Tracks `QueryBuilder` per-clause execution/selectivity and `ProcessingUnitAdmissionHandler` backpressure. |
| `stream` | `{data_segments_written, staged_segments_written, small_segments_written, rows_per_segment: {count, sum, mean, min, p50, p95, p99, max}, incomplete_chain_loads, incomplete_chain_hops, incomplete_iteration_loads, incomplete_iteration_segments, compactions: {kind -> {count, input_segments, output_segments, latency}}, conflicts: {kind -> count}}`. Tracks segment row-size fragmentation, incomplete append chain traversal, compactions (`incomplete`, `sort_merge`, `defragment`, `symbol_list`), and write/compaction conflicts. |
| `resources` | `{cpu_pool: {pending_tasks, active_threads, total_threads, max_threads}, io_pool: {...}, write_semaphore_in_flight, write_semaphore_limit, version_map_entries, config_cache_libraries, config_cache_storages, lmdb: {open_envs, map_size_bytes, used_bytes, active_readers, map_full_errors}}`. Instantaneous live resource gauges (pulled on snapshot/scrape with zero hot-path overhead) plus scoped LMDB `MDB_MAP_FULL` error counts. |

Each latency object contains `count`, `total_ms`, `mean_ms`, `min_ms`, `p50_ms`, `p95_ms`, `p99_ms`, and `max_ms`.

### Converting to pandas DataFrames

```python
storage_df = report.to_dataframe("storage")
http_df = report.to_dataframe("http")
api_df = report.to_dataframe("api")
boundary_df = report.to_dataframe("boundary")
processing_df = report.to_dataframe("processing")
stream_df = report.to_dataframe("stream")
symbols_df = report.to_dataframe("by_symbol")
pools_df = report.to_dataframe("thread_pools")
locks_df = report.to_dataframe("locks")
```

## Exporting Aggregate Metrics to Prometheus

To continuously forward aggregate storage, HTTP, thread pool, cache, codec, query pruning, and distributed lock counters/histograms to ArcticDB's `PrometheusInstance`:

```python
import arcticdb.toolbox.query_stats as qs

# Start embedded HTTP /metrics endpoint (or pass model="push" for Pushgateway)
qs.configure_prometheus(host="127.0.0.1", port=9108, model="pull")
qs.enable_prometheus_sink(include_library=True)
# ... run workload — scrape http://127.0.0.1:9108/metrics ...
qs.disable_prometheus_sink()
```

Exported metric families use strictly bounded, low-cardinality labels (**symbol names are never emitted as Prometheus labels**):

- **Library API (RED):** `arcticdb_api_ops_total`, `arcticdb_api_op_duration_seconds` (`op`, `outcome="ok"|"error"`, optional `library`).
- **Storage:** `arcticdb_storage_ops_total`, `arcticdb_storage_bytes_total`, `arcticdb_storage_op_duration_seconds`, `arcticdb_storage_pages_total`, `arcticdb_storage_partial_delete_retries_total` (`backend`, `op`, `outcome`, optional `library`).
- **HTTP:** `arcticdb_http_attempts_total`, `arcticdb_http_retries_total`, `arcticdb_http_throttled_total`, `arcticdb_http_request_duration_seconds`, `arcticdb_http_phase_duration_seconds` (`phase="dns"|"tcp_connect"|"tls_handshake"|"ttfb"|"transfer"`), `arcticdb_http_new_connections_total`, `arcticdb_http_checksum_mismatch_retries_total`, `arcticdb_http_backoff_seconds_total` (`service`, `operation`, `status_class`, optional `library`).
- **Thread pools:** `arcticdb_pool_tasks_total`, `arcticdb_pool_tasks_expired_total`, `arcticdb_pool_task_wait_duration_seconds`, `arcticdb_pool_task_run_duration_seconds` (`pool="cpu"|"io"`), `arcticdb_async_store_semaphore_waits_total`, `arcticdb_async_store_semaphore_wait_duration_seconds`.
- **Caches:** `arcticdb_cache_lookups_total` (`cache="version_map"|"dedup_map"|"config_cache"`, `result="hit"|"miss"`), `arcticdb_version_map_reloads_total`, `arcticdb_version_map_chain_hops_total`, `arcticdb_version_map_ref_retries_total`, `arcticdb_dedup_bytes_saved_total`.
- **Codec:** `arcticdb_codec_ops_total` (`op="encode"|"decode"`), `arcticdb_codec_bytes_total` (`op`, `stage="uncompressed"|"compressed"`), `arcticdb_codec_duration_seconds` (`op`).
- **Query pruning:** `arcticdb_query_reads_total`, `arcticdb_query_segments_total` (`outcome="total"|"pruned_index"|"pruned_column_stats"|"read"`), `arcticdb_query_columns_total` (`kind="total"|"projected"`), `arcticdb_query_rows_total` (`kind="total"|"selected"`).
- **Locks:** `arcticdb_lock_events_total` (`kind="storage_lock"|"reliable_storage_lock"`, `event="acquire"|"conflict"|"retry"|"timeout"`), `arcticdb_lock_wait_duration_seconds` (`kind`).
- **Python/Arrow Boundary & GIL:** `arcticdb_boundary_ops_total`, `arcticdb_boundary_duration_seconds` (`phase="normalize"|"denormalize"|"cpp_input"|"cpp_output"|"gil_wait"`).
- **Processing Clauses & Admission:** `arcticdb_processing_clause_executions_total` (`clause`), `arcticdb_processing_clause_rows_total` (`clause`, `direction="in"|"out"`), `arcticdb_processing_clause_duration_seconds` (`clause`), `arcticdb_admission_events_total` (`kind="unit_admitted"|"residency_wait"|"segment_dispatched"|"read_window_wait"`), `arcticdb_admission_wait_duration_seconds` (`kind="residency"|"read_window"`).
- **Write / Append Pathologies (`stream`):** `arcticdb_stream_segments_written_total` (`kind="data"|"staged"`), `arcticdb_stream_small_segments_written_total`, `arcticdb_stream_segment_rows`, `arcticdb_stream_incomplete_chain_loads_total` (`mode="list"|"iteration"`), `arcticdb_stream_incomplete_chain_hops_total` (`mode="list"|"iteration"`), `arcticdb_stream_compactions_total` (`kind="incomplete"|"sort_merge"|"defragment"|"symbol_list"`), `arcticdb_stream_compaction_segments_total` (`kind`, `direction="in"|"out"`), `arcticdb_stream_compaction_duration_seconds` (`kind`), `arcticdb_stream_write_conflicts_total` (`kind="non_increasing_version"|"concurrent_symbol_list_compaction"|"symbol_list_load_retry"`).
- **Live Resource Gauges & LMDB:** `arcticdb_pool_pending_tasks` (`pool="cpu"|"io"`), `arcticdb_pool_threads` (`pool`, `state="active"|"total"|"max"`), `arcticdb_async_store_write_semaphore_in_flight`, `arcticdb_async_store_write_semaphore_limit`, `arcticdb_cache_entries` (`cache="version_map"|"config_cache_libraries"|"config_cache_storages"`), `arcticdb_lmdb_open_envs`, `arcticdb_lmdb_map_bytes` (`kind="size"|"used"`), `arcticdb_lmdb_active_readers`, `arcticdb_lmdb_map_full_errors_total`.

## Legacy Global API

For existing callers expecting the original flat `storage_operations` dictionary:

```python
import arcticdb.toolbox.query_stats as qs

with qs.query_stats():
    lib.list_symbols()

stats = qs.get_query_stats()
qs.reset_stats()
```

You can also toggle global collection manually with `qs.enable()` and `qs.disable()`.

### Legacy API Caveats

1. `qs.query_stats()` / `qs.enable()` uses a single process-wide collector shared across all threads and does not support nesting (`query_stats()` raises `UserInputException` if already enabled).
2. The legacy dictionary only reports S3-family (`S3_*`) and in-memory (`Memory_*`) operations; use `qs.collect()` for LMDB, Azure, MongoDB, HTTP retries, latency percentiles, caches, codec, query pruning, or thread-isolated collection.
3. Calling `qs.reset_stats()` clears only the legacy global collector and does not affect active `qs.collect()` scopes.
