import socket
import threading
import urllib.request
import uuid

import numpy as np
import pandas as pd
import pytest

from arcticdb_ext.exceptions import LmdbMapFullError
import arcticdb_ext.tools.query_stats as qs_ext
import arcticdb.toolbox.query_stats as qs
from arcticdb import Arctic, LibraryOptions, QueryBuilder
from arcticdb.exceptions import NoDataFoundException, UserInputException
from arcticdb.util.test import config_context


def _sample_df(rows: int = 20, offset: int = 0) -> pd.DataFrame:
    return pd.DataFrame(
        {"a": range(offset, offset + rows), "b": [float(i) * 1.5 for i in range(offset, offset + rows)]},
        index=pd.date_range("2024-01-01", periods=rows, freq="D"),
    )


def test_collect_memory_and_lmdb_backends(tmp_path):
    mem_ac = Arctic(f"mem://qs_mem_{uuid.uuid4().hex[:8]}")
    mem_lib = mem_ac.get_library("lib_mem", create_if_missing=True)

    lmdb_ac = Arctic(f"lmdb://{tmp_path / 'qs_lmdb'}")
    lmdb_lib = lmdb_ac.get_library("lib_lmdb", create_if_missing=True)

    with qs.collect() as report:
        with config_context("VersionMap.ReloadInterval", 0):
            mem_lib.write("sym_m", _sample_df(10))
            mem_lib.read("sym_m")
            lmdb_lib.write("sym_l", _sample_df(15))
            lmdb_lib.read("sym_l")

    assert not qs_ext.is_active()
    assert report.wall_time_ms > 0.0
    assert "memory" in report.storage
    assert "lmdb" in report.storage

    mem_writes = report.storage["memory"]["write"]["TABLE_DATA"]
    assert mem_writes["ok"] == 1
    assert mem_writes["error"] == 0
    assert mem_writes["bytes"] > 0
    assert mem_writes["latency"]["count"] == 1
    assert mem_writes["latency"]["min_ms"] <= mem_writes["latency"]["p95_ms"] <= mem_writes["latency"]["max_ms"]

    lmdb_reads = report.storage["lmdb"]["read"]["TABLE_DATA"]
    assert lmdb_reads["ok"] == 1
    assert lmdb_reads["error"] == 0
    assert lmdb_reads["bytes"] > 0

    assert "lib_mem" in report.by_library
    assert "lib_lmdb" in report.by_library
    assert "memory" in report.by_library["lib_mem"]["storage"]
    assert "lmdb" in report.by_library["lib_lmdb"]["storage"]
    # by_symbol defaults to False
    assert report.by_symbol == {}


def test_collect_thread_isolation():
    ac = Arctic(f"mem://qs_iso_{uuid.uuid4().hex[:8]}")
    lib_main = ac.get_library("main_lib", create_if_missing=True)
    lib_bg = ac.get_library("bg_lib", create_if_missing=True)
    df = _sample_df(10)

    barrier = threading.Barrier(2)

    def bg_worker():
        barrier.wait()
        for i in range(5):
            lib_bg.write(f"bg_sym_{i}", df)

    t = threading.Thread(target=bg_worker)
    t.start()

    with qs.collect(by_library=True, by_symbol=True) as report:
        barrier.wait()
        lib_main.write("main_sym", df)
        t.join()

    assert "main_lib" in report.by_library
    assert "bg_lib" not in report.by_library
    assert "main_sym" in report.by_symbol
    assert all(not sym.startswith("bg_sym_") for sym in report.by_symbol)


def test_collect_nesting_and_isolate(clear_query_stats):
    ac = Arctic(f"mem://qs_nest_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("nest_lib", create_if_missing=True)
    df = _sample_df(10)

    with qs.query_stats():
        with qs.collect(by_symbol=True) as outer:
            lib.write("outer_sym", df)
            with qs.collect(by_symbol=True, isolate=False) as inner_shared:
                lib.write("shared_sym", df)
            with qs.collect(by_symbol=True, isolate=True) as inner_isolated:
                lib.write("isolated_sym", df)

        legacy = qs.get_query_stats()

    assert "shared_sym" in inner_shared.by_symbol
    assert "outer_sym" not in inner_shared.by_symbol

    assert "isolated_sym" in inner_isolated.by_symbol
    assert "outer_sym" not in inner_isolated.by_symbol

    # Outer sees outer_sym + shared_sym, NOT isolated_sym
    assert "outer_sym" in outer.by_symbol
    assert "shared_sym" in outer.by_symbol
    assert "isolated_sym" not in outer.by_symbol
    assert outer.storage["memory"]["write"]["TABLE_DATA"]["ok"] == 2
    assert inner_isolated.storage["memory"]["write"]["TABLE_DATA"]["ok"] == 1

    # Legacy global collector also respects isolate=True
    assert legacy["storage_operations"]["Memory_PutObject"]["TABLE_DATA"]["count"] == 2


def test_collect_by_symbol_max_symbols_and_top_symbols():
    ac = Arctic(f"mem://qs_sym_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("sym_lib", create_if_missing=True)
    small_df = _sample_df(5)
    big_df = _sample_df(500)

    with qs.collect(by_symbol=True) as report:
        with config_context("VersionMap.ReloadInterval", 0):
            lib.write("SMALL", small_df)
            lib.write("BIG", big_df)
            lib.read("SMALL")
            lib.read("BIG")

    assert "SMALL" in report.by_symbol
    assert "BIG" in report.by_symbol
    assert report.by_symbol["BIG"]["bytes_read"] > report.by_symbol["SMALL"]["bytes_read"]
    assert report.by_symbol["BIG"]["bytes_written"] > report.by_symbol["SMALL"]["bytes_written"]

    top_by_read = report.top_symbols(by="bytes_read", n=2)
    assert top_by_read[0][0] == "BIG"
    assert top_by_read[1][0] == "SMALL"

    top_in_lib = report.top_symbols(by="bytes_written", n=1, library="sym_lib")
    assert top_in_lib[0][0] == "BIG"

    with pytest.raises(UserInputException):
        report.top_symbols(by="nonexistent_metric")

    # Test cardinality cap spilling into "__other__"
    with qs.collect(by_symbol=True, max_symbols=3) as capped:
        for i in range(8):
            lib.write(f"CAP_{i}", small_df)

    assert len(capped.by_symbol) == 4  # 3 named + "__other__"
    assert "__other__" in capped.by_symbol
    assert capped.dropped_symbols == 5


def test_collect_not_found_classification_diff_and_formats():
    ac = Arctic(f"mem://qs_diff_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("diff_lib", create_if_missing=True)
    df = _sample_df(10)

    with qs.collect(by_symbol=True) as live:
        with config_context("VersionMap.ReloadInterval", 0):
            lib.write("A", df)
            snap_after_write = live.snapshot()

            lib.read("A")
            with pytest.raises(NoDataFoundException):
                lib.read("DOES_NOT_EXIST")
            snap_after_reads = live.snapshot()

    delta = snap_after_reads.diff(snap_after_write)
    assert "write" not in delta.storage.get("memory", {})
    assert delta.storage["memory"]["read"]["TABLE_DATA"]["ok"] == 1
    assert delta.storage["memory"]["read"]["VERSION_REF"]["not_found"] >= 1
    assert delta.storage["memory"]["read"]["VERSION_REF"]["error"] == 0

    as_dict = live.to_dict()
    assert set(as_dict.keys()) == {
        "wall_time_ms",
        "storage",
        "http",
        "api",
        "by_library",
        "by_symbol",
        "dropped_symbols",
        "thread_pools",
        "caches",
        "codec",
        "query",
        "locks",
        "boundary",
        "processing",
        "stream",
        "resources",
    }

    storage_df = live.to_dataframe("storage")
    assert not storage_df.empty
    assert {"backend", "op", "key_type", "ok", "not_found", "error", "p95_ms"}.issubset(storage_df.columns)

    sym_df = live.to_dataframe("by_symbol")
    assert "A" in set(sym_df["symbol"])

    http_df = live.to_dataframe("http")
    assert http_df.empty

    tp_df = live.to_dataframe("thread_pools")
    assert not tp_df.empty
    assert {"pool", "tasks", "wait_total_ms", "run_total_ms"}.issubset(tp_df.columns)

    locks_df = live.to_dataframe("locks")
    assert locks_df.empty

    with pytest.raises(UserInputException):
        live.to_dataframe("invalid_section")

    text = live.summary()
    assert "QueryStatsReport" in text
    assert "memory." in text
    assert "diff_lib" in text
    assert "Codec:" in text
    assert "Caches:" in text
    assert "Query:" in text


def test_collect_cleanup_on_exception():
    with pytest.raises(RuntimeError, match="boom"):
        with qs.collect() as report:
            raise RuntimeError("boom")

    assert not qs_ext.is_active()
    assert report.wall_time_ms >= 0.0


def test_collect_thread_pools_and_codec_metrics():
    ac = Arctic(f"mem://qs_codec_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("codec_lib", create_if_missing=True)
    df = pd.DataFrame(
        {
            "x": [42] * 500,
            "y": [float(i % 5) for i in range(500)],
            "s": ["repeated_string_value"] * 500,
        },
        index=pd.date_range("2024-01-01", periods=500, freq="D"),
    )

    with qs.collect() as live:
        lib.write("compressible", df)
        after_write = live.snapshot()
        with config_context("VersionMap.ReloadInterval", 0):
            out = lib.read("compressible").data
        after_read = live.snapshot()

    assert len(out) == 500

    # Write path exercises CPU/IO pools and encoding
    assert after_write.thread_pools["cpu"]["tasks"] >= 1
    assert after_write.thread_pools["io"]["tasks"] >= 1
    assert after_write.thread_pools["cpu"]["run_latency"]["count"] == after_write.thread_pools["cpu"]["tasks"]
    assert after_write.thread_pools["io"]["wait_latency"]["count"] == after_write.thread_pools["io"]["tasks"]

    assert after_write.codec["encodes"] >= 1
    assert after_write.codec["encode_uncompressed_bytes"] > 0
    assert after_write.codec["encode_compressed_bytes"] > 0
    assert after_write.codec["encode_compression_ratio"] > 1.0
    assert after_write.codec["encode_latency"]["count"] == after_write.codec["encodes"]

    # Read delta exercises decoding without new encodes
    read_delta = after_read.diff(after_write)
    assert read_delta.codec["encodes"] == 0
    assert read_delta.codec["decodes"] >= 1
    assert read_delta.codec["decode_compressed_bytes"] > 0
    assert read_delta.codec["decode_uncompressed_bytes"] > 0
    assert read_delta.codec["decode_compression_ratio"] > 0.0
    assert read_delta.codec["decode_latency"]["count"] == read_delta.codec["decodes"]


def test_collect_caches_version_map_and_dedup():
    ac = Arctic(f"mem://qs_cache_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("cache_lib", create_if_missing=True, library_options=LibraryOptions(dedup=True))
    df0 = _sample_df(50, offset=0)
    df1 = _sample_df(50, offset=100)
    df2 = _sample_df(50, offset=200)

    lib.write("sym", df0)
    lib.write("sym", df1)
    lib.write("sym", df2)

    # Cold read (ReloadInterval=0) forces reloads; reading as_of=0 across 3 versions
    # cannot use the VERSION_REF penultimate shortcut and traverses the version chain.
    with qs.collect() as cold:
        with config_context("VersionMap.ReloadInterval", 0):
            lib.read("sym")
            lib.read("sym", as_of=0)

    vm_cold = cold.caches["version_map"]
    assert vm_cold["misses"] >= 2
    assert vm_cold["reloads"] >= 2
    assert vm_cold["chain_hops"] >= 1

    # Warm reads within default reload TTL hit the in-memory version map
    with qs.collect() as warm:
        lib.read("sym")
        lib.read("sym")

    vm_warm = warm.caches["version_map"]
    assert vm_warm["hits"] >= 2
    assert vm_warm["misses"] == 0

    # Writing identical payload to a dedup-enabled library triggers DeDupMap hit & saves bytes
    with qs.collect() as dedup_report:
        lib.write("sym", df2)

    dd = dedup_report.caches["dedup_map"]
    assert dd["hits"] >= 1
    assert dd["bytes_saved"] > 0


def test_collect_query_pruning_index_column_stats_and_projection(column_stats_filtering_enabled):
    ac = Arctic(f"mem://qs_prune_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library(
        "prune_lib",
        create_if_missing=True,
        library_options=LibraryOptions(rows_per_segment=10),
    )

    # 4 distinct 10-row segments with monotonically increasing values in column "a"
    for seg_idx in range(4):
        start_val = seg_idx * 100
        dates = pd.date_range(f"2024-0{seg_idx + 1}-01", periods=10, freq="D")
        chunk = pd.DataFrame(
            {
                "a": range(start_val, start_val + 10),
                "b": [float(i) for i in range(10)],
                "c": [i * 2 for i in range(10)],
                "d": [i * 3 for i in range(10)],
            },
            index=dates,
        )
        if seg_idx == 0:
            lib.write("multi_seg", chunk)
        else:
            lib.append("multi_seg", chunk)

    lib._nvs.create_column_stats_experimental("multi_seg")

    # Date-range + column projection: prunes segments via index & projects subset of columns
    with qs.collect() as idx_report:
        res_idx = lib.read(
            "multi_seg",
            date_range=(pd.Timestamp("2024-02-01"), pd.Timestamp("2024-02-10")),
            columns=["a"],
        ).data

    assert len(res_idx) == 10
    assert idx_report.query["queries"] == 1
    assert idx_report.query["segments_total"] == 4
    assert idx_report.query["segments_pruned_index"] == 3
    assert idx_report.query["segments_pruned_column_stats"] == 0
    assert idx_report.query["segments_read"] == 1
    assert idx_report.query["columns_total"] == 5  # index + a, b, c, d
    assert idx_report.query["columns_projected"] == 2  # index + a
    assert idx_report.query["rows_total"] == 40
    assert idx_report.query["rows_selected"] == 10

    # Column-stats filter: only segment 3 (a in 300..309) satisfies a >= 300
    q = QueryBuilder()
    q = q[q["a"] >= 300]
    with qs.collect() as cs_report:
        res_cs = lib.read("multi_seg", query_builder=q).data

    assert len(res_cs) == 10
    assert cs_report.query["queries"] == 1
    assert cs_report.query["segments_total"] == 4
    assert cs_report.query["segments_pruned_column_stats"] == 3
    assert cs_report.query["segments_read"] == 1
    assert cs_report.query["rows_total"] == 40
    assert cs_report.query["rows_selected"] == 10


def test_collect_storage_lock_metrics():
    ac = Arctic(f"mem://qs_lock_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("lock_lib", create_if_missing=True)
    tool = lib._nvs.library_tool()
    l1 = tool.get_storage_lock("qs_lock_test")
    l2 = tool.get_storage_lock("qs_lock_test")

    with config_context("StorageLock.WaitMs", 1), config_context("StorageLock.InitialWaitMs", 1):
        with qs.collect() as report:
            l1.lock()
            snap_after_acquire = report.snapshot()

            # Contended try_lock fails against the active storage lock
            assert not l2.try_lock()

            # Contended lock_timeout fails with timeout
            with pytest.raises(RuntimeError):
                l2.lock_timeout(5)

            l1.unlock()

    assert snap_after_acquire.locks["storage_lock"]["acquires"] == 1
    assert snap_after_acquire.locks["storage_lock"]["conflicts"] == 0
    assert snap_after_acquire.locks["storage_lock"]["timeouts"] == 0

    lk = report.locks["storage_lock"]
    assert lk["acquires"] == 1
    assert lk["conflicts"] >= 2
    assert lk["timeouts"] == 1
    assert lk["wait_latency"]["count"] >= 3

    locks_df = report.to_dataframe("locks")
    assert len(locks_df) == 1
    assert locks_df.iloc[0]["lock"] == "storage_lock"
    assert "Locks:" in report.summary()


def test_collect_api_red_metrics():
    ac = Arctic(f"mem://qs_api_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("api_lib", create_if_missing=True)
    df1 = _sample_df(10, offset=0)
    df2 = pd.DataFrame(
        {"a": range(10, 20), "b": [float(i) * 1.5 for i in range(10, 20)]},
        index=pd.date_range("2024-02-01", periods=10, freq="D"),
    )

    with qs.collect() as live:
        lib.write("S1", df1)
        lib.append("S1", df2)
        after_writes = live.snapshot()

        lib.read("S1")
        with pytest.raises(NoDataFoundException):
            lib.read("MISSING")
        lib.list_symbols()
        lib.delete("S1")

    assert live.api["write"]["ok"] == 1
    assert live.api["write"]["error"] == 0
    assert live.api["append"]["ok"] == 1
    assert live.api["read"]["ok"] == 1
    assert live.api["read"]["error"] == 1
    assert live.api["read"]["count"] == 2
    assert live.api["read"]["latency"]["count"] == 2
    assert live.api["read"]["error_latency"]["count"] == 1
    assert live.api["list_symbols"]["ok"] == 1
    assert live.api["delete"]["ok"] == 1

    assert "api_lib" in live.by_library
    assert live.by_library["api_lib"]["api"]["read"]["ok"] == 1
    assert live.by_library["api_lib"]["api"]["read"]["error"] == 1

    delta = live.diff(after_writes)
    assert "write" not in delta.api
    assert "append" not in delta.api
    assert delta.api["read"]["ok"] == 1
    assert delta.api["read"]["error"] == 1
    assert delta.api["delete"]["ok"] == 1

    api_df = live.to_dataframe("api")
    assert set(api_df["op"]) >= {"write", "append", "read", "list_symbols", "delete"}
    assert {"op", "ok", "error", "count", "p95_ms", "error_p95_ms"}.issubset(api_df.columns)
    assert "API:" in live.summary()


def test_collect_boundary_normalize_denormalize_and_gil_wait():
    ac = Arctic(f"mem://qs_boundary_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library("boundary_lib", create_if_missing=True)
    df = pd.DataFrame(
        {
            "a": range(100),
            "label": [f"str_{i % 10}" for i in range(100)],
        },
        index=pd.date_range("2024-01-01", periods=100, freq="D"),
    )

    with qs.collect() as live:
        lib.write("B1", df)
        after_write = live.snapshot()
        out = lib.read("B1").data

    assert len(out) == 100
    # Write exercises Python normalize + C++ cpp_input + GIL release/reacquire
    assert after_write.boundary["normalize"]["count"] == 1
    assert after_write.boundary["cpp_input"]["count"] == 1
    assert "denormalize" not in after_write.boundary
    assert "cpp_output" not in after_write.boundary

    # Read delta exercises C++ cpp_output + Python denormalize + GIL acquire for string column
    delta = live.diff(after_write)
    assert "normalize" not in delta.boundary
    assert "cpp_input" not in delta.boundary
    assert delta.boundary["cpp_output"]["count"] == 1
    assert delta.boundary["denormalize"]["count"] == 1
    assert delta.boundary["gil_wait"]["count"] >= 1

    bdf = live.to_dataframe("boundary")
    assert set(bdf["phase"]) >= {"normalize", "denormalize", "cpp_input", "cpp_output", "gil_wait"}
    assert {"phase", "count", "total_ms", "p95_ms"}.issubset(bdf.columns)
    assert "Boundary:" in live.summary()


def test_collect_processing_clauses_and_admission():
    ac = Arctic(f"mem://qs_proc_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library(
        "proc_lib",
        create_if_missing=True,
        library_options=LibraryOptions(rows_per_segment=10),
    )
    for seg_idx in range(4):
        dates = pd.date_range(f"2024-0{seg_idx + 1}-01", periods=10, freq="D")
        chunk = pd.DataFrame(
            {
                "grp": ["A" if i % 2 == 0 else "B" for i in range(10)],
                "val": [float(seg_idx * 10 + i) for i in range(10)],
            },
            index=dates,
        )
        if seg_idx == 0:
            lib.write("PROC_SYM", chunk)
        else:
            lib.append("PROC_SYM", chunk)

    q = QueryBuilder()
    q = q[q["val"] >= 10.0].groupby("grp").agg({"val": "sum"})

    with (
        config_context("VersionStore.NumProcessingUnitsLive", 1),
        config_context("VersionStore.SegmentReadWindow", 1),
        qs.collect() as live,
    ):
        res = lib.read("PROC_SYM", query_builder=q).data

    assert len(res) == 2
    clauses = live.processing["clauses"]
    assert "filter" in clauses
    assert clauses["filter"]["executions"] >= 1
    assert clauses["filter"]["rows_in"] == 40
    assert clauses["filter"]["rows_out"] == 30
    assert "groupby_partition" in clauses
    assert "aggregation" in clauses
    assert clauses["aggregation"]["rows_out"] == 2

    adm = live.processing["admission"]
    assert adm["units_admitted"] == 4
    assert adm["residency_waits"] == 3
    assert adm["segments_dispatched"] == 4
    assert adm["residency_wait_latency"]["count"] == 3

    pdf = live.to_dataframe("processing")
    assert set(pdf["clause"]) >= {"filter", "groupby_partition", "aggregation"}
    assert {"clause", "executions", "rows_in", "rows_out", "p95_ms"}.issubset(pdf.columns)
    assert "Processing:" in live.summary()


def test_collect_stream_write_pathologies():
    ac = Arctic(f"mem://qs_stream_{uuid.uuid4().hex[:8]}")
    lib = ac.get_library(
        "stream_lib",
        create_if_missing=True,
        library_options=LibraryOptions(rows_per_segment=10),
    )
    tool = lib._nvs.library_tool()
    df_small = _sample_df(30, offset=0)
    df_large = pd.DataFrame(
        {"a": range(1200), "b": [float(i) for i in range(1200)]},
        index=pd.date_range("2024-01-01", periods=1200, freq="h"),
    )

    with qs.collect() as live:
        # 3 small TABLE_DATA segments (10 rows each)
        lib.write("SMALL_SYM", df_small)
        after_small = live.snapshot()

        # Staged append + finalize (iteration load + incomplete compaction)
        for i in range(3):
            chunk = pd.DataFrame(
                {"a": [i * 10 + j for j in range(5)], "b": [float(j) for j in range(5)]},
                index=pd.date_range(f"2024-0{i + 1}-01", periods=5, freq="D"),
            )
            lib.stage("STAGED_SYM", chunk)
        lib.finalize_staged_data("STAGED_SYM")

        # Linked-list incomplete append chain + incomplete read (chain hops)
        chain_df = pd.DataFrame(
            {"a": range(6), "b": [float(i) for i in range(6)]},
            index=pd.date_range("2024-06-01", periods=6, freq="D"),
        )
        for idx in range(3):
            tool.append_incomplete("CHAIN_SYM", chain_df.iloc[idx * 2 : (idx + 1) * 2])
        lib._nvs.read("CHAIN_SYM", date_range=(chain_df.index[0], chain_df.index[-1]), incomplete=True)

        # Symbol-list compaction
        lib.list_symbols()

    assert after_small.stream["data_segments_written"] == 3
    assert after_small.stream["small_segments_written"] == 3
    assert after_small.stream["rows_per_segment"]["count"] == 3
    assert after_small.stream["rows_per_segment"]["sum"] == 30
    assert after_small.stream["rows_per_segment"]["max"] == 10

    st = live.stream
    assert st["data_segments_written"] >= 4
    assert st["staged_segments_written"] == 6  # 3 staged writes + 3 append_incomplete
    assert st["small_segments_written"] >= 9
    assert st["incomplete_chain_loads"] >= 1
    assert st["incomplete_chain_hops"] >= 3
    assert st["incomplete_iteration_loads"] >= 1
    assert st["incomplete_iteration_segments"] >= 3
    assert "incomplete" in st["compactions"]
    assert st["compactions"]["incomplete"]["count"] == 1
    assert st["compactions"]["incomplete"]["input_segments"] == 3
    assert st["compactions"]["incomplete"]["output_segments"] >= 1
    assert "symbol_list" in st["compactions"]
    assert st["compactions"]["symbol_list"]["count"] >= 1

    # Also verify large segment (>1000 rows) is NOT counted as small
    lib_large = ac.get_library("stream_large_lib", create_if_missing=True)
    with qs.collect() as large_report:
        lib_large.write("BIG_SYM", df_large)
    assert large_report.stream["data_segments_written"] == 1
    assert large_report.stream["small_segments_written"] == 0
    assert large_report.stream["rows_per_segment"]["max"] == 1200

    delta = live.diff(after_small)
    assert delta.stream["staged_segments_written"] == 6
    assert "incomplete" in delta.stream["compactions"]

    sdf = live.to_dataframe("stream")
    assert set(sdf["compaction"]) >= {"incomplete", "symbol_list"}
    assert {"compaction", "count", "input_segments", "output_segments", "p95_ms"}.issubset(sdf.columns)
    assert "Stream:" in live.summary()


def test_collect_live_resource_gauges_and_lmdb_map_full(tmp_path):
    lmdb_dir = tmp_path / "tiny_lmdb"
    ac = Arctic(f"lmdb://{lmdb_dir}?map_size=512KB")
    lib = ac.get_library("tiny_lib", create_if_missing=True)

    # Incompressible random float payload larger than the 512KB LMDB map size to trigger MDB_MAP_FULL.
    rng = np.random.default_rng(42)
    big_df = pd.DataFrame(
        {"v0": rng.random(100_000), "v1": rng.random(100_000)},
        index=pd.date_range("2024-01-01", periods=100_000, freq="s"),
    )

    with qs.collect() as live:
        lib.write("OK_SYM", _sample_df(10))
        before_full = live.snapshot()
        with pytest.raises(LmdbMapFullError):
            lib.write("TOO_BIG", big_df)

    res = live.resources
    assert res["cpu_pool"]["max_threads"] >= 1
    assert res["io_pool"]["max_threads"] >= 1
    assert res["version_map_entries"] >= 1
    assert res["lmdb"]["open_envs"] >= 1
    assert res["lmdb"]["map_size_bytes"] > 0
    assert res["lmdb"]["used_bytes"] > 0
    assert res["lmdb"]["map_full_errors"] >= 1
    assert before_full.resources["lmdb"]["map_full_errors"] == 0

    delta = live.diff(before_full)
    assert delta.resources["lmdb"]["map_full_errors"] >= 1
    assert "Resources:" in live.summary()


def test_prometheus_sink_toggle_python_api(tmp_path):
    assert not qs_ext.is_active()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    qs.configure_prometheus(host="127.0.0.1", port=port, model="pull")
    try:
        qs.enable_prometheus_sink(include_library=True)
        assert qs_ext.is_active()
        ac = Arctic(f"mem://qs_prom_{uuid.uuid4().hex[:8]}")
        lib = ac.get_library("prom_py_lib", create_if_missing=True)
        lib.write("SECRET_SYM", _sample_df(10))
        q_prom = QueryBuilder()
        q_prom = q_prom[q_prom["a"] >= 5]
        lib.read("SECRET_SYM", query_builder=q_prom)
        lib.stage("STAGED_PROM", _sample_df(10))
        lib.finalize_staged_data("STAGED_PROM")
        lib.list_symbols()
        lmdb_prom = Arctic(f"lmdb://{tmp_path / 'prom_lmdb'}?map_size=2MB")
        lmdb_prom_lib = lmdb_prom.get_library("prom_lmdb_lib", create_if_missing=True)
        lmdb_prom_lib.write("LMDB_SYM", _sample_df(10))
        with pytest.raises(NoDataFoundException):
            lib.read("MISSING_PROM_SYM")
        lock = lib._nvs.library_tool().get_storage_lock("prom_lock")
        lock.lock()
        lock.unlock()

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            body = resp.read().decode("utf-8")
        assert "arcticdb_api_ops_total" in body
        assert "arcticdb_api_op_duration_seconds" in body
        assert "arcticdb_boundary_ops_total" in body
        assert "arcticdb_boundary_duration_seconds" in body
        assert "arcticdb_processing_clause_executions_total" in body
        assert "arcticdb_processing_clause_rows_total" in body
        assert "arcticdb_processing_clause_duration_seconds" in body
        assert "arcticdb_admission_events_total" in body
        assert "arcticdb_stream_segments_written_total" in body
        assert "arcticdb_stream_small_segments_written_total" in body
        assert "arcticdb_stream_segment_rows" in body
        assert "arcticdb_stream_incomplete_chain_loads_total" in body
        assert "arcticdb_stream_compactions_total" in body
        assert "arcticdb_stream_compaction_segments_total" in body
        assert "arcticdb_pool_pending_tasks" in body
        assert "arcticdb_pool_threads" in body
        assert "arcticdb_cache_entries" in body
        assert "arcticdb_lmdb_open_envs" in body
        assert "arcticdb_lmdb_map_bytes" in body
        assert 'clause="filter"' in body
        assert 'phase="normalize"' in body
        assert 'phase="denormalize"' in body
        assert 'phase="cpp_input"' in body
        assert 'phase="cpp_output"' in body
        assert 'phase="gil_wait"' in body
        assert 'op="read"' in body
        assert 'outcome="error"' in body
        assert "arcticdb_storage_ops_total" in body
        assert "arcticdb_pool_tasks_total" in body
        assert "arcticdb_cache_lookups_total" in body
        assert "arcticdb_codec_ops_total" in body
        assert "arcticdb_codec_bytes_total" in body
        assert "arcticdb_query_reads_total" in body
        assert "arcticdb_query_segments_total" in body
        assert "arcticdb_lock_events_total" in body
        assert 'backend="memory"' in body
        assert 'library="prom_py_lib"' in body
        assert "SECRET_SYM" not in body
        assert "MISSING_PROM_SYM" not in body
        assert 'symbol="' not in body
    finally:
        qs.disable_prometheus_sink()
    assert not qs_ext.is_active()
