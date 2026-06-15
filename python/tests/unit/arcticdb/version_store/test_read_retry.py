"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file
licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License,
use of this software will be governed by the Apache License, version 2.0.

Deterministic tests for the read-retry behaviour in read_dataframe_version_internal.

A reader can resolve a version from its (stale) cached version chain just before a concurrent writer
supersedes it and eagerly prunes its keys. The read then re-resolves the symbol to the current
version and retries instead of surfacing a missing-key error. These tests reproduce that race
deterministically using two store handles to the same storage (separate version-map caches) plus a
very large reload interval that pins the reader's cache.
"""

import pandas as pd
import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb.exceptions import KeyNotFoundException, NoDataFoundException
from arcticdb.util.test import config_context, query_stats_operation_count

# 2**62 nanoseconds (~146 years): effectively infinite — the cached version chain never expires.
STICKY_RELOAD_INTERVAL = 2**62


def _df(value):
    return pd.DataFrame({"a": [value]})


def _version_ref_reads(stats):
    return query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF")


def _version_reads(stats):
    return query_stats_operation_count(stats, "Memory_GetObject", "VERSION")


def _index_reads(stats):
    return query_stats_operation_count(stats, "Memory_GetObject", "TABLE_INDEX")


def test_read_retries_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """Reader holding a stale cache (latest = v0) reads while a writer creates v1 and eagerly prunes
    v0. The reader must transparently re-resolve to v1 and return its data, not raise."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0))
        # Warm the reader's cache so it resolves latest = v0 without re-reading the ref.
        assert reader.read(sym).data["a"][0] == 0

        # Concurrent writer supersedes v0 and physically prunes it.
        writer.write(sym, _df(1), prune_previous_version=True)

        # Reader's cache still points at v0, whose keys are now gone; the retry must recover.
        result = reader.read(sym)

    assert result.version == 1
    assert result.data["a"][0] == 1


def test_read_does_not_retry_genuinely_missing_version(in_memory_store_factory):
    """A read for a version that never existed must still fail fast with NoSuchVersion rather than
    being swallowed by the retry loop (which only catches missing-key errors)."""
    from arcticdb.exceptions import NoSuchVersionException

    lib = in_memory_store_factory()
    lib.write("sym", _df(0))
    with pytest.raises(NoSuchVersionException):
        lib.read("sym", as_of=5)


def test_retry_only_rereads_the_raced_symbols_version_ref(in_memory_store_factory, clear_query_stats):
    """The retry must be cheap and scoped: recovering a single raced symbol should re-read only that
    symbol's version ref and must NOT evict the cached chains of other symbols. This is what
    distinguishes the per-symbol invalidation from a global flush()."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write("raced", _df(0))
        writer.write("other", _df(100))
        # Warm the reader's cache for both symbols.
        assert reader.read("raced").data["a"][0] == 0
        assert reader.read("other").data["a"][0] == 100

        # Concurrent writer supersedes and eagerly prunes "raced" only.
        writer.write("raced", _df(1), prune_previous_version=True)

        qs.enable()
        qs.reset_stats()
        raced_result = reader.read("raced")  # stale cache -> one retry
        raced_stats = qs.get_query_stats()

        qs.reset_stats()
        other_result = reader.read("other")  # must still be a pure cache hit
        other_stats = qs.get_query_stats()
        qs.disable()

    assert raced_result.data["a"][0] == 1
    # One retry: two ref reads — one to detect the head change, one inside storage_reload to
    # rebuild the cache entry (LOAD_LATEST shortcut, no VERSION-chain traversal). The retry's
    # check_reload is then a pure cache hit — no further ref read, no VERSION reads.
    # Two TABLE_INDEX reads: one failed attempt on v0's (pruned) key, one successful read of v1.
    assert _version_ref_reads(raced_stats) == 2
    assert _version_reads(raced_stats) == 0
    assert _index_reads(raced_stats) == 2
    # The unrelated symbol's cached chain survived: no version-chain reads needed.
    assert _version_ref_reads(other_stats) == 0
    assert _version_reads(other_stats) == 0


def test_retry_reads_are_bounded_regardless_of_live_versions(in_memory_store_factory, clear_query_stats):
    """The read-retry overhead must be O(1) storage reads regardless of how many live versions
    exist for the symbol. We write N versions without pruning (so the version chain is N items
    long), race the latest, and assert that the extra reads on retry are bounded—not
    proportional to N."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"
    N = 15  # Large enough to detect O(N) behaviour if present

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        for i in range(N):
            writer.write(sym, _df(i), prune_previous_version=False)

        # Warm the reader's cache so it resolves latest = v{N-1} without re-reading the ref.
        assert reader.read(sym).data["a"][0] == N - 1

        # Concurrent writer creates v_N and eagerly prunes v_{N-1}.
        writer.write(sym, _df(N), prune_previous_version=True)

        qs.enable()
        qs.reset_stats()
        result = reader.read(sym)  # stale cache -> one retry
        stats = qs.get_query_stats()
        qs.disable()

    assert result.data["a"][0] == N
    # One retry: exactly 2 VERSION_REF reads (one head-comparison check, one inside storage_reload
    # which rebuilds the cache via the LOAD_LATEST shortcut). 0 VERSION reads, 0 extra
    # TABLE_INDEX reads. O(1) regardless of N.
    assert _version_ref_reads(stats) == 2
    assert _version_reads(stats) == 0


def test_specific_version_read_does_not_retry_when_pruned(in_memory_store_factory):
    """read(as_of=0) for a version that was pruned must not silently re-resolve to a later version.
    Retries are only enabled for latest-version reads; pinned queries must fail immediately."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write("sym", _df(0))
        assert reader.read("sym").data["a"][0] == 0  # warm cache so reader sees v0 in stale chain

        writer.write("sym", _df(1), prune_previous_version=True)

        # The stale cache still resolves v0, but its keys are gone. Pinned queries never retry,
        # so the error propagates immediately — no wrong-version return.
        with pytest.raises((NoDataFoundException, KeyNotFoundException)):
            reader.read("sym", as_of=0)


# Note: a snapshot test analogous to test_specific_version_read_does_not_retry_when_pruned is not
# included because ArcticDB protects snapshot-referenced keys from pruning — prune_previous_version
# leaves keys intact when they are held by a snapshot, so snapshot reads always succeed and the
# "no-retry" code path is not reachable through normal API usage.


# ---- batch_read ----


def test_read_batch_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """batch_read recovers the same way as read when a concurrent prune races a latest-version read."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0))
        assert reader.read(sym).data["a"][0] == 0

        writer.write(sym, _df(1), prune_previous_version=True)

        results = reader.batch_read([sym])

    assert results[sym].data["a"][0] == 1


def test_read_batch_partial_race_recovers_raced_symbol_only(in_memory_store_factory):
    """A 2-symbol batch where only one symbol raced: that symbol recovers, the other is untouched."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write("raced", _df(0))
        writer.write("stable", _df(100))
        assert reader.read("raced").data["a"][0] == 0
        assert reader.read("stable").data["a"][0] == 100

        writer.write("raced", _df(1), prune_previous_version=True)

        results = reader.batch_read(["raced", "stable"])

    assert results["raced"].data["a"][0] == 1
    assert results["stable"].data["a"][0] == 100


def test_read_batch_retry_is_bounded(in_memory_store_factory, clear_query_stats):
    """Retry overhead for batch_read must be O(1) storage reads, matching the single-symbol retry."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"
    N = 15

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        for i in range(N):
            writer.write(sym, _df(i), prune_previous_version=False)

        assert reader.read(sym).data["a"][0] == N - 1

        writer.write(sym, _df(N), prune_previous_version=True)

        qs.enable()
        qs.reset_stats()
        results = reader.batch_read([sym])
        stats = qs.get_query_stats()
        qs.disable()

    assert results[sym].data["a"][0] == N
    assert _version_ref_reads(stats) == 2
    assert _version_reads(stats) == 0


# ---- read_batch_and_join ----


def test_read_batch_and_join_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """batch_read_and_join recovers when a concurrent prune races a latest-version read."""
    from arcticdb import QueryBuilder

    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym_a, sym_b = "sym_a", "sym_b"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym_a, _df(0))
        writer.write(sym_b, _df(100))
        assert reader.read(sym_a).data["a"][0] == 0
        assert reader.read(sym_b).data["a"][0] == 100

        writer.write(sym_a, _df(1), prune_previous_version=True)

        q = QueryBuilder()
        q = q.concat("outer")
        result = reader.batch_read_and_join([sym_a, sym_b], q)

    assert len(result.versions) == 2


# ---- read_metadata / read_metadata_batch ----


def test_read_metadata_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """read_metadata recovers when a concurrent prune races a latest-version read."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0), metadata={"v": 0})
        assert reader.read_metadata(sym) is not None

        writer.write(sym, _df(1), metadata={"v": 1}, prune_previous_version=True)

        result = reader.read_metadata(sym)

    assert result.version == 1


def test_read_metadata_batch_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """batch_read_metadata recovers when a concurrent prune races a latest-version read."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0), metadata={"v": 0})
        assert reader.read_metadata(sym) is not None

        writer.write(sym, _df(1), metadata={"v": 1}, prune_previous_version=True)

        results = reader.batch_read_metadata([sym])

    assert sym in results
    assert results[sym].version == 1


# ---- get_info / batch_get_info (get_description / get_description_batch) ----


def test_get_info_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """get_info (backing get_description) recovers when a concurrent prune races a latest-version read."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0))
        assert reader.get_info(sym) is not None

        writer.write(sym, _df(1), prune_previous_version=True)

        info = reader.get_info(sym)

    assert info is not None
    assert info["rows"] == 1


def test_batch_get_info_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """batch_get_info (backing get_description_batch) recovers when a concurrent prune races a latest-version read."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0))
        assert reader.get_info(sym) is not None

        writer.write(sym, _df(1), prune_previous_version=True)

        results = reader.batch_get_info([sym])

    assert len(results) == 1
    assert results[0] is not None
    assert results[0]["rows"] == 1


# ---- read_modify_write ----


def test_read_modify_write_recovers_and_writes_target_once(in_memory_store_factory):
    """read_modify_write retries the source read when its version is pruned mid-flight, and the
    target version must be written exactly once — the write is outside the retry, so a retried
    source read must not produce a second target version."""
    from arcticdb import QueryBuilder

    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    src, dst = "src", "dst"

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(src, _df(0))
        assert reader.read(src).data["a"][0] == 0  # warm cache so reader sees src@v0 in stale chain

        writer.write(src, _df(1), prune_previous_version=True)

        q = QueryBuilder()
        q = q[q["a"] >= 0]
        reader._read_modify_write(src, q, dst)

        result = reader.read(dst)

    # Recovered the live source (v1), and the target was written exactly once (version 0).
    assert result.data["a"][0] == 1
    assert result.version == 0


# ---- read_column_stats / get_column_stats_info ----


def test_read_column_stats_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """read_column_stats recovers when a concurrent prune races a latest-version read. The pruned
    version's column-stats key is gone, so the read must re-resolve to the live version's stats."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"
    stats = {"a": {"MINMAX"}}

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0))
        writer.create_column_stats(sym, stats)
        assert reader.read_column_stats(sym) is not None  # warm cache so reader sees v0 in stale chain

        # Supersede and prune v0 (deletes v0's column-stats key too), then build stats for v1.
        writer.write(sym, _df(1), prune_previous_version=True)
        writer.create_column_stats(sym, stats)

        table = reader.read_column_stats(sym)

    assert table is not None


def test_get_column_stats_info_recovers_when_latest_version_pruned_concurrently(in_memory_store_factory):
    """get_column_stats_info recovers when a concurrent prune races a latest-version read."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"
    stats = {"a": {"MINMAX"}}

    with config_context("VersionMap.ReloadInterval", STICKY_RELOAD_INTERVAL):
        writer.write(sym, _df(0))
        writer.create_column_stats(sym, stats)
        assert reader.get_column_stats_info(sym) is not None

        writer.write(sym, _df(1), prune_previous_version=True)
        writer.create_column_stats(sym, stats)

        info = reader.get_column_stats_info(sym)

    assert info == stats


# Note: get_index_range (LocalVersionedEngine::get_index_range) is also wrapped with the retry
# primitive, but it has no Python binding and no other caller, so there is no deterministic
# Python-level test for it. It follows the same pattern as the methods covered above.
