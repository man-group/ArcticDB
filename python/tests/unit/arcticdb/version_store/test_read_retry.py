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
from arcticdb.util.test import config_context, config_context_multi, query_stats_operation_count

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


def test_read_without_retries_raises_when_version_pruned_concurrently(in_memory_store_factory):
    """Guard that the retry is load-bearing: with VersionStore.ReadRetries=0 the same race surfaces
    the missing-key error instead of recovering."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)
    sym = "sym"

    with config_context_multi({"VersionMap.ReloadInterval": STICKY_RELOAD_INTERVAL, "VersionStore.ReadRetries": 0}):
        writer.write(sym, _df(0))
        assert reader.read(sym).data["a"][0] == 0

        writer.write(sym, _df(1), prune_previous_version=True)

        with pytest.raises((NoDataFoundException, KeyNotFoundException)):
            reader.read(sym)


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
    With ReadRetries=5 the retry loop would fire for a latest read, but a pinned version query
    must always use max_attempts=1 regardless of the retry config."""
    writer = in_memory_store_factory()
    reader = in_memory_store_factory(reuse_name=True)

    with config_context_multi({"VersionMap.ReloadInterval": STICKY_RELOAD_INTERVAL, "VersionStore.ReadRetries": 5}):
        writer.write("sym", _df(0))
        assert reader.read("sym").data["a"][0] == 0  # warm cache so reader sees v0 in stale chain

        writer.write("sym", _df(1), prune_previous_version=True)

        # The stale cache still resolves v0, but its keys are gone. With max_attempts=1 for pinned
        # queries the error propagates immediately — no retry, no wrong-version return.
        with pytest.raises((NoDataFoundException, KeyNotFoundException)):
            reader.read("sym", as_of=0)


# Note: a snapshot test analogous to test_specific_version_read_does_not_retry_when_pruned is not
# included because ArcticDB protects snapshot-referenced keys from pruning — prune_previous_version
# leaves keys intact when they are held by a snapshot, so snapshot reads always succeed and the
# "no-retry" code path is not reachable through normal API usage.
