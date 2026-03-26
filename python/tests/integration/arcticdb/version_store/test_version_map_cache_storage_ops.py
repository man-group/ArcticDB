"""
Tests that verify VERSION key storage read counts for version map cache behavior.

Uses in-memory storage because it is instrumented with query_stats (LMDB and Azure are not).
S3 is also instrumented but requires external infrastructure. The operation key is Memory_GetObject.
"""

import time

import pandas as pd
import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb.util.test import config_context
from tests.util.mark import MEM_TESTS_MARK

pytestmark = MEM_TESTS_MARK

# Large reload interval to effectively disable TTL-based cache invalidation
MAX_RELOAD_INTERVAL = 2_000_000_000


def make_df():
    return pd.DataFrame({"col": [1, 2, 3]})


def make_ts_df(start_day=0, periods=3):
    """Create a DataFrame with a DatetimeIndex, suitable for append/update operations."""
    return pd.DataFrame({"col": range(periods)}, index=pd.date_range(f"2020-01-{start_day + 1:02d}", periods=periods))


def get_version_reads(stats):
    """Get the count of VERSION key reads from query stats."""
    return stats.get("storage_operations", {}).get("Memory_GetObject", {}).get("VERSION", {}).get("count", 0)


def get_version_ref_reads(stats):
    """Get the count of VERSION_REF key reads from query stats."""
    return stats.get("storage_operations", {}).get("Memory_GetObject", {}).get("VERSION_REF", {}).get("count", 0)


def test_read_latest_one_version(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_read_latest_one_version"
    with config_context("VersionMap.ReloadInterval", 0):
        lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        # LATEST only reads VERSION_REF, not VERSION keys
        assert get_version_ref_reads(stats) == 1
        assert get_version_reads(stats) == 0


def test_read_latest_many_versions(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_read_latest_many_versions"
    with config_context("VersionMap.ReloadInterval", 0):
        for _ in range(5):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_ref_reads(stats) == 1
        assert get_version_reads(stats) == 0


def test_read_downto_oldest(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_read_downto_oldest"
    with config_context("VersionMap.ReloadInterval", 0):
        for _ in range(5):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 5
        assert get_version_ref_reads(stats) == 1


def test_read_downto_latest(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_read_downto_latest"
    with config_context("VersionMap.ReloadInterval", 0):
        for _ in range(5):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=4)
        stats = qs.get_query_stats()
        # DOWNTO(4) finds the version in the ref, so no VERSION key reads needed
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_read_from_time_now(in_memory_store_factory, clear_query_stats):
    """FROM_TIME with a recent timestamp should find the latest version."""
    lib = in_memory_store_factory()
    sym = "test_read_from_time_now"
    with config_context("VersionMap.ReloadInterval", 0):
        for _ in range(5):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=pd.Timestamp.now())
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_list_versions(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_list_versions"
    with config_context("VersionMap.ReloadInterval", 0):
        for _ in range(5):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.list_versions(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 5
        assert get_version_ref_reads(stats) == 1


def test_read_after_delete_all(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_read_after_delete_all"
    with config_context("VersionMap.ReloadInterval", 0):
        for _ in range(3):
            lib.write(sym, make_df())
        lib.delete(sym)
        for _ in range(2):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        # LATEST only needs VERSION_REF
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_read_latest_then_read_latest(in_memory_store_factory, clear_query_stats):
    """LATEST -> LATEST should be a full cache hit (0 storage reads)."""
    lib = in_memory_store_factory()
    sym = "test_read_latest_then_read_latest"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(5):
            lib.write(sym, make_df())
        lib.read(sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_read_latest_then_read_specific(in_memory_store_factory, clear_query_stats):
    """LATEST -> DOWNTO(0) should be a cache miss (needs to load earlier versions)."""
    lib = in_memory_store_factory()
    sym = "test_read_latest_then_read_specific"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(5):
            lib.write(sym, make_df())
        lib.read(sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # Cache miss: must load all 5 VERSION keys + re-read VERSION_REF
        # The current limitation with current version map cache.
        # When we do writes, we add the version in the cache, but we never
        # update the oldest_loaded_index_version_ in the LoadProgress
        # as result of this the cache is not very useful and result in cache miss
        assert get_version_reads(stats) == 5
        assert get_version_ref_reads(stats) == 1


def test_read_oldest_then_read_latest(in_memory_store_factory, clear_query_stats):
    """DOWNTO(0) -> LATEST should be a cache hit (all versions already loaded)."""
    lib = in_memory_store_factory()
    sym = "test_read_oldest_then_read_latest"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(5):
            lib.write(sym, make_df())
        lib.read(sym, as_of=0)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_read_oldest_then_read_any(in_memory_store_factory, clear_query_stats):
    """DOWNTO(0) -> DOWNTO(3) should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_read_oldest_then_read_any"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(5):
            lib.write(sym, make_df())
        lib.read(sym, as_of=0)
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=3)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_read_latest_then_read_oldest(in_memory_store_factory, clear_query_stats):
    """DOWNTO(4) -> DOWNTO(0) should be a cache miss."""
    lib = in_memory_store_factory()
    sym = "test_read_latest_then_read_oldest"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(5):
            lib.write(sym, make_df())
        # This will trigger the cache load
        # The current limitation with current version map cache.
        # When we do writes, we add the version in the cache, but we never
        # update the oldest_loaded_index_version_ in the LoadProgress
        # as result of this the cache is not very useful and result in cache miss
        lib.read(sym, as_of=4)
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # Cache miss: must load all 5 VERSION keys + re-read VERSION_REF
        assert get_version_reads(stats) == 5
        assert get_version_ref_reads(stats) == 1


def test_list_versions_then_read(in_memory_store_factory, clear_query_stats):
    """list_versions (ALL) -> LATEST should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_list_versions_then_read"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(5):
            lib.write(sym, make_df())
        lib.list_versions(sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_read_latest_then_list_versions(in_memory_store_factory, clear_query_stats):
    """LATEST -> list_versions (ALL) should be a cache miss."""
    lib = in_memory_store_factory()
    sym = "test_read_latest_then_list_versions"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(5):
            lib.write(sym, make_df())
        lib.read(sym)
        qs.enable()
        qs.reset_stats()
        lib.list_versions(sym)
        stats = qs.get_query_stats()
        # Cache miss: must load all 5 VERSION keys + re-read VERSION_REF
        assert get_version_reads(stats) == 5
        assert get_version_ref_reads(stats) == 1


def test_from_time_then_downto_with_tombstone_all(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_from_time_then_downto_with_tombstone_all"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.delete(sym)
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        # FROM_TIME(now) loads all version keys including tombstone_all
        lib.read(sym, as_of=pd.Timestamp.now())
        qs.enable()
        qs.reset_stats()
        # DOWNTO(2) - oldest live version after delete+rewrite
        lib.read(sym, as_of=2)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_from_time_then_downto_nonexistent_with_tombstone_all(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_from_time_then_downto_nonexistent_with_tombstone_all"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.delete(sym)
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.read(sym, as_of=pd.Timestamp.now())
        qs.enable()
        qs.reset_stats()
        # DOWNTO(0) - version 0 was deleted, but cache should know this
        with pytest.raises(Exception):
            lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_alternating_reads_after_delete_rewrite(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = "test_alternating_reads_after_delete_rewrite"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.delete(sym)
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        # FROM_TIME loads all versions - writes already populated cache so no storage reads
        lib.read(sym, as_of=pd.Timestamp.now())
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0
        qs.reset_stats()
        # DOWNTO(2) - oldest live version, should be cache hit
        lib.read(sym, as_of=2)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_latest_then_downto_with_tombstone_all(in_memory_store_factory, clear_query_stats):
    lib_writer = in_memory_store_factory()
    lib_reader = in_memory_store_factory(reuse_name=True)
    sym = "test_latest_then_downto_with_tombstone_all"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib_writer.write(sym, make_df())
        lib_writer.write(sym, make_df())
        lib_writer.delete(sym)
        # Write enough versions after delete so LATEST doesn't cache all of them
        for _ in range(5):
            lib_writer.write(sym, make_df())
        lib_reader.read(sym)  # LATEST - reader only loads top of chain
        qs.enable()
        qs.reset_stats()
        # DOWNTO(2) - oldest live version, beyond what LATEST cached
        lib_reader.read(sym, as_of=2)
        stats = qs.get_query_stats()
        # Cache miss: must load 5 VERSION keys + re-read VERSION_REF
        assert get_version_reads(stats) == 5
        assert get_version_ref_reads(stats) == 1


def test_reload_interval_zero_always_reloads(in_memory_store_factory, clear_query_stats):
    """With ReloadInterval=0, every LATEST read should check VERSION_REF."""
    lib = in_memory_store_factory()
    sym = "test_reload_interval_zero_always_reloads"
    with config_context("VersionMap.ReloadInterval", 0):
        lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_cache_expires_after_ttl(in_memory_store_factory, clear_query_stats):
    """After TTL expires, the cache should be invalidated and VERSION_REF re-read."""
    lib = in_memory_store_factory()
    sym = "test_cache_expires_after_ttl"
    with config_context("VersionMap.ReloadInterval", 100):  # 100ms
        lib.write(sym, make_df())
        lib.read(sym)  # Populate cache
        time.sleep(0.2)  # Wait for TTL to expire
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_cache_valid_within_ttl(in_memory_store_factory, clear_query_stats):
    """Within TTL, the second read should be a full cache hit (0 storage reads)."""
    lib = in_memory_store_factory()
    sym = "test_cache_valid_within_ttl"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.read(sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_client_b_sees_stale_cache(in_memory_store_factory, clear_query_stats):
    """
    Client B caches v0. Client A writes v1. Client B reads latest with cache still valid
    and gets stale v0 (0 storage reads).
    """
    lib_a = in_memory_store_factory()
    lib_b = in_memory_store_factory(reuse_name=True)
    sym = "test_client_b_sees_stale_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib_a.write(sym, pd.DataFrame({"col": [0]}))
        result_b1 = lib_b.read(sym)
        assert result_b1.data["col"].iloc[0] == 0
        lib_a.write(sym, pd.DataFrame({"col": [1]}))
        qs.enable()
        qs.reset_stats()
        result_b2 = lib_b.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0
        assert result_b2.data["col"].iloc[0] == 0, "Expected stale v0 from cache"


def test_client_b_reloads_for_unknown_version(in_memory_store_factory, clear_query_stats):
    """
    Client B has v0 cached. When B tries to read v1 (not in cache), it must reload
    from storage.
    """
    lib_a = in_memory_store_factory()
    lib_b = in_memory_store_factory(reuse_name=True)
    sym = "test_client_b_reloads_for_unknown_version"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib_a.write(sym, pd.DataFrame({"col": [0]}))
        lib_b.read(sym)  # B caches v0
        lib_a.write(sym, pd.DataFrame({"col": [1]}))
        qs.enable()
        qs.reset_stats()
        result = lib_b.read(sym, as_of=1)
        stats = qs.get_query_stats()
        # B must read VERSION_REF to discover v1
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1
        assert result.data["col"].iloc[0] == 1


def test_write_populates_cache(in_memory_store_factory, clear_query_stats):
    """After a write, reading latest should be a cache hit (0 storage reads)."""
    lib = in_memory_store_factory()
    sym = "test_write_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_write_historical_read_misses_cache(in_memory_store_factory, clear_query_stats):
    """After multiple writes, reading a historical version misses the cache.

    Writes add entries to the cache but never update oldest_loaded_index_version_
    in LoadProgress, so DOWNTO queries must reload the full chain from storage.
    """
    lib = in_memory_store_factory()
    sym = "test_write_historical_read_misses_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(3):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # Cache miss: oldest_loaded_index_version_ is MAX after writes, must reload all 3
        assert get_version_reads(stats) == 3
        assert get_version_ref_reads(stats) == 1
        # After the reload, subsequent historical reads are cache hits
        qs.reset_stats()
        lib.read(sym, as_of=1)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_write_prune_populates_cache(in_memory_store_factory, clear_query_stats):
    """After a write with prune_previous_versions, reading latest should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_write_prune_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        # Note this is v1 api using prune_previous_version
        lib.write(sym, make_df(), prune_previous_version=True)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_append_populates_cache(in_memory_store_factory, clear_query_stats):
    """After write + append (2 versions), reading latest should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_append_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_ts_df(0))
        lib.append(sym, make_ts_df(3))
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_append_historical_read_one_prior_version(in_memory_store_factory, clear_query_stats):
    """After write + append, reading v0 needs only VERSION_REF (1 prior version).

    When there's only 1 version before the append, the append's LATEST reload walks
    the entire chain (1 entry) and sets is_earliest_version_loaded=true, so
    DOWNTO(0) is satisfied by the cache.
    """
    lib = in_memory_store_factory()
    sym = "test_append_historical_read_one_prior_version"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_ts_df(0))
        lib.append(sym, make_ts_df(3))
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_append_historical_read_many_prior_versions(in_memory_store_factory, clear_query_stats):
    """After 3 writes + append, reading v0 misses the cache.

    The append's LATEST reload only finds the latest version, not the full chain.
    """
    lib = in_memory_store_factory()
    sym = "test_append_historical_read_many_prior_versions"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_ts_df(0, periods=1))
        lib.write(sym, make_ts_df(0, periods=2))
        lib.write(sym, make_ts_df(0, periods=3))
        lib.append(sym, make_ts_df(3, periods=1))
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # Cache miss: 4 versions (3 writes + 1 append) must be loaded
        assert get_version_reads(stats) == 4
        assert get_version_ref_reads(stats) == 1


def test_update_populates_cache(in_memory_store_factory, clear_query_stats):
    """After write + update (2 versions), reading latest should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_update_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_ts_df(0))
        lib.update(sym, make_ts_df(1, periods=1))
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_update_historical_read_one_prior_version(in_memory_store_factory, clear_query_stats):
    """After write + update, reading v0 needs only VERSION_REF (1 prior version)."""
    lib = in_memory_store_factory()
    sym = "test_update_historical_read_one_prior_version"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_ts_df(0))
        lib.update(sym, make_ts_df(1, periods=1))
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_update_historical_read_many_prior_versions(in_memory_store_factory, clear_query_stats):
    """After 3 writes + update, reading v0 misses the cache."""
    lib = in_memory_store_factory()
    sym = "test_update_historical_read_many_prior_versions"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_ts_df(0))
        lib.write(sym, make_ts_df(0))
        lib.write(sym, make_ts_df(0))
        lib.update(sym, make_ts_df(1, periods=1))
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 4
        assert get_version_ref_reads(stats) == 1


def test_delete_version_populates_cache(in_memory_store_factory, clear_query_stats):
    """After deleting a specific version, reading latest should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_delete_version_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.delete_version(sym, 0)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_delete_version_historical_read_hits_cache(in_memory_store_factory, clear_query_stats):
    """After delete_version, reading a remaining historical version is a cache hit.

    delete_version does a full chain load, setting oldest_loaded_index_version_ correctly.
    """
    lib = in_memory_store_factory()
    sym = "test_delete_version_historical_read_hits_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(3):
            lib.write(sym, make_df())
        lib.delete_version(sym, 1)
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_write_metadata_populates_cache(in_memory_store_factory, clear_query_stats):
    """After write_metadata, reading latest should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_write_metadata_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write_metadata(sym, {"key": "value"})
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_write_metadata_historical_read_one_prior_version(in_memory_store_factory, clear_query_stats):
    """After write + write_metadata, reading v0 needs only VERSION_REF."""
    lib = in_memory_store_factory()
    sym = "test_write_metadata_historical_read_one_prior_version"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write_metadata(sym, {"key": "value"})
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_write_metadata_historical_read_many_prior_versions(in_memory_store_factory, clear_query_stats):
    """After 3 writes + write_metadata, reading v0 misses the cache."""
    lib = in_memory_store_factory()
    sym = "test_write_metadata_historical_read_many_prior_versions"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(3):
            lib.write(sym, make_df())
        lib.write_metadata(sym, {"key": "value"})
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # Cache miss: 4 versions (3 writes + 1 write_metadata)
        assert get_version_reads(stats) == 4
        assert get_version_ref_reads(stats) == 1


def test_delete_all_populates_cache(in_memory_store_factory, clear_query_stats):
    """After delete (all versions) + new writes, reading latest should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_delete_all_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.delete(sym)
        lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_delete_all_rewrite_historical_read(in_memory_store_factory, clear_query_stats):
    """After delete_all + 3 writes, reading the oldest live version is a cache hit.

    The delete operation does a full chain load, and subsequent writes preserve
    is_earliest_version_loaded=true via the tombstone_all marker.
    """
    lib = in_memory_store_factory()
    sym = "test_delete_all_rewrite_historical_read"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for _ in range(3):
            lib.write(sym, make_df())
        lib.delete(sym)
        for _ in range(3):
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        # v3 is the oldest live version after delete+rewrite
        lib.read(sym, as_of=3)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_prune_previous_versions_populates_cache(in_memory_store_factory, clear_query_stats):
    """After prune_previous_versions, reading latest should be a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_prune_previous_versions_populates_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.prune_previous_versions(sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_prune_then_read_remaining_version(in_memory_store_factory, clear_query_stats):
    """After prune, reading the only remaining version (by id) is a cache hit."""
    lib = in_memory_store_factory()
    sym = "test_prune_then_read_remaining_version"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.write(sym, make_df())
        lib.prune_previous_versions(sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=2)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_snapshot_does_not_invalidate_cache(in_memory_store_factory, clear_query_stats):
    """Creating a snapshot should not invalidate the version map cache."""
    lib = in_memory_store_factory()
    sym = "test_snapshot_does_not_invalidate_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.snapshot("snap1")
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_head_uses_cache(in_memory_store_factory, clear_query_stats):
    """head() should use the version map cache when available."""
    lib = in_memory_store_factory()
    sym = "test_head_uses_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.read(sym)  # populate cache
        qs.enable()
        qs.reset_stats()
        lib.head(sym, 2)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_tail_uses_cache(in_memory_store_factory, clear_query_stats):
    """tail() should use the version map cache when available."""
    lib = in_memory_store_factory()
    sym = "test_tail_uses_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.read(sym)  # populate cache
        qs.enable()
        qs.reset_stats()
        lib.tail(sym, 2)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_read_metadata_uses_cache(in_memory_store_factory, clear_query_stats):
    """read_metadata() should use the version map cache when available."""
    lib = in_memory_store_factory()
    sym = "test_read_metadata_uses_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df(), metadata={"key": "val"})
        lib.read(sym)  # populate cache
        qs.enable()
        qs.reset_stats()
        lib.read_metadata(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_has_symbol_uses_cache(in_memory_store_factory, clear_query_stats):
    """has_symbol() should use the version map cache when available."""
    lib = in_memory_store_factory()
    sym = "test_has_symbol_uses_cache"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.read(sym)  # populate cache
        qs.enable()
        qs.reset_stats()
        assert lib.has_symbol(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_write_populates_cache(in_memory_store_factory, clear_query_stats):
    """After batch_write, reading latest of each symbol should be a cache hit."""
    lib = in_memory_store_factory()
    syms = ["test_batch_write_0", "test_batch_write_1", "test_batch_write_2"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.batch_write(syms, [make_df() for _ in syms])
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_write_historical_read_misses_cache(in_memory_store_factory, clear_query_stats):
    """After multiple batch_writes, reading historical versions misses the cache."""
    lib = in_memory_store_factory()
    syms = ["test_batch_write_hist_0", "test_batch_write_hist_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.batch_write(syms, [make_df() for _ in syms])
        lib.batch_write(syms, [make_df() for _ in syms])
        lib.batch_write(syms, [make_df() for _ in syms])
        qs.enable()
        qs.reset_stats()
        # Read v0 of each symbol — cache miss for each
        for sym in syms:
            lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # 3 VERSION reads per symbol * 2 symbols = 6
        assert get_version_reads(stats) == 6
        assert get_version_ref_reads(stats) == 2


def test_batch_append_populates_cache(in_memory_store_factory, clear_query_stats):
    """After batch_append, reading latest of each symbol should be a cache hit."""
    lib = in_memory_store_factory()
    syms = ["test_batch_append_0", "test_batch_append_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            lib.write(sym, make_ts_df(0))
        lib.batch_append(syms, [make_ts_df(3) for _ in syms])
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_append_historical_read_many_prior_versions(in_memory_store_factory, clear_query_stats):
    """After 2 writes + batch_append, reading v0 misses the cache."""
    lib = in_memory_store_factory()
    syms = ["test_batch_append_hist_0", "test_batch_append_hist_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            lib.write(sym, make_ts_df(0, periods=1))
            lib.write(sym, make_ts_df(0, periods=2))
        lib.batch_append(syms, [make_ts_df(2, periods=1) for _ in syms])
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # 3 VERSION reads per symbol * 2 symbols = 6
        assert get_version_reads(stats) == 6
        assert get_version_ref_reads(stats) == 2


def test_batch_read_uses_cache(in_memory_store_factory, clear_query_stats):
    """batch_read should use cached version maps when available."""
    lib = in_memory_store_factory()
    syms = ["test_batch_read_0", "test_batch_read_1", "test_batch_read_2"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            lib.write(sym, make_df())
        # All writes populate cache
        qs.enable()
        qs.reset_stats()
        lib.batch_read(syms)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_read_historical_misses_cache(in_memory_store_factory, clear_query_stats):
    """batch_read with as_of for historical versions misses the cache after writes."""
    lib = in_memory_store_factory()
    syms = ["test_batch_read_hist_0", "test_batch_read_hist_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            for _ in range(3):
                lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        lib.batch_read(syms, as_ofs=[0, 0])
        stats = qs.get_query_stats()
        # 3 VERSION reads per symbol * 2 symbols = 6
        assert get_version_reads(stats) == 6
        assert get_version_ref_reads(stats) == 2


def test_batch_write_metadata_populates_cache(in_memory_store_factory, clear_query_stats):
    """After batch_write_metadata, reading latest of each symbol should be a cache hit."""
    lib = in_memory_store_factory()
    syms = ["test_batch_wm_0", "test_batch_wm_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            lib.write(sym, make_df())
        lib.batch_write_metadata(syms, [{"k": i} for i in range(len(syms))])
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_write_metadata_historical_read(in_memory_store_factory, clear_query_stats):
    """After 3 writes + batch_write_metadata, reading v0 misses the cache."""
    lib = in_memory_store_factory()
    syms = ["test_batch_wm_hist_0", "test_batch_wm_hist_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            for _ in range(3):
                lib.write(sym, make_df())
        lib.batch_write_metadata(syms, [{"k": i} for i in range(len(syms))])
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        # 4 VERSION reads per symbol (3 writes + 1 write_metadata) * 2 symbols = 8
        assert get_version_reads(stats) == 8
        assert get_version_ref_reads(stats) == 2


def test_batch_read_metadata_uses_cache(in_memory_store_factory, clear_query_stats):
    """batch_read_metadata should use cached version maps when available."""
    lib = in_memory_store_factory()
    syms = ["test_batch_rm_0", "test_batch_rm_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            lib.write(sym, make_df(), metadata={"k": "v"})
        # Writes populate cache
        qs.enable()
        qs.reset_stats()
        lib.batch_read_metadata(syms)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_delete_symbols_then_write_read(in_memory_store_factory, clear_query_stats):
    """After batch_delete_symbols + new writes, reads should be cache hits."""
    lib = in_memory_store_factory()
    syms = ["test_batch_del_0", "test_batch_del_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            lib.write(sym, make_df())
        lib.batch_delete_symbols(syms)
        for sym in syms:
            lib.write(sym, make_df())
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_delete_versions_populates_cache(in_memory_store_factory, clear_query_stats):
    """After batch_delete_versions, reading latest of each symbol should be a cache hit."""
    lib = in_memory_store_factory()
    syms = ["test_batch_delv_0", "test_batch_delv_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            lib.write(sym, make_df())
            lib.write(sym, make_df())
        lib.batch_delete_versions(syms, [[0], [0]])
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


def test_batch_delete_versions_historical_read_hits_cache(in_memory_store_factory, clear_query_stats):
    """After batch_delete_versions, reading remaining historical versions is a cache hit.

    Like delete_version, batch_delete_versions does a full chain load.
    """
    lib = in_memory_store_factory()
    syms = ["test_batch_delv_hist_0", "test_batch_delv_hist_1"]
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        for sym in syms:
            for _ in range(3):
                lib.write(sym, make_df())
        lib.batch_delete_versions(syms, [[1], [1]])
        qs.enable()
        qs.reset_stats()
        for sym in syms:
            lib.read(sym, as_of=0)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0
