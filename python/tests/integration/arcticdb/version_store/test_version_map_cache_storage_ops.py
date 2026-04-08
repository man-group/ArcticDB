"""
Tests that verify VERSION key storage read counts for version map cache behavior.

Uses in-memory storage because it is instrumented with query_stats (LMDB and Azure are not).
S3 is also instrumented but requires external infrastructure. The operation key is Memory_GetObject.

Test dimensions:
- Version chain shapes (simple, tombstone_all, individual tombstone, append, update, prune)
- Cache options (no_cache=ReloadInterval 0, cache_on=ReloadInterval MAX)
- as_of query types (None/LATEST, int/DOWNTO, Timestamp/FROM_TIME, str/SNAPSHOT)
- Read functions (read, head, tail, read_metadata, has_symbol, batch_read)
- Write operations (write, append, update, batch_write, batch_append, write_metadata, snapshot)
"""

import time
import uuid
from collections import namedtuple

import pandas as pd
import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb.exceptions import NoSuchVersionException
from arcticdb.util.test import config_context
from tests.util.mark import MEM_TESTS_MARK

pytestmark = MEM_TESTS_MARK

MAX_RELOAD_INTERVAL = 2_000_000_000_000


def make_df():
    return pd.DataFrame({"col": [1, 2, 3]})


def make_ts_df(start_day=0, periods=3):
    return pd.DataFrame({"col": range(periods)}, index=pd.date_range(f"2020-01-{start_day + 1:02d}", periods=periods))


def get_version_reads(stats):
    return stats.get("storage_operations", {}).get("Memory_GetObject", {}).get("VERSION", {}).get("count", 0)


def get_version_ref_reads(stats):
    return stats.get("storage_operations", {}).get("Memory_GetObject", {}).get("VERSION_REF", {}).get("count", 0)


# =============================================================================
# Chain setup functions
# =============================================================================
# Each returns a ChainInfo with metadata about the created version chain.

ChainInfo = namedtuple(
    "ChainInfo", ["latest_version", "oldest_undeleted_version", "mid_version", "snapshot", "mid_ts", "total_versions"]
)


def _ts_from_version_info(version_info):
    """Convert VersionedItem.timestamp (nanosecond int) to pd.Timestamp for FROM_TIME queries."""
    return pd.Timestamp(version_info.timestamp, unit="ns", tz="UTC")


def setup_simple_6(lib, sym):
    """v5 -> v4 -> v3 -> v2 -> v1 -> v0"""
    vinfos = [lib.write(sym, make_df()) for _ in range(6)]
    lib.snapshot(sym + "_snap", versions={sym: 3})
    return ChainInfo(5, 0, 3, sym + "_snap", _ts_from_version_info(vinfos[3]), 6)


def setup_multi_tombstone_all(lib, sym):
    """v4 -> v3 -> v2 -> TOMBSTONE_ALL -> v1 -> TOMBSTONE_ALL -> v0"""
    lib.write(sym, make_df())
    lib.delete(sym)
    lib.write(sym, make_df())
    lib.delete(sym)
    vinfos = [lib.write(sym, make_df()) for _ in range(3)]
    lib.snapshot(sym + "_snap", versions={sym: 3})
    return ChainInfo(4, 2, 3, sym + "_snap", _ts_from_version_info(vinfos[1]), 5)


def setup_multi_individual_tombstone(lib, sym):
    """v5 -> TOMBSTONE(v3) -> TOMBSTONE(v1) -> v4 -> v3 -> v2 -> v1 -> v0"""
    vinfos = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 1)
    lib.delete_version(sym, 3)
    lib.snapshot(sym + "_snap", versions={sym: 2})
    return ChainInfo(5, 0, 2, sym + "_snap", _ts_from_version_info(vinfos[2]), 6)


def setup_append_chain(lib, sym):
    """v2 -> v1 -> v0 via write + 2 appends"""
    vinfos = [
        lib.write(sym, make_ts_df(0, 2)),
        lib.append(sym, make_ts_df(2, 2)),
        lib.append(sym, make_ts_df(4, 2)),
    ]
    lib.snapshot(sym + "_snap", versions={sym: 1})
    return ChainInfo(2, 0, 1, sym + "_snap", _ts_from_version_info(vinfos[1]), 3)


def setup_update_chain(lib, sym):
    """v2 -> v1 -> v0 via write + 2 updates"""
    vinfos = [
        lib.write(sym, make_ts_df(0, 3)),
        lib.update(sym, make_ts_df(1, 1)),
        lib.update(sym, make_ts_df(2, 1)),
    ]
    lib.snapshot(sym + "_snap", versions={sym: 1})
    return ChainInfo(2, 0, 1, sym + "_snap", _ts_from_version_info(vinfos[1]), 3)


def setup_prune_chain(lib, sym):
    """v2 only (v0, v1 pruned)"""
    vinfos = [lib.write(sym, make_df()) for _ in range(3)]
    lib.prune_previous_versions(sym)
    lib.snapshot(sym + "_snap", versions={sym: 2})
    return ChainInfo(2, 2, 2, sym + "_snap", _ts_from_version_info(vinfos[2]), 3)


def setup_tombstone_all_then_writes(lib, sym):
    """v6 -> v5 -> v4 -> v3 -> v2 ->TOMBSTONE_ALL -> v1 (delete all first, then 6 writes)"""
    lib.write(sym, make_df())
    lib.delete(sym)
    vinfos = [lib.write(sym, make_df()) for _ in range(6)]
    lib.snapshot(sym + "_snap", versions={sym: 4})
    return ChainInfo(6, 1, 4, sym + "_snap", _ts_from_version_info(vinfos[3]), 7)


def setup_tombstone_latest(lib, sym):
    """TOMBSTONE(v5) -> v5 -> v4 -> v3 -> v2 -> v1 -> v0 (delete latest version only)"""
    vinfos = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 5)
    lib.snapshot(sym + "_snap", versions={sym: 3})
    return ChainInfo(4, 0, 3, sym + "_snap", _ts_from_version_info(vinfos[3]), 6)


def setup_tombstone_all_every_version(lib, sym):
    """v11->...->TOMBSTONE_ALL->v5->TOMBSTONE_ALL->...->v0 (delete after each of 6 writes, then 6 more)"""
    for _ in range(6):
        lib.write(sym, make_df())
        lib.delete(sym)
    vinfos = [lib.write(sym, make_df()) for _ in range(6)]
    lib.snapshot(sym + "_snap", versions={sym: 9})
    return ChainInfo(11, 6, 9, sym + "_snap", _ts_from_version_info(vinfos[3]), 12)


def setup_tombstone_even_plus_latest(lib, sym):
    """TOMBSTONE(v5)->TOMBSTONE(v4)->v5->v4->TOMBSTONE(v2)->v3->v2->TOMBSTONE(v0)->v1->v0"""
    vinfos = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 0)
    lib.delete_version(sym, 2)
    lib.delete_version(sym, 4)
    lib.delete_version(sym, 5)
    lib.snapshot(sym + "_snap", versions={sym: 3})
    return ChainInfo(3, 1, 3, sym + "_snap", _ts_from_version_info(vinfos[3]), 6)


CHAIN_SETUPS = [
    pytest.param(setup_simple_6, id="simple_6"),
    pytest.param(setup_multi_tombstone_all, id="multi_tombstone_all"),
    pytest.param(setup_multi_individual_tombstone, id="multi_individual_tombstone"),
    pytest.param(setup_append_chain, id="append_chain"),
    pytest.param(setup_update_chain, id="update_chain"),
    pytest.param(setup_prune_chain, id="prune_chain"),
    pytest.param(setup_tombstone_all_then_writes, id="tombstone_all_then_writes"),
    pytest.param(setup_tombstone_latest, id="tombstone_latest"),
    pytest.param(setup_tombstone_all_every_version, id="tombstone_all_every_version"),
    pytest.param(setup_tombstone_even_plus_latest, id="tombstone_even_plus_latest"),
]

# Expected (VERSION_reads, VERSION_REF_reads) for each (chain, cache, as_of) combination.
# Measured empirically with a fresh library per combination. Snapshot reads always bypass
# the version map cache (0, 0). Chains with delete/delete_version do a full chain load
# during setup, so cache_on results are often (0, 0) even for historical reads.
EXPECTED_COUNTS = {
    ("simple_6", "no_cache", "latest"): (0, 1),
    ("simple_6", "no_cache", "version_latest"): (0, 1),
    ("simple_6", "no_cache", "oldest_undeleted_version"): (6, 1),
    ("simple_6", "no_cache", "version_mid"): (3, 1),
    ("simple_6", "no_cache", "timestamp_mid"): (3, 1),
    ("simple_6", "no_cache", "snapshot"): (0, 0),
    ("simple_6", "no_cache", "list_versions"): (6, 1),
    ("simple_6", "cache_on", "latest"): (0, 0),
    ("simple_6", "cache_on", "version_latest"): (0, 0),
    ("simple_6", "cache_on", "oldest_undeleted_version"): (6, 1),
    ("simple_6", "cache_on", "version_mid"): (0, 0),
    ("simple_6", "cache_on", "timestamp_mid"): (0, 0),
    ("simple_6", "cache_on", "snapshot"): (0, 0),
    ("simple_6", "cache_on", "list_versions"): (6, 1),
    ("multi_tombstone_all", "no_cache", "latest"): (0, 1),
    ("multi_tombstone_all", "no_cache", "version_latest"): (0, 1),
    ("multi_tombstone_all", "no_cache", "oldest_undeleted_version"): (3, 1),
    ("multi_tombstone_all", "no_cache", "version_mid"): (0, 1),
    ("multi_tombstone_all", "no_cache", "timestamp_mid"): (0, 1),
    ("multi_tombstone_all", "no_cache", "snapshot"): (0, 0),
    ("multi_tombstone_all", "no_cache", "list_versions"): (5, 1),
    ("multi_tombstone_all", "cache_on", "latest"): (0, 0),
    ("multi_tombstone_all", "cache_on", "version_latest"): (0, 0),
    ("multi_tombstone_all", "cache_on", "oldest_undeleted_version"): (0, 0),
    ("multi_tombstone_all", "cache_on", "version_mid"): (0, 0),
    ("multi_tombstone_all", "cache_on", "timestamp_mid"): (0, 0),
    ("multi_tombstone_all", "cache_on", "snapshot"): (0, 0),
    ("multi_tombstone_all", "cache_on", "list_versions"): (0, 0),
    ("multi_individual_tombstone", "no_cache", "latest"): (3, 1),
    ("multi_individual_tombstone", "no_cache", "version_latest"): (3, 1),
    ("multi_individual_tombstone", "no_cache", "oldest_undeleted_version"): (8, 1),
    ("multi_individual_tombstone", "no_cache", "version_mid"): (6, 1),
    ("multi_individual_tombstone", "no_cache", "timestamp_mid"): (6, 1),
    ("multi_individual_tombstone", "no_cache", "snapshot"): (0, 0),
    ("multi_individual_tombstone", "no_cache", "list_versions"): (8, 1),
    ("multi_individual_tombstone", "cache_on", "latest"): (0, 0),
    ("multi_individual_tombstone", "cache_on", "version_latest"): (0, 0),
    ("multi_individual_tombstone", "cache_on", "oldest_undeleted_version"): (0, 0),
    ("multi_individual_tombstone", "cache_on", "version_mid"): (0, 0),
    ("multi_individual_tombstone", "cache_on", "timestamp_mid"): (0, 0),
    ("multi_individual_tombstone", "cache_on", "snapshot"): (0, 0),
    ("multi_individual_tombstone", "cache_on", "list_versions"): (0, 0),
    ("append_chain", "no_cache", "latest"): (0, 1),
    ("append_chain", "no_cache", "version_latest"): (0, 1),
    ("append_chain", "no_cache", "oldest_undeleted_version"): (3, 1),
    ("append_chain", "no_cache", "version_mid"): (0, 1),
    ("append_chain", "no_cache", "timestamp_mid"): (0, 1),
    ("append_chain", "no_cache", "snapshot"): (0, 0),
    ("append_chain", "no_cache", "list_versions"): (3, 1),
    ("append_chain", "cache_on", "latest"): (0, 0),
    ("append_chain", "cache_on", "version_latest"): (0, 0),
    ("append_chain", "cache_on", "oldest_undeleted_version"): (3, 1),
    ("append_chain", "cache_on", "version_mid"): (0, 0),
    ("append_chain", "cache_on", "timestamp_mid"): (0, 0),
    ("append_chain", "cache_on", "snapshot"): (0, 0),
    ("append_chain", "cache_on", "list_versions"): (3, 1),
    ("update_chain", "no_cache", "latest"): (0, 1),
    ("update_chain", "no_cache", "version_latest"): (0, 1),
    ("update_chain", "no_cache", "oldest_undeleted_version"): (3, 1),
    ("update_chain", "no_cache", "version_mid"): (0, 1),
    ("update_chain", "no_cache", "timestamp_mid"): (0, 1),
    ("update_chain", "no_cache", "snapshot"): (0, 0),
    ("update_chain", "no_cache", "list_versions"): (3, 1),
    ("update_chain", "cache_on", "latest"): (0, 0),
    ("update_chain", "cache_on", "version_latest"): (0, 0),
    ("update_chain", "cache_on", "oldest_undeleted_version"): (3, 1),
    ("update_chain", "cache_on", "version_mid"): (0, 0),
    ("update_chain", "cache_on", "timestamp_mid"): (0, 0),
    ("update_chain", "cache_on", "snapshot"): (0, 0),
    ("update_chain", "cache_on", "list_versions"): (3, 1),
    ("prune_chain", "no_cache", "latest"): (2, 1),
    ("prune_chain", "no_cache", "version_latest"): (2, 1),
    ("prune_chain", "no_cache", "oldest_undeleted_version"): (2, 1),
    ("prune_chain", "no_cache", "version_mid"): (2, 1),
    ("prune_chain", "no_cache", "timestamp_mid"): (2, 1),
    ("prune_chain", "no_cache", "snapshot"): (0, 0),
    ("prune_chain", "no_cache", "list_versions"): (3, 1),
    ("prune_chain", "cache_on", "latest"): (0, 0),
    ("prune_chain", "cache_on", "version_latest"): (0, 0),
    ("prune_chain", "cache_on", "oldest_undeleted_version"): (0, 0),
    ("prune_chain", "cache_on", "version_mid"): (0, 0),
    ("prune_chain", "cache_on", "timestamp_mid"): (0, 0),
    ("prune_chain", "cache_on", "snapshot"): (0, 0),
    ("prune_chain", "cache_on", "list_versions"): (0, 0),
    ("tombstone_all_then_writes", "no_cache", "latest"): (0, 1),
    ("tombstone_all_then_writes", "no_cache", "version_latest"): (0, 1),
    ("tombstone_all_then_writes", "no_cache", "oldest_undeleted_version"): (6, 1),
    ("tombstone_all_then_writes", "no_cache", "version_mid"): (3, 1),
    ("tombstone_all_then_writes", "no_cache", "timestamp_mid"): (3, 1),
    ("tombstone_all_then_writes", "no_cache", "snapshot"): (0, 0),
    ("tombstone_all_then_writes", "no_cache", "list_versions"): (8, 1),
    ("tombstone_all_then_writes", "cache_on", "latest"): (0, 0),
    ("tombstone_all_then_writes", "cache_on", "version_latest"): (0, 0),
    ("tombstone_all_then_writes", "cache_on", "oldest_undeleted_version"): (0, 0),
    ("tombstone_all_then_writes", "cache_on", "version_mid"): (0, 0),
    ("tombstone_all_then_writes", "cache_on", "timestamp_mid"): (0, 0),
    ("tombstone_all_then_writes", "cache_on", "snapshot"): (0, 0),
    ("tombstone_all_then_writes", "cache_on", "list_versions"): (0, 0),
    ("tombstone_latest", "no_cache", "latest"): (3, 1),
    ("tombstone_latest", "no_cache", "version_latest"): (3, 1),
    ("tombstone_latest", "no_cache", "oldest_undeleted_version"): (7, 1),
    ("tombstone_latest", "no_cache", "version_mid"): (4, 1),
    ("tombstone_latest", "no_cache", "timestamp_mid"): (4, 1),
    ("tombstone_latest", "no_cache", "snapshot"): (0, 0),
    ("tombstone_latest", "no_cache", "list_versions"): (7, 1),
    ("tombstone_latest", "cache_on", "latest"): (0, 0),
    ("tombstone_latest", "cache_on", "version_latest"): (0, 0),
    ("tombstone_latest", "cache_on", "oldest_undeleted_version"): (0, 0),
    ("tombstone_latest", "cache_on", "version_mid"): (0, 0),
    ("tombstone_latest", "cache_on", "timestamp_mid"): (0, 0),
    ("tombstone_latest", "cache_on", "snapshot"): (0, 0),
    ("tombstone_latest", "cache_on", "list_versions"): (0, 0),
    ("tombstone_all_every_version", "no_cache", "latest"): (0, 1),
    ("tombstone_all_every_version", "no_cache", "version_latest"): (0, 1),
    ("tombstone_all_every_version", "no_cache", "oldest_undeleted_version"): (6, 1),
    ("tombstone_all_every_version", "no_cache", "version_mid"): (3, 1),
    ("tombstone_all_every_version", "no_cache", "timestamp_mid"): (3, 1),
    ("tombstone_all_every_version", "no_cache", "snapshot"): (0, 0),
    ("tombstone_all_every_version", "no_cache", "list_versions"): (8, 1),
    ("tombstone_all_every_version", "cache_on", "latest"): (0, 0),
    ("tombstone_all_every_version", "cache_on", "version_latest"): (0, 0),
    ("tombstone_all_every_version", "cache_on", "oldest_undeleted_version"): (0, 0),
    ("tombstone_all_every_version", "cache_on", "version_mid"): (0, 0),
    ("tombstone_all_every_version", "cache_on", "timestamp_mid"): (0, 0),
    ("tombstone_all_every_version", "cache_on", "snapshot"): (0, 0),
    ("tombstone_all_every_version", "cache_on", "list_versions"): (0, 0),
    ("tombstone_even_plus_latest", "no_cache", "latest"): (7, 1),
    ("tombstone_even_plus_latest", "no_cache", "version_latest"): (7, 1),
    ("tombstone_even_plus_latest", "no_cache", "oldest_undeleted_version"): (9, 1),
    ("tombstone_even_plus_latest", "no_cache", "version_mid"): (7, 1),
    ("tombstone_even_plus_latest", "no_cache", "timestamp_mid"): (7, 1),
    ("tombstone_even_plus_latest", "no_cache", "snapshot"): (0, 0),
    ("tombstone_even_plus_latest", "no_cache", "list_versions"): (10, 1),
    ("tombstone_even_plus_latest", "cache_on", "latest"): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", "version_latest"): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", "oldest_undeleted_version"): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", "version_mid"): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", "timestamp_mid"): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", "snapshot"): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", "list_versions"): (0, 0),
}

AS_OF_TYPES = ["latest", "version_latest", "oldest_undeleted_version", "version_mid", "timestamp_mid", "snapshot"]


def resolve_as_of(as_of_type, chain_info):
    if as_of_type == "latest":
        return None
    elif as_of_type == "version_latest":
        return chain_info.latest_version
    elif as_of_type == "oldest_undeleted_version":
        return chain_info.oldest_undeleted_version
    elif as_of_type == "version_mid":
        return chain_info.mid_version
    elif as_of_type == "timestamp_mid":
        return chain_info.mid_ts
    elif as_of_type == "snapshot":
        return chain_info.snapshot


# =============================================================================
# Test 1: Core cross-product — chain × cache × as_of
# =============================================================================


@pytest.mark.parametrize("chain_setup", CHAIN_SETUPS)
@pytest.mark.parametrize("reload_interval", [0, MAX_RELOAD_INTERVAL], ids=["no_cache", "cache_on"])
@pytest.mark.parametrize("as_of_type", AS_OF_TYPES)
def test_read_version_chain(in_memory_store_factory, clear_query_stats, chain_setup, reload_interval, as_of_type):
    """Test VERSION/VERSION_REF read counts for read() across all chain shapes, cache states, and as_of types."""
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    cache_id = "no_cache" if reload_interval == 0 else "cache_on"
    chain_id = chain_setup.__name__.replace("setup_", "")

    with config_context("VersionMap.ReloadInterval", reload_interval):
        chain_info = chain_setup(lib, sym)
        as_of = resolve_as_of(as_of_type, chain_info)

        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=as_of)
        stats = qs.get_query_stats()

        expected_ver, expected_ref = EXPECTED_COUNTS[(chain_id, cache_id, as_of_type)]
        assert get_version_reads(stats) == expected_ver
        assert get_version_ref_reads(stats) == expected_ref


# =============================================================================
# Test 2: list_versions across chains
# =============================================================================


@pytest.mark.parametrize("chain_setup", CHAIN_SETUPS)
@pytest.mark.parametrize("reload_interval", [0, MAX_RELOAD_INTERVAL], ids=["no_cache", "cache_on"])
def test_list_versions_chain(in_memory_store_factory, clear_query_stats, chain_setup, reload_interval):
    """Test VERSION/VERSION_REF read counts for list_versions across all chain shapes."""
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    cache_id = "no_cache" if reload_interval == 0 else "cache_on"
    chain_id = chain_setup.__name__.replace("setup_", "")

    with config_context("VersionMap.ReloadInterval", reload_interval):
        chain_setup(lib, sym) if callable(chain_setup) else chain_setup.values[0](lib, sym)

        qs.enable()
        qs.reset_stats()
        lib.list_versions(sym)
        stats = qs.get_query_stats()

        expected_ver, expected_ref = EXPECTED_COUNTS[(chain_id, cache_id, "list_versions")]
        assert get_version_reads(stats) == expected_ver
        assert get_version_ref_reads(stats) == expected_ref


# =============================================================================
# Test 3: Read-like ops equivalence (warm cache)
# =============================================================================
# After list_versions warms the cache, all read-like ops should produce 0 VERSION
# and 0 VERSION_REF reads for version/latest/snapshot as_of queries.


@pytest.mark.parametrize(
    "read_fn",
    [
        pytest.param(lambda lib, sym, as_of: lib.read(sym, as_of=as_of), id="read"),
        pytest.param(lambda lib, sym, as_of: lib.head(sym, 1, as_of=as_of), id="head"),
        pytest.param(lambda lib, sym, as_of: lib.tail(sym, 1, as_of=as_of), id="tail"),
        pytest.param(lambda lib, sym, as_of: lib.read_metadata(sym, as_of=as_of), id="read_metadata"),
        pytest.param(lambda lib, sym, as_of: lib.has_symbol(sym, as_of=as_of), id="has_symbol"),
        pytest.param(lambda lib, sym, as_of: lib.batch_read([sym], as_ofs=[as_of]), id="batch_read"),
    ],
)
@pytest.mark.parametrize("as_of_type", AS_OF_TYPES)
def test_read_ops_warm_cache(in_memory_store_factory, clear_query_stats, read_fn, as_of_type):
    """After list_versions warms the cache, all read-like ops use it."""
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        chain_info = setup_simple_6(lib, sym)
        lib.list_versions(sym)  # warm cache fully

        as_of = resolve_as_of(as_of_type, chain_info)
        qs.enable()
        qs.reset_stats()
        read_fn(lib, sym, as_of)
        stats = qs.get_query_stats()

        # After list_versions fully warms the cache, all read-like ops
        # should be cache hits regardless of as_of type
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


# =============================================================================
# Test 4: Mutation → read latest (cache hit)
# =============================================================================


def _mut_write(lib, sym):
    lib.write(sym, make_df())


def _mut_append(lib, sym):
    lib.write(sym, make_ts_df(0))
    lib.append(sym, make_ts_df(3))


def _mut_update(lib, sym):
    lib.write(sym, make_ts_df(0))
    lib.update(sym, make_ts_df(1, periods=1))


def _mut_batch_write(lib, sym):
    lib.batch_write([sym], [make_df()])


def _mut_batch_append(lib, sym):
    lib.write(sym, make_ts_df(0))
    lib.batch_append([sym], [make_ts_df(3)])


def _mut_write_metadata(lib, sym):
    lib.write(sym, make_df())
    lib.write_metadata(sym, {"key": "value"})


def _mut_snapshot(lib, sym):
    lib.write(sym, make_df())
    lib.snapshot(sym + "_snap")


@pytest.mark.parametrize(
    "mutation_fn",
    [
        pytest.param(_mut_write, id="write"),
        pytest.param(_mut_append, id="append"),
        pytest.param(_mut_update, id="update"),
        pytest.param(_mut_batch_write, id="batch_write"),
        pytest.param(_mut_batch_append, id="batch_append"),
        pytest.param(_mut_write_metadata, id="write_metadata"),
        pytest.param(_mut_snapshot, id="snapshot"),
    ],
)
@pytest.mark.parametrize("reload_interval", [0, MAX_RELOAD_INTERVAL], ids=["no_cache", "cache_on"])
def test_mutation_then_read_latest(in_memory_store_factory, clear_query_stats, mutation_fn, reload_interval):
    """After any mutation, reading latest should be a cache hit (cache_on) or 1 VERSION_REF read (no_cache)."""
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    with config_context("VersionMap.ReloadInterval", reload_interval):
        mutation_fn(lib, sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        if reload_interval == 0:
            assert get_version_ref_reads(stats) == 1
        else:
            assert get_version_ref_reads(stats) == 0


# =============================================================================
# Test 5: TTL Invalidation (standalone, require time.sleep)
# =============================================================================


def test_reload_interval_zero_always_reloads(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
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
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    with config_context("VersionMap.ReloadInterval", 100_000_000):  # 100ms in nanoseconds
        lib.write(sym, make_df())
        lib.read(sym)
        time.sleep(0.2)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1


def test_cache_valid_within_ttl(in_memory_store_factory, clear_query_stats):
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib.write(sym, make_df())
        lib.read(sym)
        qs.enable()
        qs.reset_stats()
        lib.read(sym)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0


# =============================================================================
# Test 6: Two-Client Scenarios
# =============================================================================


def test_client_b_sees_stale_cache(in_memory_store_factory, clear_query_stats):
    lib_a = in_memory_store_factory()
    lib_b = in_memory_store_factory(reuse_name=True)
    sym = f"sym_{uuid.uuid4().hex[:8]}"
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
        assert result_b2.data["col"].iloc[0] == 0


def test_client_b_reloads_for_unknown_version(in_memory_store_factory, clear_query_stats):
    lib_a = in_memory_store_factory()
    lib_b = in_memory_store_factory(reuse_name=True)
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        lib_a.write(sym, pd.DataFrame({"col": [0]}))
        lib_b.read(sym)
        lib_a.write(sym, pd.DataFrame({"col": [1]}))
        qs.enable()
        qs.reset_stats()
        result = lib_b.read(sym, as_of=1)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 1
        assert result.data["col"].iloc[0] == 1


# =============================================================================
# Test 7: Cross-query-type sequences (timestamp <-> version <-> snapshot)
# =============================================================================
# Tests that a first read with one as_of type correctly populates (or doesn't
# populate) the cache for a second read with a different as_of type.
# Parametrized across all chain shapes.


def _resolve_cross_as_of(label, chain_info):
    if label == "latest":
        return None
    if label == "from_time":
        return chain_info.mid_ts
    elif label == "downto_oldest":
        return chain_info.oldest_undeleted_version
    elif label == "downto_mid":
        return chain_info.mid_version
    elif label == "snapshot":
        return chain_info.snapshot


CROSS_QUERY_SEQUENCES = [
    pytest.param("latest", "latest", id="latest_then_latest"),
    pytest.param("downto_oldest", "latest", id="downto_oldest_then_latest"),
    pytest.param("from_time", "downto_oldest", id="from_time_then_downto_oldest"),
    pytest.param("from_time", "downto_mid", id="from_time_then_downto_mid"),
    pytest.param("downto_mid", "from_time", id="downto_mid_then_from_time"),
    pytest.param("downto_oldest", "from_time", id="downto_oldest_then_from_time"),
    pytest.param("snapshot", "downto_oldest", id="snapshot_then_downto_oldest"),
    pytest.param("snapshot", "from_time", id="snapshot_then_from_time"),
    pytest.param("from_time", "snapshot", id="from_time_then_snapshot"),
    pytest.param("downto_mid", "snapshot", id="downto_mid_then_snapshot"),
    pytest.param("snapshot", "snapshot", id="snapshot_then_snapshot"),
]

# Expected (VERSION_reads, VERSION_REF_reads) for the second read in each
# (chain, sequence) combination. Measured empirically with a fresh library
# per combination.
EXPECTED_CROSS_QUERY_COUNTS = {
    ("simple_6", "latest_then_latest"): (0, 0),
    ("simple_6", "downto_oldest_then_latest"): (0, 0),
    ("simple_6", "from_time_then_downto_oldest"): (6, 1),
    ("simple_6", "from_time_then_downto_mid"): (0, 0),
    ("simple_6", "downto_mid_then_from_time"): (0, 0),
    ("simple_6", "downto_oldest_then_from_time"): (0, 0),
    ("simple_6", "snapshot_then_downto_oldest"): (6, 1),
    ("simple_6", "snapshot_then_from_time"): (0, 0),
    ("simple_6", "from_time_then_snapshot"): (0, 0),
    ("simple_6", "downto_mid_then_snapshot"): (0, 0),
    ("simple_6", "snapshot_then_snapshot"): (0, 0),
    ("multi_tombstone_all", "latest_then_latest"): (0, 0),
    ("multi_tombstone_all", "downto_oldest_then_latest"): (0, 0),
    ("multi_tombstone_all", "from_time_then_downto_oldest"): (0, 0),
    ("multi_tombstone_all", "from_time_then_downto_mid"): (0, 0),
    ("multi_tombstone_all", "downto_mid_then_from_time"): (0, 0),
    ("multi_tombstone_all", "downto_oldest_then_from_time"): (0, 0),
    ("multi_tombstone_all", "snapshot_then_downto_oldest"): (0, 0),
    ("multi_tombstone_all", "snapshot_then_from_time"): (0, 0),
    ("multi_tombstone_all", "from_time_then_snapshot"): (0, 0),
    ("multi_tombstone_all", "downto_mid_then_snapshot"): (0, 0),
    ("multi_tombstone_all", "snapshot_then_snapshot"): (0, 0),
    ("multi_individual_tombstone", "latest_then_latest"): (0, 0),
    ("multi_individual_tombstone", "downto_oldest_then_latest"): (0, 0),
    ("multi_individual_tombstone", "from_time_then_downto_oldest"): (0, 0),
    ("multi_individual_tombstone", "from_time_then_downto_mid"): (0, 0),
    ("multi_individual_tombstone", "downto_mid_then_from_time"): (0, 0),
    ("multi_individual_tombstone", "downto_oldest_then_from_time"): (0, 0),
    ("multi_individual_tombstone", "snapshot_then_downto_oldest"): (0, 0),
    ("multi_individual_tombstone", "snapshot_then_from_time"): (0, 0),
    ("multi_individual_tombstone", "from_time_then_snapshot"): (0, 0),
    ("multi_individual_tombstone", "downto_mid_then_snapshot"): (0, 0),
    ("multi_individual_tombstone", "snapshot_then_snapshot"): (0, 0),
    ("append_chain", "latest_then_latest"): (0, 0),
    ("append_chain", "downto_oldest_then_latest"): (0, 0),
    ("append_chain", "from_time_then_downto_oldest"): (3, 1),
    ("append_chain", "from_time_then_downto_mid"): (0, 0),
    ("append_chain", "downto_mid_then_from_time"): (0, 0),
    ("append_chain", "downto_oldest_then_from_time"): (0, 0),
    ("append_chain", "snapshot_then_downto_oldest"): (3, 1),
    ("append_chain", "snapshot_then_from_time"): (0, 0),
    ("append_chain", "from_time_then_snapshot"): (0, 0),
    ("append_chain", "downto_mid_then_snapshot"): (0, 0),
    ("append_chain", "snapshot_then_snapshot"): (0, 0),
    ("update_chain", "latest_then_latest"): (0, 0),
    ("update_chain", "downto_oldest_then_latest"): (0, 0),
    ("update_chain", "from_time_then_downto_oldest"): (3, 1),
    ("update_chain", "from_time_then_downto_mid"): (0, 0),
    ("update_chain", "downto_mid_then_from_time"): (0, 0),
    ("update_chain", "downto_oldest_then_from_time"): (0, 0),
    ("update_chain", "snapshot_then_downto_oldest"): (3, 1),
    ("update_chain", "snapshot_then_from_time"): (0, 0),
    ("update_chain", "from_time_then_snapshot"): (0, 0),
    ("update_chain", "downto_mid_then_snapshot"): (0, 0),
    ("update_chain", "snapshot_then_snapshot"): (0, 0),
    ("prune_chain", "latest_then_latest"): (0, 0),
    ("prune_chain", "downto_oldest_then_latest"): (0, 0),
    ("prune_chain", "from_time_then_downto_oldest"): (0, 0),
    ("prune_chain", "from_time_then_downto_mid"): (0, 0),
    ("prune_chain", "downto_mid_then_from_time"): (0, 0),
    ("prune_chain", "downto_oldest_then_from_time"): (0, 0),
    ("prune_chain", "snapshot_then_downto_oldest"): (0, 0),
    ("prune_chain", "snapshot_then_from_time"): (0, 0),
    ("prune_chain", "from_time_then_snapshot"): (0, 0),
    ("prune_chain", "downto_mid_then_snapshot"): (0, 0),
    ("prune_chain", "snapshot_then_snapshot"): (0, 0),
    ("tombstone_all_then_writes", "latest_then_latest"): (0, 0),
    ("tombstone_all_then_writes", "downto_oldest_then_latest"): (0, 0),
    ("tombstone_all_then_writes", "from_time_then_downto_oldest"): (0, 0),
    ("tombstone_all_then_writes", "from_time_then_downto_mid"): (0, 0),
    ("tombstone_all_then_writes", "downto_mid_then_from_time"): (0, 0),
    ("tombstone_all_then_writes", "downto_oldest_then_from_time"): (0, 0),
    ("tombstone_all_then_writes", "snapshot_then_downto_oldest"): (0, 0),
    ("tombstone_all_then_writes", "snapshot_then_from_time"): (0, 0),
    ("tombstone_all_then_writes", "from_time_then_snapshot"): (0, 0),
    ("tombstone_all_then_writes", "downto_mid_then_snapshot"): (0, 0),
    ("tombstone_all_then_writes", "snapshot_then_snapshot"): (0, 0),
    ("tombstone_latest", "latest_then_latest"): (0, 0),
    ("tombstone_latest", "downto_oldest_then_latest"): (0, 0),
    ("tombstone_latest", "from_time_then_downto_oldest"): (0, 0),
    ("tombstone_latest", "from_time_then_downto_mid"): (0, 0),
    ("tombstone_latest", "downto_mid_then_from_time"): (0, 0),
    ("tombstone_latest", "downto_oldest_then_from_time"): (0, 0),
    ("tombstone_latest", "snapshot_then_downto_oldest"): (0, 0),
    ("tombstone_latest", "snapshot_then_from_time"): (0, 0),
    ("tombstone_latest", "from_time_then_snapshot"): (0, 0),
    ("tombstone_latest", "downto_mid_then_snapshot"): (0, 0),
    ("tombstone_latest", "snapshot_then_snapshot"): (0, 0),
    ("tombstone_all_every_version", "latest_then_latest"): (0, 0),
    ("tombstone_all_every_version", "downto_oldest_then_latest"): (0, 0),
    ("tombstone_all_every_version", "from_time_then_downto_oldest"): (0, 0),
    ("tombstone_all_every_version", "from_time_then_downto_mid"): (0, 0),
    ("tombstone_all_every_version", "downto_mid_then_from_time"): (0, 0),
    ("tombstone_all_every_version", "downto_oldest_then_from_time"): (0, 0),
    ("tombstone_all_every_version", "snapshot_then_downto_oldest"): (0, 0),
    ("tombstone_all_every_version", "snapshot_then_from_time"): (0, 0),
    ("tombstone_all_every_version", "from_time_then_snapshot"): (0, 0),
    ("tombstone_all_every_version", "downto_mid_then_snapshot"): (0, 0),
    ("tombstone_all_every_version", "snapshot_then_snapshot"): (0, 0),
    ("tombstone_even_plus_latest", "latest_then_latest"): (0, 0),
    ("tombstone_even_plus_latest", "downto_oldest_then_latest"): (0, 0),
    ("tombstone_even_plus_latest", "from_time_then_downto_oldest"): (0, 0),
    ("tombstone_even_plus_latest", "from_time_then_downto_mid"): (0, 0),
    ("tombstone_even_plus_latest", "downto_mid_then_from_time"): (0, 0),
    ("tombstone_even_plus_latest", "downto_oldest_then_from_time"): (0, 0),
    ("tombstone_even_plus_latest", "snapshot_then_downto_oldest"): (0, 0),
    ("tombstone_even_plus_latest", "snapshot_then_from_time"): (0, 0),
    ("tombstone_even_plus_latest", "from_time_then_snapshot"): (0, 0),
    ("tombstone_even_plus_latest", "downto_mid_then_snapshot"): (0, 0),
    ("tombstone_even_plus_latest", "snapshot_then_snapshot"): (0, 0),
}


@pytest.mark.parametrize("chain_setup", CHAIN_SETUPS)
@pytest.mark.parametrize("first_label, second_label", CROSS_QUERY_SEQUENCES)
def test_cross_query_type_sequence(
    in_memory_store_factory,
    clear_query_stats,
    chain_setup,
    first_label,
    second_label,
):
    """Test that reads with different as_of types interact correctly with the cache."""
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    chain_id = chain_setup.__name__.replace("setup_", "")

    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        chain_info = chain_setup(lib, sym)
        first_as_of = _resolve_cross_as_of(first_label, chain_info)
        second_as_of = _resolve_cross_as_of(second_label, chain_info)

        # First read (warms cache in some way)
        lib.read(sym, as_of=first_as_of)

        # Measure second read
        qs.enable()
        qs.reset_stats()
        lib.read(sym, as_of=second_as_of)
        stats = qs.get_query_stats()

        seq_id = f"{first_label}_then_{second_label}"
        expected_ver, expected_ref = EXPECTED_CROSS_QUERY_COUNTS[(chain_id, seq_id)]
        assert get_version_reads(stats) == expected_ver
        assert get_version_ref_reads(stats) == expected_ref


# =============================================================================
# Test 8: Reading tombstoned versions — exception + storage read counts
# =============================================================================
# Verifies that reading a deleted version raises NoSuchVersionException and
# that the number of VERSION/VERSION_REF reads matches expectations.
# DOWNTO walks the chain until it finds the requested version or reaches the
# end; for individually tombstoned versions the walk depth depends on how
# far down the chain the version sits.

EXPECTED_TOMBSTONED_VERSIONS_COUNTS = {
    # tombstone_all_every_version: versions 0-5 deleted via tombstone_all after each write,
    # then 6 more writes (live: 6-11). delete() does LoadType::ALL during setup, so cache_on
    # has full chain loaded.
    ("tombstone_all_every_version", "no_cache", 0): (8, 1),
    ("tombstone_all_every_version", "no_cache", 3): (8, 1),
    ("tombstone_all_every_version", "no_cache", 5): (8, 1),
    ("tombstone_all_every_version", "cache_on", 0): (0, 0),
    ("tombstone_all_every_version", "cache_on", 3): (0, 0),
    ("tombstone_all_every_version", "cache_on", 5): (0, 0),
    # tombstone_even_plus_latest: versions 0,2,4,5 deleted via delete_version,
    # live: 1,3. delete_version() does LoadType::ALL during setup, so cache_on
    # has full chain loaded. no_cache walks further for older versions.
    ("tombstone_even_plus_latest", "no_cache", 0): (10, 1),
    ("tombstone_even_plus_latest", "no_cache", 2): (8, 1),
    ("tombstone_even_plus_latest", "no_cache", 4): (6, 1),
    ("tombstone_even_plus_latest", "no_cache", 5): (5, 1),
    ("tombstone_even_plus_latest", "cache_on", 0): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", 2): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", 4): (0, 0),
    ("tombstone_even_plus_latest", "cache_on", 5): (0, 0),
}


@pytest.mark.parametrize(
    "chain_setup, deleted_version",
    [
        pytest.param(setup_tombstone_all_every_version, 0, id="tombstone_all_every_version-v0"),
        pytest.param(setup_tombstone_all_every_version, 3, id="tombstone_all_every_version-v3"),
        pytest.param(setup_tombstone_all_every_version, 5, id="tombstone_all_every_version-v5"),
        pytest.param(setup_tombstone_even_plus_latest, 0, id="tombstone_even_plus_latest-v0"),
        pytest.param(setup_tombstone_even_plus_latest, 2, id="tombstone_even_plus_latest-v2"),
        pytest.param(setup_tombstone_even_plus_latest, 4, id="tombstone_even_plus_latest-v4"),
        pytest.param(setup_tombstone_even_plus_latest, 5, id="tombstone_even_plus_latest-v5"),
    ],
)
@pytest.mark.parametrize("reload_interval", [0, MAX_RELOAD_INTERVAL], ids=["no_cache", "cache_on"])
def test_read_tombstoned_version(
    in_memory_store_factory, clear_query_stats, chain_setup, deleted_version, reload_interval
):
    """Reading a tombstoned version raises NoSuchVersionException with expected storage read counts."""
    lib = in_memory_store_factory()
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    chain_id = chain_setup.__name__.replace("setup_", "")
    cache_id = "no_cache" if reload_interval == 0 else "cache_on"

    with config_context("VersionMap.ReloadInterval", reload_interval):
        chain_setup(lib, sym)

        qs.enable()
        qs.reset_stats()
        with pytest.raises(NoSuchVersionException):
            lib.read(sym, as_of=deleted_version)
        stats = qs.get_query_stats()

        expected_ver, expected_ref = EXPECTED_TOMBSTONED_VERSIONS_COUNTS[(chain_id, cache_id, deleted_version)]
        assert get_version_reads(stats) == expected_ver
        assert get_version_ref_reads(stats) == expected_ref
