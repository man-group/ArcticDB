import uuid

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


TOMBSTONE_ALL = "tombstone_all"


def _ts_from_version_info(version_info):
    return pd.Timestamp(version_info.timestamp, unit="ns", tz="UTC")


def _iter_versions(chain):
    """Yield (vinfo, is_deleted) newest-to-oldest"""
    tombstoned_ids = set()
    tombstone_all_seen = False
    for entry in chain:
        if isinstance(entry, tuple):
            tombstoned_ids.add(entry[1])
        elif entry == TOMBSTONE_ALL:
            tombstone_all_seen = True
        else:
            yield entry, entry.version in tombstoned_ids or tombstone_all_seen


def _is_tombstone_head(ci):
    return isinstance(ci[0], tuple) or ci[0] == TOMBSTONE_ALL


def _latest_version(ci):
    return next(v.version for v, deleted in _iter_versions(ci) if not deleted)


def _oldest_undeleted(ci):
    return min(v.version for v, deleted in _iter_versions(ci) if not deleted)


def _total_versions(ci):
    return sum(1 for e in ci if not isinstance(e, tuple) and e != TOMBSTONE_ALL)


def _ts_of(ci, version_id):
    return next(_ts_from_version_info(v) for v, _ in _iter_versions(ci) if v.version == version_id)


def _mid_version(ci):
    undeleted = sorted(v.version for v, deleted in _iter_versions(ci) if not deleted)
    return undeleted[len(undeleted) // 2]


def _is_index_head(ci):
    return not _is_tombstone_head(ci)


def _oldest_loaded_after_cold_read(as_of_type, ci):
    """Derive oldest_loaded_index_version_ from chain structure and as_of type."""
    is_idx_head = _is_index_head(ci)
    undeleted_asc = sorted(v.version for v, deleted in _iter_versions(ci) if not deleted)

    if not is_idx_head or len(undeleted_asc) == 1:
        if as_of_type in ("latest", "version_latest"):
            return undeleted_asc[-1]
        elif as_of_type == "version_neg_1":
            return _total_versions(ci) - 1
        elif as_of_type in ("version_mid", "version_neg_mid", "timestamp_mid"):
            return _mid_version(ci)
        elif as_of_type in ("oldest_undeleted_version", "version_neg_oldest"):
            return undeleted_asc[0]
    elif len(undeleted_asc) > 1:  # we have penultimate index also cached
        penultimate = undeleted_asc[-2]
        if as_of_type in ("latest", "version_latest", "version_neg_1"):
            return penultimate
        elif as_of_type in ("version_mid", "version_neg_mid", "timestamp_mid"):
            v = _mid_version(ci)
            return penultimate if v >= penultimate else v
        elif as_of_type in ("oldest_undeleted_version", "version_neg_oldest"):
            v = undeleted_asc[0]
            return penultimate if v >= penultimate else v
    return None


def setup_simple_6(lib, sym):
    """v5 -> v4 -> v3 -> v2 -> v1 -> v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    return [w[5], w[4], w[3], w[2], w[1], w[0]]


def setup_multi_tombstone_all(lib, sym):
    """v4 -> v3 -> v2 -> TOMBSTONE_ALL -> v1 -> TOMBSTONE_ALL -> v0"""
    v0 = lib.write(sym, make_df())
    lib.delete(sym)
    v1 = lib.write(sym, make_df())
    lib.delete(sym)
    v2, v3, v4 = lib.write(sym, make_df()), lib.write(sym, make_df()), lib.write(sym, make_df())
    return [v4, v3, v2, TOMBSTONE_ALL, v1, TOMBSTONE_ALL, v0]


def setup_multi_individual_tombstone(lib, sym):
    """("tombstone",3) -> ("tombstone",1) -> v5 -> v4 -> v3 -> v2 -> v1 -> v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 1)
    lib.delete_version(sym, 3)
    return [("tombstone", 3), ("tombstone", 1), w[5], w[4], w[3], w[2], w[1], w[0]]


def setup_append_chain(lib, sym):
    """v2 -> v1 -> v0 via write + 2 appends"""
    w = [lib.write(sym, make_ts_df(0, 2)), lib.append(sym, make_ts_df(2, 2)), lib.append(sym, make_ts_df(4, 2))]
    return [w[2], w[1], w[0]]


def setup_update_chain(lib, sym):
    """v2 -> v1 -> v0 via write + 2 updates"""
    w = [lib.write(sym, make_ts_df(0, 3)), lib.update(sym, make_ts_df(1, 1)), lib.update(sym, make_ts_df(2, 1))]
    return [w[2], w[1], w[0]]


def setup_prune_chain(lib, sym):
    """v2 only (v0, v1 pruned)"""
    w = [lib.write(sym, make_df()) for _ in range(3)]
    lib.prune_previous_versions(sym)
    return [w[2], TOMBSTONE_ALL, w[1], w[0]]


def setup_tombstone_all_then_writes(lib, sym):
    """v6 -> v5 -> v4 -> v3 -> v2 -> v1 -> TOMBSTONE_ALL -> v0"""
    v0 = lib.write(sym, make_df())
    lib.delete(sym)
    w = [lib.write(sym, make_df()) for _ in range(6)]  # v1..v6
    return [w[5], w[4], w[3], w[2], w[1], w[0], TOMBSTONE_ALL, v0]


def setup_tombstone_latest(lib, sym):
    """("tombstone",5) -> v5 -> v4 -> v3 -> v2 -> v1 -> v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 5)
    return [("tombstone", 5), w[5], w[4], w[3], w[2], w[1], w[0]]


def setup_tombstone_all_every_version(lib, sym):
    """v11->...->v6 -> TOMBSTONE_ALL->v5->...->TOMBSTONE_ALL->v0"""
    initial = []
    for _ in range(6):
        initial.append(lib.write(sym, make_df()))
        lib.delete(sym)
    rest = [lib.write(sym, make_df()) for _ in range(6)]  # v6..v11
    chain = list(reversed(rest))
    for v in reversed(initial):
        chain.extend([TOMBSTONE_ALL, v])
    return chain


def setup_tombstone_even_plus_latest(lib, sym):
    """("tombstone",5)->("tombstone",4)->("tombstone",2)->("tombstone",0)->v5->v4->v3->v2->v1->v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 0)
    lib.delete_version(sym, 2)
    lib.delete_version(sym, 4)
    lib.delete_version(sym, 5)
    return [("tombstone", 5), ("tombstone", 4), ("tombstone", 2), ("tombstone", 0), w[5], w[4], w[3], w[2], w[1], w[0]]


CHAIN_SETUPS = [
    ("simple_6", setup_simple_6),
    ("multi_tombstone_all", setup_multi_tombstone_all),
    ("multi_individual_tombstone", setup_multi_individual_tombstone),
    ("append_chain", setup_append_chain),
    ("update_chain", setup_update_chain),
    ("prune_chain", setup_prune_chain),
    ("tombstone_all_then_writes", setup_tombstone_all_then_writes),
    ("tombstone_latest", setup_tombstone_latest),
    ("tombstone_all_every_version", setup_tombstone_all_every_version),
    ("tombstone_even_plus_latest", setup_tombstone_even_plus_latest),
]

AS_OF_TYPES = [
    "latest",
    "version_latest",
    "oldest_undeleted_version",
    "version_mid",
    "timestamp_mid",
    "version_neg_1",
    "version_neg_mid",
    "version_neg_oldest",
]

# Expected VERSION key read count for each (chain, as_of) combination on a cold read.
EXPECTED_COUNTS = {
    ("simple_6", "latest"): 0,
    ("simple_6", "version_latest"): 0,
    ("simple_6", "oldest_undeleted_version"): 6,
    ("simple_6", "version_mid"): 3,
    ("simple_6", "timestamp_mid"): 3,
    ("simple_6", "version_neg_1"): 0,
    ("simple_6", "version_neg_mid"): 3,
    ("simple_6", "version_neg_oldest"): 6,
    ("multi_tombstone_all", "latest"): 0,
    ("multi_tombstone_all", "version_latest"): 0,
    ("multi_tombstone_all", "oldest_undeleted_version"): 3,
    ("multi_tombstone_all", "version_mid"): 0,
    ("multi_tombstone_all", "timestamp_mid"): 0,
    ("multi_tombstone_all", "version_neg_1"): 0,
    ("multi_tombstone_all", "version_neg_mid"): 2,
    ("multi_tombstone_all", "version_neg_oldest"): 3,
    ("multi_individual_tombstone", "latest"): 3,
    ("multi_individual_tombstone", "version_latest"): 3,
    ("multi_individual_tombstone", "oldest_undeleted_version"): 8,
    ("multi_individual_tombstone", "version_mid"): 4,
    ("multi_individual_tombstone", "timestamp_mid"): 4,
    ("multi_individual_tombstone", "version_neg_1"): 3,
    ("multi_individual_tombstone", "version_neg_mid"): 4,
    ("multi_individual_tombstone", "version_neg_oldest"): 8,
    ("append_chain", "latest"): 0,
    ("append_chain", "version_latest"): 0,
    ("append_chain", "oldest_undeleted_version"): 3,
    ("append_chain", "version_mid"): 0,
    ("append_chain", "timestamp_mid"): 0,
    ("append_chain", "version_neg_1"): 0,
    ("append_chain", "version_neg_mid"): 2,
    ("append_chain", "version_neg_oldest"): 3,
    ("update_chain", "latest"): 0,
    ("update_chain", "version_latest"): 0,
    ("update_chain", "oldest_undeleted_version"): 3,
    ("update_chain", "version_mid"): 0,
    ("update_chain", "timestamp_mid"): 0,
    ("update_chain", "version_neg_1"): 0,
    ("update_chain", "version_neg_mid"): 2,
    ("update_chain", "version_neg_oldest"): 3,
    ("prune_chain", "latest"): 2,
    ("prune_chain", "version_latest"): 2,
    ("prune_chain", "oldest_undeleted_version"): 2,
    ("prune_chain", "version_mid"): 2,
    ("prune_chain", "timestamp_mid"): 2,
    ("prune_chain", "version_neg_1"): 2,
    ("prune_chain", "version_neg_mid"): 2,
    ("prune_chain", "version_neg_oldest"): 2,
    ("tombstone_all_then_writes", "latest"): 0,
    ("tombstone_all_then_writes", "version_latest"): 0,
    ("tombstone_all_then_writes", "oldest_undeleted_version"): 6,
    ("tombstone_all_then_writes", "version_mid"): 3,
    ("tombstone_all_then_writes", "timestamp_mid"): 3,
    ("tombstone_all_then_writes", "version_neg_1"): 0,
    ("tombstone_all_then_writes", "version_neg_mid"): 3,
    ("tombstone_all_then_writes", "version_neg_oldest"): 6,
    ("tombstone_latest", "latest"): 3,
    ("tombstone_latest", "version_latest"): 3,
    ("tombstone_latest", "oldest_undeleted_version"): 7,
    ("tombstone_latest", "version_mid"): 5,
    ("tombstone_latest", "timestamp_mid"): 5,
    ("tombstone_latest", "version_neg_1"): 2,
    ("tombstone_latest", "version_neg_mid"): 5,
    ("tombstone_latest", "version_neg_oldest"): 7,
    ("tombstone_all_every_version", "latest"): 0,
    ("tombstone_all_every_version", "version_latest"): 0,
    ("tombstone_all_every_version", "oldest_undeleted_version"): 6,
    ("tombstone_all_every_version", "version_mid"): 3,
    ("tombstone_all_every_version", "timestamp_mid"): 3,
    ("tombstone_all_every_version", "version_neg_1"): 0,
    ("tombstone_all_every_version", "version_neg_mid"): 3,
    ("tombstone_all_every_version", "version_neg_oldest"): 6,
    ("tombstone_even_plus_latest", "latest"): 7,
    ("tombstone_even_plus_latest", "version_latest"): 7,
    ("tombstone_even_plus_latest", "oldest_undeleted_version"): 9,
    ("tombstone_even_plus_latest", "version_mid"): 7,
    ("tombstone_even_plus_latest", "timestamp_mid"): 7,
    ("tombstone_even_plus_latest", "version_neg_1"): 5,
    ("tombstone_even_plus_latest", "version_neg_mid"): 7,
    ("tombstone_even_plus_latest", "version_neg_oldest"): 9,
}

# Combinations where the negative as_of resolves to a tombstoned version,
# causing NoSuchVersionException. -1 resolves based on the latest version_id
# including tombstoned ones (get_first_index(include_deleted=true)).
EXPECTED_RAISES = {
    ("tombstone_latest", "version_neg_1"),
    ("tombstone_even_plus_latest", "version_neg_1"),
}


def resolve_as_of(as_of_type, ci):
    if as_of_type == "latest":
        return None
    elif as_of_type == "version_latest":
        return _latest_version(ci)
    elif as_of_type == "oldest_undeleted_version":
        return _oldest_undeleted(ci)
    elif as_of_type == "version_mid":
        return _mid_version(ci)
    elif as_of_type == "timestamp_mid":
        return _ts_of(ci, _mid_version(ci))
    elif as_of_type == "version_neg_1":
        return -1
    elif as_of_type == "version_neg_mid":
        return _mid_version(ci) - _total_versions(ci)
    elif as_of_type == "version_neg_oldest":
        return _oldest_undeleted(ci) - _total_versions(ci)


def required_version(as_of_type, ci):
    if as_of_type in ("latest", "version_latest"):
        return _latest_version(ci)
    elif as_of_type in ("oldest_undeleted_version", "version_neg_oldest"):
        return _oldest_undeleted(ci)
    elif as_of_type in ("version_mid", "version_neg_mid"):
        return _mid_version(ci)
    elif as_of_type == "timestamp_mid":
        return _ts_of(ci, _mid_version(ci))
    elif as_of_type == "version_neg_1":
        return _total_versions(ci) - 1
    return None


def is_cached(chain_id, as_of_type_first_read, as_of_type_second_read, ci):
    if EXPECTED_COUNTS.get((chain_id, as_of_type_first_read)) is None:
        return False
    oldest_loaded = _oldest_loaded_after_cold_read(as_of_type_first_read, ci)
    req = required_version(as_of_type_second_read, ci)
    if req is None or oldest_loaded is None:
        return False
    if isinstance(req, pd.Timestamp):
        return req >= _ts_of(ci, oldest_loaded)
    return req >= oldest_loaded


@pytest.mark.parametrize("chain_setup", CHAIN_SETUPS)
@pytest.mark.parametrize("as_of_type_first_read", AS_OF_TYPES)
@pytest.mark.parametrize("as_of_type_second_read", AS_OF_TYPES)
def test_read_version_chain(
    in_memory_store_factory, clear_query_stats, chain_setup, as_of_type_first_read, as_of_type_second_read
):
    lib_chain_setup = in_memory_store_factory()
    lib_read = in_memory_store_factory(reuse_name=True)
    sym = f"sym_{uuid.uuid4().hex[:8]}"
    chain_id, chain_setup_fn = chain_setup

    with config_context("VersionMap.ReloadInterval", MAX_RELOAD_INTERVAL):
        ci = chain_setup_fn(lib_chain_setup, sym)
        as_of_first_read = resolve_as_of(as_of_type_first_read, ci)
        key = (chain_id, as_of_type_first_read)

        qs.enable()

        # No cache reading
        qs.reset_stats()
        if key in EXPECTED_RAISES:
            with pytest.raises(NoSuchVersionException):
                lib_read.read(sym, as_of=as_of_first_read)
        else:
            lib_read.read(sym, as_of=as_of_first_read)
        stats = qs.get_query_stats()

        assert get_version_reads(stats) == EXPECTED_COUNTS[key]
        assert get_version_ref_reads(stats) == 1

        # Expected cache hit same read
        qs.reset_stats()
        if key in EXPECTED_RAISES:
            with pytest.raises(NoSuchVersionException):
                lib_read.read(sym, as_of=as_of_first_read)
        else:
            lib_read.read(sym, as_of=as_of_first_read)
        stats = qs.get_query_stats()
        assert get_version_reads(stats) == 0
        assert get_version_ref_reads(stats) == 0

        # Cache check with the second as_of type read
        qs.reset_stats()
        as_of_second_read = resolve_as_of(as_of_type_second_read, ci)
        second_read_key = (chain_id, as_of_type_second_read)
        if second_read_key in EXPECTED_RAISES:
            with pytest.raises(NoSuchVersionException):
                lib_read.read(sym, as_of=as_of_second_read)
        else:
            lib_read.read(sym, as_of=as_of_second_read)
        stats = qs.get_query_stats()
        if is_cached(chain_id, as_of_type_first_read, as_of_type_second_read, ci):
            assert get_version_reads(stats) == 0
            assert get_version_ref_reads(stats) == 0
        else:
            assert get_version_reads(stats) == EXPECTED_COUNTS[second_read_key]
            assert get_version_ref_reads(stats) == 1

        # Write the new version and check that it can be loaded despite the cache doesn't have it with as_of
        vinfo = lib_chain_setup.write(sym, make_df())
        assert lib_read.read(sym, as_of=vinfo.version).version == vinfo.version
