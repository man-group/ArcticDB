import uuid
from enum import Enum
from typing import NamedTuple

import pandas as pd
import pytest

import arcticdb.toolbox.query_stats as qs
from arcticdb.exceptions import NoSuchVersionException
from arcticdb.util.test import config_context, query_stats_operation_count
from tests.util.mark import MEM_TESTS_MARK

pytestmark = MEM_TESTS_MARK

MAX_RELOAD_INTERVAL = 2_000_000_000_000


def make_df():
    return pd.DataFrame({"col": [1, 2, 3]})


def make_ts_df(start_day=0, periods=3):
    return pd.DataFrame({"col": range(periods)}, index=pd.date_range(f"2020-01-{start_day + 1:02d}", periods=periods))


def get_version_reads(stats):
    return query_stats_operation_count(stats, "Memory_GetObject", "VERSION")


def get_version_ref_reads(stats):
    return query_stats_operation_count(stats, "Memory_GetObject", "VERSION_REF")


class TombstoneType(Enum):
    INDIVIDUAL = 0
    ALL = 1


class Tombstone(NamedTuple):
    version_id: int
    type: TombstoneType


def _ts_from_version_info(version_info):
    return pd.Timestamp(version_info.timestamp, unit="ns", tz="UTC")


def _iter_versions(chain):
    """Yield (vinfo, is_deleted) newest-to-oldest"""
    tombstoned_ids = set()
    tombstone_all_version = None
    for entry in chain:
        if isinstance(entry, Tombstone):
            if entry.type == TombstoneType.INDIVIDUAL:
                tombstoned_ids.add(entry.version_id)
            else:
                if tombstone_all_version is None or entry.version_id > tombstone_all_version:
                    tombstone_all_version = entry.version_id
        else:
            deleted = entry.version in tombstoned_ids or (
                tombstone_all_version is not None and entry.version <= tombstone_all_version
            )
            yield entry, deleted


def _is_tombstone_head(chain_info):
    return isinstance(chain_info[0], Tombstone)


def _latest_version(chain_info):
    return next(v.version for v, deleted in _iter_versions(chain_info) if not deleted)


def _oldest_undeleted(chain_info):
    return min(v.version for v, deleted in _iter_versions(chain_info) if not deleted)


def _total_versions(chain_info):
    return sum(1 for e in chain_info if not isinstance(e, Tombstone))


def _ts_of(chain_info, version_id):
    return next(_ts_from_version_info(v) for v, _ in _iter_versions(chain_info) if v.version == version_id)


def _mid_version(chain_info):
    undeleted = sorted(v.version for v, deleted in _iter_versions(chain_info) if not deleted)
    return undeleted[len(undeleted) // 2]


def _is_index_head(chain_info):
    return not _is_tombstone_head(chain_info)


def _oldest_loaded_after_cold_read(as_of_type, chain_info):
    """Derive oldest_loaded_index_version_ from chain structure and as_of type."""
    is_idx_head = _is_index_head(chain_info)
    undeleted_asc = sorted(v.version for v, deleted in _iter_versions(chain_info) if not deleted)

    if as_of_type == "oldest":
        return 0

    if as_of_type in ("latest", "version_latest"):
        loaded = undeleted_asc[-1]
    elif as_of_type == "version_neg_1":
        loaded = _total_versions(chain_info) - 1
    elif as_of_type in ("version_mid", "version_neg_mid", "timestamp_mid"):
        loaded = _mid_version(chain_info)
    elif as_of_type in ("oldest_undeleted_version", "version_neg_oldest"):
        loaded = undeleted_asc[0]
    else:
        raise Exception("Unexpected as_of_type")

    if is_idx_head and len(undeleted_asc) > 1:  # we have penultimate index also cached
        penultimate = undeleted_asc[-2]
        return min(loaded, penultimate)
    else:
        return loaded


def setup_simple_6(lib, sym):
    """v5 -> v4 -> v3 -> v2 -> v1 -> v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    return w[::-1]


def setup_multi_tombstone_all(lib, sym):
    """v4 -> v3 -> v2 -> Tombstone(ALL,1) -> v1 -> Tombstone(ALL,0) -> v0"""
    v0 = lib.write(sym, make_df())
    lib.delete(sym)
    v1 = lib.write(sym, make_df())
    lib.delete(sym)
    v2, v3, v4 = lib.write(sym, make_df()), lib.write(sym, make_df()), lib.write(sym, make_df())
    return [v4, v3, v2, Tombstone(v1.version, TombstoneType.ALL), v1, Tombstone(v0.version, TombstoneType.ALL), v0]


def setup_multi_individual_tombstone(lib, sym):
    """Tombstone(INDIVIDUAL,3) -> Tombstone(INDIVIDUAL,1) -> v5 -> v4 -> v3 -> v2 -> v1 -> v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 1)
    lib.delete_version(sym, 3)
    return [Tombstone(3, TombstoneType.INDIVIDUAL), Tombstone(1, TombstoneType.INDIVIDUAL)] + w[::-1]


def setup_append_chain(lib, sym):
    """v2 -> v1 -> v0 via write + 2 appends"""
    w = [lib.write(sym, make_ts_df(0, 2)), lib.append(sym, make_ts_df(2, 2)), lib.append(sym, make_ts_df(4, 2))]
    return w[::-1]


def setup_update_chain(lib, sym):
    """v2 -> v1 -> v0 via write + 2 updates"""
    w = [lib.write(sym, make_ts_df(0, 3)), lib.update(sym, make_ts_df(1, 1)), lib.update(sym, make_ts_df(2, 1))]
    return w[::-1]


def setup_prune_chain(lib, sym):
    """Tombstone(ALL,1) -> v2 -> v1 -> v0"""
    w = [lib.write(sym, make_df()) for _ in range(3)]
    lib.prune_previous_versions(sym)
    return [Tombstone(w[1].version, TombstoneType.ALL), w[2], w[1], w[0]]


def setup_tombstone_all_then_writes(lib, sym):
    """v6 -> v5 -> v4 -> v3 -> v2 -> v1 -> Tombstone(ALL,0) -> v0"""
    v0 = lib.write(sym, make_df())
    lib.delete(sym)
    w = [lib.write(sym, make_df()) for _ in range(6)]  # v1..v6
    return w[::-1] + [Tombstone(v0.version, TombstoneType.ALL), v0]


def setup_tombstone_latest(lib, sym):
    """Tombstone(INDIVIDUAL,5) -> v5 -> v4 -> v3 -> v2 -> v1 -> v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 5)
    return [Tombstone(5, TombstoneType.INDIVIDUAL)] + w[::-1]


def setup_tombstone_all_every_version(lib, sym):
    """v11->...->v6 -> Tombstone(ALL,5)->v5->...->Tombstone(ALL,0)->v0"""
    initial = []
    for _ in range(6):
        v = lib.write(sym, make_df())
        lib.delete(sym)
        initial.append(v)
        initial.append(Tombstone(v.version, TombstoneType.ALL))
    rest = [lib.write(sym, make_df()) for _ in range(6)]  # v6..v11
    return rest[::-1] + initial[::-1]


def setup_tombstone_even_plus_latest(lib, sym):
    """Tombstone(INDIVIDUAL,5)->...->Tombstone(INDIVIDUAL,0)->v5->v4->v3->v2->v1->v0"""
    w = [lib.write(sym, make_df()) for _ in range(6)]
    lib.delete_version(sym, 0)
    lib.delete_version(sym, 2)
    lib.delete_version(sym, 4)
    lib.delete_version(sym, 5)
    return [
        Tombstone(5, TombstoneType.INDIVIDUAL),
        Tombstone(4, TombstoneType.INDIVIDUAL),
        Tombstone(2, TombstoneType.INDIVIDUAL),
        Tombstone(0, TombstoneType.INDIVIDUAL),
    ] + w[::-1]


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
    "oldest",
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
    ("simple_6", "oldest"): 6,
    ("multi_tombstone_all", "latest"): 0,
    ("multi_tombstone_all", "version_latest"): 0,
    ("multi_tombstone_all", "oldest_undeleted_version"): 3,
    ("multi_tombstone_all", "version_mid"): 0,
    ("multi_tombstone_all", "timestamp_mid"): 0,
    ("multi_tombstone_all", "version_neg_1"): 0,
    ("multi_tombstone_all", "version_neg_mid"): 2,
    ("multi_tombstone_all", "version_neg_oldest"): 3,
    ("multi_tombstone_all", "oldest"): 5,
    ("multi_individual_tombstone", "latest"): 3,
    ("multi_individual_tombstone", "version_latest"): 3,
    ("multi_individual_tombstone", "oldest_undeleted_version"): 8,
    ("multi_individual_tombstone", "version_mid"): 4,
    ("multi_individual_tombstone", "timestamp_mid"): 4,
    ("multi_individual_tombstone", "version_neg_1"): 3,
    ("multi_individual_tombstone", "version_neg_mid"): 4,
    ("multi_individual_tombstone", "version_neg_oldest"): 8,
    ("multi_individual_tombstone", "oldest"): 8,
    ("append_chain", "latest"): 0,
    ("append_chain", "version_latest"): 0,
    ("append_chain", "oldest_undeleted_version"): 3,
    ("append_chain", "version_mid"): 0,
    ("append_chain", "timestamp_mid"): 0,
    ("append_chain", "version_neg_1"): 0,
    ("append_chain", "version_neg_mid"): 2,
    ("append_chain", "version_neg_oldest"): 3,
    ("append_chain", "oldest"): 3,
    ("update_chain", "latest"): 0,
    ("update_chain", "version_latest"): 0,
    ("update_chain", "oldest_undeleted_version"): 3,
    ("update_chain", "version_mid"): 0,
    ("update_chain", "timestamp_mid"): 0,
    ("update_chain", "version_neg_1"): 0,
    ("update_chain", "version_neg_mid"): 2,
    ("update_chain", "version_neg_oldest"): 3,
    ("update_chain", "oldest"): 3,
    ("prune_chain", "latest"): 2,
    ("prune_chain", "version_latest"): 2,
    ("prune_chain", "oldest_undeleted_version"): 2,
    ("prune_chain", "version_mid"): 2,
    ("prune_chain", "timestamp_mid"): 2,
    ("prune_chain", "version_neg_1"): 2,
    ("prune_chain", "version_neg_mid"): 2,
    ("prune_chain", "version_neg_oldest"): 2,
    ("prune_chain", "oldest"): 3,
    ("tombstone_all_then_writes", "latest"): 0,
    ("tombstone_all_then_writes", "version_latest"): 0,
    ("tombstone_all_then_writes", "oldest_undeleted_version"): 6,
    ("tombstone_all_then_writes", "version_mid"): 3,
    ("tombstone_all_then_writes", "timestamp_mid"): 3,
    ("tombstone_all_then_writes", "version_neg_1"): 0,
    ("tombstone_all_then_writes", "version_neg_mid"): 3,
    ("tombstone_all_then_writes", "version_neg_oldest"): 6,
    ("tombstone_all_then_writes", "oldest"): 8,
    ("tombstone_latest", "latest"): 3,
    ("tombstone_latest", "version_latest"): 3,
    ("tombstone_latest", "oldest_undeleted_version"): 7,
    ("tombstone_latest", "version_mid"): 5,
    ("tombstone_latest", "timestamp_mid"): 5,
    ("tombstone_latest", "version_neg_1"): 2,
    ("tombstone_latest", "version_neg_mid"): 5,
    ("tombstone_latest", "version_neg_oldest"): 7,
    ("tombstone_latest", "oldest"): 7,
    ("tombstone_all_every_version", "latest"): 0,
    ("tombstone_all_every_version", "version_latest"): 0,
    ("tombstone_all_every_version", "oldest_undeleted_version"): 6,
    ("tombstone_all_every_version", "version_mid"): 3,
    ("tombstone_all_every_version", "timestamp_mid"): 3,
    ("tombstone_all_every_version", "version_neg_1"): 0,
    ("tombstone_all_every_version", "version_neg_mid"): 3,
    ("tombstone_all_every_version", "version_neg_oldest"): 6,
    ("tombstone_all_every_version", "oldest"): 8,
    ("tombstone_even_plus_latest", "latest"): 7,
    ("tombstone_even_plus_latest", "version_latest"): 7,
    ("tombstone_even_plus_latest", "oldest_undeleted_version"): 9,
    ("tombstone_even_plus_latest", "version_mid"): 7,
    ("tombstone_even_plus_latest", "timestamp_mid"): 7,
    ("tombstone_even_plus_latest", "version_neg_1"): 5,
    ("tombstone_even_plus_latest", "version_neg_mid"): 7,
    ("tombstone_even_plus_latest", "version_neg_oldest"): 9,
    ("tombstone_even_plus_latest", "oldest"): 10,
}

# Combinations where the negative as_of resolves to a tombstoned version,
# causing NoSuchVersionException. -1 resolves based on the latest version_id
# including tombstoned ones (get_first_index(include_deleted=true)).
# "oldest" (as_of=0) raises when v0 is deleted.
EXPECTED_RAISES = {
    ("tombstone_latest", "version_neg_1"),
    ("tombstone_even_plus_latest", "version_neg_1"),
    ("multi_tombstone_all", "oldest"),
    ("prune_chain", "oldest"),
    ("tombstone_all_then_writes", "oldest"),
    ("tombstone_all_every_version", "oldest"),
    ("tombstone_even_plus_latest", "oldest"),
}


def resolve_as_of(as_of_type, chain_info):
    if as_of_type == "latest":
        return None
    elif as_of_type == "version_latest":
        return _latest_version(chain_info)
    elif as_of_type == "oldest_undeleted_version":
        return _oldest_undeleted(chain_info)
    elif as_of_type == "version_mid":
        return _mid_version(chain_info)
    elif as_of_type == "timestamp_mid":
        return _ts_of(chain_info, _mid_version(chain_info))
    elif as_of_type == "version_neg_1":
        return -1
    elif as_of_type == "version_neg_mid":
        return _mid_version(chain_info) - _total_versions(chain_info)
    elif as_of_type == "version_neg_oldest":
        return _oldest_undeleted(chain_info) - _total_versions(chain_info)
    elif as_of_type == "oldest":
        return 0


def required_version(as_of_type, chain_info):
    if as_of_type in ("latest", "version_latest"):
        return _latest_version(chain_info)
    elif as_of_type in ("oldest_undeleted_version", "version_neg_oldest"):
        return _oldest_undeleted(chain_info)
    elif as_of_type in ("version_mid", "version_neg_mid"):
        return _mid_version(chain_info)
    elif as_of_type == "timestamp_mid":
        return _ts_of(chain_info, _mid_version(chain_info))
    elif as_of_type == "version_neg_1":
        return _total_versions(chain_info) - 1
    elif as_of_type == "oldest":
        return 0
    return None


def is_cached(chain_id, as_of_type_first_read, as_of_type_second_read, chain_info):
    assert (
        chain_id,
        as_of_type_first_read,
    ) in EXPECTED_COUNTS, f"Missing EXPECTED_COUNTS entry for ({chain_id!r}, {as_of_type_first_read!r})"
    oldest_loaded = _oldest_loaded_after_cold_read(as_of_type_first_read, chain_info)
    req = required_version(as_of_type_second_read, chain_info)
    if req is None or oldest_loaded is None:
        return False
    if isinstance(req, pd.Timestamp):
        return req >= _ts_of(chain_info, oldest_loaded)
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
        chain_info = chain_setup_fn(lib_chain_setup, sym)
        as_of_first_read = resolve_as_of(as_of_type_first_read, chain_info)
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
        as_of_second_read = resolve_as_of(as_of_type_second_read, chain_info)
        second_read_key = (chain_id, as_of_type_second_read)
        if second_read_key in EXPECTED_RAISES:
            with pytest.raises(NoSuchVersionException):
                lib_read.read(sym, as_of=as_of_second_read)
        else:
            lib_read.read(sym, as_of=as_of_second_read)
        stats = qs.get_query_stats()
        if is_cached(chain_id, as_of_type_first_read, as_of_type_second_read, chain_info):
            assert get_version_reads(stats) == 0
            assert get_version_ref_reads(stats) == 0
        else:
            assert get_version_reads(stats) == EXPECTED_COUNTS[second_read_key]
            assert get_version_ref_reads(stats) == 1

        # Write the new version and check that it can be loaded despite the cache doesn't have it with as_of
        vinfo = lib_chain_setup.write(sym, make_df())
        assert lib_read.read(sym, as_of=vinfo.version).version == vinfo.version
