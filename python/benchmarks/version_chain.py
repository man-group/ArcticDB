"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from arcticdb import Arctic
from arcticdb.exceptions import NoSuchVersionException
from arcticdb.config import set_log_level
import arcticdb as adb
import datetime
import math
import sys
import arcticdb.toolbox.query_stats as query_stats

from .common import *
from arcticdb.util.logger import get_logger


def count_version_reads(qs):
    return qs.get("storage_operations", {}).get("Memory_GetObject", {}).get("VERSION", {}).get("count", 0)


class IterateVersionChain:
    timeout = 1000

    # rounds = repeat = 1 so that we do not run the slow setup more than we need to
    rounds = 1
    repeat = 1
    # We are taking counts, which should be more stable than timings. No need to run repeatedly.
    number = 1

    CONNECTION_STRING = f"mem://"
    DELETION_POINT = 0.99  # delete the symbol after writing this proportion of the versions
    LIB_NAME_UNDELETED = "lib_undeleted"
    LIB_NAME_DELETED = "lib_deleted"

    params = ([100], ["forever", "default", "never"], [True, False], [1, 10])

    # In the tail_deleted case we delete the symbol after writing the DELETION_POINT fraction of the versions,
    # so the tail of the version chain is deleted.
    param_names = ["num_versions", "caching", "tail_deleted", "read_delay_ms"]

    def symbol(self, num_versions):
        return f"symbol_{num_versions}"

    def __init__(self):
        self.logger = get_logger()
        self.lib = None

    def _setup(self) -> Arctic:
        ac = Arctic(IterateVersionChain.CONNECTION_STRING)

        num_versions_list, caching_list, deleted_list, read_delay_ms = IterateVersionChain.params

        lib_undeleted = ac.create_library(IterateVersionChain.LIB_NAME_UNDELETED)
        lib_deleted = ac.create_library(IterateVersionChain.LIB_NAME_DELETED)

        small_df = generate_random_floats_dataframe(2, 2)
        delete_points = {}
        for num_versions in num_versions_list:
            delete_points[num_versions] = math.floor(IterateVersionChain.DELETION_POINT * num_versions)

        adb._ext.set_config_int("VersionMap.ReloadInterval", sys.maxsize)

        for num_versions in num_versions_list:
            symbol = self.symbol(num_versions)
            deletion_point = delete_points[num_versions]
            for i in range(deletion_point):
                lib_deleted.write(symbol, small_df)
                lib_undeleted.write(symbol, small_df)
            lib_deleted.delete(symbol)

        for num_versions in num_versions_list:
            symbol = self.symbol(num_versions)
            deletion_point = delete_points[num_versions]
            for i in range(deletion_point, num_versions):
                lib_deleted.write(symbol, small_df)
                lib_undeleted.write(symbol, small_df)
            # reasonableness checks
            assert lib_deleted.read(symbol).version == num_versions - 1
            assert lib_undeleted.read(symbol).version == num_versions - 1
            # Only versions that have not been deleted are returned by list_versions
            assert len(lib_deleted.list_versions(symbol)) == num_versions - deletion_point

        adb._ext.unset_config_int("VersionMap.ReloadInterval")
        return ac

    def load_all(self, symbol):
        # Getting tombstoned versions requires a LOAD_ALL
        self.lib._nvs.version_store._get_all_tombstoned_versions(symbol)

    def read_v0(self, symbol):
        try:
            # Throws an error if version is deleted
            self.lib.read(symbol, as_of=0)
        except NoSuchVersionException:
            pass

    def read_from_epoch(self, symbol):
        try:
            # Throws an error if version is deleted
            self.lib.read(symbol, as_of=datetime.datetime.utcfromtimestamp(0))
        except NoSuchVersionException:
            pass

    def setup(self, num_versions, caching, deleted, read_delay_ms):
        # Disable warnings for version not found
        set_log_level("ERROR")

        if caching == "never":
            adb._ext.set_config_int("VersionMap.ReloadInterval", 0)
        if caching == "forever":
            adb._ext.set_config_int("VersionMap.ReloadInterval", sys.maxsize)
        if caching == "default":
            # Leave the default reload interval
            pass

        ac = self._setup()
        if deleted:
            self.lib = ac[IterateVersionChain.LIB_NAME_DELETED]
        else:
            self.lib = ac[IterateVersionChain.LIB_NAME_UNDELETED]

        if caching != "never":
            # Pre-load the cache
            self.load_all(self.symbol(num_versions))
        query_stats.enable()

    def teardown(self, num_versions, caching, deleted, read_delay_ms):
        adb._ext.unset_config_int("VersionMap.ReloadInterval")
        del self.lib

    def track_num_ver_reads_load_all_versions(self, num_versions, caching, deleted, read_delay_ms):
        query_stats.reset_stats()
        self.load_all(self.symbol(num_versions))
        stats = query_stats.get_query_stats()
        return count_version_reads(stats)

    def track_num_ver_reads_list_undeleted_versions(self, num_versions, caching, deleted, read_delay_ms):
        query_stats.reset_stats()
        self.lib.list_versions(symbol=self.symbol(num_versions))
        stats = query_stats.get_query_stats()
        return count_version_reads(stats)

    def track_num_ver_reads_read_v0(self, num_versions, caching, deleted, read_delay_ms):
        query_stats.reset_stats()
        self.read_v0(self.symbol(num_versions))
        stats = query_stats.get_query_stats()
        return count_version_reads(stats)

    def track_num_ver_reads_read_from_epoch(self, num_versions, caching, deleted, read_delay_ms):
        query_stats.reset_stats()
        self.read_from_epoch(self.symbol(num_versions))
        stats = query_stats.get_query_stats()
        return count_version_reads(stats)

    def track_num_ver_reads_time_read_alternating(self, num_versions, caching, deleted, read_delay_ms):
        query_stats.reset_stats()
        self.read_from_epoch(self.symbol(num_versions))
        self.read_v0(self.symbol(num_versions))
        stats = query_stats.get_query_stats()
        return count_version_reads(stats)
