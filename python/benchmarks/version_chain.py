"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import shutil

from arcticdb import Arctic
from arcticdb.exceptions import NoSuchVersionException
from arcticdb.config import set_log_level
import arcticdb as adb
import datetime
import math
import sys
import time

from .common import *


class IterateVersionChain:
    timeout = 6000
    sample_time = 2
    rounds = 2

    DIR_UNDELETED = "version_chain"
    DIR_TAIL_DELETED = "version_chain_tail_deleted"
    CONNECTION_STRING_UNDELETED = f"lmdb://{DIR_UNDELETED}"
    CONNECTION_STRING_TAIL_DELETED = f"lmdb://{DIR_TAIL_DELETED}"
    DELETION_POINT = 0.99  # delete the symbol after writing this proportion of the versions
    LIB_NAME = "lib"

    params = ([25_000], ["forever", "default", "never"], [True, False])

    # In the tail_deleted case we delete the symbol after writing the DELETION_POINT fraction of the versions,
    # so the tail of the version chain is deleted.
    param_names = ["num_versions", "caching", "tail_deleted"]

    def symbol(self, num_versions):
        return f"symbol_{num_versions}"

    def __init__(self):
        self.logger = get_logger()
        self.lib = None

    def setup_cache(self):
        start = time.time()
        self._setup_cache()
        self.logger.info(f"SETUP_CACHE TIME: {time.time() - start}")

    def _setup_cache(self):
        ac = Arctic(IterateVersionChain.CONNECTION_STRING_UNDELETED)

        num_versions_list, caching_list, deleted_list = IterateVersionChain.params

        ac.delete_library(IterateVersionChain.LIB_NAME)
        lib = ac.create_library(IterateVersionChain.LIB_NAME)

        small_df = generate_random_floats_dataframe(2, 2)
        delete_points = {}
        for num_versions in num_versions_list:
            delete_points[num_versions] = math.floor(IterateVersionChain.DELETION_POINT * num_versions)

        # To save setup time we populate the two libraries by:
        #
        # Step 1 - write 99% of the versions to one library
        # Step 2 - copy the library directory
        # Step 3 - delete the symbol on the copy. Write the remaining versions to both source and copy.
        start_time = time.time()
        adb._ext.set_config_int("VersionMap.ReloadInterval", sys.maxsize)

        for num_versions in num_versions_list:
            symbol = self.symbol(num_versions)
            deletion_point = delete_points[num_versions]
            for i in range(deletion_point):
                lib.write(symbol, small_df)

        del lib
        del ac

        shutil.rmtree(IterateVersionChain.DIR_TAIL_DELETED, ignore_errors=True)
        shutil.copytree(IterateVersionChain.DIR_UNDELETED, IterateVersionChain.DIR_TAIL_DELETED)

        ac = Arctic(IterateVersionChain.CONNECTION_STRING_UNDELETED)
        lib = ac[IterateVersionChain.LIB_NAME]

        for num_versions in num_versions_list:
            symbol = self.symbol(num_versions)
            deletion_point = delete_points[num_versions]
            for i in range(deletion_point, num_versions):
                lib.write(symbol, small_df)
            # reasonableness check
            assert lib.read(symbol).version == num_versions - 1

        del lib
        del ac

        ac = Arctic(IterateVersionChain.CONNECTION_STRING_TAIL_DELETED)
        lib = ac[IterateVersionChain.LIB_NAME]
        for num_versions in num_versions_list:
            symbol = self.symbol(num_versions)
            lib.delete(symbol)
            deletion_point = delete_points[num_versions]
            for i in range(deletion_point, num_versions):
                lib.write(symbol, small_df)
            # reasonableness checks
            assert lib.read(symbol).version == num_versions - 1
            # Only versions that have not been deleted are returned by list_versions
            assert len(lib.list_versions(symbol)) == num_versions - deletion_point

        adb._ext.unset_config_int("VersionMap.ReloadInterval")

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

    def setup(self, num_versions, caching, deleted):
        # Disable warnings for version not found
        set_log_level("ERROR")

        if caching == "never":
            adb._ext.set_config_int("VersionMap.ReloadInterval", 0)
        if caching == "forever":
            adb._ext.set_config_int("VersionMap.ReloadInterval", sys.maxsize)
        if caching == "default":
            # Leave the default reload interval
            pass

        if deleted:
            ac = Arctic(IterateVersionChain.CONNECTION_STRING_TAIL_DELETED)
        else:
            ac = Arctic(IterateVersionChain.CONNECTION_STRING_UNDELETED)
        self.lib = ac[IterateVersionChain.LIB_NAME]

        if caching != "never":
            # Pre-load the cache
            self.load_all(self.symbol(num_versions))

    def teardown(self, num_versions, caching, deleted):
        adb._ext.unset_config_int("VersionMap.ReloadInterval")
        del self.lib

    def time_load_all_versions(self, num_versions, caching, deleted):
        self.load_all(self.symbol(num_versions))

    def time_list_undeleted_versions(self, num_versions, caching, deleted):
        self.lib.list_versions(symbol=self.symbol(num_versions))

    def time_read_v0(self, num_versions, caching, deleted):
        self.read_v0(self.symbol(num_versions))

    def time_read_from_epoch(self, num_versions, caching, deleted):
        self.read_from_epoch(self.symbol(num_versions))

    def time_read_alternating(self, num_versions, caching, deleted):
        self.read_from_epoch(self.symbol(num_versions))
        self.read_v0(self.symbol(num_versions))
