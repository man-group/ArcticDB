"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import datetime
import os

from arcticdb import Arctic
from arcticdb.exceptions import NoSuchVersionException
from arcticdb.config import set_log_level
import arcticdb as adb
import datetime
import math
import sys

from .common import *


class IterateVersionChain:
    number = 10
    timeout = 6000
    CONNECTION_STRING = "lmdb://version_chain?map_size=20GB"
    LIB_NAME = "lib"

    # TODO: Investigate why setup is taking ~50mins with 50k versions on ec2 runners.
    # Locally it looks like it shouldn't take more than 15.
    params = ([25_000], ["forever", "default", "never"], [0.0, 0.99])
    param_names = ["num_versions", "caching", "deleted"]

    def symbol(self, num_versions, deleted):
        return f"symbol_{num_versions}_{deleted}"

    def setup_cache(self):
        self.ac = Arctic(IterateVersionChain.CONNECTION_STRING)
        num_versions_list, caching_list, deleted_list = IterateVersionChain.params

        self.ac.delete_library(IterateVersionChain.LIB_NAME)
        lib = self.ac.create_library(IterateVersionChain.LIB_NAME)

        small_df = generate_random_floats_dataframe(2, 2)

        for num_versions in num_versions_list:
            for deleted in deleted_list:
                symbol = self.symbol(num_versions, deleted)
                for i in range(num_versions):
                    lib.write(symbol, small_df)
                    if (i == math.floor(deleted * num_versions)):
                        lib.delete(symbol)

        del self.ac

    def load_all(self, symbol):
        # Getting tombstoned versions requires a LOAD_ALL
        self.lib._nvs.version_store._get_all_tombstoned_versions(symbol)

    def read_v0(self, symbol):
        try:
            # Throws an error if version is deleted
            self.lib.read(symbol, as_of=0)
        except(NoSuchVersionException):
            pass

    def read_from_epoch(self, symbol):
        try:
            # Throws an error if version is deleted
            self.lib.read(symbol, as_of=datetime.datetime.utcfromtimestamp(0))
        except(NoSuchVersionException):
            pass

    def setup(self, num_versions, caching, deleted):
        # Disable warnings for version not found
        set_log_level("ERROR")

        if caching=="never":
            adb._ext.set_config_int("VersionMap.ReloadInterval", 0)
        if caching=="forever":
            adb._ext.set_config_int("VersionMap.ReloadInterval", sys.maxsize)
        if caching=="default":
            # Leave the default reload interval
            pass

        self.ac = Arctic(IterateVersionChain.CONNECTION_STRING)
        self.lib = self.ac[IterateVersionChain.LIB_NAME]

        if caching != "never":
            # Pre-load the cache
            self.load_all(self.symbol(num_versions, deleted))

    def teardown(self, num_versions, caching, deleted):
        adb._ext.unset_config_int("VersionMap.ReloadInterval")
        del self.lib
        del self.ac


    def time_load_all_versions(self, num_versions, caching, deleted):
        self.load_all(self.symbol(num_versions, deleted))

    def time_list_undeleted_versions(self, num_versions, caching, deleted):
        self.lib.list_versions(symbol=self.symbol(num_versions, deleted))

    def time_read_v0(self, num_versions, caching, deleted):
        self.read_v0(self.symbol(num_versions, deleted))

    def time_read_from_epoch(self, num_versions, caching, deleted):
        self.read_from_epoch(self.symbol(num_versions, deleted))

    def time_read_alternating(self, num_versions, caching, deleted):
        self.read_from_epoch(self.symbol(num_versions, deleted))
        self.read_v0(self.symbol(num_versions, deleted))
