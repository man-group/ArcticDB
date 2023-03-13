"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb.version_store.helper import create_test_lmdb_cfg, Defaults, ArcticMemoryConfig
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from numpy.random import RandomState

_rnd = RandomState(0x42)


def get_temp_dbdir(tmpdir, num):
    return str(tmpdir.mkdir("lmdb.{:x}".format(num)))


def test_many_version_store(tmpdir):
    for i in range(10):
        lib_name = "local.test{}".format(i)
        config = create_test_lmdb_cfg(lib_name=lib_name, db_dir=get_temp_dbdir(tmpdir, i))

        arcticc = ArcticMemoryConfig(config, env=Defaults.ENV)
        version_store = arcticc[lib_name]
        idx2 = np.arange(10, 20)
        d2 = {"x": np.arange(20, 30, dtype=np.int64)}
        df2 = pd.DataFrame(data=d2, index=idx2)
        symbol = "sym_{}".format(i)
        version_store.write(symbol, df2)
        vit = version_store.read(symbol)
        assert_frame_equal(vit.data, df2)
