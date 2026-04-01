### New Stats Types

We want to test what happens to the client from PR #2990 if new column stats types are added.

We want maintenance operations (create_column_stats, drop_column_stats) to fail. When we add stats
at read time, we will want read to still use the stats it does understand.

This branch adds,

```
    COLUMN_STATS_MIN_V2 = 3;
    COLUMN_STATS_MAX_V2 = 4;
```

and starts to write them.

Using code built from this branch ran,

```
from arcticdb import Arctic
import pandas as pd

ac = Arctic("lmdb:///users/is/aseaton/lmdb_tst")
lib = ac.create_library("compat_test")

column_stats_dict = {"col_1": {"MINMAX"}}

df = pd.DataFrame({"col_1": [10, 20]}, index=[pd.Timestamp(0), pd.Timestamp(1)])
lib.write("sym", df)
lib.write("sym-2", df)

lib._nvs.create_column_stats("sym", column_stats_dict)
lib._nvs.create_column_stats("sym-2", column_stats_dict)

lib._nvs.read_column_stats("sym")

"""
In [8]: lib._nvs.read_column_stats("sym")
   ...:
Out[8]:
                                end_index  v2_MIN(col_1)  v2_MAX(col_1)
start_index
1970-01-01  1970-01-01 00:00:00.000000002             10             20
"""
```

Using code from PR 2990 - venv `~/venvs/compat_tst`

```
from arcticdb import Arctic
import pandas as pd

ac = Arctic("lmdb:///users/is/aseaton/lmdb_tst")
lib = ac["compat_test"]

column_stats_dict = {"col_1": {"MINMAX"}}

lib._nvs.read_column_stats("sym")

"""
In [3]: lib._nvs.read_column_stats("sym")
Out[3]:
                                end_index  v2_MIN(col_1)  v2_MAX(col_1)
start_index
1970-01-01  1970-01-01 00:00:00.000000002             10             20
"""

"""
In [4]: lib._nvs.get_column_stats_info("sym")
20260401 14:29:26.923621 3026238 W arcticdb | Unrecognised column stats type in header. Upgrade your ArcticDB installation. Skipping stat.
20260401 14:29:26.923640 3026238 W arcticdb | Unrecognised column stats type in header. Upgrade your ArcticDB installation. Skipping stat.
Out[4]: {}

"""

A no-op behaviour here would be better...

"""
In [5]: lib._nvs.drop_column_stats("sym", column_stats_dict)
20260401 14:30:06.324658 3026238 W arcticdb | Unrecognised column stats type in header. Upgrade your ArcticDB installation. Skipping stat.
20260401 14:30:06.324675 3026238 W arcticdb | Unrecognised column stats type in header. Upgrade your ArcticDB installation. Skipping stat.
20260401 14:30:06.324684 3026238 W arcticdb | Requested column stats drop but column 'col_1' does not have any column stats

In [6]: lib._nvs.get_column_stats_info("sym")
---------------------------------------------------------------------------
StorageException                          Traceback (most recent call last)
Cell In[6], line 1
----> 1 lib._nvs.get_column_stats_info("sym")

File ~/venvs/compat_tst/lib/python3.10/site-packages/arcticdb/version_store/_store.py:1323, in NativeVersionStore.get_column_stats_info(self, symbol, as_of, **kwargs)
   1306 """
   1307 Read the column statistics dictionary for the given symbol.
   1308
   (...)
   1320     In the same format as the `column_stats` argument provided to `create_column_stats` and `drop_column_stats`.
   1321 """
   1322 version_query = self._get_version_query(as_of, **kwargs)
-> 1323 return self.version_store.get_column_stats_info_version(symbol, version_query).to_map()

StorageException: E_KEY_NOT_FOUND Failed to read column stats key: Not found: [S:sym:0:0x465eac98ba9b3485@1775035396990262322[0,0]]
"""

lib.drop_column_stats("sym") ## should succeed

lib.create_column_stats("sym-2", column_stats_dict)  ## should fail

```

### Major Version Testing

c08ba7d9d bumps the major version. Older clients should just raise if they see this as they cannot trust any of their
understanding of what they are seeing.

```
// Stored in the user defined metadata for KeyType::COLUMN_STATS
message ColumnStatsHeader {
    // This version number refers to the format of this header structure.
    // For example if we ever want to stop using StatColMapping and move to a different encoding,
    // we would increment the version number. This helps to avoid older clients mis-interpreting
    // existing fields (like an empty StatColMapping in this example).
    uint32 version = 1;  # bumped to 2
    repeated StatColMapping stats = 2;
    // end of fields in version 1
}
```

Using code built from this branch ran,

```
from arcticdb import Arctic
import pandas as pd

ac = Arctic("lmdb:///users/is/aseaton/lmdb_tst")
lib = ac.create_library("compat_test_major_version")

column_stats_dict = {"col_1": {"MINMAX"}}
df = pd.DataFrame({"col_1": [10, 20]}, index=[pd.Timestamp(0), pd.Timestamp(1)])

lib.write("sym", df)
lib._nvs.create_column_stats("sym", column_stats_dict)
```

Using code from PR 2990 - venv `~/venvs/compat_tst`:

```
from arcticdb import Arctic
import pandas as pd

ac = Arctic("lmdb:///users/is/aseaton/lmdb_tst")
lib = ac.get_library("compat_test_major_version")

lib._nvs.get_column_stats_info("sym")
>>> StorageException: E_KEY_NOT_FOUND Failed to read column stats key: E_UNRECOGNISED_COLUMN_STATS_VERSION This client only understands column stats version 1 but has encountered version=2. Upgrade your ArcticDB installation.

In [2]: lib._nvs.read_column_stats("sym")
   ...:
Out[2]:
                                end_index  v2_MIN(col_1)  v2_MAX(col_1)
start_index
1970-01-01  1970-01-01 00:00:00.000000002             10             20

column_stats_dict = {"col_1": {"MINMAX"}}
In [6]: lib._nvs.drop_column_stats("sym", column_stats_dict)
---------------------------------------------------------------------------
CompatibilityException                    Traceback (most recent call last)
Cell In[6], line 1
----> 1 lib._nvs.drop_column_stats("sym", column_stats_dict)

File ~/venvs/compat_tst/lib/python3.10/site-packages/arcticdb/version_store/_store.py:1281, in NativeVersionStore.drop_column_stats(self, symbol, column_stats, as_of)
   1279 column_stats = self._get_column_stats(column_stats)
   1280 version_query = self._get_version_query(as_of)
-> 1281 self.version_store.drop_column_stats_version(symbol, column_stats, version_query)

CompatibilityException: E_UNRECOGNISED_COLUMN_STATS_VERSION This client only understands column stats version 1 but has encountered version=2. Upgrade your ArcticDB installation.

lib._nvs.drop_column_stats("sym")
>>> succeeds

lib._nvs.create_column_stats("sym", column_stats_dict)
>>> succeeds! This is a bug, we should fail here
```
