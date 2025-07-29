Set the feature flag,

In code,

```
from arcticdb.config import set_config_int
set_config_int("dev.stage_new_api_enabled", 1)
```

or as an environment variable,

```
export ARCTICDB_dev_stage_new_api_enabled=1
```

Now use the staged data API:

```
from arcticdb_ext.version_store import StageResult  # will be moved to a proper import when we do the non-beta API

lib: NativeVersionStore

df = pd.DataFrame({"a": [1,2,3]})
df.index = [1, 2, 3]
df_2 = pd.DataFrame({"a": [4,5,6]})
df_2.index = [4, 5, 6]

# "stage" now returns a StageResult object you can use as a token to your compact_incomplete call
# The StageResult is pickleable, to support IPC
staged_result: StageResult = lib.stage("sym", df)
staged_result_2: StageResult = lib.stage("sym", df_2)

# Only finalize particular staging results
# Will be stage_results on the non-beta API
lib.compact_incomplete("sym", _stage_results=[staged_result], append=False, convert_int_to_float=False)

# "sym" will now be equal to df

# The old API still works and we have no plans to deprecate it
lib.compact_incomplete("sym", append=True, convert_int_to_float=False)
# "sym" will now be equal to pd.concat([df, df_2])

# TODO what if _stage_results is empty?
```