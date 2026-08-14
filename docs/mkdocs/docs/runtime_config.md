# Runtime Configuration

ArcticDB features a variety of options that can be tuned at runtime. This page details the most commonly modified options, and how to configure them.

## Configuration methods

Every option on this page can be configured either in code or by an environment variable. Most options are integer
options; the exceptions are noted in each option's own section, and log levels, which are explained in their own
section. Each option has a fixed type, and you must use the setter matching that type.

The values of string options are case-sensitive (e.g. `md5`, not `MD5`). Each option's own section lists the values it accepts; setting anything else raises `UserInputException` when the option is used. Option names are not case-sensitive.

### In code

For integer options:

```python
from arcticdb.config import set_config_int
set_config_int(setting, value)
```

where `setting` is a string containing the setting name (e.g. `VersionMap.ReloadInterval`), and `value` is an int to set the option to.

For string options:

```python
from arcticdb.config import set_config_string
set_config_string(setting, value)
```

### Environment variables

Environment variables can be used to set options as follows, where the suffix selects the option's type:

```
ARCTICDB_<setting>_int=<value>
ARCTICDB_<setting>_str=<value>
```

e.g. `ARCTICDB_VersionMap_ReloadInterval_int=0`, or `ARCTICDB_S3Storage_DeleteObjectsChecksum_str=md5`. Note that `.` characters in setting names are replaced with underscores when setting them by environment variables.

### Priority

If both the environment variable is set, and the corresponding `set_config_*` function is called, then the latter takes priority.

### Reactivity

Configuration options are read once when the `Library` instance is created, and are not monitored after that point, so all options should be configured before the `Library` object is constructed.

## Configuration options

### VersionMap.ReloadInterval

ArcticDB library instances maintain a short-lived cache containing what it believes is the latest version for every encountered symbol.  This cache is invalidated after 2 seconds by default.

As a result of this caching, it is theoretically possible for two independent library instances to disagree as to what the latest version of a symbol is for a short period of time.

This caching is designed to reduce load on storage - if this is not a concern it can be safely disabled by setting this option to `0`.

Other than this, there is no client-side caching in ArcticDB.

### SymbolList.MaxDelta

The [symbol list cache](technical/on_disk_storage.md#symbol-list-caching) is compacted when there are more than `SymbolList.MaxDelta` objects on disk in the symbol list cache.

The default is 500.

### S3Storage.DeleteBatchSize

The S3 API supports the `DeleteObjects` method, whereby a single HTTP request can be used to delete multiple objects. This parameter can be used to control how many objects are requested to be deleted at a time.

The default is 1000.

### S3Storage.DeleteObjectsChecksum

Controls which checksum ArcticDB sends on the S3 `DeleteObjects` request. By default the AWS SDK attaches a crc64nvme checksum (`x-amz-checksum-crc64nvme`).

Some S3-compatible backends (e.g. `Scality`) do not support this and reject the request with a checksum related error, but do accept the legacy `Content-MD5` header. Set this option to `md5` for those backends.

This is a string option, so it is set with `set_config_string` or `ARCTICDB_S3Storage_DeleteObjectsChecksum_str` (see [Configuration methods](#configuration-methods)).

Values:
* `crc64nvme`: Send the SDK's default crc64nvme checksum (Default).
* `md5`: Send md5 checksum instead.

Any other value, including a differently cased `MD5` or `CRC64NVME`, raises `UserInputException` on the first
`DeleteObjects` request rather than silently falling back to the default.

!!! warning "Has no effect in `when_supported` checksum mode"

    Note if either environment variable `AWS_REQUEST_CHECKSUM_CALCULATION` or `AWS_RESPONSE_CHECKSUM_VALIDATION` is set to `when_supported`, the setting of `S3Storage.DeleteObjectsChecksum` will have no effect. crc64nvme checksum will always be sent.
    See the [AWS documentation](https://docs.aws.amazon.com/sdkref/latest/guide/feature-dataintegrity.html) for more details.

### S3Storage.VerifySSL

Control whether the client should verify the SSL certificate of the storage. If set, this will override the library option set upon library creation.

Values:
* 0: Do not perform SSL verification.
* 1: Perform SSL verification.

### S3Storage.UseWinINet

This setting only has an effect on the Windows operating system.

Control whether the client should use the WinINet HTTP backend rather than the default WinHTTP backend.

WinINet can provide better error messages in AWS SDK debug logs, for example for diagnosing SSL issues. See the logging
configuration section below for notes on how to set up AWS SDK debug logs.

The INet backend does not allow SSL verification to be disabled with the current AWS SDK.

Values:
* 0: Use WinHTTP
* 1: Use WinINet

### VersionStore.NumCPUThreads and VersionStore.NumIOThreads

ArcticDB uses two threadpools in order to manage computational resources:

* CPU - used for CPU intensive tasks such as decompressing or filtering data
* IO - used to read/write data from/to the underlying storage

By default, ArcticDB attempts to infer sensible sizes for these threadpools based on the number of cores<sup>\*</sup> available on the host machine. The CPU threadpool size defaults to the number of cores available on the host machine, while the IO threadpool size defaults to x1.5 the CPU threadpool size. If these defaults are not suitable for a particular use case, these threadpool sizes can be set directly .

If only `NumCPUThreads` is set, `NumIOThreads` will still default to x1.5 `NumCPUThreads`.

<sup>\*</sup>On Linux machines, this core count takes cgroups into account. In particular, this means that CPU limits are respected in processes running in Kubernetes.

### TaskScheduler.LogTaskStats

Enable per-task diagnostics for the CPU and IO threadpools. When set to `1`, ArcticDB logs one record per completed task to the `schedule` log stream, recording which pool and worker thread ran the task, how long it waited in the queue, and how long it ran:

```
task_stats pool=IO thread=IOPool7 task_id=42 enqueue_ns=... wait_ns=... run_ns=... expired=false priority=0
```

Values:
* 0: Disable (Default)
* 1: Enable

The records are logged at the `DEBUG` level, so you must also raise the `schedule` stream to `DEBUG` (see [Logging configuration](#logging-configuration) below). For example, to capture the records to a file:

```
ARCTICDB_TaskScheduler_LogTaskStats_int=1 ARCTICDB_schedule_loglevel=DEBUG \
    python your_app.py 2> task_stats.log
```

The `thread` field is not supported on Windows and reads `unknown` there.

The [`scripts/analyze_task_scheduler_queues.py`](https://github.com/man-group/ArcticDB/blob/master/scripts/analyze_task_scheduler_queues.py) script parses the captured log and provides a visualization.

### Fork.WarnOnFork

Control whether ArcticDB logs a warning when the process calls `fork()`, for example through
`multiprocessing` with the `fork` start method.

The warning is logged at most once per process, no matter how many times the process forks.

It is only logged once ArcticDB has started its background threads, which happens on the first read or write.
A process that imports `arcticdb`, or that creates libraries without reading or writing, and then forks has
nothing to warn about and stays silent.

Values:
* 0: Disable
* 1: Enable (Default)

The warning is only present in builds for Python 3.12 and later, the version where CPython itself began raising a
`DeprecationWarning` for the same problem. It is also not present on macOS: unlike Linux, macOS's `subprocess` and
`multiprocessing` routinely use a real `fork()` followed by `exec()` to launch child processes (e.g. `spawn` and
`forkserver`), which is indistinguishable from an unsafe fork at the point the warning would be logged, so it would
fire for those safe cases too.

### VersionStore.NumProcessingUnitsLive

`QueryBuilder` operations (`filter`, `groupby`, `resample`, etc.) read data in units of one or
more segments. This setting bounds how many of those units can be admitted into memory at once: an admitted unit's segments
are decoded and held in memory until processing on that unit finishes. Without this bound, if in-memory processing falls
behind the read rate, decoded segments for the whole symbol can accumulate in memory.

Defaults to `VersionStore.SegmentReadWindow` divided by the largest number of segments in a single processing unit
(rounded up), plus `VersionStore.NumCPUThreads`. The first term admits enough units to keep the read window full; the
second lets every CPU thread hold a unit for processing without taking capacity away from the window, so reads are not
gated on processing completing.

Note that the first term scales with `NumIOThreads` and the second with `NumCPUThreads`, so on a machine with large
threadpools the default is correspondingly large and will not meaningfully reduce peak memory use. It is a guard-rail
against processing falling a long way behind reads, not a tight bound.

**To bound memory in absolute terms, set this explicitly to a small value** such as 8. That can have a huge effect, but
costs wall time.

Values:
* A positive integer: the maximum number of processing units resident in memory at once.
* `0`: disables the bound (residency is unbounded, so `SegmentReadWindow` alone governs how many segment reads are in
  flight).

### VersionStore.SegmentReadWindow

Bounds how many segment reads can be submitted to the IO threadpool but not yet completed at any one time.

Applies to the same operations as `VersionStore.NumProcessingUnitsLive`, that is `QueryBuilder` reads and manual column
stats creation. A plain `read` with no `QueryBuilder` takes a different code path and is not affected by either setting.

Defaults to `2 * VersionStore.NumIOThreads`.

Must be at least 1.

### VersionStore.WillItemBePickledWarningMsg

Control whether a detailed message explaining how the item is normalized is logged when calling the `will_item_be_pickled` function.
Please note that this message is logged as a warning. Therefore, setting the log level to below `warning` will also suppress the log in the `will_item_be_pickled` function.

Values:
* 0: Disable
* 1: Enable (Default)

### VersionStore.RecursiveNormalizerMetastructure

Controls whether the recursive normalizer will use meta structure V2

**Read Compatibility:**

| Meta Structure Version | Read Support |
|------------------------|--------------|
| V1 | All existing and future ArcticDB releases |
| V2 | ArcticDB v6.7.0 and later |

**V1 meta structure phase-out plan:**

| Version | Change |
|---------|--------|
| >= v6.7.0 | Deprecation warning when writing V1 meta structure; V2 meta structure can be enabled optionally |
| >= v7.0.0 | V2 meta structure will be enabled by default |

Values: * 1: V1 (Default) * 2: V2  

### VersionStore.VersionStore.RecursiveNormalizerMetastructureV1DeprecationWarning

Control whether deprecation warning will be given if meta structure V1 for recursive normalizer is still in use

Values:
* 0: Disable
* 1: Enable (Default)

Please note that if meta structure V2 is read by < v6.7.0, exception KeyError will be raised

### Compact.LogProgressPercentage

Controls how frequently progress is logged during `finalize_staged_data` operations. A log line is emitted each time this percentage of segments has been processed.

For example, with the default value of `10`, logs appear at 10%, 20%, 30%, ... 100% completion:
```
do_compact: processed 19500/195000 segments for symbol my_symbol, elapsed 1234s
```

Setting to `0` disables progress logging entirely.

The default is 10.

## Logging configuration

ArcticDB has multiple log streams, and the verbosity of each can be configured independently. 
The available streams are visible in the [source code](https://github.com/man-group/ArcticDB/blob/master/python/arcticdb/log.py), although the most commonly useful logs are in:
 
* `version` - contains information about versions being read, created, or destroyed, and traversal of the [version layer](technical/on_disk_storage.md#version-layer) linked list
* `storage` - contains information about individual operations that interact with the storage device (read object, write object, delete object, etc)

The available log levels in decreasing order of verbosity are `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL`, `OFF`. 
By default, all streams are set to the `INFO` level.

There are two ways to configure log levels: 

1. **Setting an environment variable**: `ARCTICDB_<stream>_loglevel=<level>`, for example: `ARCTICDB_version_loglevel=DEBUG`. All streams can be configured together via `ARCTICDB_all_loglevel`. 
2. **In code**: Calling `set_log_level` from the `arcticdb.config` module. This takes two optional arguments:

* `default_level` - the default level for all streams. Should be a string such as `"DEBUG"`
* `specific_log_levels` - a dictionary from stream names to log levels used to override the default such as `{"version": "DEBUG""}`.

If both environment variables are set, and `set_log_level` is called, then the latter takes priority.

AWS SDK (S3) logging is controlled by the `s3` log stream: set `ARCTICDB_s3_loglevel=DEBUG` (for
example). Because AWS SDK output is very noisy, the `s3` stream is opt-in: it defaults to `CRITICAL` and is *not*
affected by `ARCTICDB_all_loglevel`. Raise it explicitly with `ARCTICDB_s3_loglevel` to see S3 logs.

To write these S3 logs to a file in the present working directory instead, set `ARCTICDB_AWS_LogToFile_int=1`.
See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/logging.html) for more details.

!!! note "Deprecated"
    The older `ARCTICDB_AWS_LogLevel_int=<0-6>` variable still works but is deprecated and logs a warning. When both are
    set, the more verbose of `ARCTICDB_AWS_LogLevel_int` and `ARCTICDB_s3_loglevel` is used for both the AWS SDK and the
    `s3` stream. Prefer `ARCTICDB_s3_loglevel`.

### Logging destinations

By default, all logging from ArcticDB goes to `stderr`. This can be configured using the `set_log_level` method.

To configure logging to only a file:

```
from arcticdb.config import set_log_level
set_log_level(console_output=False, file_output_path="/tmp/arcticdb.log")
```

To configure logging to both `stderr` and a file:

```
from arcticdb.config import set_log_level
set_log_level(console_output=True, file_output_path="/tmp/arcticdb.log")
```

To configure logging to only `stderr` (this is the default configuration):

```
from arcticdb.config import set_log_level
set_log_level(console_output=True, file_output_path=None)
```
