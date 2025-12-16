# Runtime Configuration

ArcticDB features a variety of options that can be tuned at runtime. This page details the most commonly modified options, and how to configure them.

## Configuration methods

All of the integer options detailed on this page can be configured using the following two methods. All of the options listed on this page are integer options except for log levels, which will be explained in their own section.

### In code

For integer options, the following code snippet demonstrates how to set values in code:

```python
from arcticdb.config import set_config_int
set_config_int(setting, value)
```

where `setting` is a string containing the setting name (e.g. `VersionMap.ReloadInterval`), and `value` is an int to set the option to.

### Environment variables

For integer options, environment variables can be used to set options as follows:

```
ARCTICDB_<setting>_int=<value>
```

e.g. `ARCTICDB_VersionMap_ReloadInterval_int=0`. Note that `.` characters in setting names are replaced with underscores when setting them by environment variables.

### Priority

If both the environment variable is set, and `set_config_int` is called, then the latter takes priority.

### Reactivity

Configuration options are read once when the `Library` instance is created, and are not monitored after that point, so all options should be configured before the `Library` object is constructed.

## Configuration options

### VersionMap.ReloadInterval

ArcticDB library instances maintain a short-lived cache containing what it believes is the latest version for every encountered symbol.  This cache is invalidated after 5 seconds by default.

As a result of this caching, it is theoretically possible for two independent library instances to disagree as to what the latest version of a symbol is for a short period of time.

This caching is designed to reduce load on storage - if this is not a concern it can be safely disabled by setting this option to `0`.

Other than this, there is no client-side caching in ArcticDB.

### SymbolList.MaxDelta

The [symbol list cache](technical/on_disk_storage.md#symbol-list-caching) is compacted when there are more than `SymbolList.MaxDelta` objects on disk in the symbol list cache.

The default is 500.

### S3Storage.DeleteBatchSize

The S3 API supports the `DeleteObjects` method, whereby a single HTTP request can be used to delete multiple objects. This parameter can be used to control how many objects are requested to be deleted at a time.

The default is 1000.

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

## Logging configuration

ArcticDB has multiple log streams, and the verbosity of each can be configured independently. 
The available streams are visible in the [source code](https://github.com/man-group/ArcticDB/blob/master/python/arcticdb/log.py), although the most commonly useful logs are in:
 
* `version` - contains information about versions being read, created, or destroyed, and traversal of the [version layer](technical/on_disk_storage.md#version-layer) linked list
* `storage` - contains information about individual operations that interact with the storage device (read object, write object, delete object, etc)

The available log levels in decreasing order of verbosity are are `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL`, `OFF`. 
By default, all streams are set to the `INFO` level.

There are two ways to configure log levels: 

1. **Setting an environment variable**: `ARCTICDB_<stream>_loglevel=<level>`, for example: `ARCTICDB_version_loglevel=DEBUG`. All streams can be configured together via `ARCTICDB_all_loglevel`. 
2. **In code**: Calling `set_log_level` from the `arcticdb.config` module. This takes two optional arguments:

* `default_level` - the default level for all streams. Should be a string such as `"DEBUG"`
* `specific_log_levels` - a dictionary from stream names to log levels used to override the default such as `{"version": "DEBUG""}`.

If both environment variables are set, and `set_log_level` is called, then the latter takes priority.

S3 logging can also be enabled by setting the environment variable `ARCTICDB_AWS_LogLevel_int=6`, which will output all S3 logs to a file in the present working directory. 
See the [AWS documentation](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/logging.html) for more details.

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
