# Runtime Configuration

ArcticDB features a variety of options that can be tuned at runtime. This page details the most commonly modified options, and how to configure them.

## Configuration methods

All of the integer options detailed on this page can be configured using the following two methods. All of the options listed on this page are integer options except for log levels, which will be explained in their own section.

### In code

For integer options, the following code snippet demonstrates how to set values in code:

```python
from arcticdb_ext import set_config_int
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

The [symbol list cache](/technical/on_disk_storage#symbol-list-caching) is compacted when there are more than `SymbolList.MaxDelta` objects on disk in the symbol list cache.

The default is 500.

### S3Storage.CheckBucketMaxWait

The `Arctic` constructor will check the given S3 bucket is accessible.
It will wait at most this amount of time in milliseconds.

The default is 1000ms.

### S3Storage.ConnectTimeoutMs and S3Storage.RequestTimeoutMs

Refer to [AWS documentation](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html).

The defaults are 30000 and 200000 respectively.

### S3Storage.DeleteBatchSize

The S3 API supports the `DeleteObjects` method, whereby a single HTTP request can be used to delete multiple objects. This parameter can be used to control how many objects are requested to be deleted at a time.

The default is 1000.

### S3Storage.VerifySSL

Control whether the client should verify the SSL certificate of the storage. If set, this will override the library option set upon library creation.

Values:
* 0: Do not perform SSL verification.
* 1: Perform SSL verification.

### VersionStore.NumCPUThreads and VersionStore.NumIOThreads

ArcticDB uses two threadpools in order to manage computational resources:

* CPU - used for CPU intensive tasks such as decompressing or filtering data
* IO - used to read/write data from/to the underlying storage

By default, ArcticDB attempts to infer sensible sizes for these threadpools based on the number of cores<sup>\*</sup> available on the host machine. The CPU threadpool size defaults to the number of cores available on the host machine, while the IO threadpool size defaults to x1.5 the CPU threadpool size. If these defaults are not suitable for a particular use case, these threadpool sizes can be set directly .

If only `NumCPUThreads` is set, `NumIOThreads` will still default to x1.5 `NumCPUThreads`.

<sup>\*</sup>On Linux machines, this core count takes cgroups into account. In particular, this means that CPU limits are respected in processes running in Kubernetes.

## Logging configuration

ArcticDB has multiple log streams, and the verbosity of each can be configured independently. 
The available streams are visible in the [source code](https://github.com/man-group/ArcticDB/blob/master/python/arcticdb/log.py), although the most commonly useful logs are in:

* `version` - contains information about versions being read, created, or destroyed, and traversal of the [version layer](/technical/on_disk_storage#version-layer) linked list
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
