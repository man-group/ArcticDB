# Error Messages

This page details the exceptions and associated error messages users are most likely to encounter,
what they mean, and what (if anything) can be done to resolve the issue.

For legacy reasons, the terms `symbol`, `stream`, and `stream ID` are used interchangeably.

## Errors with numeric error codes

!!! note

    We are in the process of adding error codes to all user-facing errors. As a result, this section will expand as error codes are added to existing errors.

### Internal Errors

| Error Code | Cause                                       | Resolution                                                                                                                                                            |
|------------|---------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1000       | An invalid date range has been passed in.   | ArcticDB date ranges must be in increasing order. Ensure the requested range is sorted.                                                          |
| 1001       | Invalid Argument                            | An invalid argument has been passed in. This error is an internal error and not expected to be exposed to the user - please create an issue on the GitHub repository. |
| 1002       | An internal ArcticDB assertion has failed.  | This error is an internal error and not expected to be exposed to the user - please create an issue on the GitHub repository.                                         |
| 1003       | ArcticDB has encountered an internal error. | This error is an internal error and not expected to be exposed to the user - please create an issue on the GitHub repository.                                         |
| 1004       | Unsupported config found in storage         | Follow the instructions in the error message to repair configuration within your Arctic instance.                                                                     |


### Normalization Errors

| Error Code | Cause                                                                             | Resolution                                                                                                                                                      |
|------------|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2000       | Attempting to update or append an existing type with an incompatible object type | NumPy arrays or Pandas DataFrames can only be mutated by a matching type. Read the latest version of the symbol and update/append with the corresponding type.  |
| 2001       | Input type cannot be converted to an ArcticDB type.                               | Please ensure all input types match supported [ArcticDB types](https://github.com/man-group/ArcticDB/blob/master/python/arcticdb/version_store/library.py#L25). |
| 2003       | A write of an incompatible index type has been attempted.                         | ArcticDB only supports defined Pandas index types. Please see the documentation for more information on what types are supported.                               |
| 2004       | A NumPy append is attempting to change the shape of the previous version.         | When storing NumPy arrays, append operations must have the same shape as the previous version.                                                                  |

### Missing Data Errors

| Error Code | Cause                                             | Resolution                                                                                                            |
|------------|---------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| 3000       | A missing version has been requested of a symbol. | Please request a valid version - see the documentation for the `list_versions` method to enumerate existing versions. |

### Schema Error

| Error Code | Cause                                                      | Resolution                                                                                                                                                                                                                                                                             |
|------------|------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 4000       | The number, type, or name of the columns has been changed. | Ensure that the type and order of the columns has not changed when appending or updating the previous version. This restriction only applies when `Dynamic Schema` is disabled - if you require the columns sets to change, please enable the `Dynamic Schema` option on your library. |
| 4001       | The specified column does not exist. | Please specify a valid column - use the `get_description` method to see all of the columns associated with a given symbol. |
| 4002       | The requested operation is not supported with the type of column provided. | Certain operations are not supported over all column types e.g. arithmetic with the `QueryBuilder` over string columns - use the `get_description` method to see all of the columns associated with a given symbol, along with their types. |
| 4003       | The requested operation is not supported with the index type of the symbol provided. | Certain operations are not supported over all index types e.g. column statistics generation with a string index - use the `get_description` method to see the index(es) associated with a given symbol, along with their types. |
| 4004       | The requested operation is not supported with pickled data. | Certain operations are not supported with pickled data e.g. `date_range` filtering. If such operations are required, you must ensure that the data is of a normalizable type, such that it can be written using the `write` method, and does not require the `write_pickle` method. |


### Storage Errors

| Error Code | Cause                                                                  | Resolution                                                                                                                                          |
|------------|------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 5000       | A missing key has been requested.                                      | ArcticDB has requested a key that does not exist in storage. Please ensure that you have requested a `symbol`, `snapshot`, `version`, or column statistic that exists. |
| 5001       | ArcticDB is attempting to write to an already-existing key in storage. | This error is unexpected - please ensure that no other tools are writing data the same storage location that may conflict with ArcticDB.            |

### Sorting Errors

| Error Code | Cause                                                                  | Resolution                                                                                                                                          |
|------------|------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 6000       | Data should be sorted for this operation.                              | The requested operation requires data to be sorted. If this is a modification operation such as update, sort the input data. ArcticDB relies on Pandas to detect if data is sorted - you can call DataFrame.index.is_monotonic_increasing on your input DataFrame to see if Pandas believes the data to be sorted

### User Input Errors

| Error Code | Cause                                                                  | Resolution                                                                                                                                          |
|------------|------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 7000       | The input provided by the user is invalid in some fashion.             | The resolution will depend on the nature of the incorrect input, and should be explained in the associated error message. |
| 7001       | The input was expected to be a valid decimal string but it is not a valid decimal string.             | Pass a valid decimal string. |
| 7002       | An unsupported character was found in a symbol name.             | We support only the ASCII characters between 32-127 inclusive. Change your symbol name so it contains only valid characters. **If you want to bypass this check, you can define an environment variable called - ARCTICDB_VersionStore_NoStrictSymbolCheck_int=1**. |

### Compatibility Errors

| Error Code | Cause                                                                  | Resolution                                                                                                                                          |
|------------|------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 8000       | The version of ArcticDB being used to read the column statistics does not understand the statistics format. | Update ArcticDB to (at least) the same version as that being used to create the column statistics. |

## Errors without numeric error codes

### Pickling errors

These errors relate to data being pickled, which limits the operations available. Internally, pickled symbols are stored as opaque, serialised binary blobs in the [data layer](/technical/on_disk_storage/#data-layer). No index or column information is maintained in this serialised object which is in contrast to non-pickled data, where this information is stored in the [index layer](/technical/on_disk_storage/#index-layer).

Furthermore, it is not possible to partially read/update/append the data using the ArcticDB API or use the QueryBuilder with pickled symbols. 

All of these errors are of type `arcticdb.exceptions.ArcticException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Cannot append to pickled data <br><br> Cannot update pickled data | A symbol has been created with the `write_pickle` method, and now `append`/`update` has been called on this symbol. | Pickled data cannot be appended to or updated, due to the lack of indexing or column information in the index layer as explained above. If appending is required, the symbol must be created with `write`, and must therefore only contain normalizeable data types. |
| Cannot delete date range of pickled data <br><br> Cannot use head/tail/row_range with pickled data, use plain read instead <br><br> Cannot filter pickled data <br> <br> The data for this symbol is pickled and does not support date_range, row_range, or column queries | A symbol has been created with the `write_pickle` method, and now `delete_data_in_range`/`head`/`tail`/`read` with a `QueryBuilder argument` has been called on this symbol. | For reading operations, unpickling is inherently a Python-layer process. Therefore any operation that would cut down the amount of data returned to a user compared to a call to `read` with no optional parameters cannot be performed in the C++ layer, and would be no faster than calling `read` and then filtering the result down in Python. |

### Snapshot errors

Errors that can be encountered when creating  and deleting snapshots, or trying to read data from a specific snapshot.

All of these errors are of type `arcticdb.exceptions.ArcticException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Snapshot with name <name\> already exists | The `snapshot` method was called, but a snapshot with the specified name already exists. | The old snapshot must first be deleted with `delete_snapshot`. |
| Cannot snapshot version(s) that have been deleted... | A `versions` dictionary was provided to the `snapshot` method, but one of the symbol-version pairs specified does not exist. | The `list_versions` method can be used to see which versions of which symbols are in which snapshots. |
| Only one of skip_symbols and versions can be set | The `snapshot` method was called with both the `skip_symbols` and `versions` optional arguments set. | Just specify `versions` on its own in this case. |

### Require live version errors

A select few operations with ArcticDB require the symbol to exist and have at least one live version. These errors occur when this is not the case.

All of these errors are of type `arcticdb.exceptions.ArcticException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Cannot update non-existent stream <symbol\> | The `update` method was called with the optional `upsert` defaulted or set to `False`, but this symbol has no live versions. | If the symbol is expected to have a live version, then this is a genuine error. Otherwise, set `upsert` to `True`. |

### Date-range related errors

All calls to `delete_data_in_range` and `update`, and calls to `read` using the `date_range` optional argument, require the existing data to have a *sorted* timestamp index. ArcticDB does not check this condition at write time.

All of these errors are of type `arcticdb.exceptions.ArcticException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Cannot apply date range filter to symbol with non-timestamp index | `read` method called with the optional `date_range` argument specified, but the symbol does not have a timestamp index. | None, the `date_range` parameter does not make sense without a timestamp index. |
| Non-contiguous rows, range search on unsorted data?... | `read` method called with the optional `date_range` argument specified, and the symbol has a timestamp index, but it is not sorted. | To use the `date_range` argument to `read`, the user must ensure the data is sorted on the index at write time. |
| Delete in range will not work as expected with a non-timeseries index | `delete_data_in_range` method called, but the symbol does not have a timestamp index. | None, the `delete_data_in_range` method does not make sense without a timestamp index. |

### QueryBuilder errors

Due to the client-only nature of ArcticDB, it is not possible to know if a `QueryBuilder` provided to `read` makes sense for the given symbol without interacting with the storage. In particular, we do not know:

* Whether a specified column exists
* What the type of the data held in a specified column is if it does exist

All of these errors are of type `arcticdb.exceptions.ArcticException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Unexpected column name | A column name was specified with the `QueryBuilder` that does not exist for this symbol, and the library has dynamic schema disabled. | None of the supported `QueryBuilder` operations (filtering, projections, group-bys and aggregations) make sense with non-existent columns. |
| Non-numeric type provided to binary operation: <typename\> | Error messages like this imply that an operation that ArcticDB does not support was provided in the `QueryBuilder` argument e.g. adding two string columns together. | The `get_description` method can be used to inspect the types of the columns. A full list of supported operations are provided in the `QueryBuilder` [API documentation](/api/query_builder). |
| Cannot compare <typename 1\> to <typename 2\> (possible categorical?) | If `get_description` indicates that a column is of categorical type, and this categorical is being used to store string values, then comparisons to other strings will fail with an error message like this one. | Categorical support in ArcticDB is [extremely limited](/faq#does-arcticdb-support-categorical-data), but may be added in the future. |

### Encoding errors

These errors should be extremely rare, however it is possible that the encoding in the storage may change from time to time. Whilst the changes will always be backwards compatible (new clients can always read the old data), it's possible they may not be forward-compatible, and data that has been written by a new client cannot be read by an older one

All of these errors are of type `arcticdb.exceptions.ArcticException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
|  Error decoding | A column was unable to be decoded by the compression algorithm. | Upgrade to a later version of the client. |

## Exception Hierarchy

ArcticDB exceptions are exposed in `arcticdb.exceptions` and sit in a hierarchy:

```
RuntimeError
└-- ArcticException
    |-- ArcticDbNotYetImplemented
    |-- DuplicateKeyException
    |-- MissingDataException
    |-- NoDataFoundException
    |-- NoSuchVersionException
    |-- NormalizationException
    |-- PermissionException
    |-- SchemaException
    |-- SortingException
    |   └-- UnsortedDataException
    |-- StorageException
    |-- StreamDescriptorMismatch
    └-- InternalException
```

