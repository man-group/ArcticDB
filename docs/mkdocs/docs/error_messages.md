# Error Messages

This page details the exceptions and associated error messages users are most likely to encounter, what they mean, and what (if anything) can be done to resolve the issue.

Note that for legacy reasons, the terms `symbol`, `stream`, and `stream ID` are used interchangeably.

## Errors with numeric error codes

!!! note

    Please note that we are in the process of adding error codes to all user-facing errors. As a rsult, this section will expand as error codes are added to existing errors.

### Internal Errors

| Error Code | Cause                                       | Resolution                                                                                                                                                            |
|------------|---------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1000       | An invalid date range has been passed in.   | ArcticDB ranges must be positive and in increasing order. ENsure the requested range is positive and sorted.                                                          |
| 1001       | Invalid Argument                            | An invalid argument has been passed in. This error is an internal error and not expected to be exposed to the user - please create an issue on the GitHub repository. |
| 1002       | In internal ArcticDB assertion has failed.  | This error is an internal error and not expected to be exposed to the user - please create an issue on the GitHub repository.                                         |
| 1003       | ArcticDB has encountered an internal error. | This error is an internal error and not expected to be exposed to the user - please create an issue on the GitHub repository.                                         |


### Normalization Errors

| Error Code | Cause                                                                             | Resolution                                                                                                                                                      |
|------------|-----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2000       | Attempting to update or append an existing type with an incompatible object typpe | NumPy arrays or Pandas DataFrames can only be mutated by a matching type. Read the latest version of the symbol and update/append with the corresponding type.  |
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


### Storage Errors

| Error Code | Cause                                                                  | Resolution                                                                                                                                          |
|------------|------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 5000       | A missing key has been requested.                                      | ArcticDB has requested a key that does not exist in storage. Please ensure that you have requested a `symbol`, `snapshot` or `version` that exists. |
| 5001       | ArcticDB is attempting to write to an already-existing key in storage. | This error is unexpected - please ensure that no other tools are writing data the same storage location that may conflict with ArcticDB.            |

## Errors without numeric error codes

!!! note

    Please note that we are in the process of adding error codes to all user-facing errors. 

### Pickling errors

These errors relate to data being pickled, which limits the operations available. Internally, pickled symbols are stored as a single object in the [data layer](/technical/on_disk_storage/#data-layer), with any index or column information maintained only in this serialised object. This is in contrast to non-pickled data, where this information is also available in the [index layer](/technical/on_disk_storage/#data-layer).

All of these errors are of type `ArcticNativeCxxException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Cannot append to pickled data <br><br> Cannot update pickled data | A symbol has been created with the `write_pickle` method, and now `append`/`update` has been called on this symbol. | Pickled data cannot be appended to or updated, due to the lack of indexing or column information in the index layer as explained above. If appending is required, the symbol must be created with `write`, and must therefore only contain normalizeable data types. |
| Cannot delete date range of pickled data <br><br> Cannot use head/tail/row_range with pickled data, use plain read instead <br><br> Cannot filter pickled data <br> <br> The data for this symbol is pickled and does not support date_range, row_range, or column queries | A symbol has been created with the `write_pickle` method, and now `delete_data_in_range`/`head`/`tail`/`read` with a `QueryBuilder argument` has been called on this symbol. | For reading operations, unpickling is inherently a Python-layer process. Therefore any operation that would cut down the amount of data returned to a user compared to a call to `read` with no optional parameters cannot be performed in the C++ layer, and would be no faster than calling `read` and then filtering the result down in Python. |

### Snapshot errors

Errors that can be encountered when creating  and deleting snapshots, or trying to read data from a specific snapshot.

All of these errors are of type `ArcticNativeCxxException` unless specified otherwise.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Snapshot with name <name\> already exists | The `snapshot` method was called, but a snapshot with the specified name already exists. | The old snapshot must first be deleted with `delete_snapshot`. |
| Cannot snapshot version(s) that have been deleted... | A `versions` dictionary was provided to the `snapshot` method, but one of the symbol-version pairs specified does not exist. | The `list_versions` method can be used to see which versions of which symbols are in which snapshots. |
| Only one of skip_symbols and versions can be set | The `snapshot` method was called with both the `skip_symbols` and `versions` optional arguments set. | It does not make sense to specify both of these optional arguments, just `versions` should be used on its own in this case. |
| `NoDataFoundException` | `delete_snapshot` called, but no snapshot with this name exists. <br><br> A string was provided to the `as_of` argument of a read operation, but no snapshot with this name exists. | The `list_snapshots` method can be used to list all of the snapshots in a library. <br><br> The `list_versions` method can be used to see which versions of which symbols are in which snapshots. |

### Require live version errors

A select few operations with ArcticDB require the symbol to exist and have at least one live version. These errors occur when this is not the case.

All of these errors are of type `ArcticNativeCxxException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Cannot update non-existent stream <symbol\> | The `update` method was called with the optional `upsert` defaulted or set to `False`, but this symbol has no live versions. | If the symbol is expected to have a live version, then this is a genuine error. Otherwise, set `upsert` to `True`. |

### Date-range related errors

All calls to `delete_data_in_range` and `update`, and calls to `read` using the `date_range` optional argument, require the existing data to have a *sorted* timestamp index. ArcticDB does not check this condition at write time.

All of these errors are of type `ArcticNativeCxxException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Cannot apply date range filter to symbol with non-timestamp index | `read` method called with the optional `date_range` argument specified, but the symbol does not have a timestamp index. | None, the `date_range` parameter does not make sense without a timestamp index. |
| Non-contiguous rows, range search on unsorted data?... | `read` method called with the optional `date_range` argument specified, and the symbol has a timestamp index, but it is not sorted. | To use the `date_range` argument to `read`, the user must ensure the data is sorted on the index at write time. |
| Delete in range will not work as expected with a non-timeseries index | `delete_data_in_range` method called, but the symbol does not have a timestamp index. | None, the `delete_data_in_range` method does not make sense without a timestamp index. |

### QueryBuilder errors

Due to the client-only nature of ArcticDB, it is not possible to know if a `QueryBuilder` provided to `read` makes sense for the given symbol without interacting with the storage. In particular, we do not know:

* Whether a specified column exists
* What the type of the data held in a specified column is if it does exist

All of these errors are of type `ArcticNativeCxxException`.

| Error messages | Cause | Resolution |
|:--------------|:-------|:-----------|
| Unexpected column name | A column name was specified with the `QueryBuilder` that does not exist for this symbol, and the library has dynamic schema disabled. | None of the supported `QueryBuilder` operations (filtering, projections, group-bys and aggregations) make sense with non-existent columns. |
| Non-numeric type provided to binary operation: <typename\> | Error messages like this imply that an operation that ArcticDB does not support was provided in the `QueryBuilder` argument e.g. adding two string columns together. | The `get_description` method can be used to inspect the types of the columns. A full list of supported operations are provided in the `QueryBuilder` [API documentation](/api/query_builder). |
| Cannot compare <typename 1\> to <typename 2\> (possible categorical?) | If `get_description` indicates that a column is of categorical type, and this categorical is being used to store string values, then comparisons to other strings will fail with an error message like this one. | Categorical support in ArcticDB is [extremely limited](/faq#does-arcticdb-support-categorical-data), but may be added in the future. |
