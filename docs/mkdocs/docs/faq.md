# Frequently Asked Questions

!!! note

    This FAQ document covers multiple topic areas - please see the contents table on the 
    right for more information.

##Product

###*What is ArcticDB?*

ArcticDB is a high performance DataFrame database built for the modern Python
Data Science ecosystem. ArcticDB is an embedded database engine - which means
that installing ArcticDB is as simple as installing a Python package.
This also means that ArcticDB does not require any server infrastructure to function.

ArcticDB is optimised for numerical datasets spanning millions of rows and columns,
enabling you to store and retrieve massive datasets within a Pythonic,
dataframe-like API that researchers, data scientists and software engineers will
find immediately familiar.

###*How does ArcticDB differ from the version of Arctic on GitHub?*

Please see the [history page](../history).

###*How does ArcticDB differ from Apache Parquet?*

Both ArcticDB and Parquet enable the storage of columnar data without requiring
additional infrastructure.

ArcticDB however uses a custom storage format that means it offers the following functionality over Parquet:

- Versioned modifications ("time travel") - ArcticDB is bitemporal.
- Timeseries indexes. ArcticDB is a timeseries database and as such is optimised for slicing 
and dicing timeseries data containing billions of rows.
- Data discovery - ArcticDB is built for teams. Data is structured into libraries and symbols rather 
than raw filepaths.
- Support for streaming data. ArcticDB is a fully functional streaming/tick database, enabling the storage 
of both batch and streaming data.
- Support for "dynamic schemas" - ArcticDB supports datasets with changing schemas (column sets) over time.
- Support for automatic data deduplication.

###*What sort of data is ArcticDB best suited to?*

ArcticDB is an OLA(nalytical)P DBMS, rather than an OLT(ransactional)P DBMS.

In practice, this means that ArcticDB is optimised for large numerical datasets
and for queries that operate over many rows at a time.

###*Does ArcticDB require a server?*

No. ArcticDB is a fully fledged embedded analytical database system,
designed for modern cloud and on-premises object storage that does not require
a server for any of the core features.

###*What languages can I use ArcticDB with?*

Bindings are currently only available for Python. 

###*How can I get started using ArcticDB?*

Please see our [getting started guide](../)!

## Licensing

###*How is ArcticDB licensed?*

ArcticDB can be installed straight from PyPi:

```
pip install arcticdb
```

Once installed, ArcticDB has two modes; *licensed* and *unlicensed*. Without a license, ArcticDB is only valid for 
non-production use and the software will only function up to the trial end-date. This date is stated in the description 
of the package visible on *PyPi*.

For a 3-month trial license that permits production use, please acquire a **FREE** license from [https://www.arcticdb.com](https://www.arcticdb.com). Once installed, ArcticDB is 
licensed for production use.

For perpetual production use for yourself and your team, please contact us as described on [https://www.arcticdb.com](https://www.arcticdb.com).

## Technical

### *Does ArcticDB use SQL?*

No. ArcticDB enables data access and modifications with a Python API that speaks in terms of Pandas DataFrames. See the reference documentation for more details.

### *Does ArcticDB deduplicate data?*

Yes.

On each `write`, ArcticDB will check the previous version of the symbol that you are writing (and _only_ this version - other symbols will not be scanned!) and skip the write of identical [segments](../technical/on_disk_storage_format). Please keep in mind however that this is most effective when version `n` is equal to version `n-1` plus additional data at the end - and only at the end! If there is additional data inserted into the in the middle, then all segments occuring after that modification will almost certainly differ. ArcticDB segments data at fixed intervals and data is only de-duplicated if the hashes of the data segments are identical - as a result, a one row offset will prevent effective de-duplication. 

### *How does ArcticDB enable advanced analytics?*

ArcticDB is primarily focused on filtering and transfering data from storage through to memory - at which point Pandas, NumPy, or other standard analytical packages can be utilised for analytics.

That said, ArcticDB does offer a limited set of analytical functions that are executed inside the C++ storage engine offering significant performance benefits over Pandas. For more information, see the documentation for the *QueryBuilder* class.

### *What does Pickling mean?*

ArcticDB has two means for storing data:

1. ArcticDB can store your data using the [Arctic On-Disk Storage Format](../technical/on_disk_storage).
2. ArcticDB can [Pickle](https://docs.python.org/3/library/pickle.html) your data, storing it as a giant binary blob.

(1) is vastly more performant (i.e. reads and writes are faster), space efficient and unlocks data slicing as described in the getting started guide. There are no practical advantages to storing your data as a Pickled binary-blob - _other than_ certain data types must be Pickled for ArcticDB to be able to store them at all!

ArcticDB is only able to store the following data types natively:

1. Pandas DataFrames
2. NumPy arrays
3. Integers (including timestamps - though timezone information in timestamps is removed)
4. Floats
5. Bools
6. Strings (written as part of a DataFrame/NumPy array)

Note that ArcticDB cannot efficiently store custom Python objects, even if inserted into a Pandas DataFrames/NumPy array. 
Pickled data cannot be index or column-sliced, and neither `update` nor `append` primitives will function on pickled data. 

### *How does indexing work in ArcticDB?*

ArcticDB stores a single-level index over the underlying data. For more information how this is structured, see [On-Disk Storage](./technical/on_disk_storage.md).

If given a Pandas DataFrame, the index data is taken from the DataFrame with ArcticDB supporting the following index types:

* `DatetimeIndex`
* `pandas.Index` containing `int64` or `float64` (or the corresponding dedicated types `Int64Index`, `UInt64Index` and `Float64Index`)
* `RangeIndex` with the restrictions noted below
* `MultiIndex` composed of above supported types

Currently, ArcticDB only supports `append()`-ing to a `RangeIndex` with a continuing `RangeIndex` (i.e. the appending `RangeIndex.start` == `RangeIndex.stop` of the existing data and they have the same `RangeIndex.step`). If a non-contiuing one is pass to `append()`, ArcticDB does _not_ convert it `Int64Index` like Pandas and will produce an error.

Also note, the "row" concept in `read(row_range)/head()/tail()` refers to the physical row, not the value in the `pandas.Index`.

### *Can I `append` with additional columns / What is Dynamic Schema?*

You can `append` (or `update`) with differing column sets to symbols for which the containing library has `Dynamic Schema` enabled. See the documentation for the `create_library` method for more information.

You can also change the type of numerical columns - for example, integers will be promoted to floats on read.

### *How does ArcticDB segment data?*

See [On Disk Storage Format](../technical/on_disk_storage) for more details. 

### *How does ArcticDB handle streaming data?*

ArcticDB has full support for streaming data workflows - including high throughput tick data pipelines. Please contact us at `arctic AT man.com` for more information.

### *How does ArcticDB handle concurrent writers?*

Without a centralising server, ArcticDB does not support transactions. Instead, ArcticDB supports concurrent writers across symbols - **but not to a single symbol** (unless "staging the writes"). It is up to the writer to ensure that clients do not concurrently modify a single symbol.

In the case of concurrent writers to a single symbol, the behaviour will be *last-writer-wins*. Data is not lost per se, but only the version of the *last-writer* will be accessible through the version chain.

To reiterate, ArcticDB supports concurrent writers to multiple symbols, even within a single library.

!!! note

    ArcticDB does support staging multiple single-symbol concurrent writes. See the documentation for `staged`. 

### *Does ArcticDB cache any data locally?*

Yes. ArcticDB library instances maintain a short-lived cache containing what it believes is the latest version for every encountered symbol. 
This cache is invalidated after 5 seconds by default - this invalidation period can be modified via the following snippet:

```
from arcticcxx import set_config_int
set_config_int("VersionMap.ReloadInterval", <new cache invalidation period>)  # set to 0 to disable caching entirely
```

As a result of this caching, it is theoretically possible for two independent library instances to disagree as to what the latest version of a symbol is 
for a short period of time.

This caching is designed to reduce load on storage - if this is not a concern it can be safely disabled as described above.

Other than this, there is no client-side caching in ArcticDB.

### *How can I enable detailed logging?*

You can set the following environment variables to enable logging:

```
ARCTICDB_timings_loglevel=debug
ARCTICDB_version_loglevel=debug
ARCTICDB_storage_loglevel=debug
```

The following will enable S3 logging, which will output all S3 logs to a file in the present working directory:

```
ARCTICDB_AWS_LogLevel_int=6
```
