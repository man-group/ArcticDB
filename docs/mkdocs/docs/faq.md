# Frequently Asked Questions

!!! note

    This FAQ document covers multiple topic areas - please see the contents table on the 
    right for more information.

## Product

### *What is ArcticDB?*

ArcticDB is a high performance DataFrame database built for the modern Python
Data Science ecosystem. ArcticDB is an embedded database engine - which means
that installing ArcticDB is as simple as installing a Python package.
This also means that ArcticDB does not require any server infrastructure to function.

ArcticDB is optimised for numerical datasets spanning millions of rows and columns,
enabling you to store and retrieve massive datasets within a Pythonic,
DataFrame-like API that researchers, data scientists and software engineers will
find immediately familiar.

### *How does ArcticDB differ from the version of Arctic on GitHub?*

Please see the [history page](history.md).

### *How does ArcticDB differ from Apache Parquet?*

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

### *What sort of data is ArcticDB best suited to?*

ArcticDB is an OLA(nalytical)P DBMS, rather than an OLT(ransactional)P DBMS.

In practice, this means that ArcticDB is optimised for large numerical datasets
and for queries that operate over many rows at a time.

### *Does ArcticDB require a server?*

No. ArcticDB is a fully fledged embedded analytical database system,
designed for modern cloud and on-premises object storage that does not require
a server for any of the core features.

### *What languages can I use ArcticDB with?*

Bindings are currently only available for Python. 

### *What is the best practice for saving Data to ArcticDB?*

Users should consider how they store data based on their specific use cases. See the guide [here](tutorials/data_organisation.md).

### *What are the Limitations of ArcticDB being client-side?*

The serverless nature of ArcticDB provides excellent performance, making it ideal for data science applications where speed and efficiency are key.  It ensures atomicity, consistency, and durability but does not isolate transactions. Changes to symbols are performed on a last-writer-wins principle, and without isolation, write-after-read transactions are not supported, which is not suitable for use cases requiring strong transactional guarantees. 

### *What storage options does ArcticDB support?*

ArcticDB offers compatibility with a wide array of storage choices, both on-premises and in the cloud. It is verified to work with multiple storage systems such as AWS S3, Azure Blob Storage, LMDB, In-memory, Ceph, MinIO (Linux), Pure Flashblade S3, Scality S3, and VAST Data S3, with plans to support additional options soon. 

### *What are the trade offs with ArcticDB Versioning?* 

ArcticDB versions data by default, allowing for point-in-time analysis and efficient data updates, including daily appends and historical corrections, making it ideal for research datasets. The database is capable of de-duplicating data that has not changed between versions, using storage space efficiently. The ArcticDB enterprise tools including data pruning and compaction to help manage storage and data-fragmentation as new versions are created. Storing large numbers of versions of data does require more storage.

More information can be found [here](tutorials/data_organisation.md)!

### *What granularity of authorization does ArcticDB support?*  

Authentication is at storage account level, and authorization can be done at ArcticDB Library level for most S3 backends (with directory/path permissions), otherwise also at storage account level.  There are many third-party authentication and authorization integrations available for the backends.

### *How is ArcticDB data catalogued and discoverable by consumers?* 

ArcticDB offers capabilities to list libraries and symbols, complete with metadata. You can use these functions to discover and browse data stored in ArcticDB.

### *How can I get started using ArcticDB?*

Please see our [getting started guide](index.md)!

## Technical

### *Does ArcticDB support SQL?*

Yes! ArcticDB supports SQL queries via its DuckDB integration. Use `lib.sql()` to query data with
familiar SQL syntax, including SELECT, WHERE, JOIN, GROUP BY, and more. Data is streamed to DuckDB
segment-by-segment, so even very large datasets can be queried without loading them fully into memory.
SQL queries are read-only — use the Python API (`write`, `append`, `update`) for data modifications.
See the [SQL Queries tutorial](tutorials/sql_queries.md) for details.

### *Does ArcticDB de-duplicate data?*

Yes.

On each `write`, ArcticDB will check the previous version of the symbol that you are writing (and _only_ this version - other symbols will not be scanned!) and skip the write of identical [segments](technical/on_disk_storage.md). Please keep in mind however that this is most effective when version `n` is equal to version `n-1` plus additional data at the end - and only at the end! If there is additional data inserted into the in the middle, then all segments occuring after that modification will almost certainly differ. ArcticDB segments data at fixed intervals and data is only de-duplicated if the hashes of the data segments are identical - as a result, a one row offset will prevent effective de-duplication.

Note that this is a library configuration option that is off by default, see [`help(LibraryOptions)`](api/arctic.md#arcticdb.LibraryOptions) for details of how to enable it.

### *How does ArcticDB enable advanced analytics?*

ArcticDB is primarily focused on filtering and transfering data from storage through to memory - at which point Pandas, NumPy, or other standard analytical packages can be utilised for analytics.

That said, ArcticDB does offer a limited set of analytical functions that are executed inside the C++ storage engine offering significant performance benefits over Pandas. For more information, see the [documentation](api/processing.md) for the `LazyDataFrame`, `LazyDataFrameCollection`, and `QueryBuilder` classes.

### *What does Pickling mean?*

ArcticDB has two means for storing data:

1. ArcticDB can store your data using the [Arctic On-Disk Storage Format](technical/on_disk_storage.md).
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

See the [Getting Started](index.md#reading-and-writing-data) page for details of supported index types.

### *Can I `append` with additional columns / What is Dynamic Schema?*

You can `append` (or `update`) with differing column sets to symbols for which the containing library has `Dynamic Schema` enabled. See the documentation for the `create_library` method for more information.

You can also change the type of numerical columns - for example, integers will be promoted to floats on read.

### *How does ArcticDB segment data?*

See [On Disk Storage Format](technical/on_disk_storage.md) and the [documentation](api/arctic.md#arcticdb.LibraryOptions) for the `rows_per_segment` and `columns_per_segment` library configuration options for more details. 

### *How does ArcticDB handle streaming data?*

ArcticDB support for streaming data is on our roadmap for the coming months

### *How does ArcticDB handle concurrent writers?*

Without a centralised server, ArcticDB does not support transactions. Instead, ArcticDB supports concurrent writers across symbols - **but not to a single symbol** (unless "staging the writes"). It is up to the writer to ensure that clients do not concurrently modify a single symbol.

In the case of concurrent writers to a single symbol, the behaviour will be *last-writer-wins*. Data is not lost per se, but only the version of the *last-writer* will be accessible through the version chain.

To reiterate, ArcticDB supports concurrent writers to multiple symbols, even within a single library.

!!! note

    ArcticDB does support staging multiple single-symbol concurrent writes. See the documentation for `staged`. 

### *Does ArcticDB cache any data locally?*

Yes, please see the [Runtime Configuration](runtime_config.md#versionmapreloadinterval) page for details.

### *How can I enable detailed logging?*

Please see the [Runtime Configuration](runtime_config.md#logging-configuration) page for details.

### *How can I tune the performance of ArcticDB?*

Please see the [Runtime Configuration](runtime_config.md#versionstorenumcputhreads-and-versionstorenumiothreads) page for details.

### Does ArcticDB support categorical data?

ArcticDB currently offers extremely limited support for categorical data. Series and DataFrames with categorical columns can be provided to the `write` and `write_batch` methods, and will then behave as expected on `read`.
However, `append` and `update` are not yet supported with categorical data, and will raise an exception if attempted.
Analytics such as filtering using the `LazyDataFrame` or `QueryBuilder` classes is also not supported with categorical data, and will either raise an exception, or give incorrect results, depending on the exact operations requested.

### How does ArcticDB handle `NaN`?

The handling of `NaN` in ArcticDB depends on the type of the column under consideration:

* For string columns, `NaN`, as well as Python `None`, are fully supported.
* For floating-point numeric columns, `NaN` is also fully supported.
* For integer numeric columns `NaN` is not supported. A column that otherwise contains only integers will be treated as a floating point column if a `NaN` is encountered by ArcticDB, at which point [the usual rules](api/arctic.md#arcticdb.LibraryOptions) around type promotion for libraries configured with or without dynamic schema all apply as usual.
