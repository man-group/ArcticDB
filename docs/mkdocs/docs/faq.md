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

Users are encouraged to store data in its original vendor format initially, then optimize storage based on their specific use cases, which may include maintaining multiple versions of the data to support different analysis needs. In ArcticDB, best practices for data storage involve considering the balance between read and update performance, data layout’s impact on index performance, and auditability against original data sources. It’s recommended to store data in a format that facilitates efficient querying. 

A full guide can be found [here](tutorials/data_organisation.md)!

### *What are the Limitations of ArcticDB being client-side?*

The serverless nature of ArcticDB provides excellent performance, making it ideal for data science applications where speed and efficiency are key. Although it may not be ideal for scenarios that demand strong transactionality and concurrent updates, ArcticDB excels in complex, multi-tenant environments where its attributes align closely with those of popular NoSQL databases such as MongoDB and Cassandra, offering flexibility and scalability. 

### *What storage options does ArcticDB support?*

ArcticDB offers compatibility with a wide array of storage choices, both on-premises and in the cloud, to guarantee adaptability and efficiency across different settings. It is verified to work with multiple storage systems such as AWS S3, Azure Blob, lmdb, In-memory, Ceph, MinIO, Pure Storage S3, Scality S3, and VAST Data S3, with plans to support additional options soon. 


### *What are the trade offs with ArcticDB Versioning* 

ArcticDB offers robust versioning capabilities, allowing for point-in-time analysis and efficient data updates, including daily appends and historical corrections, making it ideal for research datasets. The database efficiently stores only the differences between versions, conserving storage space. Frequent, small updates can lead to fragmentation and slower reads, but ArcticDB's enterprise features can mitigate this through background compaction and data pruning.  

More information can be found [here](tutorials/data_organisation.md)!

### *Does ArcticDB support Authorization*  

Although ArcticDB, being fully client-side, does not inherently implement authorization for untrusted clients, it is compatible with many first and third-party authentication solutions. Fine-grained authorization is not natively supported. 

### *How is ArcticDB data catalogued and discoverable by consumers?* 

ArcticDB offers capabilities to list libraries and symbols, complete with metadata. For those seeking enhanced functionality, we suggest leveraging a third-party catalogue.  

### *How can I get started using ArcticDB?*

Please see our [getting started guide](index.md)!

## Technical

### *Does ArcticDB use SQL?*

No. ArcticDB enables data access and modifications with a Python API that speaks in terms of Pandas DataFrames. See the reference documentation for more details.

### *Does ArcticDB de-duplicate data?*

Yes.

On each `write`, ArcticDB will check the previous version of the symbol that you are writing (and _only_ this version - other symbols will not be scanned!) and skip the write of identical [segments](technical/on_disk_storage.md). Please keep in mind however that this is most effective when version `n` is equal to version `n-1` plus additional data at the end - and only at the end! If there is additional data inserted into the in the middle, then all segments occuring after that modification will almost certainly differ. ArcticDB segments data at fixed intervals and data is only de-duplicated if the hashes of the data segments are identical - as a result, a one row offset will prevent effective de-duplication.

Note that this is a library configuration option that is off by default, see [`help(LibraryOptions)`](api/arctic.md#arcticdb.LibraryOptions) for details of how to enable it.

### *How does ArcticDB enable advanced analytics?*

ArcticDB is primarily focused on filtering and transfering data from storage through to memory - at which point Pandas, NumPy, or other standard analytical packages can be utilised for analytics.

That said, ArcticDB does offer a limited set of analytical functions that are executed inside the C++ storage engine offering significant performance benefits over Pandas. For more information, see the documentation for the *QueryBuilder* class.

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

See the [Getting Started](index.md#reading-and-writing-dataframes) page for details of supported index types.

### *Can I `append` with additional columns / What is Dynamic Schema?*

You can `append` (or `update`) with differing column sets to symbols for which the containing library has `Dynamic Schema` enabled. See the documentation for the `create_library` method for more information.

You can also change the type of numerical columns - for example, integers will be promoted to floats on read.

### *How does ArcticDB segment data?*

See [On Disk Storage Format](technical/on_disk_storage.md) and the [documentation](api/arctic.md#LibraryOptions) for the `rows_per_segment` and `columns_per_segment` library configuration options for more details. 

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

ArcticDB currently offers extremely limited support for categorical data. Series and DataFrames with categorical columns can be provided to the `write` and `write_batch` methods, and will then behave as expected on `read`. However, `append` and `update` are not yet supported with categorical data, and will raise an exception if attempted. The `QueryBuilder` is also not supported with categorical data, and will either raise an exception, or give incorrect results, depending on the exact operations requested.

### How does ArcticDB handle `NaN`?

The handling of `NaN` in ArcticDB depends on the type of the column under consideration:

* For string columns, `NaN`, as well as Python `None`, are fully supported.
* For floating-point numeric columns, `NaN` is also fully supported.
* For integer numeric columns `NaN` is not supported. A column that otherwise contains only integers will be treated as a floating point column if a `NaN` is encountered by ArcticDB, at which point [the usual rules](api/arctic.md#LibraryOptions) around type promotion for libraries configured with or without dynamic schema all apply as usual.
