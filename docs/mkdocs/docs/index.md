<img src="images/ArcticDB Logo Purple Stacked.png" alt="logo" width="400"/>


## What is ArcticDB?

ArcticDB is a serverless DataFrame database engine designed for the Python Data Science ecosystem. 

ArcticDB enables you to store, retrieve and process DataFrames at scale, backed by commodity object storage (S3-compatible storages and Azure Blob Storage).

ArcticDB requires *zero additional infrastructure* beyond a running Python environment and access to object storage and can be **installed in seconds.**

ArcticDB is:

- **Fast**
    - Process up to 100 million rows per second for a single consumer
    - Process a billion rows per second across all consumers
    - Quick and easy to install: `pip install arcticdb`
- **Flexible**
    - Data schemas are not required
    - Supports streaming data ingestion
    - Bitemporal - stores all previous versions of stored data
    - Easy to setup both locally and on cloud
    - Scales from dev/research to production environments
- **Familiar**
    - ArcticDB is the world's simplest shareable database
    - Easy to learn for anyone with Python and Pandas experience
    - Just you and your data - the cognitive overhead is very low.

## Getting Started

This section will cover installation, setup and basic usage. More details on basics and advanced features can be found in the [tutorials](tutorials/fundamentals.md) section.

### Installation

ArcticDB supports Python 3.8 - 3.13.

To install, simply run:


```python
pip install arcticdb
```

### Setup

ArcticDB is a storage engine designed for object storage and also supports local-disk storage using LMDB.

!!! Storage Compatibility

    ArcticDB supports any S3 API compatible storage, including AWS and Azure, and storage appliances like [VAST Universal Storage](https://vastdata.com/) and [Pure Storage](https://purestorage.com/).

    ArcticDB also supports LMDB for local/file based storage - to use LMDB, pass an LMDB path as the URI: `adb.Arctic('lmdb://path/to/desired/database')`.

To get started, we can import ArcticDB and instantiate it:

```python
import arcticdb as adb
# this will set up the storage using the local file system
uri = "lmdb://tmp/arcticdb_intro"
ac = adb.Arctic(uri)
```

For more information on how to correctly format the `uri` string for other storages, please view the docstring ([`help(Arctic)`](api/arctic.md#arcticdb.Arctic)) or read the [storage access](#storage-access) section (click the link or keep reading below this section).

### Library Setup

ArcticDB is geared towards storing many (potentially millions) of tables. Individual tables (DataFrames) are called _symbols_ and 
are stored in collections called _libraries_. A single _library_ can store many symbols.

_Libraries_ must first be initialized prior to use:

```python
ac.create_library('intro')  # static schema - see note below
ac.list_libraries()
```
output
```python
['intro']
```

The library must then be instantiated in the code ready to read/write data:

```python
library = ac['intro']
```

Sometimes it is more convenient to combine library creation and instantiation using this form, which will automatically create the library if needed, to save you checking if it exists already:

```python
library = ac.get_library('intro', create_if_missing=True)
```

!!! info "ArcticDB Static & Dynamic Schemas"

    ArcticDB does not need data schemas, unlike many other databases. You can write any DataFrame and read it back later. If the shape of the data is changed and then written again, it will all just work. Nice and simple.

    The one exception where schemas are needed is in the case of functions that modify existing symbols: `update` and `append`. When modifying a symbol, the new data must have the same schema as the existing data. The schema here means the index type and the name, order, and type of each column in the DataFrame. In other words when you are appending new rows they must look like the existing rows. This is the default option and is called `static schema`.

    However, if you need to add, remove or change the type of columns via `update` or `append`, then you can do that. You simply need to create the library with the `dynamic_schema` option set. See the `library_options` parameter of the ([`create_library`](api/arctic.md#arcticdb.Arctic.create_library)) method.

    So you have the best of both worlds - you can choose to either enforce a static schema on your data so it cannot be changed by modifying operations, or allow it to be flexible.
    
    The choice to use static or dynamic schemas must be set at library creation time.

    In this section we are using `static schema`, just to be clear.

### Reading And Writing Data

Now we have a library set up, we can get to reading and writing data. ArcticDB has a set of simple functions for DataFrame storage.

Let's write a DataFrame to storage.

First create the data:

```python
# 50 columns, 25 rows, random data, datetime indexed.
import pandas as pd
import numpy as np
from datetime import datetime
cols = ['COL_%d' % i for i in range(50)]
df = pd.DataFrame(np.random.randint(0, 50, size=(25, 50)), columns=cols)
df.index = pd.date_range(datetime(2000, 1, 1, 5), periods=25, freq="h")
df.head(5)
```
_output (the first 5 rows of the data)_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     18     48     10     16     38     34     25     44  ...
2000-01-01 06:00:00     48     10     24     45     22     36     30     19  ...
2000-01-01 07:00:00     25     16     36     29     25      9     48      2  ...
2000-01-01 08:00:00     38     21      2      1     37      6     31     31  ...
2000-01-01 09:00:00     45     17     39     47     47     11     33     31  ...
```

Then write to the library:

```python
library.write('test_frame', df)
```
_output (information about what was written)_
```
VersionedItem(symbol=test_frame,library=intro,data=n/a,version=0,metadata=None,host=<host>)
```

The `'test_frame'` DataFrame will be used for the remainder of this guide.

!!! info "ArcticDB index"

    When writing Pandas DataFrames, ArcticDB supports the following index types:
    
    * `pandas.Index` containing `int64` (or the corresponding dedicated types `Int64Index`, `UInt64Index`)
    * `RangeIndex`
    * `DatetimeIndex`
    * `MultiIndex` composed of above supported types
    
    The "row" concept in `head()/tail()` refers to the row number ('iloc'), not the value in the `pandas.Index` ('loc').

Read the data back from storage:

```Python
from_storage_df = library.read('test_frame').data
from_storage_df.head(5)
```
_output (the first 5 rows but read from the database)_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     18     48     10     16     38     34     25     44  ...
2000-01-01 06:00:00     48     10     24     45     22     36     30     19  ...
2000-01-01 07:00:00     25     16     36     29     25      9     48      2  ...
2000-01-01 08:00:00     38     21      2      1     37      6     31     31  ...
2000-01-01 09:00:00     45     17     39     47     47     11     33     31  ...
```

The data read matches the original data, of course.


### Slicing and Filtering

ArcticDB enables you to slice by row and by column. 

!!! info "ArcticDB indexing"

    ArcticDB will construct a full index for _ordered numerical and timeseries (e.g. DatetimeIndex) Pandas indexes_. This will enable
    optimised slicing across index entries. If the index is unsorted or not numeric your data can still be stored but row-slicing will
    be slower.

#### Row-slicing

```Python
library.read('test_frame', date_range=(df.index[5], df.index[8])).data
```
_output (the rows in the data range requested)_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 10:00:00     23     39      0     45     15     28     10     17  ...
2000-01-01 11:00:00     36     28     22     43     23      6     10      1  ...
2000-01-01 12:00:00     18     42      1     15     19     36     41     36  ...
2000-01-01 13:00:00     28     32     47     37     17     44     29     24  ...
```

#### Column slicing

```Python
_range = (df.index[5], df.index[8])
_columns = ['COL_30', 'COL_31']
library.read('test_frame', date_range=_range, columns=_columns).data
```
_output (the rows in the date range and columns requested)_
```
                     COL_30  COL_31
2000-01-01 10:00:00      31       2
2000-01-01 11:00:00       3      34
2000-01-01 12:00:00      24      43
2000-01-01 13:00:00      18       8
```

#### Filtering and Analytics

ArcticDB supports many common DataFrame analytics operations, including filtering, projections, group-bys, aggregations, and resampling. The most intuitive way to access these operations is via the [`LazyDataFrame`](api/processing.md#arcticdb.LazyDataFrame) API, which should feel familiar to experienced users of Pandas or Polars.

The legacy [`QueryBuilder`](api/processing.md#arcticdb.QueryBuilder) class can also be created directly and passed into `read` calls with the same effect.

!!! info "ArcticDB Analytics Philosphy"

    In most cases this is more memory efficient and performant than the equivalent Pandas operation as the processing is within the C++ storage engine and parallelized over multiple threads of execution. 

```python
import arcticdb as adb
_range = (df.index[5], df.index[8])
_cols = ['COL_30', 'COL_31']
# Using lazy evaluation
lazy_df = library.read('test_frame', date_range=_range, columns=_cols, lazy=True)
lazy_df = lazy_df[(lazy_df["COL_30"] > 10) & (lazy_df["COL_31"] < 40)]
df = lazy_df.collect().data
# Using the legacy QueryBuilder class gives the same result
q = adb.QueryBuilder()
q = q[(q["COL_30"] > 10) & (q["COL_31"] < 40)]
library.read('test_frame', date_range=_range, columns=_cols, query_builder=q).data
```
_output (the data filtered by date range, columns and the query which filters based on the data values)_
```
                     COL_30  COL_31
2000-01-01 10:00:00      31       2
2000-01-01 13:00:00      18       8
```

###  Modifications, Versioning (aka Time Travel)

ArcticDB fully supports modifying stored data via two primitives: _update_ and _append_.

These operations are atomic but do not lock the symbol. Please see the section on [transactions](#transactions) for more on this.

#### Append

Let's append data to the end of the timeseries.

To start, we will take a look at the last few records of the data (before it gets modified)

```python
library.tail('test_frame', 4).data
```
_output_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-02 02:00:00     46     12     38     47      4     31      1     42  ...
2000-01-02 03:00:00     46     20      5     42      8     35     12      2  ...
2000-01-02 04:00:00     17     48     36     43      6     46      5      8  ...
2000-01-02 05:00:00     20     19     24     44     29     32      2     19  ...
```
Then create 3 new rows to append. For append to work the new data must have its first `datetime` starting after the existing data.

```python
random_data = np.random.randint(0, 50, size=(3, 50))
df_append = pd.DataFrame(random_data, columns=['COL_%d' % i for i in range(50)])
df_append.index = pd.date_range(datetime(2000, 1, 2, 7), periods=3, freq="h")
df_append
```
_output_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-02 07:00:00      9     15      4     48     48     35     34     49  ...
2000-01-02 08:00:00     35      4     12     30     30     12     38     25  ...
2000-01-02 09:00:00     25     17      3      1      1     15     33     49  ...
```

Now _append_ that DataFrame to what was written previously

```Python
library.append('test_frame', df_append)
```
_output_
```
VersionedItem(symbol=test_frame,library=intro,data=n/a,version=1,metadata=None,host=<host>)
```
Then look at the final 5 rows to see what happened

```python
library.tail('test_frame', 5).data
```
_output_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-02 04:00:00     17     48     36     43      6     46      5      8  ...
2000-01-02 05:00:00     20     19     24     44     29     32      2     19  ...
2000-01-02 07:00:00      9     15      4     48     48     35     34     49  ...
2000-01-02 08:00:00     35      4     12     30     30     12     38     25  ...
2000-01-02 09:00:00     25     17      3      1      1     15     33     49  ...
```

The final 5 rows consist of the last two rows written previously followed by the 3 new rows that we have just appended.

Append is very useful for adding new data to the end of a large timeseries.

#### Update

The update primitive enables you to overwrite a contiguous chunk of data. This results in modifying some rows and deleting others as we will see in the example below.

Here we create a new DataFrame for the update, with only 2 rows that are 2 hours apart

```python
random_data = np.random.randint(0, 50, size=(2, 50))
df = pd.DataFrame(random_data, columns=['COL_%d' % i for i in range(50)])
df.index = pd.date_range(datetime(2000, 1, 1, 5), periods=2, freq="2h")
df
```
_output (rows 0 and 2 only as selected by the `iloc[]`)_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     47     49     15      6     22     48     45     22  ...
2000-01-01 07:00:00     46     10      2     49     24     49      8      0  ...
```
now update the symbol
```python
library.update('test_frame', df)
```
_output (information about the update)_
```
VersionedItem(symbol=test_frame,library=intro,data=n/a,version=2,metadata=None,host=<host>)
```

Now let's look at the first 4 rows in the symbol:

```python
library.head('test_frame', 4).data  # head/tail are similar to the equivalent Pandas operations
```
_output_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     47     49     15      6     22     48     45     22  ...
2000-01-01 07:00:00     46     10      2     49     24     49      8      0  ... 
2000-01-01 08:00:00     38     21      2      1     37      6     31     31  ... 
2000-01-01 09:00:00     45     17     39     47     47     11     33     31  ...
```

Let's unpack how we end up with that result. The update has

* replaced the data in the symbol with the new data where the index matched (in this case the 05:00 and 07:00 rows)
* removed any rows within the date range of the new data that are not in the index of the new data (in this case the 06:00 row)
* kept the rest of the data the same (in this case 09:00 onwards)

Logically, this corresponds to replacing the complete date range of the old data with the new data, which is what you would expect from an update.


#### Versioning

You might have noticed that `read` calls do not return the data directly - but instead returns a `VersionedItem` structure. You may also have noticed that modification operations (`write`, `append` and `update`) increment the version number. ArcticDB versions all modifications, which means you can retrieve earlier versions of data - it is a bitemporal database:

```Python
library.tail('test_frame', 7, as_of=0).data
```
_output_
```
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 23:00:00     16     46      3     45     43     14     10     27  ...
2000-01-02 00:00:00     37     37     20      3     49     38     23     46  ...
2000-01-02 01:00:00     42     47     40     27     49     41     11     26  ...
2000-01-02 02:00:00     46     12     38     47      4     31      1     42  ...
2000-01-02 03:00:00     46     20      5     42      8     35     12      2  ...
2000-01-02 04:00:00     17     48     36     43      6     46      5      8  ...
2000-01-02 05:00:00     20     19     24     44     29     32      2     19  ...
```

Note the timestamps - we've read the data prior to the `append` operation. Please note that you can also pass a `datetime` into any `as_of` argument, which will result in reading the last version earlier than the `datetime` passed.

!!! note "Versioning, Prune Previous & Snapshots"

    By default, `write`, `append`, and `update` operations will **not** remove the previous versions. Please be aware that this will consume more space.
    
    This behaviour can be can be controlled via the `prune_previous_versions` keyword argument. Space will be saved but the previous versions will then not be available.

    A compromise can be achieved by using snapshots, which allow states of the library to be saved and read back later. This allows certain versions to be protected from deletion, they will be deleted when the snapshot is deleted. See [snapshot documentation](api/library.md#arcticdb.version_store.library.Library.snapshot) for details.



### Storage Access

#### S3 configuration

There are two methods to configure S3 access. If you happen to know the access and secret key, simply connect as follows:

```python
import arcticdb as adb
ac = adb.Arctic('s3://ENDPOINT:BUCKET?region=blah&access=ABCD&secret=DCBA')
```

Otherwise, you can delegate authentication to the AWS SDK (obeys standard [AWS configuration options](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)):

```python
ac = adb.Arctic('s3://ENDPOINT:BUCKET?aws_auth=true')
```

Same as above, but using HTTPS:

```python
ac = adb.Arctic('s3s://ENDPOINT:BUCKET?aws_auth=true')
```

!!! s3 vs s3s

    Use `s3s` if your S3 endpoint used HTTPS

##### Connecting to a defined storage endpoint

Connect to local storage (not AWS - HTTP endpoint of s3.local) with a pre-defined access and storage key:

```python
ac = adb.Arctic('s3://s3.local:arcticdb-test-bucket?access=EFGH&secret=HGFE')
```

##### Connecting to AWS

Connecting to AWS with a pre-defined region:

```python
ac = adb.Arctic('s3s://s3.eu-west-2.amazonaws.com:arcticdb-test-bucket?aws_auth=true')
```

Note that no explicit credential parameters are given. When `aws_auth` is passed, authentication is delegated to the AWS SDK which is responsible for locating the appropriate credentials in the `.config` file or
in environment variables. You can manually configure which profile is being used by setting the `AWS_PROFILE` environment variable as described in the
[AWS Documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

#### Using a specific path within a bucket

You may want to restrict access for the ArcticDB library to a specific path within the bucket. To do this, you can use the `path_prefix` parameter:

```python
ac = adb.Arctic('s3s://s3.eu-west-2.amazonaws.com:arcticdb-test-bucket?path_prefix=test&aws_auth=true')
```

#### Azure

ArcticDB uses the [Azure connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) to define the connection: 

```python
import arcticdb as adb
ac = adb.Arctic('azure://AccountName=ABCD;AccountKey=EFGH;BlobEndpoint=ENDPOINT;Container=CONTAINER')
```

For example: 

```python
import arcticdb as adb
ac = adb.Arctic("azure://CA_cert_path=/etc/ssl/certs/ca-certificates.crt;BlobEndpoint=https://arctic.blob.core.windows.net;Container=acblob;SharedAccessSignature=sp=awd&st=2001-01-01T00:00:00Z&se=2002-01-01T00:00:00Z&spr=https&rf=g&sig=awd%3D")
```

For more information, [see the Arctic class reference](api/arctic.md#arcticdb.Arctic).

#### LMDB

LMDB supports configuring its map size. See its [documentation](http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5).

You may need to tweak it on Windows, whereas on Linux the default is much larger and should suffice. This is because Windows allocates physical
space for the map file eagerly, whereas on Linux the map size is an upper bound to the physical space that will be used.

You can set a map size in the connection string:

```python
import arcticdb as adb
ac = adb.Arctic('lmdb://path/to/desired/database?map_size=2GB')
```

The default on Windows is 2GiB. Errors with `lmdb errror code -30792` indicate that the map is getting full and that you should
increase its size. This will happen if you are doing large writes.

In each Python process, you should ensure that you only have one Arctic instance open over a given LMDB database.

LMDB does not work with remote filesystems.

#### In-memory configuration

An in-memory backend is provided mainly for testing and experimentation. It could be useful when creating files with LMDB is not desired.

There are no configuration parameters, and the memory is owned solely by the Arctic instance.

For example:

```python
import arcticdb as adb
ac = adb.Arctic('mem://')
```

For concurrent access to a local backend, we recommend LMDB connected to tmpfs, see [LMDB and In-Memory Tutorial](tutorials/lmdb_and_in_memory.md).

### Transactions

- Transactions can be be very useful but are often expensive and slow
- If we unpack ACID: Atomicity, Consistency and Durability are useful, Isolation less so
- Most analytical workflows can be constructed to run without needing transactions at all
- So why pay the cost of transactions when they are often not needed?
- ArcticDB doesn't have transactions because it is designed for high throughput analytical workloads


