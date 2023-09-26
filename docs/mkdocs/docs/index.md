![](images/FullWithBorder.png)

## What is ArcticDB?

ArcticDB is an embedded/serverless database engine designed to integrate with Pandas and the Python Data Science ecosystem. ArcticDB enables 
you to store, retrieve and process DataFrames at scale, backed by commodity object storage (S3-compatible storages and Azure Blob Storage).

ArcticDB requires *zero additional infrastructure* beyond a running Python environment and access to object storage and can be **installed in seconds.**

ArcticDB is:

- **Fast**: ArcticDB is incredibly fast, able to process millions of (on-disk) rows a second, and is very easy to install: `pip install arcticdb`!
- **Flexible**: Supporting data with and without a schema, ArcticDB is also fully compatible with streaming data ingestion. The platform is bitemporal, allowing you to see all previous versions of stored data.
- **Familiar**: ArcticDB is the world's simplest database, designed to be immediately familiar to anyone with prior Python and Pandas experience.

#### What is ArcticDB _not_?

ArcticDB is designed for high throughput analytical workloads. It is _not_ a transactional database and as such is not a replacement for tools such as PostgreSQL.

## Getting Started

The below guide covers installation, setup and basic usage. More detailed information on advanced functionality such as _snapshots_ and _parallel writers_ can be found in the tutorials section.

### Installation

ArcticDB supports Python 3.6 - 3.11. To install, simply run:

```
pip install arcticdb
```

### Setup

ArcticDB is a storage engine designed for object storage, but also supports local RAM storage and local-disk storage using LMDB.

!!! Storage Compatibility

    ArcticDB supports any S3 API compatible storage. It has been tested against AWS S3 and storage appliances like [VAST Universal Storage](https://vastdata.com/).

    ArcticDB also supports LMDB for local/file based storage - to use LMDB, pass an LMDB path as the URI: `Arctic('lmdb://path/to/desired/database')`.

To get started, we can import ArcticDB and instantiate it:

```python
>>> from arcticdb import Arctic
>>> ac = Arctic(<URI>)
```

For more information on the format of _<URI\>_, please view the docstring ([`>>> help(Arctic)`](https://docs.arcticdb.io/api/arcticdb#arcticdb.Arctic)). Below we'll run through some setup examples.

#### S3 configuration

There are two methods to configure S3 access. If you happen to know the access and secret key, simply connect as follows:

```python
>>> from arcticdb import Arctic
>>> ac = Arctic('s3://ENDPOINT:BUCKET?region=blah&access=ABCD&secret=DCBA')
```

Otherwise, you can delegate authentication to the AWS SDK (obeys standard [AWS configuration options](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)):

```python
>>> ac = Arctic('s3://ENDPOINT:BUCKET?aws_auth=true')
```

Same as above, but using HTTPS:

```python
>>> ac = Arctic('s3s://ENDPOINT:BUCKET?aws_auth=true')
```

!!! s3 vs s3s

    Use `s3s` if your S3 endpoint used HTTPS

##### Connecting to a defined storage endpoint

Connect to local storage (not AWS - HTTP endpoint of s3.local) with a pre-defined access and storage key:

```python
>>> ac = Arctic('s3://s3.local:arcticdb-test-bucket?access=EFGH&secret=HGFE')
```

##### Connecting to AWS

Connecting to AWS with a pre-defined region:

```python
>>> ac = Arctic('s3s://s3.eu-west-2.amazonaws.com:arcticdb-test-bucket?aws_auth=true')
```

Note that no explicit credential parameters are given. When `aws_auth` is passed, authentication is delegated to the AWS SDK which is responsible for locating the appropriate credentials in the `.config` file or
in environment variables. You can manually configure which profile is being used by setting the `AWS_PROFILE` environment variable as described in the
[AWS Documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

#### Using a specific path within a bucket

You may want to restrict access for the ArcticDB library to a specific path within the bucket. To do this, you can use the `path_prefix` parameter:

```python
>>> ac = Arctic('s3s://s3.eu-west-2.amazonaws.com:arcticdb-test-bucket?path_prefix=test&aws_auth=true')
```

#### Azure configuration

ArcticDB uses the [Azure connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) to define the connection: 

```python
>>> from arcticdb import Arctic
>>> ac = Arctic('azure://AccountName=ABCD;AccountKey=EFGH;BlobEndpoint=ENDPOINT;Container=CONTAINER')
```

For example: 

```python
>>> from arcticdb import Arctic
>>> ac = Arctic("azure://CA_cert_path=/etc/ssl/certs/ca-certificates.crt;BlobEndpoint=https://arctic.blob.core.windows.net;Container=acblob;SharedAccessSignature=sp=awd&st=2001-01-01T00:00:00Z&se=2002-01-01T00:00:00Z&spr=https&rf=g&sig=awd%3D")
```

For more information, [see the Arctic class reference](https://docs.arcticdb.io/api/arcticdb#arcticdb.Arctic.__init__).

#### LMDB configuration

LMDB supports configuring its map size. See its [documentation](http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5).

You may need to tweak it on Windows, whereas on Linux the default is much larger and should suffice. This is because Windows allocates physical
space for the map file eagerly, whereas on Linux the map size is an upper bound to the physical space that will be used.

You can set a map size in the connection string:

```python
>>> from arcticdb import Arctic
>>> ac = Arctic('lmdb://path/to/desired/database?map_size=2GB')
```

The default on Windows is only 128MB. Errors with `lmdb errror code -30792` indicate that the map is getting full and that you should
increase its size. This will happen if you are doing large writes. You should ensure that you only have one Arctic instance open over
a given LMDB database.

For more information on the different endpoints, [see the Arctic class reference](https://docs.arcticdb.io/api/arcticdb#arcticdb.Arctic.__init__).

#### In-memory configuration

An in-memory backend is provided mainly for testing and experimentation. It could be useful when creating files with LMDB is not desired.

There are no configuration parameters, and the memory is owned solely be the Arctic instance.

For example:

```python
>>> from arcticdb import Arctic
>>> ac = Arctic('mem://')
```

TODO: For concurrent access to a local backend, LMDB connected to tmpfs is recommended.

### Usage

#### Library Setup

ArcticDB is geared towards storing many (potentially millions) of tables. Individual tables are called _symbols_ and 
are stored in collections called _libraries_. A single _library_ can store an effectively unlimited number of symbols.

_Libraries_ must first be initialized prior to use:

```python
>>> ac.create_library('data')  # fixed schema - see note below
>>> ac.list_libraries()
['data']
```

A library can then be retrieved:

```python
>>> library = ac['data']
```

!!! info "ArcticDB Schemas & the Dynamic Schema library option"

    ArcticDB enforces a strict schema that is defined on first write. This schema defines the name, order, index type and type of each column in the DataFrame. 

    If you wish to add, remove or change the type of columns via `update` or `append` options, please see the documentation for the `dynamic_schema` 
    option within the `library_options` parameter of the `create_library` method. Note that whether to use fixed or dynamic schemas must be set at 
    library creation time.

##### Reading And Writing Data(Frames)!

Now we have a library set up, we can get to reading and writing data! ArcticDB exposes a set of simple API primitives to enable DataFrame storage. 

Let's first look at writing a DataFrame to storage:

```Python
# 50 columns, 25 rows, random data, datetime indexed. 
>>> from datetime import datetime
>>> cols = ['COL_%d' % i for i in range(50)]
>>> df = pd.DataFrame(np.random.randint(0, 50, size=(25, 50)), columns=cols)
>>> df.index = pd.date_range(datetime(2000, 1, 1, 5), periods=25, freq="H")
>>> df.head(2)
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     35     46      4      0     17     35     33     25  ...
2000-01-01 06:00:00      9     24     18     30      0     39     43     20  ...
```

Write the DataFrame:

```Python
>>> library.write('test_frame', df)
VersionedItem(symbol=test_frame,library=data,data=n/a,version=0,metadata=None,host=<host>)
```

The `'test_frame'` DataFrame will be used for the remainder of this guide.

!!! info "ArcticDB index"

    When writing Pandas DataFrames, ArcticDB supports the following index types:
    
    * `pandas.Index` containing `int64` or `float64` (or the corresponding dedicated types `Int64Index`, `UInt64Index` and `Float64Index`)
    * `RangeIndex` with the restrictions noted below
    * `DatetimeIndex`
    * `MultiIndex` composed of above supported types
    
    Currently, ArcticDB only supports `append()`-ing to a `RangeIndex` with a continuing `RangeIndex` (i.e. the appending `RangeIndex.start` == `RangeIndex.stop` of the existing data and they have the same `RangeIndex.step`). If a DataFrame with a non-continuing `RangeIndex` is passed to `append()`, ArcticDB does _not_ convert it `Int64Index` like Pandas and will produce an error.
    
    Also note, the "row" concept in `head()/tail()` refers to the physical row, not the value in the `pandas.Index`.

Read it back:

```Python
>>> from_storage_df = library.read('test_frame').data
>>> from_storage_df.head(2)
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     35     46      4      0     17     35     33     25  ...
2000-01-01 06:00:00      9     24     18     30      0     39     43     20  ...
```

##### Slicing and Filtering

ArcticDB enables you to slice by _row_ and by _column_. 

!!! info "ArcticDB indexing"

    ArcticDB will construct a full index for _ordered numerical and timeseries (e.g. DatetimeIndex) Pandas indexes_. This will enable
    optimised slicing across index entries. If the index is unsorted or not numeric your data can still be stored but row-slicing will
    be slower.

###### Row-slicing

```Python
>>> library.read('test_frame', date_range=(df.index[5], df.index[8])).data
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 10:00:00     43     28     36     18     10     37     31     32  ...
2000-01-01 11:00:00     36      5     30     18     44     15     31     28  ...
2000-01-01 12:00:00      6     34      0      5     19     41     17     15  ...
2000-01-01 13:00:00     14     48      6      6      2      3     44     42  ...
```

##### Column slicing

```Python
>>> _range = (df.index[5], df.index[8])
>>> _columns = ['COL_30', 'COL_31']
>>> library.read('test_frame', date_range=_range, columns=_columns).data
                     COL_30  COL_31
2000-01-01 10:00:00       7      26
2000-01-01 11:00:00      29      18
2000-01-01 12:00:00      36      26
2000-01-01 13:00:00      48      42
```

###### Filtering

ArcticDB uses a Pandas-_like_ syntax to describe how to filter data. For more details including the limitations, please view the docstring ([`help(QueryBuilder)`](https://docs.arcticdb.io/api/query_builder)).

!!! info "ArcticDB Filtering Philosphy & Restrictions"

    Note that in most cases this should be more memory efficient and performant than the equivalent Pandas operation as the processing is within the C++ storage engine and parallelized over multiple threads of execution. 

    We do not intend to re-implement the entirety of the Pandas filtering/masking operations, but instead target a maximally useful subset. 

```Python
>>> _range = (df.index[5], df.index[8])
>>> _cols = ['COL_30', 'COL_31']
>>> from arcticdb import QueryBuilder
>>> q = QueryBuilder()
>>> q = q[(q["COL_30"] > 30) & (q["COL_31"] < 50)]
>>> library.read('test_frame', date_range=_range, columns=_cols, query_builder=q).data
>>>
                     COL_30  COL_31
2000-01-01 12:00:00      36      26
2000-01-01 13:00:00      48      42
```

####  Modifications, Versioning (time travel!)

ArcticDB fully supports modifying stored data via two primitives: _update_ and _append_.

##### Update

The update primitive enables you to overwrite a contiguous chunk of data. In the below example, we use `update` to modify _2000-01-01 05:00:00_, 
remove _2000-01-01 06:00:00_ and modify _2000-01-01 07:00:00_.

```Python
# Recreate the DataFrame with new (and different!) random data, and filter to only the first and third row
>>> random_data = np.random.randint(0, 50, size=(25, 50))
>>> df = pd.DataFrame(random_data, columns=['COL_%d' % i for i in range(50)])
>>> df.index = pd.date_range(datetime(2000, 1, 1, 5), periods=25, freq="H")
# Filter!
>>> df = df.iloc[[0,2]]
>>> df
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     46     24      4     20      7     32      1     18  ...
2000-01-01 07:00:00     44     37     16     27     30      1     35     25  ...
>>> library.update('test_frame', df)
VersionedItem(symbol=test_frame,library=data,data=n/a,version=1,metadata=None,host=<host>)
```

Now let's look at the first 2 rows in the symbol:

```Python
>>> library.head('test_frame', 2)  # head/tail are similar to the equivalent Pandas operations
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 05:00:00     46     24      4     20      7     32      1     18  ...
2000-01-01 07:00:00     44     37     16     27     30      1     35     25  ...
```

##### Append

Let's append data to the end of the timeseries:

```Python
>>> random_data = np.random.randint(0, 50, size=(5, 50))
>>> df_append = pd.DataFrame(random_data, columns=['COL_%d' % i for i in range(50)])
>>> df_append.index = pd.date_range(datetime(2000, 1, 2, 7), periods=5, freq="H")
>>> df_append
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-02 07:00:00     38     44      4     37      3     26     12     10  ...
2000-01-02 08:00:00     44     32      4     12     15     13     17     16  ...
2000-01-02 09:00:00     44     43     28     38     20     34     46     37  ...
2000-01-02 10:00:00     46     22     34     33     18     35      5      3  ...
2000-01-02 11:00:00     30     47     14     41     43     40     22     45  ...
```

** Note the starting date of this DataFrame is after the final row written previously! **

Let's now _append_ that DataFrame to what was written previously, and then pull back the final 7 rows from storage:

```Python
>>> library.append('test_frame', df_append)
VersionedItem(symbol=test_frame,library=data,data=n/a,version=2,metadata=None,host=<host>)
>>> library.tail('test_frame', 7).data
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-02 04:00:00      4     13      8     14     25     11     11     11  ...
2000-01-02 05:00:00     14     41     24      7     16     10     15     36  ...
2000-01-02 07:00:00     38     44      4     37      3     26     12     10  ...
2000-01-02 08:00:00     44     32      4     12     15     13     17     16  ...
2000-01-02 09:00:00     44     43     28     38     20     34     46     37  ...
2000-01-02 10:00:00     46     22     34     33     18     35      5      3  ...
2000-01-02 11:00:00     30     47     14     41     43     40     22     45  ...
```

The final 7 rows consist of the last two rows written previously followed by the 5 rows that we have just appended.

##### Versioning

You might have noticed that _read_ calls do not return the data directly - but instead returns a _VersionedItem_ structure. You may also have noticed that modification operations 
(_write_, _append_ and _update_) increment the version counter. ArcticDB versions all modifications, which means you can retrieve earlier versions of data (ArcticDB is a bitemporal database!):

```Python
>>> library.tail('test_frame', 7, as_of=0).data
                     COL_0  COL_1  COL_2  COL_3  COL_4  COL_5  COL_6  COL_7  ...
2000-01-01 23:00:00     26     38     12     30     25     29     47     27  ...
2000-01-02 00:00:00     12     14     42     11     44     32     19     11  ...
2000-01-02 01:00:00     12     47      4     45     28     38     35     36  ...
2000-01-02 02:00:00     22      0     12     48     37     11     18     14  ...
2000-01-02 03:00:00     14     16     38     30     19     41     29     43  ...
2000-01-02 04:00:00      4     13      8     14     25     11     11     11  ...
2000-01-02 05:00:00     14     41     24      7     16     10     15     36  ...
```

Note the timestamps - we've read the data prior to the _append_ operation. Please note that you can also pass a _datetime_ into any _as\_of_ argument. 

!!! note "Versioning & Prune Previous"

    By default, `write`, `append`, and `update` operations will **not** remove the previous versions. Please be aware that this will consume more space.
    
    This behaviour can be can be controlled via the `prune_previous_versions` keyword argument. 
