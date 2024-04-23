
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

There are no configuration parameters, and the memory is owned solely be the Arctic instance.

For example:

```python
import arcticdb as adb
ac = adb.Arctic('mem://')
```

For concurrent access to a local backend, we recommend LMDB connected to tmpfs.

