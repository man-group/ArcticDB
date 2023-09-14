Arctic URI
==========

The URI provided to an `Arctic` instance is used to specify the storage backend and its configuration.

## S3

The S3 URI connection scheme has the form `s3(s)://<s3 end point>:<s3 bucket>[?options]`.

Use s3s as the protocol if communicating with a secure endpoint.

Options is a query string that specifies connection specific options as `<name>=<value>` pairs joined with
`&`.

Available options for S3:

| Option                | Description                                                                                                                                                     |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| port                  | port to use for S3 connection                                                                                                                                   |
| region                | S3 region                                                                                                                                                       |
| use_virtual_addressing| Whether to use virtual addressing to access the S3 bucket                                                                                                       |
| access                | S3 access key                                                                                                                                                   |
| secret                | S3 secret access key                                                                                                                                            |
| path_prefix           | Path within S3 bucket to use for data storage                                                                                                                   |
| aws_auth              | If true, authentication to endpoint will be computed via AWS environment vars/config files. If no options are provided `aws_auth` will be assumed to be true.   |

Note: When connecting to AWS, `region` can be automatically deduced from the endpoint if the given endpoint
specifies the region and `region` is not set.

## Azure

The Azure URI connection scheme has the form `azure://[options]`.
It is based on the Azure Connection String, with additional options for configuring ArcticDB.
Please refer to [Azure](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) for more details.

`options` is a string that specifies connection specific options as `<name>=<value>` pairs joined with `;` (the final key value pair should not include a trailing `;`).

Additional options specific for ArcticDB:

| Option        | Description   |
|---------------|---------------|
| Container     | Azure container for blobs |
| Path_prefix   | Path within Azure container to use for data storage |
| CA_cert_path  | (Linux platform only) Azure CA certificate path. If not set, python ``ssl.get_default_verify_paths().cafile`` will be used. If the certificate cannot be found in the provided path, an Azure exception with no meaningful error code will be thrown. For more details, please see [here](https://github.com/Azure/azure-sdk-for-cpp/issues/4738). For example, `Failed to iterate azure blobs 'C' 0:`.|
| CA_cert_dir   | (Linux platform only) Azure CA certificate directory. If not set, python ``ssl.get_default_verify_paths().capath`` will be used. Certificates can only be used if corresponding hash files [exist](https://www.openssl.org/docs/man1.0.2/man3/SSL_CTX_load_verify_locations.html). If the certificate cannot be found in the provided path, an Azure exception with no meaningful error code will be thrown. For more details, please see [here](https://github.com/Azure/azure-sdk-for-cpp/issues/4738). For example, `Failed to iterate azure blobs 'C' 0:`.|

For non-Linux platforms, neither `CA_cert_path` nor `CA_cert_dir` may be set. Please set CA certificate related options using operating system settings.
For Windows, please see [here](https://learn.microsoft.com/en-us/skype-sdk/sdn/articles/installing-the-trusted-root-certificate)

Exception: Azure exceptions message always ends with `{AZURE_SDK_HTTP_STATUS_CODE}:{AZURE_SDK_REASON_PHRASE}`.

Please refer to [azure-sdk-for-cpp](https://github.com/Azure/azure-sdk-for-cpp/blob/24ed290815d8f9dbcd758a60fdc5b6b9205f74e0/sdk/core/azure-core/inc/azure/core/http/http_status_code.hpp) for
more details of provided status codes.

Note that due to a [bug](https://github.com/Azure/azure-sdk-for-cpp/issues/4738) in Azure C++ SDK, Azure may not give meaningful status codes and
reason phrases in the exception. To debug these instances, please set the environment variable `export AZURE_LOG_LEVEL` to `1` to turn on the SDK debug logging.


## LMDB

The LMDB connection scheme has the form `lmdb:///<path to store LMDB files>[?options]`.

Options is a query string that specifies connection specific options as `<name>=<value>` pairs joined with
`&`.

| Option   | Description   |
|----------|---------------|
| map_size | LMDB map size (see [here](http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5)). String. Supported formats are:<br/><br>"150MB" / "20GB" / "3TB"<br/><br>The only supported units are MB / GB / TB.<br><br/>On Windows and MacOS, LMDB will materialize a file of this size, so you need to set it to a reasonable value that your system has room for, and it has a small default (order of 1GB). On Linux, this is an upper bound on the space used by LMDB and the default is large (order of 100GB). |

Example connection strings are `lmdb:///home/user/my_lmdb` or `lmdb:///home/user/my_lmdb?map_size=2GB`.

## In-Memory

The in-memory connection scheme has the form `mem://`.

The storage is local to the `Arctic` instance.
