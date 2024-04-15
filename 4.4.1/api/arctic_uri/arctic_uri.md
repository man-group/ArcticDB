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
Please refer to https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string for more details.

`options` is a string that specifies connection specific options as `<name>=<value>` pairs joined with `;` (the final key value pair should not include a trailing `;`).

Additional options specific for ArcticDB:

| Option        | Description   |
|---------------|---------------|
| Container     | Azure container for blobs |
| Path_prefix   | Path within Azure container to use for data storage |
| CA_cert_path  | (Non-Windows platform only) Azure CA certificate path. If not set, default path will be used. Note: For Linux distribution, default path is set to `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`. If the certificate cannot be found in the provided path, an Azure exception with no meaningful error code will be thrown. For more details, please see [here](https://github.com/Azure/azure-sdk-for-cpp/issues/4738). For example, `Failed to iterate azure blobs 'C' 0:`.<br><br>Default certificate path in various Linux distributions:<br>`/etc/ssl/certs/ca-certificates.crt` for Debian/Ubuntu/Gentoo etc.<br>`/etc/pki/tls/certs/ca-bundle.crt` for Fedora/RHEL 6<br>`/etc/ssl/ca-bundle.pem` for OpenSUSE<br>`/etc/pki/tls/cacert.pem` for OpenELEC<br>`/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem` for CentOS/RHEL 7<br>`/etc/ssl/cert.pem` for Alpine Linux |

For Windows user, `CA_cert_path` cannot be set. Please set CA certificate related option on Windows setting.
For details, you may refer to https://learn.microsoft.com/en-us/skype-sdk/sdn/articles/installing-the-trusted-root-certificate

Exception: Azure exceptions message always ends with `{AZURE_SDK_HTTP_STATUS_CODE}:{AZURE_SDK_REASON_PHRASE}`.

Please refer to https://github.com/Azure/azure-sdk-for-cpp/blob/24ed290815d8f9dbcd758a60fdc5b6b9205f74e0/sdk/core/azure-core/inc/azure/core/http/http_status_code.hpp for
more details of provided status codes.

Note that due to a bug in Azure C++ SDK (https://github.com/Azure/azure-sdk-for-cpp/issues/4738), Azure may not give meaningful status codes and
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
