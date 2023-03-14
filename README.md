<p align="center">
<img src="https://github.com/man-group/ArcticDB/raw/master/static/ArcticDBCropped.png" width="40%">
</p>

---

**ArcticDB** is a high performance, serverless Dataframe database built for the Python Data Science ecosystem. Launched in February 2023, it is the successor to [Arctic](https://github.com/man-group/arctic). ArcticDB offers an intuitive Python-centric API, with a fast C++ data-processing and compression engine that is compatible with object stores such as AWS S3, making it suitable for public cloud and on-premesis deployments. 

---

<p align="center">
<img src="https://github.com/man-group/ArcticDB/raw/master/static/ArcticDBTerminal.gif" width="100%">
</p>

---

ArcticDB allows you to:

 * Use standard data types and integrate effortlessly with the Python data science ecosystem - Pandas in, Pandas out
 * Efficiently index and query time-series data
 * Store tiled dataframes, for fast selection of rows and columns
 * Travel back in time to see previous versions of your data and create customizable snapshots of the database
 * Append and update data without being constrained by the existing schema
 * Handle sparse values and missing columns
 * Filter, aggregate and create new columns on-the-fly with a Pandas-like syntax
 * Accelerate analytics though concurrency in the C++ data-processing engine

ArcticDB handles data that is big in both row count and column count, so a 20-year history of more than 400,000 unique securities can be stored in a single *symbol*. Each *symbol* is maintained as a separate entity with no shared data which means ArcticDB can scale horizontally across *symbols*, maximising the peformance potential of your compute, storage and network.

ArcticDB is designed from the outset to be resilient; there is no single point of failure, and persistent data structures in the storage mean that once a version of a *symbol* has been written, it can never be corrupted by subsequent updates. Pulling compressed data directly from  storage to the client means that there is no server to overload, so your data is always available when you need it.

## Quickstart

Install ArcticDB:

```bash
$ pip install arcticdb
```

Import ArcticDB:

```Python
>>> from arcticdb import Arctic
```

Create an instance on your S3 storage (with or without explicit credentials):

```Python
>>> ac = Arctic('s3://MY_ENDPOINT:MY_BUCKET')  # Leave AWS to derive credential information
>>> ac = Arctic('s3://MY_ENDPOINT:MY_BUCKET?region=YOUR_REGION&access=ABCD&secret=DCBA') # Manually specify creds
```

Or create an instance on your local disk:

```Python
>>> ac = Arctic("lmdb:///<path>)  
```

Create your first library and list the libraries in the instance:

```Python
>>> ac.create_library('travel_data')
>>> ac.list_libraries()
```

Create a test dataframe:
```Python
>>> NUM_COLUMNS=10
>>> NUM_ROWS=100_000
>>> df = pd.DataFrame(np.random.randint(0,100,size=(NUM_ROWS, NUM_COLUMNS)), columns=[f"COL_{i}" for i in range(NUM_COLUMNS)], index=pd.date_range('2000', periods=NUM_ROWS, freq='h'))
```

Get the library, write some data to it, and read it back:

```Python
>>> lib = ac['travel_data']
>>> lib.write("my_data", df)
>>> data = lib.read("my_data")
```

To find out more about working with data, visit our [docs](https://github.com/man-group/ArcticDB/blob/docs/README.md)

---

## Build From Source

Instructions for building from source coming soon. 

## Documentation

The documentation for ArcticDB is contained elsewhere and is in the process of being ported to this repository.

## License

ArcticDB is released under a [Business Source License 1.1 (BSL)](https://github.com/man-group/ArcticDB/blob/c11ed9a257661e18a80e39482d35a0c260514d94/LICENSE.txt)

BSL features are free to use and the source code is available, but users may not use ArcticDB for production use or for a Database Service, without agreement with Man Group Operations Limited. 

Use of ArcticDB in production or for a Database Service requires a paid for license from Man Group Operations Limited and is licensed under the ArcticDB Software License Agreement. For more information please contact [arcticdb@man.com](mailto:ArcticDB@man.com). 

The BSL is not certified as an open-source license, but most of the [Open Source Initiative (OSI)](https://opensource.org/) criteria are met.

For each BSL release all associated alpha, beta, major, and minor (point) releases will become Apache Lisensed, version 2.0 on the same day two years after the major release date. For the license conversion dates, see the table below.

| ArcticDB Version | License | Converts to Apache 2.0 |
| ------------- | ------------- | ------------- |
| 1.0 | Business Source License 1.1 | Mar 15, 2025 |

## Code of Conduct

[Code of Conduct](https://github.com/man-group/ArcticDB/blob/9a869152833ccb27a6605d99a89dff7899f1466a/CODE_OF_CONDUCT.md)

This project has adopted a Code of Conduct. If you have any concerns about the Code, or behaviour that you have experienced in the project, please contact us at [arcticdb@man.com](mailto:ArcticDB@man.com). 

## Contributing

We welcome your contributions to help us improve and extend this project!

Below you will find some basic steps required to be able to contribute to the project. If you have any questions about this process or any other aspect of contributing to our project, feel free to send an email to [arcticdb@man.com](mailto:ArcticDB@man.com) and we'll get your questions answered as quickly as we can.

We are also always looking for feedback from our dedicated community! If you have used ArcticDB please let us know, we would love to hear about your experience!

### Contribution Licensing

Since this project is distributed under the terms of the [BSL license](https://github.com/man-group/ArcticDB/blob/c11ed9a257661e18a80e39482d35a0c260514d94/LICENSE.txt), contributions that you make are licensed under the same terms. For us to be able to accept your contributions, we will need explicit confirmation from you that you are able and willing to provide them under these terms, and the mechanism we use to do this is the [ArcticDB Individual Contributor License Agreement](https://github.com/man-group/ArcticDB/blob/fe6bc9d74a9922775d0bfea5938f69a3f3c5c953/Individual%20Contributor%20License%20Agreement.md). 

**Individuals** - To participate under these terms, please include the following line as the last line of the commit message for each commit in your contribution. You must use your real name (no pseudonyms, and no anonymous contributions). 

Signed-Off By: Random J. Developer <random@developer.example.org>. By including this sign-off line I agree to the terms of the Contributor License Agreement.

**Corporations** - For corporations who wish to make contributions to ArcticDB, please contact arcticdb@man.com and we will arrange for the CLA to be sent to the signing authority within your corporation.

## Community

Do you have any questions or issues? Chat to us and other users through our dedicated Slack Workspace - sign up for Slack access on [our website](https://arcticdb.io).

Alternatively email us at arcticdb@man.com or come chat to us on [Twitter](https://www.twitter.com/arcticdb)!
