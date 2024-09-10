# MongoDB C++ Driver 
[![codecov](https://codecov.io/gh/mongodb/mongo-cxx-driver/branch/master/graph/badge.svg)](https://codecov.io/gh/mongodb/mongo-cxx-driver)
[![Documentation](https://img.shields.io/badge/docs-doxygen-blue.svg)](https://mongocxx.org/api/mongocxx-v3/)
[![Documentation](https://img.shields.io/badge/docs-mongocxx-green.svg)](https://mongocxx.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mongodb/mongo-cxx-driver/blob/master/LICENSE)

Welcome to the MongoDB C++ Driver!

## Branches - `releases/stable` versus `master`

The default checkout branch of this repository is `releases/stable`. 
This will always contain the latest stable release of the driver. The
 `master` branch is used for active development. `master` should 
**only** be used when making contributions back to the driver, as it 
is not stable for use in production.

See [Driver Status by family and version](#driver-status-by-family-and-version)
for more details about the various versions of the driver.

## Resources

* [MongoDB C++ Driver Installation](https://mongocxx.org/mongocxx-v3/installation/)
* [MongoDB C++ Driver Quickstart](https://mongocxx.org/mongocxx-v3/tutorial/)
* [MongoDB CXX Driver Examples](https://github.com/mongodb/mongo-cxx-driver/tree/master/examples)
* [MongoDB C++ Driver Manual](https://mongocxx.org)
* [MongoDB C++ Driver Documentation](https://www.mongodb.com/docs/drivers/cxx/)
* [MongoDB C++ Driver API Documentation](https://mongocxx.org/api/current/)
* [MongoDB C++ Driver Contribution guidelines](https://mongocxx.org/contributing/)
* [MongoDB Database Manual](https://www.mongodb.com/docs/manual/)
* [MongoDB Developer Center](https://www.mongodb.com/developer/languages/cpp/)
* [StackOverflow](https://stackoverflow.com/questions/tagged/mongodb%20c%2b%2b)

## Driver status by family and version

Stability indicates whether this driver is recommended for production use.
Currently, no drivers guarantee API or ABI stability.

| Family/version       | Stability   | Development         | Purpose                             |
| -------------------- | ----------- | ------------------- | ----------------------------------- |
| (repo master branch) | Unstable    | Active development  | New feature development             |
| mongocxx 3.10.x      | Stable      | Bug fixes only      | Current stable C++ driver release   |
| mongocxx 3.9.x       | Stable      | None                | Previous stable C++ driver release   |
| mongocxx 3.8.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.7.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.6.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.5.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.4.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.3.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.2.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.1.x       | Stable      | None                | Previous stable C++ driver release  |
| mongocxx 3.0.x       | Stable      | None                | Previous stable C++ driver release  |

## MongoDB compatibility

Compatibility of each C++ driver version with each MongoDB server is documented in the [MongoDB manual](https://www.mongodb.com/docs/drivers/cxx#mongodb-compatibility).

## Bugs and issues

See our [JIRA project](https://jira.mongodb.com/browse/CXX).

## License

The source files in this repository are made available under the terms of
the Apache License, version 2.0.
