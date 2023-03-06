<p align="center">
<img src="https://github.com/man-group/ArcticDB/raw/master/static/ArcticDBCropped.png" width="40%">
</p>

---

<p align="center">
<img src="https://github.com/man-group/ArcticDB/raw/master/static/flowDiagram.png" width="100%">
</p>

---

**ArcticDB** is a cloud-native, timeseries-optimised columnar database designed from the ground up for DataFrames.ArcticDB is the successor to [Arctic 1.0](https://github.com/man-group/arctic). This next generation version was launched in February 2023 and offers the same intuitive Python-centric API whilst utilizing a custom C++ storage engine and modern S3 compatible object storage to provide a timeseries database that is:

* **Fast**: Capable of processing billions of rows in seconds
* **Flexible**: Designed to handle complex real-world datasets
* **Familiar**: Built for the modern Python Data Science ecosystem - Pandas In/Pandas Out!

---

## This Repository is Under Development!

As part of our Open Sourcing initiative, we are actively migrating our code and build tooling to this repository. As a result, this repository (and this readme) is under active development.

### Outstanding Tasks

#### Automated build tooling

The code in this repository is currently being built in-house at Man Group. 
The build process is run within a Docker container, backed by a Man-Specific Docker image that contains a number of third party dependencies that are 
required for the ArcticDB code to build. 

These dependencies are:

* zlib v1.2.13
* jemalloc v5.1.0
* LZ4 v1.8.3
* ZSTD v1.4.5
* OpenSSL v1.1.1
* Libcurl v7.62.0
* ProtoBuf-cpp v3.6.1
* xxHash v0.6.5
* libpcre v8.45
* Cyrus Sasl v2.1.28
* libsodium v1.0.17
* libevent v2.1.12
* googletest v1.12.1
* double-conversion v3.1.4
* glog v0.3.5
* fmtlib v8.1.1
* folly v2022.10.31.00
* mongo-c-driver v1.12.0
* mongo-cxx-driver v3.3.1
* aws-sdk-cpp v1.9.212
* BitMagic v7.2.0
* fizz v2022.10.31.00
* wangle v2022.10.31.00
* librdkafka v.1.5.2

We are in the process of adding automated tooling to this repository that will handle dependency installation thus enabling building the code in this repository outside of Man Group.

#### Documentation

The documentation for ArcticDB is contained elsewhere and is in the process of being ported to this repository.

#### License & Contributor Agreement

This repository is currently missing the full license and CA text.

#### Finalise the README

This README is currently unfinished! 
