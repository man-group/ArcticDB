# Build Tooling

## Introduction

This page aims to provide instructions on working with the scripts in the *build_tooling* folder.
The scripts are designed to run as part of the CI process
This page looks into how they are used and how to run them locally, when needed (e.g. for testing).

## Python scripts related to Persistent Storage testing

There are some Python utility functions to the persistent storages.
They can be found in python\arcticdb\util\storage_test.py
This is also a callable script, that can be used to perform the operations described below.

The file contains implementations for the following steps:
1. Seed - seeding some libraries in the real storages
    - in order to access this, you need to set the **ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX** environment variable 
    - these libraries are used to test that the versions that we are building can read data from previous versions
    - the libraries are used in the following test python\tests\integration\arcticdb\test_persistent_storage.py::test_real_s3_storage_read
    - the libraries' names are predetermined from this pattern:
https://github.com/man-group/ArcticDB/blob/3dfbce874e9c914327592cfc0d38b7e18f335f1d/.github/workflows/persistent_storage.yml#L75
2. Verify - verifying libraries that were written by the pytests
    - **important** - these libraries are not the same as the ones from step 1.
    - normally these libraries are written by the following test python\tests\integration\arcticdb\test_persistent_storage.py::test_real_s3_storage_write
    - to test the script locally you need to set the **ARCTICDB_PERSISTENT_STORAGE_BRANCH_NAME** environment variable
    - and you need to have data written in a real storage (e.g. S3) in libraries that follow the format above
    - the libraries' names are predetermined from this pattern:
https://github.com/man-group/ArcticDB/blob/3dfbce874e9c914327592cfc0d38b7e18f335f1d/.github/workflows/build_steps.yml#L288
3. Cleanup - cleans up all of the libraries that were created for testing
    - test the script locally you need to set the **ARCTICDB_PERSISTENT_STORAGE_BRANCH_NAME** environment variable
    - the script will then attempt to clean all of the **seed** and **test** libraries that were created for the given *branch_name*

### Running the persistent storage python tests locally

For more information on running the persistent storage tests locally, please refer to the **Running the persistent storage Python** tests section in the [ArcticDB Development Setup](https://manwiki.maninvestments.com/display/AlphaTech/ArcticDB+Development+Setup).
