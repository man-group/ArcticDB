# Build Tooling

## Introduction

This page aims to provide instructions on working with the scripts in the *build_tooling* folder.
The scripts are designed to run as part of the CI process
This page looks into how they are used and how to run them locally, when needed (e.g. for testing).

## Optional CI caches

`vcpkg_caching.sh` is sourced by CI builds. `VCPKG_BINARY_CACHE_MODE` accepts `off`, `read`, or `readwrite` (the default
for existing callers). `off` disables remote binary caches; `read` configures downloads without an upload API key;
`readwrite` retains publication for trusted credentials. Missing credentials, inaccessible feeds, or failed NuGet setup
leave those providers disabled so vcpkg builds dependencies from source. An optional Man feed is always read-only.
The normal build and ASV workflows select `off` when a repository NuGet token is absent, including approved fork PRs.

The S3 asset cache is configured only when AWS credentials are present. `sccache_config.sh` selects S3 when both AWS
credentials are present and the GitHub Actions backend otherwise. It leaves the unused backend's settings unset;
empty S3 variables still select S3 in the pinned sccache version. The GHA cache namespace defaults to `1` and respects
the `SCCACHE_GHA_VERSION` repository variable. Linux wheel builds pass the v2 service flag, results URL, and runtime
token into the manylinux container.

`resolve_cibw_image.sh` inspects public image manifests without authentication and emits the selected tag and
`build_locally` flag. Its `publish_image` input is false for fork PRs. On a cache miss, each Linux wheel runner builds
`build_many_linux_image.sh` locally; trusted callers can publish the image to GHCR.

Regression coverage for these scripts and workflow routing lives in `.github/scripts/tests/test_contributor_ci.py`.
Run it with `python -m pytest .github/scripts/tests/` after installing `pytest`, `pyyaml`, `markdown`, and `pymdown-extensions`.

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

## Analyzing pytest results
Trusted CI runs record pytest results into an ArcticDB library on AWS S3. Fork PRs retain pytest logs and JUnit results
as GitHub Actions artifacts and skip S3 publication.

### On the CI
A cron workflow is running weekly that shows both a week and a 6-month table with failing tests.

### Locally
Run `aggregate_flaky_tests.py` script to visuallize most failing tests. `aggregate_flaky_tests.py -h` for more info and options
