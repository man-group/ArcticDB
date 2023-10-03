# Contribution Licensing

Since this project is distributed under the terms of the [BSL license](https://github.com/man-group/ArcticDB/blob/master/LICENSE.txt),
contributions that you make are licensed under the same terms.
For us to be able to accept your contributions, we will need explicit confirmation from you that you are able and
willing to provide them under these terms, and the mechanism we use to do this is the [ArcticDB Individual Contributor License Agreement](https://github.com/man-group/ArcticDB/blob/master/Individual%20Contributor%20License%20Agreement.md).

**Individuals** - To participate under these terms, please include the following line as the last line of the commit
message for each commit in your contribution.
You must use your real name (no pseudonyms, and no anonymous contributions).

```
Signed-Off By: Random J. Developer <random@developer.example.org>. By including this sign-off line I agree to the terms of the Contributor License Agreement.
```

**Corporations** - For corporations who wish to make contributions to ArcticDB, please contact arcticdb@man.com and
we will arrange for the CLA to be sent to the signing authority within your corporation.

Docker Quickstart
=================

This quickstart builds a release using build dependencies from vcpkg.
ArcticDB releases on PyPi use vcpkg dependencies in the manner as described below.

Note the below instructions will build a Linux X86_64 release.

#### 1) Start the ArcticDB build docker image

Run in a Linux terminal:

```bash
docker pull ghcr.io/man-group/cibuildwheel_manylinux:2.12.1-3a897
docker run -it ghcr.io/man-group/cibuildwheel_manylinux:2.12.1-3a897
```

> :warning: The below instructions do not have to be run in the provided docker image. They can be run against any Python installation on any Linux distribution as long as the basic build [dependencies are available](#setting-up-linux).
>
> If running outside the provided docker image, please change `/opt/python/cp39-cp39/bin/python3` in the examples below
> to an appropriate path for Python.

#### 2) Check out ArcticDB including submodules

```bash
cd
git clone https://github.com/man-group/ArcticDB.git
cd ArcticDB
git submodule init && git submodule update
```

#### 3) Kick off the build

```bash
MY_PYTHON=/opt/python/cp39-cp39/bin/python3  # change if outside docker container/building against a different python
$MY_PYTHON -m pip install -U pip setuptools wheel grpcio-tools
ARCTIC_CMAKE_PRESET=skip $MY_PYTHON setup.py develop
# Change the below Python_EXECUTABLE value to build against a different Python version
cmake -DPython_EXECUTABLE=$MY_PYTHON -DTEST=off --preset linux-debug cpp
pushd cpp
cmake --build --preset linux-debug
popd
```

#### 4) Run ArcticDB

Ensure the below is run in the Git project root:

```bash
# PYTHONPATH first part = Python module, second part compiled C++ binary
PYTHONPATH=`pwd`/python:`pwd`/cpp/out/linux-debug-build/arcticdb/ $MY_PYTHON
```

Now, inside the Python shell:

```python
from arcticdb import Arctic
```

Rather than setting the `PYTHONPATH` environment variable, you could install the appropriate paths into your Python environment by running (note that this will invoke the build tooling so will compile any changed files since the last compilation):

```bash
$MY_PYTHON -m pip install -ve .
```

Note that as this will copy the binary to your Python installation this will have to be run after each and every change of a C++ file.

Building using mamba and conda-forge
====================================

This section uses build dependencies from [conda-forge](https://conda-forge.org/).
It is a pre-requisite for releasing ArcticDB on conda-forge.

**⚠️ At the time of writing, installing ArcticDB with this setup under Windows is not possible
due to a linkage problems with libprotobuf.
See: https://github.com/man-group/ArcticDB/pull/449**

 - [Install `mamba`](https://mamba.readthedocs.io/en/latest/installation.html)
 - Create the `arcticdb` environment from its specification (`environment_unix.yml`):

  ```bash
  mamba env create -f environment_unix.yml
  ```

 - Activate the `arcticdb` environment (you will need to do this for every new shell session):

  ```bash
  mamba activate arcticdb
  ```

#### Building CMake targets

[Several CMake presets](https://github.com/man-group/ArcticDB/blob/master/cpp/CMakePresets.json) are defined for build types, OS's, and build system.

For instance:

 - for debug build on Linux with mamba and conda-forge, use:
 
  ```bash
  export ARCTICDB_USING_CONDA=1
  cmake -DTEST=off --preset linux-conda-debug cpp
  cd cpp

  # You might need to use fewer threads than what's possible your machine
  # not to have it swap and freeze (e.g. we use 4 of them here).
  cmake --build --preset linux-conda-debug -j 4
  ```

 - for release build on MacOS with mamba and conda-forge, use:

  ```bash
  export ARCTICDB_USING_CONDA=1
  cmake -DTEST=off --preset darwin-conda-release cpp
  cd cpp

  # You might need to use fewer threads than what's possible your machine
  # not to have it swap and freeze (e.g. we use 4 of them here).
  cmake --build --preset linux-conda-debug -j 4
  ```

#### Building and installing the Python Package

 - Build and install ArcticDB in the `arcticdb` environment using dependencies installed in this environment.
   We recommend using the [`editable` installation](https://setuptools.pypa.io/en/latest/userguide/development_mode.html) for development:

  ```bash
  export ARCTICDB_USING_CONDA=1
  # Adapt the CMake preset to your setup.
  export ARCTIC_CMAKE_PRESET=linux-conda-debug
  python -m pip install --no-build-isolation --no-deps --verbose --editable .
  ```

 - Use ArcticDB from Python:

  ```python
  from arcticdb import Arctic
  ```

FAQ
===

#### How do I build against different Python versions?

Run `cmake` (configure, not build) with either:

1. A different version of Python as the _first_ version of Python on your PATH or...
2. Point the `Python_EXECUTABLE` CMake variable to a different Python binary

Note that to build the ArcticDB C++ tests **you must have the Python static library available in your installation!**

#### How do I run the Python tests?

See [running tests below](#running-python-tests).

#### How do I run the C++ tests?

See [running tests below](#running-c-tests).

#### How do I specify how many cores to build using?

This is determined auto-magically by CMake at build time, but can be manually set by passing in `--parallel <num cores>` into the build command.

Detailed Build Information
==========================

Docker Image Construction
-------------------------

The above docker image is built from [ManyLinux](https://github.com/pypa/manylinux).
Build script is located [here](https://github.com/man-group/ArcticDB/blob/master/build_tooling/build_many_linux_image.sh).

GitHub output [here](https://github.com/man-group/ArcticDB/pkgs/container/cibuildwheel_manylinux).

**We recommend you use this image for compilation and testing!**

Setting up Linux
----------------
The codebase and build system can work with any reasonably recent Linux distribution with at least
GCC 8 (10+ recommended) and CMake 3.12 (these instructions assume 3.21+).

A development install of Python 3.6+ (with `libpython.a` or `.so` and full headers) is also necessary.
See [pybind11 configuration](#pybind11-configuration).

We require a Mongo executable for a couple of Python tests on Linux. You can check whether you have
it with `mongod --version`.

Search the internet for "mongo installation Linux" for instructions for your distro if you do not
already have `mongod` available.

### Dependencies by distro
| Distro | Versions reported to work | Packages |
|---|---|---|
| Ubuntu | 20.04, 22.04 | build-essential g++-10 libpcre3-dev libsasl2-dev libsodium-dev libkrb5-dev libcurl4-openssl-dev python3-dev |
| Centos | 7 | devtoolset-10-gcc-c++ openssl-devel cyrus-sasl-devel devtoolset-10-libatomic-devel libcurl-devel python3-devel |

Setting up Windows
------------------
We recommend using Visual Studio 2022 (or later) to install the compiler (MSVC v142 or newer) and tools
(Windows SDK, CMake, Python).

The Python that comes with Visual Studio is sufficient for creating release builds, but for debug builds, you will have
to separately download from [Python.org](https://www.python.org/downloads/windows/).

pre-commit hooks setup
----------------------

We use [pre-commit](https://pre-commit.com/) to run some checks on the codebase before committing.

To install the pre-commit hooks, run:

```bash
pip install pre-commit
pre-commit install
```

This will install the pre-commit hooks into your local git repository.

If you want to run the pre-commit hooks on all files, run:

```bash
pre-commit run --all-files
```

Running Python tests
--------------------

With `python` pointing to a Python interpeter with `ArcticDB` installed/on the `PYTHON_PATH`:

```bash
python -m pip install arcticdb[Testing]
python -m pytest python/tests
```

Running C++ tests
-----------------

Configure ArcticDB with TEST=on (default):

```bash
cmake -DPython_EXECUTABLE=<path to python> --preset linux-debug cpp
```

Note that `<path to python>` must point to a Python that is compatible with [Development.Embed](https://cmake.org/cmake/help/latest/module/FindPython.html). This will probably be the result of installing `python3-devel` from your dependency manager.

Inside the provided docker image, `python3-devel` resolves to Python 3.6 installed at `/usr/bin/python3`, so the resulting command will be:

```bash
cmake -DPython_EXECUTABLE=/usr/bin/python3 -DTEST=ON --preset linux-debug cpp
```

Then invoke the CMake build as normal and run the compiled test binary.

CIBuildWheel
------------

Our source repo works with [CIBuildWheel](https://github.com/pypa/cibuildwheel) which runs the compilation and tests
against all supported Python versions in isolated environments.
Please follow their documentation.

Configurations
==============

## CMake presets
To make it easier to set and share all the environment variables, config and commands, we recommend using the [CMake
presets](https://cmake.org/cmake/help/latest/manual/cmake-presets.7.html) feature.

Recent versions of some popular C++ IDEs support reading/importing these presets:
* [Visual Studio & Code](https://devblogs.microsoft.com/cppblog/cmake-presets-integration-in-visual-studio-and-visual-studio-code/)
* [CLion](https://www.jetbrains.com/help/clion/cmake-presets.html)

And it's equally easy to use on the command line ([see example](#compiling-step-by-step)).

We already ship a `CMakePresets.json` in the cpp directory, which is used by our builds.
You can add a `CMakeUserPresets.json` in the same directory for local overrides.
Inheritance is supported.

If you're working on Linux but not using our Docker image, you may want to create a preset with these `cacheVariables`:
* `CMAKE_MAKE_PROGRAM` - `make` or `ninja` should work
* `CMAKE_C_COMPILER` and `CMAKE_CXX_COMPILER` - If your preferred compiler is not `cc` and `cxx`

More examples:

<details>
<summary>Windows Preset to specify a Python version</summary>

```json
{
  "version": 3,
  "configurePresets": [
    {
      "name": "alt-vcpkg-debug:py3.10",
      "inherits": "windows-cl-debug",
      "cacheVariables": {
        "Python_ROOT_DIR": "C:\\Program Files\\Python310"
      },
      "environment": {
        "PATH": "C:\\Users\\me\\AppData\\Roaming\\Python\\Python310\\Scripts;C:\\Program Files\\Python310;$penv{PATH}",
        "PYTHONPATH": "C:\\Program Files\\Python310\\Lib;C:\\Users\\me\\AppData\\Roaming\\Python\\Python310\\site-packages"
      }
    }
  ],
  "buildPresets": [
    {
      "name": "alt-vcpkg-debug:py3.10",
      "configurePreset": "alt-vcpkg-debug:py3.10",
      "inheritConfigureEnvironment": true
    }
  ]
}
```
</details>

## vcpkg caching
We use [vcpkg](https://vcpkg.io/) to manage the C++ dependencies.

Compiling the dependencies uses a lot of disk space.
Once CMake configuration is done, you can remove the `cpp\vcpkg\buildtrees` folder.

You may also want to configure some caches:
* [Binary caching](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching)
* [Asset caching](https://learn.microsoft.com/en-us/vcpkg/users/assetcaching)

## pybind11 configuration
We augmented pybind11's [Python discovery](https://pybind11.readthedocs.io/en/stable/compiling.html#building-with-cmake)
with our own [PythonUtils](https://github.com/man-group/ArcticDB/blob/master/cpp/CMake/PythonUtils.cmake) to improve
diagnostics.
Please pay attention to warning messages from `PythonUtils.cmake` in the CMake output which highlights any configuration
issues with Python.

We compile against the first `python` on the `PATH` by default.

To override that, use one of the following CMake variables*:

* `Python_ROOT_DIR` - The common path "prefix" for a particular Python install.
  Usually, the `python` executable is in the same directory or the `bin` *sub*directory.
  This directory should also contain the `include` and `lib(rary)` *sub*directories.<br>
  E.g. `/usr` for a system-wide install on *nix;
  `/opt/pythonXX` for a locally-managed Python install;
  `/home/user/my_virtualenv` for a virtual env;
  `C:\Program Files\PythonXX` for a Windows Python install

* `Python_EXECUTABLE` - The path to the Python executable. (CMake 3.15+)
  CMake will try to extract the `include` and `library` paths by running this program.
  This differs from the default behaviour of [FindPython](https://cmake.org/cmake/help/latest/module/FindPython.html).


(* Note CMake variables are set with `-D` on the CMake command line or with the `cacheVariables` key in `CMake*Presets.json`.
   The names are case-sensitive.

   (Only) `Python_ROOT_DIR` can also be set as an environment variable.
   Setting the others in the environment might have no effect.)

Development Guidelines
======================

ArcticDB has a lot of write-time options to configure how the data is stored on disk.
When adding new features, it is important to test not only the simplest case, but also how the new feature interacts with these various options.
This section serves as a guide as to what should be considered when adding a new feature.

## Backwards compatibility

### Data stored on disk

Due to ArcticDB being a purely client-side database, it is possible for data to be written by clients that are of later versions than clients that need to read the same data.
If the change is breaking, such that older clients will not be able to read the data, then this should be clearly documented both in the PR and in the online documentation.
If possible, a version number should also be added such that future changes in the same area will display a more clear error message.

### API

Any changes to the API (including when exceptions are thrown and the type of the exception) must be weighed up against the change breaking behaviour for existing users.
Please make this as clear to reviewers as possible by ensuring **API changes are clearly described in the PR description**. 
If the change is breaking, please also ensure that that is appropriately highlighted in the PR description as well.
This is particularly true of the `NativeVersionStore` API, as this has many users inside Man Group.

### Snapshots

Symbols are almost completely decoupled from one another in ArcticDB.
The major feature for which this is not the case is snapshots.
Whenever a change involves deleting any keys from the storage, care must be taken that those keys are not in fact still needed by a snapshot.
Similarly, any operation that modifies a snapshot, including deleting it, must ensure that any keys for which this snapshot was the last reference are also deleted.
See the [tutorial](https://docs.arcticdb.io/tutorials/snapshots/) and [API documentation](https://docs.arcticdb.io/api/library#arcticdb.version_store.library.Library.snapshot) for more details.

## Batch methods

Almost all of the major reading and writing methods have batch equivalents.
If a feature is added for the non-batch version, it should almost always work with the batch version as well.
We should also strive for consistency of behaviour across the batch methods in circumstances that could be encountered by any of them.
e.g. `read_batch` and `read_metadata_batch` should have the same behaviour if the specified version does not exist.

## Dynamic schema

Static schema is much simpler than dynamic schema, and so features added and only tested with static schema may not "just work" with dynamic schema.
In particular, the following cases should be considered:

* A column that **is not** present in an initial call to `write` **is** present in a subsequent call to `append`.
* A column that **is** present in an initial call to `write` **is not** present in a subsequent call to `append`.
* A numeric column with a "small" type (e.g. `uint8`) is appended to with a numeric column of a larger type (e.g. int16), and vice versa.

## Segmentation

Most tests are written with small amounts of data to keep the runtime down as low as possible.
However, many bugs are only exposed when data being written is column-sliced and/or row-sliced.
Using test fixtures configured to slice into small data segments (e.g. 2x2) can help to catch these issues early.

## Data sub-selection

Many use cases of ArcticDB involve writing huge symbols with hundreds of thousands of columns or billions of rows.
In these cases, methods for reading data will almost never be called without some parameters to cut down the amount of data returned to the user. The most commonly used are:

* The stand-alone methods `head` and `tail`*
* `columns` - select only a subset of the columns
* `date_range` and `row_range` - select a contiguous range of rows
* `query_builder` - see section on processing pipeline

## Processing pipeline

Reading data using only `columns`, `date_range`, and `row_range` arguments is heavily optimised to avoid copying data in memory unnecessarily.
 Any read-like method provided with the `query_builder` optional argument takes quite a different code path, as we do not know the shape of the output data a priori.
 While it is not necessary to exhaustively test all new features against every possible `query_builder` argument, it is generally worth having a test using a simple filter that excludes some of the data that would otherwise be returned to the user.

## Sparse columns

The majority of data written into ArcticDB uses dense columns, meaning that within a given row-slice, every row has an associated value in every column (even if that value is `NaN`).
The concept of sparse columns is implemented as well, where columns may have values missing for none, some, or all rows within a row-slice.
Any read-like method should be able to handle both types of stored column.

## Pickling

When data that cannot be normalized to a supported data type in ArcticDB, it can still be stored using `write_pickle` and similar.
There are many operations that cannot be performed on pickled data, such as `append`, `update`, `date_range` search, and many more.
It is important that if a user attempts an operation that is not supported with pickled data, that they receive a helpful error message.
