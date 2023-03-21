Setting up your OS dependencies
===============================

ManyLinux Docker image
----------------------
ManyLinux(https://github.com/pypa/manylinux) captures the common denominator of many Linux distributions,
giving the confidence that code worked in this image runs everywhere.

We [extend](https://github.com/man-group/ArcticDB/blob/master/build_tooling/build_many_linux_image.sh) it with
additional dependencies for compilation purposes.
You can download the output [here](https://github.com/man-group/ArcticDB/pkgs/container/cibuildwheel_manylinux).

**We recommend you use this image for compilation and testing.**

Linux
-----
The codebase and build system can work with any reasonably recent Linux distribution with at least
GCC 8 (10+ recommended) and CMake 3.12 (these instructions assume 3.21+).

A development install of Python 3.6+ (with `libpython.a` or `.so` and full headers) is also necessary.
See [pybind11 configuration](#pybind11-configuration).

### Dependencies by distro
| Distro | Versions reported to work | Packages |
|---|---|---|
| Ubuntu | 20.04, 22.04 | build-essential g++-10 libpcre3-dev libsasl2-dev libsodium-dev libkrb5-dev libcurl4-openssl-dev python3-dev |
| Centos | 7 | devtoolset-10-gcc-c++ openssl-devel cyrus-sasl-devel devtoolset-10-libatomic-devel libcurl-devel python3-devel |

Windows
-------
We recommend using Visual Studio 2022 (or later) to install the compiler (MSVC v142 or newer) and tools
(Windows SDK, CMake, Python).

The Python that comes with Visual Studio is sufficient for creating release builds, but for debug builds, you will have
to separately download from [Python.org](https://www.python.org/downloads/windows/).

Ways to build the code
======================
First steps
-----------
On Windows, you need to [configure `git` to allow symbolic links](https://stackoverflow.com/a/59761201).

Recursively clone the repository:

```bash
git clone https://github.com/man-group/ArcticDB.git
cd ArcticDB
git submodule init && git submodule update
```

Quick check
-----------
If you just want to verify your setup, try running from the command line:

```bash
# Recursively checkout ArcticDB
# Make sure the Python version you want is the first on the PATH

python -m pip install -ve .
```

This will build and install ArcticDB in the current working directory.

Compiling step by step
----------------------
During day-to-day development, it's quicker to rebuild only the part that changed.

Below is `pip install -e .` broken down step by step:

```bash
# Install the necessary dependencies to run setup.py
python -m pip install -U pip setuptools wheel grpcio-tools

# Install the Python dependencies without compiling the C++
ARCTIC_CMAKE_PRESET=skip python setup.py develop

# Configure CMake (which compiles the dependencies using vcpkg)
cmake --preset linux-debug

# Compile the code
cmake --build --preset linux-debug

# Install the compiled module
cmake --install cpp/out/linux-debug --component Python_Lib --prefix $PWD/python /cmake_install.cmake
```
<!-- TODO: make the install command simpler -->

Running the tests
-----------------

```bash
python -m pip install arcticdb[Testing]
python -m pytest python/tests
```

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
