Setting up your OS dependencies
===============================

Linux
-----
The codebase and build system can work with reasonably recent Linux distributions that ship at least
GCC 8 (10 recommended) and CMake 3.12 (3.18+ recommended).

A development install of Python 3.6+ (with libpython.a or .so and full headers) is also necessary.
If your distro doesn't ship one or if it's too old, you can compile from source or use the "manylinux" docker image.

Different distros/installs might have different names for their GCC and Python executables, so you might want to use the
[CMake presets]() feature.

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
to separately download from Python.org.


Common Tasks
============

## Recursive checkout
```bash
git clone https://github.com/man-group/ArcticDB.git
cd ArcticDB
git submodule init && git submodule update
```

## CMake presets
To make it easier to set and share all the environment variables, settings and commands, we recommend using the [CMake
presets](https://cmake.org/cmake/help/latest/manual/cmake-presets.7.html) feature.

Recent versions of popular C++ IDEs support reading/importing these presets:
* [Visual Studio & Code](https://devblogs.microsoft.com/cppblog/cmake-presets-integration-in-visual-studio-and-visual-studio-code/)
* [CLion](https://www.jetbrains.com/help/clion/cmake-presets.html)

And it's equally easy to use on the command line (see the next section).

We already ship a `CMakePresets.json` in the cpp directory, which is used by our builds.
You can add a `CMakeUserPresets.json` in the same directory to set custom, localised config.
Inheritance is supported.

Example:

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

vcpkg caching
-------------
We use [vcpkg](https://vcpkg.io/) to manage the C++ dependencies.

The CMakePresets we provided use a vcpkg git submodule as the toolchain, so
it's not necessary to separately download and install vcpkg.
You can, however, maintain a separate vcpkg install and override the CMake presets.

This setup downloads the dependencies' source packages from the Internet and compiles them locally during CMake
configuration, so it's critical to cache this process to save time (and to comply with any local IT policy).

Please refer to below documentation:
* [Binary caching](https://learn.microsoft.com/en-us/vcpkg/users/binarycaching)
* [Asset caching](https://learn.microsoft.com/en-us/vcpkg/users/assetcaching)

For example, you can use a network shared as the caching folder and put the caching configuration environment
variables into the CMake presets.

Please also note, compiling the dependencies locally uses a lot of disk space.
Once all dependencies are compiled and saved into the binary cache, you can remove the `cpp\vcpkg\buildtrees` folder.

pybind11 configuration
----------------------



Building
=========

Quick check
-----------
If you just want to verify your setup, try running from the command line:

```bash
# Recursively checkout ArcticDB
# Make sure the devel python is the first one on the PATH

python3 -m pip install -e .
```

Note pip only prints the C++/CMake build output when the compilation completes.
You will have to use `ps/htop` to monitor the progress when using this command.
Full compilation can take a long time, so once this has been running for


Compiling step by step
----------------------
During day-to-day development, it's quicker to rebuild only the part that changed and
below is the equivalent content of `pip install -e .`:

```bash
# Install the necessary dependencies to run setup.py
python3 -m pip install -U pip setuptools wheel grpcio-tools

# Tell setup.py not to compile the C++ extension and just install rest of the dev dependencies
ARCTIC_CMAKE_PRESET=skip python3 setup.py develop

# Configure CMake and build the code
ARCTIC_CMAKE_PRESET=${name of your preset. See above}
cmake --preset $ARCTIC_CMAKE_PRESET
cmake --build --preset $ARCTIC_CMAKE_PRESET

# (Optional) Build a wheel for distribution
export ARCTIC_CMAKE_PRESET
python3 setup.py bdist_wheel
```
