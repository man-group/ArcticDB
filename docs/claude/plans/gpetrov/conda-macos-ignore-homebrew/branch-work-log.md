# Branch work log: gpetrov/conda-macos-ignore-homebrew

- `osx_arm64 / OS compat` was failing on master and PRs: the conda build on macos-26 linked Homebrew's
  `openssl@4` (`/opt/homebrew/opt/openssl@4/lib/libssl.4.dylib`), which macos-15 doesn't have. LZ4 and the
  AWS SDK's `crypto` lookup were also resolving to `/opt/homebrew`.
- `cpp/CMakeLists.txt`: macOS conda builds now prepend `$CONDA_PREFIX` to `CMAKE_PREFIX_PATH`, ignore the
  `/opt/homebrew` prefix, and point `PKG_CONFIG_LIBDIR` at conda. A scratch CMake project showed that
  `CMAKE_IGNORE_PREFIX_PATH` alone doesn't stop `FindOpenSSL`'s pkg-config `HINTS` reaching
  `/opt/homebrew/Cellar`, but the combination does.
- `compile_and_test_with_conda.yml`: added a macOS step after the build that fails if `otool -L` shows any
  `/opt/homebrew` dependency, so the leak is caught in the Compile job.
