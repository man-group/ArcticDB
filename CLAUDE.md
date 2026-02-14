# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArcticDB is a high-performance, serverless DataFrame database for the Python Data Science ecosystem. It provides a Python API backed by a C++ data-processing and compression engine, supporting S3, LMDB, Azure Blob Storage, and MongoDB backends.

## Documentation

### User-Facing Documentation (`docs/mkdocs/docs/`)

**New features must include documentation:**

- **Tutorials** (`tutorials/`): Step-by-step guides for features (e.g., `sql_queries.md`)
- **API Reference** (`api/`): Auto-generated from docstrings via mkdocstrings
- **Technical docs** (`technical/`): Architecture and implementation details

When adding a new feature:

1. **Add/update docstrings** in the Python code (NumPy format)
2. **Create a tutorial** if the feature has multiple use cases or nuances
3. **Update `mkdocs.yml`** nav section to include new pages
4. **Build docs locally** to verify: `cd docs/mkdocs && mkdocs serve`

Documentation checklist:
- [ ] Public API has complete docstrings (Parameters, Returns, Raises, Examples)
- [ ] Complex features have a tutorial with code examples
- [ ] Edge cases and limitations are documented
- [ ] When to use feature A vs feature B is explained (if applicable)

### Claude-Maintained Technical Docs (`docs/claude/`)

Technical documentation in `docs/claude/` is **owned and maintained by Claude**. Consult these documents when working on related areas.

- **Read** the relevant doc when starting work in an area (e.g., read `CACHING.md` before modifying version map cache)
- **Update** the doc only when making changes to that area
- Do NOT proactively read or update docs for unrelated areas

Keep documentation **high-level and terse**: reference `file_path:ClassName:method_name` instead of copying code; use tables and bullet points over code blocks; avoid duplicating what's already in source code.

| Area | Document |
|------|----------|
| Architecture | [docs/claude/ARCHITECTURE.md](docs/claude/ARCHITECTURE.md) |
| C++ modules | [docs/claude/cpp/](docs/claude/cpp/) (CACHING, VERSIONING, STORAGE_BACKENDS, ENTITY, CODEC, COLUMN_STORE, PIPELINE, PROCESSING, STREAM, ASYNC, PYTHON_BINDINGS, ARROW) |
| Python modules | [docs/claude/python/](docs/claude/python/) (ARCTIC_CLASS, LIBRARY_API, NATIVE_VERSION_STORE, QUERY_PROCESSING, NORMALIZATION, ADAPTERS, TOOLBOX, DUCKDB) |

## User-Specific Settings

Check `CLAUDE_USER_SETTINGS.md` (git-ignored) for user-specific configuration:
- Python virtual environment paths (Claude should use its own venvs, not user's)
- Preferred CMake presets for debug/release/profiling builds

## Build Commands

### Building the Python Wheel

```bash
# Build with a specific CMake preset (limit parallelism to avoid overloading the system)
CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=linux-debug pip install -ve .
```

Note: Limit `CMAKE_BUILD_PARALLEL_LEVEL` to min(16, nproc) to avoid memory pressure during compilation.

### Building on Man Linux VMs

The vcpkg-based build requires certain system packages that may not be installed by default:

```bash
# Required system packages for vcpkg build
sudo apt install pkg-config flex bison libsasl2-dev -y
```

Use Pegasus for Python environment management:

```bash
# Create a Python 3.11 environment
pegasus create -d 311-1 /turbo/<username>/pyenvs/arcticdb-dev
source /turbo/<username>/pyenvs/arcticdb-dev/bin/activate

# Initialize git submodules (required for vcpkg)
git submodule update --init --recursive

# Build with linux-debug preset (limit parallelism, use protobuf 4)
ARCTICDB_PROTOC_VERS=4 CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=linux-debug pip install -ve .
```

To install packages which aren't available internally, use the following custom index:
```bash
pip install -i https://repo.prod.m/artifactory/api/pypi/external-pypi/simple/ hypothesis==6.72.4
```

### Building a Wheel

```bash
# Build wheel (use ARCTICDB_PROTOC_VERS=4 to skip protobuf 5 on Man VMs)
ARCTICDB_PROTOC_VERS=4 CMAKE_BUILD_PARALLEL_LEVEL=16 ARCTIC_CMAKE_PRESET=linux-debug pip wheel . --no-deps -w dist/
```

The wheel will be created in `dist/arcticdb-<version>-cp311-cp311-linux_x86_64.whl`.

### CMake Presets

Key presets in `cpp/CMakePresets.json`:
- `linux-debug` / `linux-release` - Linux with vcpkg
- `linux-conda-debug` / `linux-conda-release` - Linux with conda-forge deps (set `ARCTICDB_USING_CONDA=1`)
- `windows-cl-debug` / `windows-cl-release` - Windows with MSVC
- `macos-debug` / `macos-release` - macOS

User-specific presets can be defined in `cpp/CMakeUserPresets.json` (git-ignored).

## Git Submodules

The project uses several git submodules. **Do not directly edit files inside submodule directories** - instead update the submodule reference.

### Submodule Locations

| Submodule | Path | Purpose |
|-----------|------|---------|
| vcpkg | `cpp/vcpkg` | Package manager with custom ports (e.g., `arcticdb-sparrow`) |
| pybind11 | `cpp/third_party/pybind11` | Python bindings |
| lmdb | `cpp/third_party/lmdb` | LMDB storage backend |
| lmdbxx | `cpp/third_party/lmdbxx` | C++ wrapper for LMDB |
| recycle | `cpp/third_party/recycle` | Memory recycling |
| rapidcheck | `cpp/third_party/rapidcheck` | Property-based testing |
| entt | `cpp/third_party/entt` | Entity component system |

### Upgrading a Dependency via vcpkg Submodule

When upgrading a dependency like `sparrow` that has a custom port in vcpkg:

1. **Fetch and checkout the vcpkg commit** containing the new version:
   ```bash
   cd cpp/vcpkg
   git fetch origin
   git log --oneline origin/master | grep -i <package-name>  # Find the commit
   git checkout <commit-hash>
   cd ../..
   ```

2. **Update the version override** in `cpp/vcpkg.json`:
   ```json
   "overrides": [
     { "name": "arcticdb-sparrow", "version": "X.Y.Z" }
   ]
   ```

3. **Update conda environment** in `environment-dev.yml` if applicable

4. **Rebuild** - vcpkg will fetch the new version on next build

### Building C++ Tests

Use the preset from `CLAUDE_USER_SETTINGS.md` (or `linux-debug` as default):

```bash
cmake -DTEST=ON --preset <preset> cpp
cmake --build cpp/out/<preset>-build --target test_unit_arcticdb

# Run a single test
cpp/out/<preset>-build/arcticdb/test_unit_arcticdb --gtest_filter="TestSuite.TestName"
```

## Running Python Tests

```bash
# Run all tests (use -n for parallel execution via pytest-xdist)
python -m pytest -n 8 python/tests

# Run a single test file
python -m pytest python/tests/unit/arcticdb/test_arctic.py

# Run a specific test
python -m pytest python/tests/unit/arcticdb/test_arctic.py::test_function_name

# Run tests in a subdirectory in parallel
python -m pytest -n 8 python/tests/unit/arcticdb/version_store/duckdb/
```

## Benchmarking

### C++ Benchmarks (Google Benchmark)

```bash
cmake -DTEST=ON --preset <preset> cpp
cmake --build cpp/out/<preset>-build --target benchmarks

# Run specific benchmarks
cpp/out/<preset>-build/arcticdb/benchmarks --benchmark_filter=<regex>
```

Benchmark sources are in `cpp/arcticdb/*/test/benchmark_*.cpp`.

### Python Benchmarks (ASV)

ASV benchmarks live in `python/benchmarks/`. Requires `asv` and `virtualenv` installed.

**First-time setup** — register the machine (one-off):
```bash
asv machine --yes
```

**Run from the repo root** (not `python/`):
```bash
# Run a specific benchmark suite against the current environment (fastest — no rebuild)
asv run --python=$(which python) -v --show-stderr --bench BasicFunctions

# Run all benchmarks
asv run --python=$(which python) -v --show-stderr

# Run benchmarks matching a regex
asv run --python=$(which python) -v --show-stderr --bench "QueryBuilder|Resample"
```

Note: `--python=$(which python)` uses the active virtualenv directly, avoiding a full wheel build. Do **not** combine this with a commit range (`HEAD^!`) — they are mutually exclusive.

**Available benchmark suites**: `BasicFunctions`, `Arrow`, `QueryBuilder`, `Resample`, `ModificationFunctions`, `ListSymbols`, `ListVersions`, `ListSnapshots`, `VersionChain`, `RecursiveNormalizer`, `FinalizeStagedData`.

By default only LMDB storage is tested. Set `ARCTICDB_STORAGE_AWS_S3=1` with appropriate credentials to include S3. Set `ARCTICDB_SLOW_TESTS=1` for additional slow benchmarks.

See: [ASV Benchmarks Wiki](https://github.com/man-group/ArcticDB/wiki/Dev:-ASV-Benchmarks)

## Key Development Guidelines

### Test-Driven Development

**Every code change must be accompanied by a failing test that the change fixes.** This ensures:
- The bug or missing feature is properly understood before fixing
- The fix actually addresses the issue
- Regressions are caught if the code is modified later

When fixing a bug or adding a feature:
1. Write a test that demonstrates the bug or missing functionality
2. Verify the test fails
3. Implement the fix
4. Verify the test passes

### Git Workflow

**Always confirm with the developer before committing and pushing changes upstream.** Do not assume that passing tests means the changes are ready for review. The developer may want to:
- Review the implementation approach
- Make additional changes or refinements
- Squash or reorganize commits
- Add to the commit message or PR description

Wait for explicit confirmation like "commit and push" or "looks good, push it" before pushing to remote.

### Branch Work Logs

When working on a feature branch, maintain a work log in `docs/claude/plans/<branch-name>/branch-work-log.md`. Update it at the end of each task with a few bullet points summarizing what was done. This provides continuity across sessions and helps with PR descriptions.

### Backwards Compatibility

- Data written by newer clients should be readable by older clients - document breaking changes clearly
- API changes affecting V1 or V2 public APIs must be highlighted in PR descriptions

### Code Style

Code style is enforced by `./build_tooling/format.py`. **Always run the formatter after making code changes, but only on files changed on the branch:**

```bash
# Format only files changed on the branch
git diff --name-only origin/master..HEAD -- '*.py' | xargs -r -n1 python ./build_tooling/format.py --in-place --type python --file
git diff --name-only origin/master..HEAD -- '*.cpp' '*.hpp' | xargs -r -n1 python ./build_tooling/format.py --in-place --type cpp --file
```


## Code Review

When reviewing changes on a branch before submitting upstream, see **[docs/claude/skills/code-review.md](docs/claude/skills/code-review.md)** for detailed instructions covering:

- C++ memory safety (Rule of Five, Arrow C Data Interface, RAII)
- Python code quality (exception handling, duplicate code, state management)
- Test coverage analysis (happy path, error handling, edge cases, parameter coverage)
- Error handling review (fail fast, helpful messages, exception types)
- Type handling (numeric, temporal, string, complex types)
- Documentation and performance considerations

Use sub-agents to review in parallel. Write findings to `docs/claude/plans/` for tracking.


### Git Commits

- Do not add "Generated with AI" or "Co-Authored-By" lines to commit messages
