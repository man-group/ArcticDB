# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArcticDB is a high-performance, serverless DataFrame database for the Python Data Science ecosystem. It provides a Python API backed by a C++ data-processing and compression engine, supporting S3, LMDB, Azure Blob Storage, and MongoDB backends.

## Documentation

**New features must include documentation.** Full requirements and checklists for both
user-facing docs and Claude-maintained technical docs are in
[`.claude/skills/update-docs/SKILL.md`](.claude/skills/update-docs/SKILL.md) — read that
file when adding or modifying public API, and follow its checklists before considering the
work complete.

### Claude-Maintained Technical Docs (`docs/claude/`)

| Area | Document |
|------|----------|
| Architecture | [docs/claude/ARCHITECTURE.md](docs/claude/ARCHITECTURE.md) |
| C++ modules | [docs/claude/cpp/](docs/claude/cpp/) (CACHING, VERSIONING, STORAGE_BACKENDS, ENTITY, CODEC, COLUMN_STORE, PIPELINE, PROCESSING, STREAM, ASYNC, PYTHON_BINDINGS) |
| Python modules | [docs/claude/python/](docs/claude/python/) (ARCTIC_CLASS, LIBRARY_API, NATIVE_VERSION_STORE, QUERY_PROCESSING, NORMALIZATION, ADAPTERS, TOOLBOX) |

## User-Specific Settings

Check `CLAUDE_USER_SETTINGS.md` (git-ignored) for user-specific configuration:
- Python virtual environment paths (Claude should use its own venvs, not user's)
- Preferred CMake presets for debug/release/profiling builds

## Build Commands

### Prerequisites on Man Linux VMs

The vcpkg-based build requires certain system packages that may not be installed by default:

```bash
sudo apt install pkg-config flex bison libsasl2-dev ccache -y
```

`ccache` is optional but strongly recommended — it is auto-detected and used for both ArcticDB
and vcpkg third-party dependency builds, giving ~3–5× faster rebuilds after the first clean build.

Initialize git submodules (required for vcpkg):

```bash
git submodule update --init --recursive
```

Copy `Makefile.local.example` to `Makefile.local` for Man-specific settings (proxy, TMPDIR, protobuf version).

### Environment Setup

If `Makefile.local` is missing, prompt the user to create it, using `Makefile.local.example` as an example.

If `VIRTUAL_ENV` is not set:

- Ask for the NAME they want to use for the venv
- If it already exists in `~/venvs/<NAME>` inform the user. They can either use it as is, or you can
    run `make setup NAME=<name> CLEAN=1` to recreate it.
- Otherwise if it does not already exist, create it, `make setup NAME=<name>`.

Do not warn the user that it will take a while - it's usually fast.

### Makefile Targets

A root `Makefile` provides shortcuts for common tasks. User-specific overrides (presets, proxy, TMPDIR) go in `Makefile.local` (gitignored; see `Makefile.local.example`).

| Target | Description | Key variables |
|--------|-------------|---------------|
| `make help` | List all targets and current variable values | |
| `make setup NAME=x` | Full setup from scratch: submodules, venv, protoc, build, symlink | `CLEAN=1` to replace existing venv |
| `make protoc` | Generate protobuf stubs | `PROTOC_VERS`, `PROXY_CMD` |
| `make venv NAME=x` | Create a dev venv with all deps (`CLEAN=1` to replace existing) | `VENV_DIR`, `PROXY_CMD` |
| `make activate NAME=x` | Print activate path. Use: `source $(make activate NAME=x)` | `VENV_DIR` |
| `make lint` | Run formatters in-place | |
| `make lint-check` | Check formatting without changes | |
| `make build` / `build-debug` | Configure, build, and symlink `arcticdb_ext` | `RELEASE_PRESET` / `DEBUG_PRESET`, `CMAKE_JOBS` |
| `make configure` / `configure-debug` | CMake configure only | |
| `make test-cpp` / `test-cpp-debug` | Build and run C++ unit tests | `FILTER=` for gtest_filter |
| `make symlink` / `symlink-debug` | Symlink built extension into `python/` | |
| `make test-py` | Run Python tests | `TYPE=unit\|integration\|...`, `FILE=` path to file/test, `ARGS=` |
| `make build-and-test-py` | Release build + symlink + Python tests | `RELEASE_PRESET`, `CMAKE_JOBS`, `TYPE=`, `FILE=`, `ARGS=` |
| `make build-and-test-py-debug` | Debug build + symlink + Python tests | `DEBUG_PRESET`, `CMAKE_JOBS`, `TYPE=`, `FILE=`, `ARGS=` |
| `make wheel` | Build a pip wheel into `dist/` | |
| `make bench-cpp` | Build and run C++ benchmarks | `FILTER=` |
| `make install-editable` | Install arcticdb in editable mode (no C++ rebuild) | |
| `make bench-py` | Run ASV Python benchmarks (runs `install-editable` first) | `BENCH=` |

Prefer the `-debug` targets over the release mode targets unless told otherwise.

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

## Benchmarking

C++ benchmark sources are in `cpp/arcticdb/*/test/benchmark_*.cpp`. ASV Python benchmarks live in `python/benchmarks/`. See [ASV Benchmarks Wiki](https://github.com/man-group/ArcticDB/wiki/Dev:-ASV-Benchmarks).

## Code Review Guidelines

When writing or modifying code, follow the standards in [`docs/claude/PR_REVIEW_GUIDELINES.md`](docs/claude/PR_REVIEW_GUIDELINES.md). These cover API stability, memory safety, on-disk format compatibility, concurrency, testing, and other quality gates enforced during PR review.

## Key Development Guidelines

Stop and ask clarifying questions when you are confused.

It is unlikely you need to catch `std::exception`. Handle less broad exceptions, like `KeyNotFoundException`.

It is important that you do not submit tasks to the threadpools from within a task that is already executing within the
same threadpool, as this can deadlock. This means you might need to use synchronous APIs like `read_sync` from within tasks.

Do not write comments except where they are very valuable. Keep them as brief as possible. Do not delete existing comments
unless they are incorrect.

Prefer writing pytests as standalone functions rather than wrapping them in a class.

### Test-Driven Development

**Every code change must be accompanied by a failing test that the change fixes.**

When fixing a bug or adding a feature:
1. Write a test that demonstrates the bug or missing functionality
2. Verify the test fails
3. Implement the fix
4. Verify the test passes

### Git Workflow

**Always confirm with the developer before committing and pushing changes upstream.**

Wait for explicit confirmation like "commit and push" or "looks good, push it" before pushing to remote.

### Branch Work Logs

When working on a feature branch, maintain a work log in `docs/claude/plans/<branch-name>/branch-work-log.md`. Update it at the end of each task with a few bullet points summarizing what was done. This provides continuity across sessions and helps with PR descriptions.

### Backwards Compatibility

- Data written by newer clients should be readable by older clients - document breaking changes clearly
- API changes affecting V1 or V2 public APIs must be highlighted in PR descriptions

### Code Style

Code style is enforced by `make lint` **Always run `make lint` after making code changes.**

### Git Commits

- Do not add "Generated with AI" or "Co-Authored-By" lines to commit messages
