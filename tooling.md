## Plan

I want to write tooling to manage various commands we run routinely while working on this project.
I think that a Makefile in the project root, combined with some new bash or Python scripts in build_tooling/ might be a good approach
but explore and discuss other designs too.

If working in Man group:
- All setup.py or dependency installation commands, or cmake configure steps, must be prefixed `withproxy`
- Set the env var TMPDIR=$HOME/.tmp

### 1

Generate protobuf stubs, necessary if .proto files have changed or for a new installation:

```
python setup.py protoc --build-lib python
```

### 2

Create a new venv,

```
python3 -m venv ~/venvs/<name>
```

Within the new venv,

Install all deps including test deps from setup.cfg.

Install linters,

```
python ./build_tooling/format.py --install-tools
```

### 3

Run linters:

```
python build_tooling/format.py --in-place --type all
```

### 4

Build the `arcticdb_ext` target with either `release` or `debug` preset. Exact preset to use should be user configurable,
I want `linux-release-py310` and `linux-debug-py310`, other users may not. Default to `linux-release` and `linux-debug`.

### 5

Build and run the `test_unit_arcticdb` target, again with either release or debug preset. Allow providing a test filter.

### 6

Create a symlink in ./python pointing at the built `arcticdb_ext` extension, specifying either `release` or `debug`.

### 7

Run pytests in `python/test` with options for the different test types (unit, integration, etc).

### Other

Include any remaining build commands listed in the CLAUDE.md file that are not duplicated and are useful.
Update the CLAUDE.md file to refer to this new tooling.

