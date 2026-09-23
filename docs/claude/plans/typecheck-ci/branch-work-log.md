# typecheck-ci work log

- Added `[tool.mypy]` to `pyproject.toml` covering `python/arcticdb`; the 34 modules that had errors at introduction are
  listed under `ignore_errors` as a baseline to shrink over time (#3427).
- Pinned tools and runtime deps in `build_tooling/requirements-typecheck.txt`; added `make typecheck` and a "Type check
  Python" step to the `run_linting_checks` job in `build.yml`.
- Fixed all errors in `arctic.py`, `options.py`, `version_store/library.py` and `dependencies.py`.
- Fixed stale `# type:` comments whose arity no longer matched their signatures (blocking errors in mypy).
- Bug found by mypy: `Arctic.get_library(create_if_missing=True, ...)` passed `output_format` positionally into
  `enterprise_library_options`, raising `AttributeError`. Fixed; regression test in `test_arrow_api.py`.
