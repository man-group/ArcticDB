# ArcticDB PR Review & Code Quality Guidelines

These guidelines apply when **writing**, **reviewing**, or **modifying** code in this
repository. Apply the sections relevant to the files being changed.

Style nitpicks are handled by `build_tooling/format.py` — focus on substantive issues.

---

## 1. PR TITLE & DESCRIPTION

- **Title clarity**: Concise, grammatically correct summary of *what* the PR does.
  Avoid vague titles like "Changes" or "WIP".
- **Grammar and spelling**: Check for typos and unclear phrasing.
- **Description completeness**: Explain *what* changed and *why*. Flag any significant
  diff changes not mentioned in the description.
- **API changes called out**: Public Python API changes (signatures, return types,
  defaults, exceptions) must be explicitly called out.
- **Breaking changes labelled**: On-disk format or breaking changes must be highlighted
  and the PR labelled appropriately (`enhancement`, `bug`, etc.).
- **Linked issues**: Reference related issues (e.g., "Fixes #1234").
- **Scope match**: Title/description should match the actual scope of changes.

---

## 2. PUBLIC API STABILITY (Python)

ArcticDB has a layered Python API: `Arctic` -> `Library` -> `NativeVersionStore`
-> C++ `PythonVersionStore` via pybind11.

- **Breaking changes** to `Arctic`, `Library`, or `NativeVersionStore` must be called
  out in PR descriptions and labelled.
- **Deprecation protocol**: Removed or renamed parameters must go through a deprecation
  cycle.
- **Library option defaults**: Changes to defaults like rows_per_segment,
  columns_per_segment, dynamic_schema affect all users silently.
- **QueryBuilder**: New operations must follow the existing fluent builder pattern.
- **Return type changes** from read/write operations affect downstream code.
- **Exception hierarchy**: Preserve error codes and exception types users may catch.
- **Docstrings**: Public methods must have accurate docstrings matching signatures.

---

## 3. PYTHON-C++ BOUNDARY (pybind11 Bindings)

- **GIL management**: Long-running C++ operations must release the GIL
  (`py::call_guard<py::gil_scoped_release>()`). Callbacks into Python must reacquire it.
- **Exception translation**: C++ exceptions must map to appropriate Python exceptions.
- **Object lifetime**: pybind11 `py::object`, `py::array` held across GIL release can
  cause use-after-free.
- **Type conversion**: Handle None/NaN, empty arrays, and dtype edge cases.
- **Normalization checks**: Data validation at the boundary must not be bypassed.
- **String handling**: Python str/bytes to C++ must handle Unicode correctly (UTF-8).

---

## 4. MEMORY MANAGEMENT (C++)

ArcticDB has a custom memory management system with detachable memory for Arrow interop,
buffer pools, chunked buffers, and an optional slab allocator.

- **Allocator usage**: Use correct `AllocationType` (DYNAMIC, PRESIZED, DETACHABLE).
- **Buffer ownership**: Buffers must not be used after being moved or returned to a pool.
- **ChunkedBuffer**: Correct chunk management and iterator validity.
- **RAII compliance**: All resources must use RAII. No raw `new`/`delete`.
- **Move semantics**: Use `ARCTICDB_MOVE_ONLY_DEFAULT`, `ARCTICDB_NO_MOVE_OR_COPY`, etc.
  Accidental copies of large buffers are performance bugs.
- **Slab allocator compatibility**: Allocation patterns must be compatible when enabled.
- **Memory leaks**: New leak sanitizer suppressions need strong justification.

---

## 5. ON-DISK FORMAT & BACKWARDS COMPATIBILITY

ArcticDB guarantees that newer clients can read data written by older clients and vice
versa.

- **Binary layout changes**: Packed structs (`FixedHeader`, `Block`, `EncodedField`,
  `FieldStats`) have fixed sizes validated by `static_assert`. ANY change breaks stored
  data.
- **Magic numbers**: Section markers in encoded segments must not be changed.
- **Encoding version compatibility**: Both V1 (protobuf headers) and V2 (binary headers)
  paths must remain readable.
- **Key type changes**: `KeyType` enum values are persisted. Adding is fine, removing or
  renumbering breaks storage.
- **ValueType enum**: Must match the protobuf descriptor exactly. New types require
  fallback handling in older clients.
- **Version chain markers**: `__write__`, `__tombstone__`, `__tombstone_all__`, etc.
  must not change.
- **Protobuf schema**: No removed/renumbered fields, use `optional` for new fields.
- **Compat tests**: On-disk format changes must update pinned-version compatibility
  tests.

---

## 6. CODEC & COMPRESSION

- **Segment structure**: Changes to segment format affect all read/write paths (high-risk).
- **Codec correctness**: LZ4, ZSTD, Passthrough, and PFOR must produce lossless
  roundtrips.
- **Hash integrity**: Block-level hashing changes break data validation.
- **Encoded field ordering**: Must maintain deterministic ordering.
- **Default codec changes**: Affect storage efficiency and compatibility.
- **Buffer size calculations**: Must be exact. Off-by-one errors cause buffer overflows.

---

## 7. STORAGE BACKENDS

Each backend (S3, Azure, LMDB, MongoDB, Memory) implements an abstract Storage
interface with methods for write, read, remove, iterate, and key_exists.

- **Interface compliance**: Implement ALL required virtual methods.
- **Atomic write support**: Classification (NO, YES, NEEDS_TEST) must be correct.
  Incorrect classification causes data corruption under concurrent access.
- **Error handling**: Throw appropriate storage exception subtypes, not generic
  exceptions.
- **Key encoding**: Consistent format across backends.
- **Mock consistency**: Mocks must mirror real client behaviour.
- **URI parsing**: Changes can break user connection strings.
- **Credential handling**: No hardcoded credentials, tokens, or secrets. No logging of
  sensitive connection parameters.

---

## 8. VERSION ENGINE & CONCURRENCY

The versioning engine manages a version chain (linked list of versions per symbol),
a symbol list, and snapshots with a lock-free concurrency model (last-writer-wins).

- **Version chain integrity**: Incorrect pointer manipulation corrupts the chain.
- **Tombstone handling**: Incorrect load types cause stale reads or missed tombstones.
- **Symbol list consistency**: Race conditions cause `list_symbols()` to return
  incorrect results.
- **Snapshot safety**: Snapshots must be atomic — partial snapshots are corrupt.
- **De-duplication**: Incorrect dedup causes data loss or storage waste.
- **Schema checks**: Compatibility validation on `append()`/`update()` must not be
  bypassed.
- **Batch operations**: Must maintain atomicity guarantees.

---

## 9. PIPELINE & QUERY PROCESSING

The pipeline handles read/write data serialization, and the processing layer implements
pushdown query execution (filter, project, aggregate, resample).

- **Slicing correctness**: Default 100K rows x 127 columns. Off-by-one errors cause
  data loss or duplication.
- **Frame reconstruction**: Must preserve column ordering, index alignment, and dtypes.
- **Query clause composition**: Clauses must compose correctly
  (filter -> project -> aggregate -> concat).
- **Expression evaluation**: Type promotion must follow NumPy rules.
- **Aggregation correctness**: Handle NaN, empty groups, and overflow for sum/mean.
- **Resample operations**: Handle timezone-aware timestamps, DST transitions, irregular
  intervals.
- **String pool**: Pool corruption causes garbled string data.
- **Sparse data**: Correctly track present/absent values via bitmap format.

---

## 10. ASYNC & THREADING

The async infrastructure uses Folly executors with CPU and IO thread pools.

- **Thread pool sizing**: Unbounded task submission can exhaust memory.
- **Future composition**: Dropped futures silently discard errors.
- **Deadlock potential**: No `.get()` calls from within thread pool callbacks.
- **Exception propagation**: Exceptions must be captured in futures and re-thrown on
  `.get()`.

---

## 11. ARROW INTEGRATION

- **Arrow output format**: Must produce valid Arrow arrays/tables.
- **Memory interop**: Arrow buffers backed by ArcticDB's detachable allocator must have
  correct lifetime management.
- **Type mapping**: Arrow types must map correctly to ArcticDB's `ValueType` enum.
- **String format options**: Handle both small and large string types.

---

## 12. COLUMN STORE & DATA TYPES

- **Column type handling**: All `ValueType` variants must be handled in switch
  statements. Missing cases cause undefined behaviour.
- **Type promotion**: Mixed-type columns must promote correctly following descriptor
  merging logic.
- **Empty column handling**: Empty DataFrames, all-NaN columns, zero-row appends must
  not crash or corrupt state.
- **Statistics**: Must be updated correctly on append/update.
- **Memory segment**: Correct reference counting, bounds checking, iterator validity.

---

## 13. ENTITY & KEY TYPES

- **Key immutability**: `AtomKey` and `RefKey` should be treated as immutable after
  construction.
- **StreamDescriptor changes**: Must remain backwards-compatible.
- **FieldCollection**: Bounds-checked index-based access.
- **NativeTensor**: Correctly handle stride and shape matching numpy's memory layout.

---

## 14. PROTOBUF & SERIALIZATION

ArcticDB supports protobuf versions 3 through 6 via separate generated bindings.

- **Proto backwards compatibility**: No removed or renumbered fields. New fields must be
  `optional`.
- **Multi-version support**: Changes to `.proto` files must regenerate bindings for all
  supported versions.
- **msgpack compatibility**: Preserve wire format.
- **Protobuf mapping**: C++ proto-to-native type mappings must be correct.

---

## 15. CODE DUPLICATION

- **Copy-pasted logic**: Search the repo to confirm before duplicating. Extract shared
  functions or use existing utilities.
- **Parallel implementations**: C++ and Python implementations of the same concept must
  be consistent.
- **Storage backend boilerplate**: Use shared base class methods or templates.
- **Test duplication**: Parametrize or extend existing tests instead of near-identical
  new ones.
- **Error handling boilerplate**: Use existing error handling macros and utilities.
- **Switch statement duplication**: Use dispatch tables or templated approaches.

---

## 16. TESTING ADEQUACY

- **Test coverage**: Every behavioural change needs corresponding tests (Google Test for
  C++, pytest for Python).
- **Edge cases**: Empty DataFrames, single-row/single-column, max segment size,
  Unicode/special characters, concurrent access.
- **Integration tests**: Required for storage backend or version engine changes.
- **Property-based tests**: Useful for type promotion, slicing, aggregation.
- **Benchmark regression**: Watch for O(n^2) patterns, unnecessary copies, allocation
  hotspots.
- **Backwards compat tests**: Required for on-disk format changes.
- **Mock consistency**: Mocks must accurately represent real backend behaviour.

---

## 17. ERROR HANDLING & LOGGING

- **Error codes**: Use correct `ErrorCategory` and unique codes.
- **Preconditions**: Internal assertion macros for invariants, not user input validation.
- **User-facing errors**: Clear, actionable messages referencing the failed operation.
- **Log levels**: No debug logging in hot paths. No sensitive data in log output.
- **Storage exceptions**: Use storage-specific exception types.

---

## 18. BUILD SYSTEM & DEPENDENCIES

- **CMake targets**: New source files must be added to the correct target.
- **Python dependencies**: Changes to install_requires/build deps affect all users.
- **Submodule updates**: Verify new commits are from upstream on official branches/tags.
- **Compiler compatibility**: C++20 features must work on GCC, Clang, MSVC.
- **Sanitizer compatibility**: Code must work under ASan, TSan, UBSan.

### vcpkg & C++ Dependency Management

- **vcpkg submodule pin**: Must point to a specific commit on `microsoft/vcpkg` master.
- **Manifest version pinning**: All dependencies must have explicit version pins or
  overrides.
- **Version override consistency**: Overrides must match versions at the pinned vcpkg
  commit.
- **Custom overlay ports**: Must be updated when dependencies are upgraded.
- **Builtin-baseline**: Must match the checked-out vcpkg submodule commit hash.
- **New dependencies**: Must include version pin and not introduce license-incompatible
  transitive deps.
- **Removed dependencies**: Also remove overlay ports, CMake find modules, and
  CMakeLists usage.
- **Conda environment sync**: Update conda dev environment to match vcpkg version
  changes.

---

## 19. SECURITY

- **No hardcoded credentials**: Secrets, tokens, API keys must not appear in code or
  config.
- **Input validation at boundaries**: Validate URIs, symbol names, metadata, query
  expressions.
- **Buffer overflows**: Validate sizes before accessing memory in C++ code handling raw
  bytes.

---

## 20. PERFORMANCE

- **Unnecessary copies**: Use `std::move`, prefer `std::string_view` over
  `const std::string` parameters. Watch for pass-by-value of large types.
- **Allocation patterns**: Minimize allocations in hot paths. Prefer stack allocation,
  buffer reuse, or pool allocation.
- **Algorithmic complexity**: Watch for O(n^2) patterns in version chain traversal,
  segment iteration, and query processing.
- **Lazy evaluation**: Use lazy DataFrame evaluation where possible.

---

## REVIEW SUMMARY CHECKLIST

```markdown
## ArcticDB Code Review Summary

### API & Compatibility
- [ ] No breaking changes to public Python API (`Arctic`, `Library`, `NativeVersionStore`)
- [ ] Deprecation protocol followed for removed/renamed parameters
- [ ] On-disk format unchanged (or migration path documented)
- [ ] Protobuf schema backwards-compatible
- [ ] Key types and ValueType enum unchanged (or additive only)

### Memory & Safety
- [ ] RAII used for all resource management
- [ ] No use-after-move or use-after-free patterns
- [ ] Buffer size calculations correct (no overflow potential)
- [ ] GIL correctly managed at Python-C++ boundary
- [ ] No accidental copies of large objects

### Correctness
- [ ] All ValueType/KeyType switch statements exhaustive
- [ ] Edge cases handled (empty data, NaN, Unicode, concurrent access)
- [ ] Error codes unique and in correct ErrorCategory
- [ ] Storage backend interface fully implemented
- [ ] Query clause composition correct

### Code Quality
- [ ] No duplicated logic (search repo for existing utilities)
- [ ] C++ and Python implementations of shared concepts are consistent
- [ ] Tests are not duplicating existing test scenarios

### Testing
- [ ] Behavioural changes have corresponding tests
- [ ] Edge cases covered in tests
- [ ] Integration tests for storage/version engine changes
- [ ] No regression in existing test expectations

### Build & Dependencies
- [ ] New source files added to CMakeLists.txt
- [ ] Dependency changes justified and version-pinned
- [ ] Cross-platform compatibility maintained
- [ ] vcpkg submodule points to official upstream commit
- [ ] vcpkg manifest versions pinned and consistent with submodule baseline
- [ ] Custom overlay ports up-to-date with dependency versions
- [ ] Conda environment synced with vcpkg dependency versions

### Security
- [ ] No hardcoded credentials or secrets
- [ ] Input validation at system boundaries
- [ ] No buffer overflow potential in C++ code

### PR Title & Description
- [ ] Title is clear, concise, and uses imperative verb
- [ ] Title and description are free of typos and grammatical errors
- [ ] Description explains what changed and why
- [ ] All significant changes in the diff are mentioned in the description
- [ ] API/breaking changes explicitly called out in the description
- [ ] Linked issues referenced where applicable
- [ ] PR labelled appropriately (enhancement, bug, etc.)

### Documentation
- [ ] Public API docstrings accurate
- [ ] Breaking changes flagged with appropriate labels
```
