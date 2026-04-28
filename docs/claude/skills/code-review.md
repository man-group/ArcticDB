# Code Review Skill

This document provides instructions for reviewing changes on a branch before submitting upstream.

## Overview

When asked to review a branch, use sub-agents to review different aspects in parallel for efficiency. Write findings to a plan document under `docs/claude/plans/` for tracking and fixing issues.

## Getting Branch Changes

```bash
# See all files changed on the branch
git diff --name-only $(git merge-base HEAD master)..HEAD

# See full diff
git diff $(git merge-base HEAD master)..HEAD

# List changed files by type
git diff --name-only $(git merge-base HEAD master)..HEAD | grep '\.cpp$\|\.hpp$'  # C++
git diff --name-only $(git merge-base HEAD master)..HEAD | grep '\.py$'           # Python
```

## Review Categories

Launch parallel sub-agents for each category relevant to the changes:

1. **C++ Memory Safety** - For any C++ changes
2. **Python Code Quality** - For any Python changes
3. **Test Coverage** - For all changes
4. **Type Handling** - For changes involving data types

---

## C++ Memory Safety

Review all C++ changes for:

### Resource Management (Rule of Five)

Classes holding resources (pointers, file handles, Arrow structures) must implement:
- Destructor
- Copy constructor (or delete it)
- Copy assignment operator (or delete it)
- Move constructor
- Move assignment operator

```cpp
// Example: Proper resource management
class ResourceHolder {
public:
    ResourceHolder() : data_(nullptr) {}

    // Destructor - release resources
    ~ResourceHolder() { cleanup(); }

    // Delete copy operations to prevent double-free
    ResourceHolder(const ResourceHolder&) = delete;
    ResourceHolder& operator=(const ResourceHolder&) = delete;

    // Move constructor - transfer ownership
    ResourceHolder(ResourceHolder&& other) noexcept : data_(other.data_) {
        other.data_ = nullptr;
    }

    // Move assignment - transfer ownership
    ResourceHolder& operator=(ResourceHolder&& other) noexcept {
        if (this != &other) {
            cleanup();
            data_ = other.data_;
            other.data_ = nullptr;
        }
        return *this;
    }

private:
    void cleanup() { delete data_; data_ = nullptr; }
    SomeResource* data_;
};
```

### Arrow C Data Interface

`ArrowArray` and `ArrowSchema` require calling their `release` callbacks:

```cpp
struct ArrowDataHolder {
    ArrowArray array_;
    ArrowSchema schema_;

    ArrowDataHolder() {
        array_.release = nullptr;
        schema_.release = nullptr;
    }

    ~ArrowDataHolder() {
        if (array_.release != nullptr) {
            array_.release(&array_);
        }
        if (schema_.release != nullptr) {
            schema_.release(&schema_);
        }
    }

    // Move operations must null out source release pointers
    ArrowDataHolder(ArrowDataHolder&& other) noexcept
        : array_(other.array_), schema_(other.schema_) {
        other.array_.release = nullptr;
        other.schema_.release = nullptr;
    }
};
```

### Other C++ Checks

- **Smart pointer usage**: Prefer `std::shared_ptr`/`std::unique_ptr` over raw pointers
- **RAII violations**: Look for `new` without corresponding `delete`
- **Use-after-move**: Ensure moved-from objects aren't accessed
- **Thread safety**: Shared mutable state needs synchronization (`std::mutex`, `std::atomic`)
- **Exception safety**: Resources acquired before exceptions must be released

---

## Python Code Quality

Review all Python changes for:

### Silent Exception Swallowing

**Bad:**
```python
try:
    do_something()
except Exception:
    pass  # Hides bugs, makes debugging impossible
```

**Good:**
```python
import logging
logger = logging.getLogger(__name__)

try:
    do_something()
except Exception as e:
    logger.debug("Failed to do something: %s", e)
    # Continue with fallback behavior
```

### Duplicate Code

Extract shared logic into helper functions:

```python
# Bad: Duplicated logic
def process_a(data):
    # 20 lines of parsing logic
    result = parse(data)
    return result + "_a"

def process_b(data):
    # Same 20 lines of parsing logic
    result = parse(data)
    return result + "_b"

# Good: Shared helper
def _parse_common(data):
    # 20 lines of parsing logic
    return parse(data)

def process_a(data):
    return _parse_common(data) + "_a"

def process_b(data):
    return _parse_common(data) + "_b"
```

### Duplicate Work

Look for repeated expensive operations that could be cached or combined:

```python
# Bad: Parses SQL twice
symbols = extract_symbols_from_sql(query)  # Parses SQL
pushdown = extract_pushdown_from_sql(query)  # Parses SQL again

# Good: Single parse
pushdown, symbols = extract_pushdown_from_sql(query)  # Returns both
```

### State Management

Mutable state should be validated before use:

```python
class Iterator:
    def __init__(self):
        self._exhausted = False

    def __iter__(self):
        if self._exhausted:
            raise RuntimeError(
                "Cannot iterate over exhausted iterator. "
                "Create a new instance to iterate again."
            )
        return self
```

### API Consistency

Public methods should validate inputs and provide helpful error messages:

```python
def query(self, sql: str) -> pd.DataFrame:
    if self._connection is None:
        raise RuntimeError("Must be used within a 'with' block")

    if not self._registered_tables:
        raise RuntimeError(
            "No tables registered. "
            "Use register_table() before querying."
        )

    return self._execute(sql)
```

---

## Test Coverage Analysis

For each new/modified module, verify:

### Happy Path Tests

Basic functionality works as documented:

```python
def test_basic_query(self, library):
    df = pd.DataFrame({"x": [1, 2, 3]})
    library.write("symbol", df)

    result = library.sql("SELECT * FROM symbol")

    assert len(result) == 3
```

### Error Handling Tests

Invalid inputs raise appropriate exceptions:

```python
def test_query_without_registration_raises(self, library):
    with library.context() as ctx:
        with pytest.raises(RuntimeError, match="No tables registered"):
            ctx.query("SELECT * FROM nonexistent")

def test_invalid_sql_raises(self, library):
    with pytest.raises(ValueError, match="Could not parse"):
        library.sql("SLECT * FORM invalid")
```

### Edge Cases

```python
def test_empty_dataframe(self, library):
    df = pd.DataFrame({"x": pd.Series([], dtype=np.int64)})
    library.write("empty", df)
    result = library.sql("SELECT * FROM empty")
    assert len(result) == 0

def test_null_values(self, library):
    df = pd.DataFrame({"x": [1, None, 3]})
    library.write("nulls", df)
    result = library.sql("SELECT * FROM nulls WHERE x IS NOT NULL")
    assert len(result) == 2

def test_special_characters(self, library):
    df = pd.DataFrame({"text": ["hello", "world's", '"quoted"']})
    library.write("special", df)
    result = library.sql("SELECT * FROM special")
    assert len(result) == 3

def test_special_float_values(self, library):
    df = pd.DataFrame({"x": [1.0, float("inf"), float("nan")]})
    library.write("floats", df)
    result = library.sql("SELECT * FROM floats WHERE x = 1.0")
    assert len(result) == 1
```

### Parameter Coverage

Each public parameter has at least one test:

```python
def test_with_as_of_version(self, library):
    library.write("sym", pd.DataFrame({"x": [1]}))  # v0
    library.write("sym", pd.DataFrame({"x": [2]}))  # v1

    result = library.read("sym", as_of=0)
    assert result["x"].iloc[0] == 1

def test_with_row_range(self, library):
    df = pd.DataFrame({"x": range(100)})
    library.write("sym", df)

    result = library.read("sym", row_range=(10, 20))
    assert len(result) == 10
```

### Code Path Coverage

Each branch/condition is exercised:

```python
# For code like:
# if self._exhausted:
#     return None
# else:
#     return self._get_next()

def test_returns_none_when_exhausted(self):
    reader = create_reader()
    while reader.read_next() is not None:
        pass
    assert reader.read_next() is None  # Tests exhausted branch

def test_returns_data_when_not_exhausted(self):
    reader = create_reader()
    assert reader.read_next() is not None  # Tests non-exhausted branch
```

---

## Error Handling Review

### Fail Fast

Validate preconditions early:

```python
def process(self, data: pd.DataFrame) -> pd.DataFrame:
    # Validate at entry point, not deep in the call stack
    if data.empty:
        raise ValueError("Input DataFrame cannot be empty")

    if "required_column" not in data.columns:
        raise ValueError(
            f"Missing required column 'required_column'. "
            f"Available columns: {list(data.columns)}"
        )

    return self._do_processing(data)
```

### Helpful Error Messages

Error messages should explain what went wrong AND how to fix it:

```python
# Bad
raise ValueError("Invalid input")

# Good
raise ValueError(
    f"Expected output_format to be one of 'pandas', 'arrow', 'polars', "
    f"but got '{output_format}'"
)

# Good - with recovery instructions
raise RuntimeError(
    "Cannot iterate over exhausted reader. "
    "ArcticRecordBatchReader is single-use - create a new reader to iterate again."
)
```

### Exception Types

Use appropriate exception types:
- `ValueError` - Invalid argument values
- `TypeError` - Wrong argument types
- `RuntimeError` - Invalid state or operation
- `KeyError` - Missing keys/symbols
- `FileNotFoundError` - Missing files
- `ImportError` - Missing optional dependencies

---

## Type Handling (ArcticDB-specific)

When adding new data type support, verify handling of all variants:

### Numeric Types
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`

### Temporal Types
- `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]`
- `date32`, `date64`
- `time32`, `time64`
- `duration[s/ms/us/ns]`

### String Types
- `string`, `large_string`
- `binary`, `large_binary`

### Complex Types
- `decimal128`, `decimal256`
- `list`, `large_list`
- `struct`
- `map`

### Null Handling
- All types should handle null/NA values correctly
- Test with all-null columns
- Test with mixed null/non-null values

---

## Documentation Review

### Docstrings

Public functions/classes need complete docstrings:

```python
def read(
    self,
    symbol: str,
    as_of: Optional[int] = None,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Read data for a symbol from the library.

    Parameters
    ----------
    symbol : str
        The symbol name to read.
    as_of : int, optional
        Version number to read. Default is latest version.
    columns : list of str, optional
        Subset of columns to read. Default is all columns.

    Returns
    -------
    pd.DataFrame
        The data for the requested symbol and version.

    Raises
    ------
    KeyError
        If the symbol does not exist.
    ValueError
        If as_of refers to a non-existent version.

    Examples
    --------
    >>> df = library.read("my_symbol")
    >>> df = library.read("my_symbol", as_of=0, columns=["price", "volume"])
    """
```

### Type Hints

All public function signatures should have type annotations.

---

## Performance Considerations

### Unnecessary Copies

```python
# Bad: Creates copy of large list
def process(items):
    items_copy = list(items)  # Unnecessary copy
    return [x * 2 for x in items_copy]

# Good: Iterate directly
def process(items):
    return [x * 2 for x in items]
```

### Lazy Evaluation

```python
# Bad: Loads all data upfront
def get_all_data(symbols):
    return [load_data(s) for s in symbols]  # Loads everything into memory

# Good: Generator for lazy evaluation
def get_all_data(symbols):
    for s in symbols:
        yield load_data(s)  # Loads one at a time
```

### Memory Efficiency

```python
# Bad: Loads entire dataset into memory
table = reader.read_all()
filtered = table.filter(condition)

# Good: Stream and filter
for batch in reader:
    filtered_batch = batch.filter(condition)
    yield filtered_batch
```

### Algorithmic Complexity

Watch for O(n²) or worse in hot paths:

```python
# Bad: O(n²) - nested loop
def find_duplicates(items):
    duplicates = []
    for i, item in enumerate(items):
        for j, other in enumerate(items):
            if i != j and item == other:
                duplicates.append(item)
    return duplicates

# Good: O(n) - use set
def find_duplicates(items):
    seen = set()
    duplicates = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
        seen.add(item)
    return list(duplicates)
```

---

## Review Output

After completing the review, create a plan document at `docs/claude/plans/<branch-name>-review.md` with:

1. **Summary**: Brief overview of changes reviewed
2. **Issues Found**: Categorized by severity (Critical, High, Medium, Low)
3. **Recommendations**: Suggested fixes for each issue
4. **Test Gaps**: Missing test coverage identified

Then fix issues in order of severity, adding tests for each bug before fixing it.
