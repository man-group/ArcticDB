# Language Bindings (Java & .NET)

ArcticDB provides native language bindings for **Java** and **.NET** via a C shared library (`libarcticdb_c.so`). These bindings use the [Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html) for zero-copy data access.

## How It Works

```
Your Application (Java / .NET / ...)
        │
        ▼
Language Binding (ArcticLibrary wrapper)
        │
        ▼
libarcticdb_c.so (C API)
        │
        ▼
ArcticDB C++ Engine → LMDB Storage
```

The C API provides:

- **Library lifecycle** — open/close an LMDB-backed database
- **Symbol listing** — enumerate all symbols in a library
- **Streaming reads** — read data as Arrow record batches via `ArrowArrayStream`
- **Test data writes** — write synthetic numeric data for testing

## Prerequisites

Build `libarcticdb_c.so` from the ArcticDB source:

```bash
# Build the C shared library
cmake -DTEST=ON --preset linux-debug cpp
cmake --build cpp/out/linux-debug-build --target arcticdb_c
```

The shared library will be at `cpp/out/linux-debug-build/arcticdb/libarcticdb_c.so`.

## Java

### Requirements

- Java 21 (Panama FFM API, preview feature)
- Maven 3.5+

### Setup

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.arcticdb</groupId>
    <artifactId>arcticdb-java</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Configure the compiler and surefire plugins for Java 21 preview features:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <source>21</source>
        <target>21</target>
        <compilerArgs>
            <arg>--enable-preview</arg>
        </compilerArgs>
    </configuration>
</plugin>
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <argLine>
            --enable-preview
            --enable-native-access=ALL-UNNAMED
            -Darcticdb.native.path=${arcticdb.native.path}
        </argLine>
    </configuration>
</plugin>
```

### Usage

```java
import com.arcticdb.ArcticLibrary;

try (var lib = ArcticLibrary.openLmdb("/path/to/database")) {
    // Write test data: 1000 rows, 5 float64 columns
    lib.writeTestData("prices", 1000, 5);

    // List symbols
    List<String> symbols = lib.listSymbols();
    System.out.println("Symbols: " + symbols);

    // Read data as Arrow stream
    ArcticLibrary.ReadResult result = lib.readStream("prices");
    System.out.println("Rows: " + result.totalRows());
    System.out.println("Columns: " + result.columnNames());
    System.out.println("Batches: " + result.batchCount());

    // Read a specific version
    ArcticLibrary.ReadResult v0 = lib.readStream("prices", 0);
}
```

### Running Tests

```bash
cd java
JAVA_HOME=/path/to/java21 mvn test \
    -Darcticdb.native.path=/path/to/dir/containing/libarcticdb_c.so
```

### How It Works

The Java bindings use the [Foreign Function & Memory (FFM) API](https://openjdk.org/jeps/442) (Panama) introduced as a preview in Java 21. The library is loaded with `dlopen(RTLD_LAZY)` via FFM to defer resolution of unused symbols. Arrow function pointers in the stream struct are invoked through `Linker.downcallHandle()`.

## .NET

### Requirements

- .NET 8 SDK
- Linux x86_64 (for `libarcticdb_c.so`)

### Setup

Add a project reference to the `ArcticDB` library:

```xml
<ItemGroup>
    <ProjectReference Include="../ArcticDB/ArcticDB.csproj" />
</ItemGroup>
```

Set the `ARCTICDB_NATIVE_PATH` environment variable to the directory containing `libarcticdb_c.so`.

### Usage

```csharp
using ArcticDB;

using var lib = ArcticLibrary.OpenLmdb("/path/to/database");

// Write test data: 1000 rows, 5 float64 columns
lib.WriteTestData("prices", 1000, 5);

// List symbols
List<string> symbols = lib.ListSymbols();
Console.WriteLine($"Symbols: {string.Join(", ", symbols)}");

// Read data as Arrow stream
ReadResult result = lib.ReadStream("prices");
Console.WriteLine($"Rows: {result.TotalRows}");
Console.WriteLine($"Columns: {string.Join(", ", result.ColumnNames)}");
Console.WriteLine($"Batches: {result.BatchCount}");

// Read a specific version
ReadResult v0 = lib.ReadStream("prices", 0);
```

### Running Tests

```bash
cd dotnet
ARCTICDB_NATIVE_PATH=/path/to/dir/containing/libarcticdb_c.so \
    dotnet test
```

### How It Works

The .NET bindings use [P/Invoke](https://learn.microsoft.com/en-us/dotnet/standard/native-interop/pinvoke) with `DllImport` for calling the C API. A custom `DllImportResolver` locates `libarcticdb_c.so` via the `ARCTICDB_NATIVE_PATH` environment variable. Arrow function pointers are converted to callable delegates with `Marshal.GetDelegateForFunctionPointer<T>()`.

## Data Model

Both bindings return a `ReadResult` containing:

| Field | Description |
|-------|-------------|
| **Column names** | Names of data columns from the Arrow schema |
| **Total rows** | Sum of row counts across all Arrow record batches |
| **Batch count** | Number of Arrow record batches consumed |

The underlying data is transferred as Arrow record batches via the [Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html). Each batch contains the raw columnar data — future versions will expose the full Arrow arrays for direct processing.

## Limitations

- **LMDB backend only** — S3 and Azure backends are not yet supported via the C API
- **Read-only for real data** — `writeTestData()` is a test helper; writing arbitrary DataFrames requires the Python API
- **Linux x86_64 only** — the C shared library is currently built and tested on Linux
- **Streaming metadata not exposed** — the `ReadResult` provides summary statistics; raw Arrow array access is planned
