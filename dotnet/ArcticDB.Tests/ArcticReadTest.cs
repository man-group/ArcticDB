// Copyright 2026 Man Group Operations Limited
//
// Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, version 2.0.

using Xunit;

namespace ArcticDB.Tests;

/// <summary>
/// Integration tests for ArcticDB .NET bindings.
///
/// Requires ARCTICDB_NATIVE_PATH environment variable pointing to the directory
/// containing libarcticdb_c.so.
/// </summary>
public class ArcticReadTest : IDisposable
{
    private readonly string _tempDir;

    public ArcticReadTest()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"arcticdb_dotnet_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); }
            catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void TestOpenClose()
    {
        using var lib = ArcticLibrary.OpenLmdb(Path.Combine(_tempDir, "db1"));
        Assert.NotNull(lib);
    }

    [Fact]
    public void TestWriteAndListSymbols()
    {
        using var lib = ArcticLibrary.OpenLmdb(Path.Combine(_tempDir, "db2"));
        lib.WriteTestData("sym_a", 10, 2);
        lib.WriteTestData("sym_b", 20, 3);

        var symbols = lib.ListSymbols();
        Assert.Equal(2, symbols.Count);
        Assert.Contains("sym_a", symbols);
        Assert.Contains("sym_b", symbols);
    }

    [Fact]
    public void TestReadStream()
    {
        using var lib = ArcticLibrary.OpenLmdb(Path.Combine(_tempDir, "db3"));
        lib.WriteTestData("prices", 100, 3);

        var result = lib.ReadStream("prices");

        Assert.Equal(100, result.TotalRows);
        Assert.True(result.BatchCount >= 1);
        // The schema includes the timestamp index + 3 data columns
        Assert.Contains(result.ColumnNames, n => n.Contains("col_0"));
        Assert.Contains(result.ColumnNames, n => n.Contains("col_1"));
        Assert.Contains(result.ColumnNames, n => n.Contains("col_2"));
    }

    [Fact]
    public void TestReadSpecificVersion()
    {
        using var lib = ArcticLibrary.OpenLmdb(Path.Combine(_tempDir, "db4"));
        lib.WriteTestData("versioned", 50, 2);  // version 0
        lib.WriteTestData("versioned", 75, 2);  // version 1

        var v0 = lib.ReadStream("versioned", 0);
        Assert.Equal(50, v0.TotalRows);

        var v1 = lib.ReadStream("versioned", 1);
        Assert.Equal(75, v1.TotalRows);

        // Latest should be v1
        var latest = lib.ReadStream("versioned");
        Assert.Equal(75, latest.TotalRows);
    }

    [Fact]
    public void TestReadMissingSymbolThrows()
    {
        using var lib = ArcticLibrary.OpenLmdb(Path.Combine(_tempDir, "db5"));
        Assert.Throws<ArcticException>(() => lib.ReadStream("nonexistent"));
    }
}
