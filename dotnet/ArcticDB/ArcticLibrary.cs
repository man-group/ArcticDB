// Copyright 2026 Man Group Operations Limited
//
// Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, version 2.0.

using System.Runtime.InteropServices;

namespace ArcticDB;

/// <summary>
/// High-level wrapper around the ArcticDB C API.
/// Implements <see cref="IDisposable"/> for deterministic resource cleanup.
/// </summary>
/// <example>
/// <code>
/// using var lib = ArcticLibrary.OpenLmdb("/tmp/test_db");
/// lib.WriteTestData("prices", 1000, 5);
/// var result = lib.ReadStream("prices");
/// Console.WriteLine($"Read {result.TotalRows} rows");
/// </code>
/// </example>
public class ArcticLibrary : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    private ArcticLibrary(IntPtr handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Open an LMDB-backed ArcticDB library at the given path.
    /// </summary>
    /// <param name="path">Filesystem path for LMDB storage (created if absent).</param>
    /// <returns>A new ArcticLibrary instance (caller must dispose).</returns>
    public static ArcticLibrary OpenLmdb(string path)
    {
        var err = new ArcticNative.ArcticError();
        int rc = ArcticNative.arctic_library_open_lmdb(path, out IntPtr handle, ref err);
        ArcticNative.CheckError(rc, ref err);
        return new ArcticLibrary(handle);
    }

    /// <summary>
    /// Write synthetic test data: a timeseries-indexed DataFrame with float64 columns.
    /// </summary>
    /// <param name="symbol">Symbol name.</param>
    /// <param name="numRows">Number of rows.</param>
    /// <param name="numColumns">Number of float64 columns (named col_0..col_N).</param>
    public void WriteTestData(string symbol, long numRows, long numColumns)
    {
        var err = new ArcticNative.ArcticError();
        int rc = ArcticNative.arctic_write_test_data(_handle, symbol, numRows, numColumns, ref err);
        ArcticNative.CheckError(rc, ref err);
    }

    /// <summary>
    /// Read the latest version of a symbol as a streaming Arrow result.
    /// </summary>
    public ReadResult ReadStream(string symbol) => ReadStream(symbol, -1);

    /// <summary>
    /// Read a specific version of a symbol as a streaming Arrow result.
    /// </summary>
    /// <param name="symbol">Symbol name.</param>
    /// <param name="version">Version number, or -1 for latest.</param>
    /// <returns>Summary of the data read.</returns>
    public ReadResult ReadStream(string symbol, long version)
    {
        var stream = new ArcticNative.ArcticArrowArrayStream();
        var err = new ArcticNative.ArcticError();
        int rc = ArcticNative.arctic_read_stream(_handle, symbol, version, ref stream, ref err);
        ArcticNative.CheckError(rc, ref err);

        try
        {
            // 1. Get schema
            var schema = new ArcticNative.ArrowSchema();
            var getSchema = Marshal.GetDelegateForFunctionPointer<ArcticNative.GetSchemaDelegate>(stream.GetSchema);
            int schemaRc = getSchema(ref stream, ref schema);
            if (schemaRc != 0)
                throw new ArcticException(schemaRc, "get_schema failed");

            // Read column names from schema children
            var columnNames = new List<string>();
            if (schema.NChildren > 0 && schema.Children != IntPtr.Zero)
            {
                for (long i = 0; i < schema.NChildren; i++)
                {
                    IntPtr childPtr = Marshal.ReadIntPtr(schema.Children, (int)(i * IntPtr.Size));
                    if (childPtr != IntPtr.Zero)
                    {
                        var child = Marshal.PtrToStructure<ArcticNative.ArrowSchema>(childPtr);
                        if (child.Name != IntPtr.Zero)
                        {
                            string? name = Marshal.PtrToStringUTF8(child.Name);
                            if (name != null)
                                columnNames.Add(name);
                        }
                    }
                }
            }

            // Release schema
            if (schema.Release != IntPtr.Zero)
            {
                var releaseSchema = Marshal.GetDelegateForFunctionPointer<ArcticNative.ReleaseArrowDelegate>(schema.Release);
                releaseSchema(ref schema);
            }

            // 2. Consume batches
            long totalRows = 0;
            int batchCount = 0;

            var getNext = Marshal.GetDelegateForFunctionPointer<ArcticNative.GetNextDelegate>(stream.GetNext);

            while (true)
            {
                var array = new ArcticNative.ArrowArray();
                int nextRc = getNext(ref stream, ref array);
                if (nextRc != 0)
                    throw new ArcticException(nextRc, "get_next failed");

                // release == NULL means end of stream
                if (array.Release == IntPtr.Zero)
                    break;

                totalRows += array.Length;
                batchCount++;

                // Release this array
                var releaseArray = Marshal.GetDelegateForFunctionPointer<ArcticNative.ReleaseArrayDelegate>(array.Release);
                releaseArray(ref array);
            }

            return new ReadResult(columnNames, totalRows, batchCount);
        }
        finally
        {
            // 3. Release stream
            if (stream.Release != IntPtr.Zero)
            {
                var releaseStream = Marshal.GetDelegateForFunctionPointer<ArcticNative.ReleaseStreamDelegate>(stream.Release);
                releaseStream(ref stream);
            }
        }
    }

    /// <summary>
    /// List all symbols in this library.
    /// </summary>
    public List<string> ListSymbols()
    {
        var err = new ArcticNative.ArcticError();
        int rc = ArcticNative.arctic_list_symbols(_handle, out IntPtr symbolsPtr, out long count, ref err);
        ArcticNative.CheckError(rc, ref err);

        var result = new List<string>();
        if (count > 0 && symbolsPtr != IntPtr.Zero)
        {
            for (long i = 0; i < count; i++)
            {
                IntPtr strPtr = Marshal.ReadIntPtr(symbolsPtr, (int)(i * IntPtr.Size));
                string? s = Marshal.PtrToStringUTF8(strPtr);
                if (s != null) result.Add(s);
            }
            ArcticNative.arctic_free_symbols(symbolsPtr, count);
        }

        return result;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            if (_handle != IntPtr.Zero)
            {
                ArcticNative.arctic_library_close(_handle);
                _handle = IntPtr.Zero;
            }
            GC.SuppressFinalize(this);
        }
    }

    ~ArcticLibrary()
    {
        Dispose();
    }
}

/// <summary>
/// Summary of data read from an Arrow stream.
/// </summary>
/// <param name="ColumnNames">Names of data columns (excludes the index).</param>
/// <param name="TotalRows">Total number of rows across all batches.</param>
/// <param name="BatchCount">Number of Arrow record batches consumed.</param>
public record ReadResult(
    List<string> ColumnNames,
    long TotalRows,
    int BatchCount
);
