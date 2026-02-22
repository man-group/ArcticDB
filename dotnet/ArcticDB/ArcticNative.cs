// Copyright 2026 Man Group Operations Limited
//
// Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, version 2.0.

using System.Runtime.InteropServices;

namespace ArcticDB;

/// <summary>
/// Low-level P/Invoke bindings to libarcticdb_c.so.
/// </summary>
public static class ArcticNative
{
    /// <summary>
    /// Resolves the native library path from the ARCTICDB_NATIVE_PATH environment variable
    /// or falls back to system library search.
    /// </summary>
    static ArcticNative()
    {
        NativeLibrary.SetDllImportResolver(typeof(ArcticNative).Assembly, (name, assembly, path) =>
        {
            if (name != "arcticdb_c") return IntPtr.Zero;

            var envPath = Environment.GetEnvironmentVariable("ARCTICDB_NATIVE_PATH");
            if (!string.IsNullOrEmpty(envPath))
            {
                var fullPath = Path.Combine(envPath, "libarcticdb_c.so");
                if (NativeLibrary.TryLoad(fullPath, out var handle))
                    return handle;
            }

            return IntPtr.Zero;
        });
    }

    // ── Structs ────────────────────────────────────────────────────────

    /// <summary>ArcticError: { int code; char message[512]; }</summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct ArcticError
    {
        public int Code;
        public fixed byte Message[512];

        public string GetMessage()
        {
            fixed (byte* ptr = Message)
            {
                return Marshal.PtrToStringUTF8((IntPtr)ptr) ?? string.Empty;
            }
        }
    }

    /// <summary>Arrow C Stream Interface: 5 function pointers.</summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct ArcticArrowArrayStream
    {
        public IntPtr GetSchema;    // int (*)(stream*, ArrowSchema*)
        public IntPtr GetNext;      // int (*)(stream*, ArrowArray*)
        public IntPtr GetLastError; // const char* (*)(stream*)
        public IntPtr Release;      // void (*)(stream*)
        public IntPtr PrivateData;
    }

    /// <summary>ArrowSchema (72 bytes on x86_64)</summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct ArrowSchema
    {
        public IntPtr Format;      // const char*
        public IntPtr Name;        // const char*
        public IntPtr Metadata;    // const char*
        public long Flags;
        public long NChildren;
        public IntPtr Children;    // ArrowSchema**
        public IntPtr Dictionary;  // ArrowSchema*
        public IntPtr Release;     // void (*)(ArrowSchema*)
        public IntPtr PrivateData;
    }

    /// <summary>ArrowArray (80 bytes on x86_64)</summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct ArrowArray
    {
        public long Length;
        public long NullCount;
        public long Offset;
        public long NBuffers;
        public long NChildren;
        public IntPtr Buffers;     // const void**
        public IntPtr Children;    // ArrowArray**
        public IntPtr Dictionary;  // ArrowArray*
        public IntPtr Release;     // void (*)(ArrowArray*)
        public IntPtr PrivateData;
    }

    // ── Delegates for function pointers ────────────────────────────────

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int GetSchemaDelegate(ref ArcticArrowArrayStream stream, ref ArrowSchema schemaOut);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int GetNextDelegate(ref ArcticArrowArrayStream stream, ref ArrowArray arrayOut);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void ReleaseStreamDelegate(ref ArcticArrowArrayStream stream);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void ReleaseArrowDelegate(ref ArrowSchema schema);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void ReleaseArrayDelegate(ref ArrowArray array);

    // ── P/Invoke imports ───────────────────────────────────────────────

    [DllImport("arcticdb_c", CallingConvention = CallingConvention.Cdecl)]
    public static extern int arctic_library_open_lmdb(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path,
        out IntPtr libraryOut,
        ref ArcticError err);

    [DllImport("arcticdb_c", CallingConvention = CallingConvention.Cdecl)]
    public static extern void arctic_library_close(IntPtr lib);

    [DllImport("arcticdb_c", CallingConvention = CallingConvention.Cdecl)]
    public static extern int arctic_write_test_data(
        IntPtr lib,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string symbol,
        long numRows,
        long numColumns,
        ref ArcticError err);

    [DllImport("arcticdb_c", CallingConvention = CallingConvention.Cdecl)]
    public static extern int arctic_read_stream(
        IntPtr lib,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string symbol,
        long version,
        ref ArcticArrowArrayStream streamOut,
        ref ArcticError err);

    [DllImport("arcticdb_c", CallingConvention = CallingConvention.Cdecl)]
    public static extern int arctic_list_symbols(
        IntPtr lib,
        out IntPtr symbolsOut,
        out long countOut,
        ref ArcticError err);

    [DllImport("arcticdb_c", CallingConvention = CallingConvention.Cdecl)]
    public static extern void arctic_free_symbols(IntPtr symbols, long count);

    // ── Error checking ─────────────────────────────────────────────────

    public static void CheckError(int rc, ref ArcticError err)
    {
        if (rc != 0)
        {
            throw new ArcticException(err.Code, err.GetMessage());
        }
    }
}

/// <summary>
/// Exception thrown when an ArcticDB C API call fails.
/// </summary>
public class ArcticException : Exception
{
    public int ErrorCode { get; }

    public ArcticException(int errorCode, string message)
        : base($"ArcticDB error {errorCode}: {message}")
    {
        ErrorCode = errorCode;
    }
}
