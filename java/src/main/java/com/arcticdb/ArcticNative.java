/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

package com.arcticdb;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * Low-level FFM (Panama) bindings to libarcticdb_c.so.
 *
 * <p>Provides MethodHandles for each C API function and struct layouts matching the
 * x86_64 Linux ABI. All methods are static; callers are responsible for memory management
 * via {@link Arena}.
 */
public final class ArcticNative {

    private ArcticNative() {}

    // ── Struct Layouts ──────────────────────────────────────────────────

    /** ArcticError: { int code; char message[512]; } → 516 bytes */
    public static final StructLayout ARCTIC_ERROR_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_INT.withName("code"),
            MemoryLayout.sequenceLayout(512, ValueLayout.JAVA_BYTE).withName("message")
    );

    /** ArcticArrowArrayStream: 5 pointers (get_schema, get_next, get_last_error, release, private_data) → 40 bytes */
    public static final StructLayout STREAM_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("get_schema"),
            ValueLayout.ADDRESS.withName("get_next"),
            ValueLayout.ADDRESS.withName("get_last_error"),
            ValueLayout.ADDRESS.withName("release"),
            ValueLayout.ADDRESS.withName("private_data")
    );

    /** ArrowSchema: 72 bytes on x86_64 */
    public static final StructLayout ARROW_SCHEMA_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("format"),       // 0
            ValueLayout.ADDRESS.withName("name"),         // 8
            ValueLayout.ADDRESS.withName("metadata"),     // 16
            ValueLayout.JAVA_LONG.withName("flags"),      // 24
            ValueLayout.JAVA_LONG.withName("n_children"), // 32
            ValueLayout.ADDRESS.withName("children"),     // 40
            ValueLayout.ADDRESS.withName("dictionary"),   // 48
            ValueLayout.ADDRESS.withName("release"),      // 56
            ValueLayout.ADDRESS.withName("private_data")  // 64
    );

    /** ArrowArray: 80 bytes on x86_64 */
    public static final StructLayout ARROW_ARRAY_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("length"),      // 0
            ValueLayout.JAVA_LONG.withName("null_count"),  // 8
            ValueLayout.JAVA_LONG.withName("offset"),      // 16
            ValueLayout.JAVA_LONG.withName("n_buffers"),   // 24
            ValueLayout.JAVA_LONG.withName("n_children"),  // 32
            ValueLayout.ADDRESS.withName("buffers"),       // 40
            ValueLayout.ADDRESS.withName("children"),      // 48
            ValueLayout.ADDRESS.withName("dictionary"),    // 56
            ValueLayout.ADDRESS.withName("release"),       // 64
            ValueLayout.ADDRESS.withName("private_data")   // 72
    );

    // ── Library + Function Handles ──────────────────────────────────────

    static final SymbolLookup LIB;
    static final Linker LINKER = Linker.nativeLinker();

    static final MethodHandle LIBRARY_OPEN_LMDB;
    static final MethodHandle LIBRARY_CLOSE;
    static final MethodHandle WRITE_TEST_DATA;
    static final MethodHandle READ_STREAM;
    static final MethodHandle LIST_SYMBOLS;
    static final MethodHandle FREE_SYMBOLS;

    // RTLD_LAZY defers resolution of unused symbols (Python symbols in arcticdb_core_static)
    private static final int RTLD_LAZY = 0x00001;

    /**
     * Load a shared library with dlopen(RTLD_LAZY) and return a SymbolLookup backed by dlsym.
     * This is necessary because libarcticdb_c.so contains unresolved Python symbols from
     * arcticdb_core_static that are never called at runtime from the C API.
     */
    private static SymbolLookup lazyLoad(String libPath) {
        Linker linker = Linker.nativeLinker();
        // dlopen/dlsym are in libc on glibc ≥ 2.34; also search libdl.so.2 as fallback
        MemorySegment dlopenSym = linker.defaultLookup().find("dlopen").orElse(null);
        if (dlopenSym == null) {
            // Try libdl.so.2 explicitly
            SymbolLookup libdl = SymbolLookup.libraryLookup("libdl.so.2", Arena.global());
            dlopenSym = libdl.find("dlopen").orElseThrow(() ->
                    new UnsatisfiedLinkError("Cannot find dlopen"));
        }
        MemorySegment dlsymSym = linker.defaultLookup().find("dlsym").orElse(null);
        if (dlsymSym == null) {
            SymbolLookup libdl = SymbolLookup.libraryLookup("libdl.so.2", Arena.global());
            dlsymSym = libdl.find("dlsym").orElseThrow(() ->
                    new UnsatisfiedLinkError("Cannot find dlsym"));
        }

        try {
            MethodHandle dlopen = linker.downcallHandle(dlopenSym,
                    FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            MethodHandle dlsym = linker.downcallHandle(dlsymSym,
                    FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

            MemorySegment pathSeg = Arena.global().allocateUtf8String(libPath);
            MemorySegment handle = (MemorySegment) dlopen.invokeExact(pathSeg, RTLD_LAZY);
            if (handle.address() == 0) {
                throw new UnsatisfiedLinkError("dlopen failed for: " + libPath);
            }

            final MemorySegment libHandle = handle;
            final MethodHandle dlsymHandle = dlsym;
            return name -> {
                try {
                    MemorySegment nameSeg = Arena.global().allocateUtf8String(name);
                    MemorySegment sym = (MemorySegment) dlsymHandle.invokeExact(libHandle, nameSeg);
                    return sym.address() == 0
                            ? java.util.Optional.empty()
                            : java.util.Optional.of(sym);
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            };
        } catch (Throwable t) {
            if (t instanceof RuntimeException re) throw re;
            throw new RuntimeException("Failed to load native library: " + libPath, t);
        }
    }

    static {
        String nativePath = System.getProperty("arcticdb.native.path", ".");
        Path libPath = Path.of(nativePath).resolve("libarcticdb_c.so").toAbsolutePath();
        LIB = lazyLoad(libPath.toString());

        LIBRARY_OPEN_LMDB = downcall("arctic_library_open_lmdb",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        LIBRARY_CLOSE = downcall("arctic_library_close",
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

        WRITE_TEST_DATA = downcall("arctic_write_test_data",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        READ_STREAM = downcall("arctic_read_stream",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        LIST_SYMBOLS = downcall("arctic_list_symbols",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        FREE_SYMBOLS = downcall("arctic_free_symbols",
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    }

    private static MethodHandle downcall(String name, FunctionDescriptor desc) {
        return LINKER.downcallHandle(LIB.find(name).orElseThrow(() ->
                new UnsatisfiedLinkError("Symbol not found: " + name)), desc);
    }

    // ── Function pointer invocation helpers for ArrowArrayStream ────────

    private static final FunctionDescriptor GET_SCHEMA_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

    private static final FunctionDescriptor GET_NEXT_DESC =
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

    private static final FunctionDescriptor RELEASE_DESC =
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

    /** Call stream->get_schema(stream, schemaOut) */
    public static int callGetSchema(MemorySegment stream, MemorySegment schemaOut) throws Throwable {
        MemorySegment fnPtr = stream.get(ValueLayout.ADDRESS, 0); // get_schema at offset 0
        MethodHandle mh = LINKER.downcallHandle(fnPtr, GET_SCHEMA_DESC);
        return (int) mh.invokeExact(stream, schemaOut);
    }

    /** Call stream->get_next(stream, arrayOut) */
    public static int callGetNext(MemorySegment stream, MemorySegment arrayOut) throws Throwable {
        MemorySegment fnPtr = stream.get(ValueLayout.ADDRESS, 8); // get_next at offset 8
        MethodHandle mh = LINKER.downcallHandle(fnPtr, GET_NEXT_DESC);
        return (int) mh.invokeExact(stream, arrayOut);
    }

    /** Call stream->release(stream) */
    public static void callStreamRelease(MemorySegment stream) throws Throwable {
        MemorySegment fnPtr = stream.get(ValueLayout.ADDRESS, 24); // release at offset 24
        if (fnPtr.equals(MemorySegment.NULL)) return;
        MethodHandle mh = LINKER.downcallHandle(fnPtr, RELEASE_DESC);
        mh.invokeExact(stream);
    }

    /** Call schema->release(schema) or array->release(array) */
    public static void callArrowRelease(MemorySegment arrowStruct, long releaseOffset) throws Throwable {
        MemorySegment fnPtr = arrowStruct.get(ValueLayout.ADDRESS, releaseOffset);
        if (fnPtr.equals(MemorySegment.NULL)) return;
        MethodHandle mh = LINKER.downcallHandle(fnPtr, RELEASE_DESC);
        mh.invokeExact(arrowStruct);
    }

    // ── Error checking ──────────────────────────────────────────────────

    /** If rc != 0, read the error message from errSeg and throw RuntimeException. */
    public static void checkError(int rc, MemorySegment errSeg) {
        if (rc != 0) {
            int code = errSeg.get(ValueLayout.JAVA_INT, 0);
            String msg = errSeg.getUtf8String(4); // message starts at offset 4
            throw new RuntimeException("ArcticDB error " + code + ": " + msg);
        }
    }
}
