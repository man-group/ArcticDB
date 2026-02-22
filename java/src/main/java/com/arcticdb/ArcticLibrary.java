/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

package com.arcticdb;

import java.lang.foreign.*;
import java.util.ArrayList;
import java.util.List;

/**
 * High-level wrapper around the ArcticDB C API.
 *
 * <p>Implements {@link AutoCloseable} for deterministic resource cleanup.
 * Each instance owns a confined {@link Arena} that is closed when the library is closed.
 *
 * <pre>{@code
 * try (var lib = ArcticLibrary.openLmdb("/tmp/test_db")) {
 *     lib.writeTestData("prices", 1000, 5);
 *     ReadResult result = lib.readStream("prices");
 *     System.out.println("Read " + result.totalRows() + " rows");
 * }
 * }</pre>
 */
public class ArcticLibrary implements AutoCloseable {

    private final Arena arena;
    private final MemorySegment handle;

    private ArcticLibrary(Arena arena, MemorySegment handle) {
        this.arena = arena;
        this.handle = handle;
    }

    /**
     * Open an LMDB-backed ArcticDB library at the given path.
     *
     * @param path filesystem path for LMDB storage (created if absent)
     * @return a new ArcticLibrary instance (caller must close)
     */
    public static ArcticLibrary openLmdb(String path) {
        Arena arena = Arena.ofConfined();
        try {
            MemorySegment pathSeg = arena.allocateUtf8String(path);
            MemorySegment outPtr = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment errSeg = arena.allocate(ArcticNative.ARCTIC_ERROR_LAYOUT);

            int rc = (int) ArcticNative.LIBRARY_OPEN_LMDB.invokeExact(pathSeg, outPtr, errSeg);
            ArcticNative.checkError(rc, errSeg);

            MemorySegment handle = outPtr.get(ValueLayout.ADDRESS, 0);
            return new ArcticLibrary(arena, handle);
        } catch (RuntimeException e) {
            arena.close();
            throw e;
        } catch (Throwable t) {
            arena.close();
            throw new RuntimeException(t);
        }
    }

    /**
     * Write synthetic test data: a timeseries-indexed DataFrame with float64 columns.
     *
     * @param symbol     symbol name
     * @param numRows    number of rows
     * @param numColumns number of float64 columns (named col_0..col_N)
     */
    public void writeTestData(String symbol, long numRows, long numColumns) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment symSeg = local.allocateUtf8String(symbol);
            MemorySegment errSeg = local.allocate(ArcticNative.ARCTIC_ERROR_LAYOUT);

            int rc = (int) ArcticNative.WRITE_TEST_DATA.invokeExact(handle, symSeg, numRows, numColumns, errSeg);
            ArcticNative.checkError(rc, errSeg);
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Read the latest version of a symbol as a streaming Arrow result.
     */
    public ReadResult readStream(String symbol) {
        return readStream(symbol, -1);
    }

    /**
     * Read a specific version of a symbol as a streaming Arrow result.
     *
     * @param symbol  symbol name
     * @param version version number, or -1 for latest
     * @return summary of the data read (column names, row count, batch count)
     */
    public ReadResult readStream(String symbol, long version) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment symSeg = local.allocateUtf8String(symbol);
            MemorySegment streamSeg = local.allocate(ArcticNative.STREAM_LAYOUT);
            MemorySegment errSeg = local.allocate(ArcticNative.ARCTIC_ERROR_LAYOUT);

            int rc = (int) ArcticNative.READ_STREAM.invokeExact(handle, symSeg, version, streamSeg, errSeg);
            ArcticNative.checkError(rc, errSeg);

            try {
                // 1. Get schema
                MemorySegment schemaSeg = local.allocate(ArcticNative.ARROW_SCHEMA_LAYOUT);
                int schemaRc = ArcticNative.callGetSchema(streamSeg, schemaSeg);
                if (schemaRc != 0) {
                    throw new RuntimeException("get_schema failed with code " + schemaRc);
                }

                // Read column names from schema children
                long nChildren = schemaSeg.get(ValueLayout.JAVA_LONG, 32); // n_children offset
                MemorySegment childrenPtr = schemaSeg.get(ValueLayout.ADDRESS, 40); // children offset
                List<String> columnNames = new ArrayList<>();

                if (nChildren > 0 && !childrenPtr.equals(MemorySegment.NULL)) {
                    // children is ArrowSchema**, an array of pointers
                    MemorySegment childrenArray = childrenPtr.reinterpret(nChildren * ValueLayout.ADDRESS.byteSize());
                    for (long i = 0; i < nChildren; i++) {
                        MemorySegment childPtr = childrenArray.get(ValueLayout.ADDRESS, i * ValueLayout.ADDRESS.byteSize());
                        if (!childPtr.equals(MemorySegment.NULL)) {
                            MemorySegment child = childPtr.reinterpret(ArcticNative.ARROW_SCHEMA_LAYOUT.byteSize());
                            MemorySegment namePtr = child.get(ValueLayout.ADDRESS, 8); // name offset
                            if (!namePtr.equals(MemorySegment.NULL)) {
                                columnNames.add(namePtr.reinterpret(256).getUtf8String(0));
                            }
                        }
                    }
                }

                // Release schema
                ArcticNative.callArrowRelease(schemaSeg, 56); // release at offset 56

                // 2. Consume batches
                long totalRows = 0;
                int batchCount = 0;

                while (true) {
                    MemorySegment arraySeg = local.allocate(ArcticNative.ARROW_ARRAY_LAYOUT);
                    int nextRc = ArcticNative.callGetNext(streamSeg, arraySeg);
                    if (nextRc != 0) {
                        throw new RuntimeException("get_next failed with code " + nextRc);
                    }

                    // Check if release is NULL → end of stream
                    MemorySegment releasePtr = arraySeg.get(ValueLayout.ADDRESS, 64); // release at offset 64
                    if (releasePtr.equals(MemorySegment.NULL)) {
                        break;
                    }

                    long length = arraySeg.get(ValueLayout.JAVA_LONG, 0); // length at offset 0
                    totalRows += length;
                    batchCount++;

                    // Release this array
                    ArcticNative.callArrowRelease(arraySeg, 64);
                }

                return new ReadResult(columnNames, totalRows, batchCount);
            } finally {
                // 3. Release stream
                ArcticNative.callStreamRelease(streamSeg);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * List all symbols in this library.
     */
    public List<String> listSymbols() {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment outSymbols = local.allocate(ValueLayout.ADDRESS);
            MemorySegment outCount = local.allocate(ValueLayout.JAVA_LONG);
            MemorySegment errSeg = local.allocate(ArcticNative.ARCTIC_ERROR_LAYOUT);

            int rc = (int) ArcticNative.LIST_SYMBOLS.invokeExact(handle, outSymbols, outCount, errSeg);
            ArcticNative.checkError(rc, errSeg);

            long count = outCount.get(ValueLayout.JAVA_LONG, 0);
            MemorySegment symbolsArray = outSymbols.get(ValueLayout.ADDRESS, 0);

            List<String> result = new ArrayList<>();
            if (count > 0 && !symbolsArray.equals(MemorySegment.NULL)) {
                MemorySegment arr = symbolsArray.reinterpret(count * ValueLayout.ADDRESS.byteSize());
                for (long i = 0; i < count; i++) {
                    MemorySegment strPtr = arr.get(ValueLayout.ADDRESS, i * ValueLayout.ADDRESS.byteSize());
                    result.add(strPtr.reinterpret(256).getUtf8String(0));
                }
                // Free the native symbol list
                ArcticNative.FREE_SYMBOLS.invokeExact(symbolsArray, count);
            }

            return result;
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void close() {
        try {
            ArcticNative.LIBRARY_CLOSE.invokeExact(handle);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            arena.close();
        }
    }

    /**
     * Summary of data read from an Arrow stream.
     *
     * @param columnNames names of data columns (excludes the index)
     * @param totalRows   total number of rows across all batches
     * @param batchCount  number of Arrow record batches consumed
     */
    public record ReadResult(
            List<String> columnNames,
            long totalRows,
            int batchCount
    ) {}
}
