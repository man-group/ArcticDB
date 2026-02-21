/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

package com.arcticdb;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ArcticDB Java bindings via Panama FFM.
 *
 * <p>Requires {@code -Darcticdb.native.path} pointing to the directory containing
 * {@code libarcticdb_c.so}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ArcticReadTest {

    @TempDir
    Path tempDir;

    @Test
    @Order(1)
    void testOpenClose() {
        try (var lib = ArcticLibrary.openLmdb(tempDir.resolve("db1").toString())) {
            assertNotNull(lib);
        }
    }

    @Test
    @Order(2)
    void testWriteAndListSymbols() {
        try (var lib = ArcticLibrary.openLmdb(tempDir.resolve("db2").toString())) {
            lib.writeTestData("sym_a", 10, 2);
            lib.writeTestData("sym_b", 20, 3);

            List<String> symbols = lib.listSymbols();
            assertEquals(2, symbols.size());
            assertTrue(symbols.contains("sym_a"));
            assertTrue(symbols.contains("sym_b"));
        }
    }

    @Test
    @Order(3)
    void testReadStream() {
        try (var lib = ArcticLibrary.openLmdb(tempDir.resolve("db3").toString())) {
            lib.writeTestData("prices", 100, 3);

            ArcticLibrary.ReadResult result = lib.readStream("prices");

            assertEquals(100, result.totalRows());
            assertTrue(result.batchCount() >= 1);
            // The schema includes the timestamp index + 3 data columns
            // Column names should include col_0, col_1, col_2
            assertTrue(result.columnNames().stream().anyMatch(n -> n.contains("col_0")),
                    "Expected col_0 in " + result.columnNames());
            assertTrue(result.columnNames().stream().anyMatch(n -> n.contains("col_1")),
                    "Expected col_1 in " + result.columnNames());
            assertTrue(result.columnNames().stream().anyMatch(n -> n.contains("col_2")),
                    "Expected col_2 in " + result.columnNames());
        }
    }

    @Test
    @Order(4)
    void testReadSpecificVersion() {
        try (var lib = ArcticLibrary.openLmdb(tempDir.resolve("db4").toString())) {
            lib.writeTestData("versioned", 50, 2);  // version 0
            lib.writeTestData("versioned", 75, 2);  // version 1

            ArcticLibrary.ReadResult v0 = lib.readStream("versioned", 0);
            assertEquals(50, v0.totalRows());

            ArcticLibrary.ReadResult v1 = lib.readStream("versioned", 1);
            assertEquals(75, v1.totalRows());

            // Latest should be v1
            ArcticLibrary.ReadResult latest = lib.readStream("versioned");
            assertEquals(75, latest.totalRows());
        }
    }

    @Test
    @Order(5)
    void testReadMissingSymbolThrows() {
        try (var lib = ArcticLibrary.openLmdb(tempDir.resolve("db5").toString())) {
            assertThrows(RuntimeException.class, () -> lib.readStream("nonexistent"));
        }
    }
}
