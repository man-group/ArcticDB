/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

// Pure C smoke test for the ArcticDB C API.
// Compiled as C++ but uses only the C API surface — proving the API is C-compatible.

#include <arcticdb/bindings/arcticdb_c.h>
#include <sparrow/c_interface.hpp>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>

static const char* TEST_PATH = nullptr;
static char test_path_buf[512];

static void setup_test_path() {
    auto tmp = std::filesystem::temp_directory_path() / "arcticdb_c_api_smoke_test";
    std::filesystem::remove_all(tmp);
    std::strncpy(test_path_buf, tmp.c_str(), sizeof(test_path_buf) - 1);
    test_path_buf[sizeof(test_path_buf) - 1] = '\0';
    TEST_PATH = test_path_buf;
}

static void cleanup_test_path() {
    if (TEST_PATH) {
        std::filesystem::remove_all(TEST_PATH);
    }
}

static void test_open_close() {
    std::printf("  test_open_close...\n");
    ArcticLibrary* lib = nullptr;
    ArcticError err = {};
    int rc = arctic_library_open_lmdb(TEST_PATH, &lib, &err);
    assert(rc == 0 && "open should succeed");
    assert(lib != nullptr);

    arctic_library_close(lib);
    std::printf("    PASSED\n");
}

static void test_write_and_list() {
    std::printf("  test_write_and_list...\n");
    ArcticLibrary* lib = nullptr;
    ArcticError err = {};
    int rc = arctic_library_open_lmdb(TEST_PATH, &lib, &err);
    assert(rc == 0);

    // Write test data
    rc = arctic_write_test_data(lib, "test_sym", 100, 3, &err);
    assert(rc == 0 && "write should succeed");

    // List symbols
    char** symbols = nullptr;
    int64_t count = 0;
    rc = arctic_list_symbols(lib, &symbols, &count, &err);
    assert(rc == 0 && "list should succeed");
    assert(count == 1 && "should have 1 symbol");

    bool found = false;
    for (int64_t i = 0; i < count; ++i) {
        if (std::strcmp(symbols[i], "test_sym") == 0)
            found = true;
    }
    assert(found && "should find test_sym");

    arctic_free_symbols(symbols, count);
    arctic_library_close(lib);
    std::printf("    PASSED\n");
}

static void test_read_stream() {
    std::printf("  test_read_stream...\n");
    ArcticLibrary* lib = nullptr;
    ArcticError err = {};
    int rc = arctic_library_open_lmdb(TEST_PATH, &lib, &err);
    assert(rc == 0);

    // Write test data: 100 rows, 3 columns
    rc = arctic_write_test_data(lib, "read_test", 100, 3, &err);
    assert(rc == 0);

    // Open read stream (version -1 = latest)
    ArcticArrowArrayStream stream = {};
    rc = arctic_read_stream(lib, "read_test", -1, &stream, &err);
    assert(rc == 0 && "read_stream should succeed");
    assert(stream.release != nullptr && "stream should be valid");

    // Get schema
    // We use the raw ArrowSchema type from the stream's get_schema callback.
    // ArrowSchema is defined in sparrow/c_interface.hpp and available since we compile as C++.
    struct ArrowSchema schema = {};
    rc = stream.get_schema(&stream, &schema);
    assert(rc == 0 && "get_schema should succeed");
    // 3 data columns + 1 index column = 4 children
    assert(schema.n_children == 4);
    if (schema.release)
        schema.release(&schema);

    // Consume all batches
    int64_t total_rows = 0;
    int batch_count = 0;
    while (1) {
        struct ArrowArray array = {};
        rc = stream.get_next(&stream, &array);
        assert(rc == 0 && "get_next should succeed");
        if (array.release == nullptr)
            break; // end of stream

        assert(array.n_children == 4); // index + 3 data columns
        total_rows += array.length;
        batch_count++;

        array.release(&array);
    }

    assert(total_rows == 100 && "should read 100 rows total");
    assert(batch_count > 0 && "should have at least 1 batch");

    // Release stream
    stream.release(&stream);
    assert(stream.release == nullptr && "release should null itself");

    arctic_library_close(lib);
    std::printf("    PASSED (rows=%ld, batches=%d)\n", (long)total_rows, batch_count);
}

static void test_error_missing_symbol() {
    std::printf("  test_error_missing_symbol...\n");
    ArcticLibrary* lib = nullptr;
    ArcticError err = {};
    int rc = arctic_library_open_lmdb(TEST_PATH, &lib, &err);
    assert(rc == 0);

    ArcticArrowArrayStream stream = {};
    rc = arctic_read_stream(lib, "nonexistent_symbol", -1, &stream, &err);
    assert(rc != 0 && "read of missing symbol should fail");
    assert(std::strlen(err.message) > 0 && "error message should be set");

    arctic_library_close(lib);
    std::printf("    PASSED (error: %s)\n", err.message);
}

int main() {
    std::printf("ArcticDB C API Smoke Test\n");
    std::printf("========================\n");

    setup_test_path();

    test_open_close();
    test_write_and_list();
    test_read_stream();
    test_error_missing_symbol();

    cleanup_test_path();

    std::printf("\nAll tests PASSED\n");
    return 0;
}
