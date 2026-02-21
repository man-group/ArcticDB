/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

// GTest-based smoke test exercising the C API exactly as a Java JNI / .NET P/Invoke wrapper would:
// all interaction happens through C function pointers on ArcticArrowArrayStream.

#include <gtest/gtest.h>

#include <arcticdb/bindings/arcticdb_c.h>
#include <sparrow/c_interface.hpp>

#include <cstring>
#include <filesystem>
#include <string>

namespace {

class CApiStreamTest : public ::testing::Test {
  protected:
    void SetUp() override {
        test_path_ = (std::filesystem::temp_directory_path() / "arcticdb_c_stream_test").string();
        std::filesystem::remove_all(test_path_);

        ArcticError err = {};
        int rc = arctic_library_open_lmdb(test_path_.c_str(), &lib_, &err);
        ASSERT_EQ(rc, 0) << "Failed to open library: " << err.message;
        ASSERT_NE(lib_, nullptr);
    }

    void TearDown() override {
        arctic_library_close(lib_);
        lib_ = nullptr;
        std::filesystem::remove_all(test_path_);
    }

    ArcticLibrary* lib_ = nullptr;
    std::string test_path_;
};

TEST_F(CApiStreamTest, WriteAndReadRoundTrip) {
    ArcticError err = {};
    constexpr int64_t NUM_ROWS = 200;
    constexpr int64_t NUM_COLS = 5;

    // Write test data
    int rc = arctic_write_test_data(lib_, "stream_test", NUM_ROWS, NUM_COLS, &err);
    ASSERT_EQ(rc, 0) << "Write failed: " << err.message;

    // Open read stream (latest version)
    ArcticArrowArrayStream stream = {};
    rc = arctic_read_stream(lib_, "stream_test", -1, &stream, &err);
    ASSERT_EQ(rc, 0) << "Read stream failed: " << err.message;
    ASSERT_NE(stream.release, nullptr);

    // Get schema via C function pointer
    ArrowSchema schema = {};
    rc = stream.get_schema(&stream, &schema);
    ASSERT_EQ(rc, 0) << "get_schema failed: " << stream.get_last_error(&stream);
    // index + NUM_COLS data columns
    EXPECT_EQ(schema.n_children, NUM_COLS + 1);

    // Verify column names
    ASSERT_NE(schema.children, nullptr);
    // First child is the index column ("time")
    EXPECT_STREQ(schema.children[0]->name, "time");
    for (int64_t c = 0; c < NUM_COLS; ++c) {
        auto expected = "col_" + std::to_string(c);
        EXPECT_STREQ(schema.children[c + 1]->name, expected.c_str());
    }

    if (schema.release)
        schema.release(&schema);

    // Consume all batches via C function pointers
    int64_t total_rows = 0;
    int batch_count = 0;
    while (true) {
        ArrowArray array = {};
        rc = stream.get_next(&stream, &array);
        ASSERT_EQ(rc, 0) << "get_next failed: " << stream.get_last_error(&stream);
        if (array.release == nullptr)
            break; // end of stream

        EXPECT_EQ(array.n_children, NUM_COLS + 1);
        EXPECT_GT(array.length, 0);
        total_rows += array.length;
        batch_count++;

        array.release(&array);
    }

    EXPECT_EQ(total_rows, NUM_ROWS);
    EXPECT_GE(batch_count, 1);

    // Release stream
    stream.release(&stream);
    EXPECT_EQ(stream.release, nullptr) << "release should null itself";
}

TEST_F(CApiStreamTest, ReadMissingSymbolReturnsError) {
    ArcticError err = {};
    ArcticArrowArrayStream stream = {};
    int rc = arctic_read_stream(lib_, "no_such_symbol", -1, &stream, &err);
    EXPECT_NE(rc, 0);
    EXPECT_GT(std::strlen(err.message), 0u);
}

TEST_F(CApiStreamTest, ListSymbolsEmpty) {
    ArcticError err = {};
    char** symbols = nullptr;
    int64_t count = -1;
    int rc = arctic_list_symbols(lib_, &symbols, &count, &err);
    ASSERT_EQ(rc, 0) << "list_symbols failed: " << err.message;
    EXPECT_EQ(count, 0);
    arctic_free_symbols(symbols, count);
}

TEST_F(CApiStreamTest, ListSymbolsAfterWrite) {
    ArcticError err = {};
    int rc = arctic_write_test_data(lib_, "alpha", 10, 1, &err);
    ASSERT_EQ(rc, 0);
    rc = arctic_write_test_data(lib_, "beta", 10, 1, &err);
    ASSERT_EQ(rc, 0);

    char** symbols = nullptr;
    int64_t count = 0;
    rc = arctic_list_symbols(lib_, &symbols, &count, &err);
    ASSERT_EQ(rc, 0) << "list_symbols failed: " << err.message;
    EXPECT_EQ(count, 2);

    // Check both symbols are present (order unspecified)
    bool found_alpha = false, found_beta = false;
    for (int64_t i = 0; i < count; ++i) {
        if (std::strcmp(symbols[i], "alpha") == 0)
            found_alpha = true;
        if (std::strcmp(symbols[i], "beta") == 0)
            found_beta = true;
    }
    EXPECT_TRUE(found_alpha);
    EXPECT_TRUE(found_beta);

    arctic_free_symbols(symbols, count);
}

TEST_F(CApiStreamTest, ReadSpecificVersion) {
    ArcticError err = {};
    // Write version 0
    int rc = arctic_write_test_data(lib_, "versioned", 50, 2, &err);
    ASSERT_EQ(rc, 0);
    // Write version 1 (with different data)
    rc = arctic_write_test_data(lib_, "versioned", 75, 2, &err);
    ASSERT_EQ(rc, 0);

    // Read version 0 specifically
    ArcticArrowArrayStream stream = {};
    rc = arctic_read_stream(lib_, "versioned", 0, &stream, &err);
    ASSERT_EQ(rc, 0) << "Read version 0 failed: " << err.message;

    int64_t total_rows = 0;
    while (true) {
        ArrowArray array = {};
        rc = stream.get_next(&stream, &array);
        ASSERT_EQ(rc, 0);
        if (array.release == nullptr)
            break;
        total_rows += array.length;
        array.release(&array);
    }
    stream.release(&stream);
    EXPECT_EQ(total_rows, 50) << "Version 0 should have 50 rows";

    // Read latest (version 1)
    rc = arctic_read_stream(lib_, "versioned", -1, &stream, &err);
    ASSERT_EQ(rc, 0);

    total_rows = 0;
    while (true) {
        ArrowArray array = {};
        rc = stream.get_next(&stream, &array);
        ASSERT_EQ(rc, 0);
        if (array.release == nullptr)
            break;
        total_rows += array.length;
        array.release(&array);
    }
    stream.release(&stream);
    EXPECT_EQ(total_rows, 75) << "Latest version should have 75 rows";
}

TEST_F(CApiStreamTest, NullArgumentsReturnError) {
    ArcticError err = {};

    // NULL library
    int rc = arctic_read_stream(nullptr, "sym", -1, nullptr, &err);
    EXPECT_NE(rc, 0);

    // NULL symbol
    ArcticArrowArrayStream stream = {};
    rc = arctic_read_stream(lib_, nullptr, -1, &stream, &err);
    EXPECT_NE(rc, 0);

    // NULL out pointer for open
    rc = arctic_library_open_lmdb("/tmp/x", nullptr, &err);
    EXPECT_NE(rc, 0);

    // close with NULL is safe
    arctic_library_close(nullptr);
}

} // anonymous namespace
