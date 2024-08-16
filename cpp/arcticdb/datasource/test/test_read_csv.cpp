/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/memory_mapped_file.hpp>
#include <arcticdb/datasource/csv_reader.hpp>

TEST(ReadCsv, DoMemoryMap) {
    using namespace arcticdb;
    std::string filename = "/opt/arcticdb/arcticdb_link/cpp/arcticdb/datasource/test/artifacts/nfl.csv";
    MemoryMappedFile file;
    file.open_file(filename);
    std::string_view strv(reinterpret_cast<char*>(file.data()), 20);
    log::version().info("{}", strv);
}

TEST(ReadCsv, GetIndexes) {
    using namespace arcticdb;
    std::string filename = "/opt/arcticdb/arcticdb_link/cpp/arcticdb/datasource/test/artifacts/nfl.csv";
    MemoryMappedFile file;
    file.open_file(filename);
    CsvIndexes csv_indexes;
    auto success = find_indexes(file.data(), file.bytes(), csv_indexes);
    ASSERT_EQ(success, true);
}

TEST(ReadCsv, PrintElements) {
    using namespace arcticdb;
    std::string filename = "/opt/arcticdb/arcticdb_link/cpp/arcticdb/datasource/test/artifacts/nfl.csv";
    MemoryMappedFile file;
    file.open_file(filename);
    CsvIndexes csv_indexes;
    auto success = find_indexes(file.data(), file.bytes(), csv_indexes);
    auto current = 0U;
    const auto* data = file.data();
    for(auto i = 0U; i < csv_indexes.n_indexes; ++i) {
        std::string_view strv(reinterpret_cast<const char*>(data) + current + 1, (csv_indexes.indexes[i] - current) - 1);
        log::version().info("{}", strv);
        current = csv_indexes.indexes[i];
    }
    ASSERT_EQ(success, true);
}

namespace arcticdb {

struct CsvColumnIterator {
    const size_t start_;
    const size_t num_columns_;
    const char* data_;
    const CsvIndexes& csv_indexes_;
    size_t pos_ = 0UL;

    CsvColumnIterator(
        size_t start,
        size_t num_columns,
        const uint8_t* data,
        const CsvIndexes& csv_indexes) :
            start_(start),
            num_columns_(num_columns),
            data_(reinterpret_cast<const char*>(data)),
            csv_indexes_(csv_indexes) {

    }

    size_t pos() const {
        return pos_;
    }

    void advance() {
        pos_ += num_columns_;
    }

    std::string_view get() const {
        const size_t start = csv_indexes_.indexes[pos_] + 1;
        const size_t end = csv_indexes_.indexes[pos_ + 1] - 1;
        return {data_ + start, end - start};
    }
};

size_t get_num_columns(const char* str, int length) {
    size_t count = 0;

    for (int i = 0; i < length; i++) {
        if (str[i] == ',') {
            ++count;
        } else if (str[i] == '\n') {
            break;
        }
    }

    return count + 1;
}

} // namespace arcticdb

TEST(ReadCsv, GetNumColumns) {
    using namespace arcticdb;
    std::string filename = "/opt/arcticdb/arcticdb_link/cpp/arcticdb/datasource/test/artifacts/nfl.csv";
    MemoryMappedFile file;
    file.open_file(filename);
    const auto num_columns = get_num_columns(reinterpret_cast<const char *>(file.data()), file.bytes());
    ASSERT_EQ(num_columns, 13);
}

TEST(ReadCsv, IterateColumn) {
    using namespace arcticdb;
    std::string filename = "/opt/arcticdb/arcticdb_link/cpp/arcticdb/datasource/test/artifacts/nfl.csv";
    MemoryMappedFile file;
    file.open_file(filename);
    CsvIndexes csv_indexes;
    const auto num_columns = get_num_columns(reinterpret_cast<const char*>(file.data()), file.bytes());
    auto success = find_indexes(file.data(), file.bytes(), csv_indexes);
    CsvColumnIterator iterator{0, num_columns, file.data(), csv_indexes};
    while (iterator.pos() < csv_indexes.n_indexes) {
        log::version().info(iterator.get());
        iterator.advance();
    }
    ASSERT_EQ(success, true);
}