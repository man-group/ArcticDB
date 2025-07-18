/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/test/benchmark_common.hpp>

namespace arcticdb {

std::random_device rd;
std::mt19937 gen(rd());

std::string generate_string() {
    static const std::string characters =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::uniform_int_distribution<std::string::size_type > dis(0, characters.size() - 1);
    std::string res;
    for (size_t idx = 0; idx < 10; ++idx) {
        res += characters.at(dis(gen));
    }
    return res;
}

ColumnWithStrings generate_string_dense_column(const size_t num_rows, const size_t unique_strings, DataType dt) {
    auto string_pool = std::make_shared<StringPool>();
    std::vector<entity::position_t> offsets;
    offsets.reserve(unique_strings);
    for (size_t _ = 0; _ < unique_strings; _++) {
        auto str = generate_string();
        if (dt == DataType::UTF_FIXED64) {
            auto utf32_str = boost::locale::conv::utf_to_utf<char32_t>(str.c_str(), str.c_str() + str.size());
            std::string utf32_bytes(reinterpret_cast<const char*>(utf32_str.data()), utf32_str.size() * sizeof(char32_t));
            offsets.emplace_back(string_pool->get(utf32_bytes, false).offset());
        } else {
            offsets.emplace_back(string_pool->get(str, false).offset());
        }
    }

    std::vector<int64_t> data;
    data.reserve(num_rows);
    std::uniform_int_distribution<uint64_t> dis(0, unique_strings - 1);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        data.emplace_back(offsets.at(dis(gen)));
    }
    Column col(make_scalar_type(dt), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), data.data(), num_rows * sizeof(int64_t));
    col.set_row_data(num_rows - 1);
    return {std::move(col), string_pool, ""};
}

ColumnWithStrings generate_numeric_sparse_column(const size_t num_rows) {
    Column col(make_scalar_type(DataType::INT64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    std::uniform_int_distribution<int64_t> dis(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) < 0) {
            col.set_scalar<int64_t>(static_cast<ssize_t>(idx), dis(gen));
        }
    }
    col.set_row_data(num_rows - 1);
    return {std::move(col), {}, ""};
}

util::BitSet generate_bitset(const size_t num_rows) {
    util::BitSet bitset;
    bitset.resize(num_rows);
    util::BitSet::bulk_insert_iterator inserter(bitset);
    std::uniform_int_distribution<> dis(0, 1);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) == 0) {
            inserter = idx;
        }
    }
    inserter.flush();
    return bitset;
}

Value generate_numeric_value() {
    std::uniform_int_distribution<int64_t> dis(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max());
    return construct_value<int64_t>(dis(gen));
}

Value generate_string_value() {
    return construct_string_value(generate_string());
}


ColumnWithStrings generate_numeric_dense_column(const size_t num_rows) {
    std::vector<int64_t> data;
    data.reserve(num_rows);
    std::uniform_int_distribution<int64_t> dis(std::numeric_limits<int64_t>::lowest(), std::numeric_limits<int64_t>::max());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        data.emplace_back(dis(gen));
    }
    Column col(make_scalar_type(DataType::INT64), num_rows, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    memcpy(col.ptr(), data.data(), num_rows * sizeof(int64_t));
    col.set_row_data(num_rows - 1);
    return {std::move(col), {}, ""};
}

ColumnWithStrings generate_string_sparse_column(const size_t num_rows, const size_t unique_strings) {
    Column col(make_scalar_type(DataType::UTF_DYNAMIC64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    auto string_pool = std::make_shared<StringPool>();
    std::vector<entity::position_t> offsets;
    offsets.reserve(unique_strings);
    for (size_t _ = 0; _ < unique_strings; _++) {
        auto str = generate_string();
        offsets.emplace_back(string_pool->get(str, false).offset());
    }
    std::uniform_int_distribution<uint64_t> dis(0, unique_strings - 1);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (dis(gen) < unique_strings / 2) {
            col.set_scalar<int64_t>(static_cast<ssize_t>(idx), offsets.at(dis(gen)));
        }
    }
    col.set_row_data(num_rows - 1);
    return {std::move(col), string_pool, ""};
}
}