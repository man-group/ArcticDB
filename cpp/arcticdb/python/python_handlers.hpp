 /* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/type_handler.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/python/python_handler_data.hpp>
#include <arcticdb/arrow/arrow_handlers.hpp>

// Handlers for various non-trivial Python types,
// that conform to the interface ITypeHandler
namespace arcticdb {

struct ColumnMapping;
class Column;

struct PythonEmptyHandler {
    void handle_type(
        const uint8_t *&data,
        Column& dest_column,
        const EncodedFieldImpl &field,
        const ColumnMapping& m,
        const DecodePathData& shared_data,
        std::any& handler_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool
    );

    [[nodiscard]] int type_size() const;

    void convert_type(
        const Column& source_column,
        Column& dest_column,
        const ColumnMapping& mapping,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const std::shared_ptr<StringPool>& string_pool) const;

    [[nodiscard]] TypeDescriptor output_type(const TypeDescriptor& input_type) const;

    void default_initialize(
        ChunkedBuffer& buffer,
        size_t offset,
        size_t byte_size,
        const DecodePathData& shared_data,
        std::any& handler_data) const;
};

struct PythonStringHandler {
    void handle_type(
        const uint8_t *&data,
        Column& dest_column,
        const EncodedFieldImpl &field,
        const ColumnMapping& m,
        const DecodePathData& shared_data,
        std::any& handler_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool
    );

    [[nodiscard]] int type_size() const;

    [[nodiscard]] TypeDescriptor output_type(const TypeDescriptor& input_type) const;
    
    void convert_type(
        const Column& source_column,
        Column& dest_column,
        const ColumnMapping& mapping,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const std::shared_ptr<StringPool>& string_pool) const;

    void default_initialize(
        ChunkedBuffer& buffer,
        size_t offset,
        size_t byte_size,
        const DecodePathData& shared_data,
        std::any& handler_data) const;
};

struct PythonBoolHandler {
    void handle_type(
        const uint8_t *&data,
        Column& dest_column,
        const EncodedFieldImpl &field,
        const ColumnMapping& m,
        const DecodePathData& shared_data,
        std::any& handler_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool
    );

    [[nodiscard]] int type_size() const;

    void convert_type(
        const Column& source_column,
        Column& dest_column,
        const ColumnMapping& mapping,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const std::shared_ptr<StringPool>& string_pool) const;

    [[nodiscard]] TypeDescriptor output_type(const TypeDescriptor& input_type) const;

    void default_initialize(
        ChunkedBuffer& buffer,
        size_t offset,
        size_t byte_size,
        const DecodePathData& shared_data,
        std::any& handler_data) const;
};

struct PythonArrayHandler {
    void handle_type(
        const uint8_t *&data,
        Column& dest_column,
        const EncodedFieldImpl &field,
        const ColumnMapping& m,
        const DecodePathData& shared_data,
        std::any& handler_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>& string_pool
    );

    [[nodiscard]] int type_size() const;

    [[nodiscard]] TypeDescriptor output_type(const TypeDescriptor& input_type) const;

    void default_initialize(
        ChunkedBuffer& buffer,
        size_t offset,
        size_t byte_size,
        const DecodePathData& shared_data,
        std::any& handler_data) const;

    void convert_type(
        const Column& source_column,
        Column& dest_column,
        const ColumnMapping& mapping,
        const DecodePathData& shared_data,
        std::any& handler_data,
        const std::shared_ptr<StringPool>& string_pool) const;
};

inline void register_python_array_types() {
    using namespace arcticdb;
    constexpr std::array<DataType, 5> array_data_types = {
        DataType::INT64, DataType::FLOAT64, DataType::EMPTYVAL, DataType::FLOAT32, DataType::INT32};

    for (auto data_type : array_data_types) {
        TypeHandlerRegistry::instance()->register_handler(OutputFormat::PANDAS, make_array_type(data_type), arcticdb::PythonArrayHandler());
    }
}

inline void register_python_string_types() {
    using namespace arcticdb;
    constexpr std::array<DataType, 5> string_data_types = {
        DataType::ASCII_DYNAMIC64, DataType::UTF_DYNAMIC64};

    for (auto data_type :string_data_types) {
        TypeHandlerRegistry::instance()->register_handler(OutputFormat::PANDAS, make_scalar_type(data_type), arcticdb::PythonStringHandler());
    }
}

inline void register_python_handler_data_factory() {
    TypeHandlerRegistry::instance()->set_handler_data(OutputFormat::PANDAS, std::make_unique<PythonHandlerDataFactory>());
}
} //namespace arcticdb
