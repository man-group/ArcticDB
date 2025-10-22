/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/type_handler.hpp>

namespace arcticdb {

struct ArrowStringHandler {
    void handle_type(
            const uint8_t*& data, Column& dest_column, const EncodedFieldImpl& field, const ColumnMapping& m,
            const DecodePathData& shared_data, std::any& handler_data, EncodingVersion encoding_version,
            const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
    );

    void convert_type(
            const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
            const DecodePathData& shared_data, std::any& handler_data, const std::shared_ptr<StringPool>& string_pool,
            const ReadOptions& read_options
    ) const;

    [[nodiscard]] std::pair<entity::TypeDescriptor, std::optional<size_t>> output_type_and_extra_bytes(
            const entity::TypeDescriptor& input_type, const std::string_view& column_name,
            const ReadOptions& read_options
    ) const;

    void default_initialize(
            ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData& shared_data,
            std::any& handler_data
    ) const;
};

struct ArrowHandlerDataFactory : public TypeHandlerDataFactory {
    std::any get_data() const override { return {}; }
};

inline void register_arrow_handler_data_factory() {
    TypeHandlerRegistry::instance()->set_handler_data(OutputFormat::ARROW, std::make_unique<ArrowHandlerDataFactory>());
}

inline void register_arrow_string_types() {
    using namespace arcticdb;
    constexpr std::array<entity::DataType, 4> dynamic_string_data_types = {
            entity::DataType::ASCII_DYNAMIC64,
            entity::DataType::UTF_DYNAMIC64,
            entity::DataType::ASCII_FIXED64,
            entity::DataType::UTF_FIXED64
    };

    for (auto data_type : dynamic_string_data_types) {
        TypeHandlerRegistry::instance()->register_handler(
                OutputFormat::ARROW, make_scalar_type(data_type), arcticdb::ArrowStringHandler{}
        );
    }
}

} // namespace arcticdb