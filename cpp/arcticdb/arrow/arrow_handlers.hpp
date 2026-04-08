/* Copyright 2026 Man Group Operations Limited
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
        // input is compressed data, which we need to decompress onto dest
    void handle_type(
            const uint8_t*& data, Column& dest_column, const EncodedFieldImpl& field, const ColumnMapping& m,
            const DecodePathData& shared_data, std::any& handler_data, EncodingVersion encoding_version,
            const std::shared_ptr<StringPool>& string_pool, const ReadOptions& read_options
    );

    // lib.read(query_builder) codepath 
    // input is a decompressed column
    void convert_type(
            const Column& source_column, Column& dest_column, const ColumnMapping& mapping,
            const DecodePathData& shared_data, std::any& handler_data, const std::shared_ptr<StringPool>& string_pool,
            const ReadOptions& read_options
    ) const;

    [[nodiscard]] std::pair<entity::TypeDescriptor, entity::DetachableBlockConfig> output_type_and_block_config(
            const entity::TypeDescriptor& input_type, std::string_view column_name, const ReadOptions& read_options
    ) const;

    void default_initialize(
            ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData& shared_data,
            std::any& handler_data
    ) const;

    [[nodiscard]] ArrowOutputStringFormat output_string_format(
            std::string_view column_name, const ReadOptions& read_options
    ) const;
};

struct ArrowBoolHandler {
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

    [[nodiscard]] std::pair<entity::TypeDescriptor, entity::DetachableBlockConfig> output_type_and_block_config(
            const entity::TypeDescriptor& input_type, std::string_view column_name, const ReadOptions& read_options
    ) const;

    void default_initialize(
            ChunkedBuffer& buffer, size_t offset, size_t byte_size, const DecodePathData& shared_data,
            std::any& handler_data
    ) const;
};

ArrowTimestampHandler {
        ....
        /*
                for each element in data:
                   if data == pd.NaT:
                      bv.set_bit(i, 0)

                if (bv->count !- bv.size){
                    //same as above
                }
            }
                */
}

struct ArrowHandlerDataFactory : public TypeHandlerDataFactory {
    std::any get_data() const override { return {}; }
};

inline void register_arrow_handler_data_factory() {
    TypeHandlerRegistry::instance()->set_handler_data(OutputFormat::ARROW, std::make_unique<ArrowHandlerDataFactory>());
}

inline void register_arrow_bool_type() {
    TypeHandlerRegistry::instance()->register_handler(
            OutputFormat::ARROW, make_scalar_type(entity::DataType::BOOL8), arcticdb::ArrowBoolHandler{}
    );
}

inline void register_arrow_string_types() {
    using namespace arcticdb;
    constexpr std::array<entity::DataType, 5> dynamic_string_data_types = {
            entity::DataType::ASCII_DYNAMIC64,
            entity::DataType::UTF_DYNAMIC64,
            entity::DataType::UTF_DYNAMIC32,
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