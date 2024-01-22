/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <entity/types.hpp>
#include <util/type_handler.hpp>

namespace arcticdb {
    struct ColumnMapping;

    struct EmptyHandler {
        /// @see arcticdb::ITypeHandler
        void handle_type(
            const uint8_t*& data,
            uint8_t* dest,
            const VariantField& encoded_field,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder> buffers,
            EncodingVersion encding_version,
            const ColumnMapping& columnMapping
        );

        int type_size() const;
    };

    struct BoolHandler {
        /// @see arcticdb::ITypeHandler
        void handle_type(
            const uint8_t *&data,
            uint8_t *dest,
            const VariantField &encoded_field,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder> buffers,
            EncodingVersion encding_version,
            const ColumnMapping& columnMapping
        );
        int type_size() const;
    };

    struct DecimalHandler {
        void handle_type(
            const uint8_t*& data,
            uint8_t* dest,
            const VariantField& encoded_field,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder> buffers,
            EncodingVersion encding_version,
            const ColumnMapping& m
        );
        int type_size() const;
    };

    struct ArrayHandler {
        /// @see arcticdb::ITypeHandler
        void handle_type(
            const uint8_t*& data,
            uint8_t* dest,
            const VariantField& encoded_field,
            size_t dest_bytes,
            std::shared_ptr<BufferHolder> buffers,
            EncodingVersion encding_version,
            const ColumnMapping& columnMapping
        );
        int type_size() const;
        static std::mutex initialize_array_mutex;
    };
} //namespace arcticdb
