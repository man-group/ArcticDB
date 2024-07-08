 /* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <entity/types.hpp>
#include <util/type_handler.hpp>

// Handlers for various non-trivial Python types,
// that conform to the interface ITypeHandler
namespace arcticdb {

struct ColumnMapping;

struct EmptyHandler {

    void handle_type(
        const uint8_t *&data,
        uint8_t *dest,
        const EncodedFieldImpl &encoded_field,
        const ColumnMapping &mapping,
        size_t dest_bytes,
        const DecodePathData& shared_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>&
    );

    int type_size() const;

    void default_initialize(void *dest, size_t byte_size) const;
};

struct StringHandler {
    void handle_type(
        const uint8_t *&data,
        uint8_t *dest,
        const EncodedFieldImpl &encoded_field,
        const ColumnMapping &mapping,
        size_t dest_bytes,
        const DecodePathData& shared_data,
        EncodingVersion encding_version,
        const std::shared_ptr<StringPool>& string_pool
    );

    int type_size() const;

    void default_initialize(void *dest, size_t byte_size) const;
};

struct BoolHandler {
    void handle_type(
        const uint8_t *&data,
        uint8_t *dest,
        const EncodedFieldImpl &encoded_field,
        const ColumnMapping &mapping,
        size_t dest_bytes,
        const DecodePathData& shared_data,
        EncodingVersion encoding_version,
        const std::shared_ptr<StringPool>&
    );

    int type_size() const;

    void default_initialize(void *dest, size_t byte_size) const;
};

struct DecimalHandler {
    void handle_type(
        const uint8_t *&data,
        uint8_t *dest,
        const EncodedFieldImpl &encoded_field,
        const ColumnMapping &mapping,
        size_t dest_bytes,
        const DecodePathData& shared_data
    );

    int type_size() const;
};

struct ArrayHandler {
    /// @see arcticdb::ITypeHandler
    void handle_type(
        const uint8_t *&data,
        uint8_t *dest,
        const EncodedFieldImpl &encoded_field,
        const ColumnMapping &mapping,
        size_t dest_bytes,
        const DecodePathData& shared_data,
        EncodingVersion encding_version,
        const std::shared_ptr<StringPool>&
    );

    int type_size() const;

        void default_initialize(void* dest, size_t byte_size) const;

        static std::mutex initialize_array_mutex;
    };
} //namespace arcticdb
