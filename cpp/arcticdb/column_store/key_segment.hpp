/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>

#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {

enum class SymbolStructure: uint8_t {
    UNKNOWN,
    SAME, // e.g. in index keys
    UNIQUE // e.g. in snapshot ref keys
};

/*
 * Class to wrap segments in memory that only contain keys. Examples include:
 *  - VERSION_REF
 *  - VERSION
 *  - TABLE_INDEX
 *  - SNAPSHOT_REF
 *  - MULTI_KEY
 *  - LOG_COMPACTED
 *  - SNAPSHOT (deprecated)
 *  This is to avoid materialising the AtomKey class for each row, particularly when operations involve many such keys.
 *  Many of these key types have special properties, such as all the contained keys having the same stream id, or all
 *  of the contained keys being of the same type, in which case further optimisations can be made.
 */
class KeySegment {
public:
    KeySegment(SegmentInMemory&& segment, SymbolStructure symbol_structure);

    ARCTICDB_NO_MOVE_OR_COPY(KeySegment)

    // Returns AtomKeyPacked vector for SymbolStructure::SAME keys with numeric indexes, and AtomKey vector otherwise
    [[nodiscard]] std::variant<std::vector<AtomKeyPacked>, std::vector<AtomKey>> materialise() const;

private:
    [[nodiscard]] bool check_symbols_all_same() const;
    [[nodiscard]] bool check_symbols_all_unique() const;

    SegmentInMemory segment_;
    // Convenience columns, just referencing columns in segment_
    std::shared_ptr<Column> stream_ids_;
    std::shared_ptr<Column> version_ids_;
    std::shared_ptr<Column> creation_timestamps_;
    std::shared_ptr<Column> content_hashes_;
    std::shared_ptr<Column> start_indexes_;
    std::shared_ptr<Column> end_indexes_;
    std::shared_ptr<Column> key_types_;

    using stream_id_TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
    using version_TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
    using creation_ts_TDT = ScalarTagType<DataTypeTag<DataType::INT64>>;
    using content_hash_TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
    using index_start_numeric_TDT = ScalarTagType<DataTypeTag<DataType::INT64>>;
    using index_end_numeric_TDT = ScalarTagType<DataTypeTag<DataType::INT64>>;
    using index_start_string_TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
    using index_end_string_TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
    using key_type_TDT = ScalarTagType<DataTypeTag<DataType::UINT8>>;

    SymbolStructure symbol_structure_{SymbolStructure::UNKNOWN};
    // If all the keys in this segment have the same symbol, stored here. std::nullopt otherwise
    std::optional<StreamId> symbol_;
};

} // arcticdb