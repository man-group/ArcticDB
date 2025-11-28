/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>

#include <arcticdb/entity/types.hpp>

namespace arcticdb {
class SegmentInMemory;
class Column;
class StringPool;
namespace entity {
class AtomKeyImpl;
using AtomKey = AtomKeyImpl;
struct AtomKeyPacked;
} // namespace entity
enum class SymbolStructure : uint8_t {
    UNKNOWN,
    SAME,  // e.g. in index keys
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
 *  For now, this class does the bare minimum required for it's one use. Useful extensions could include:
 *  - IndexKeySegment: inheriting from this class. Would also contain start/end row/column columns, and use sortedness
 * information
 *  - ShapshotRefKeySegment: inheriting from this class. Sorted on StreamId, so would allow rapid checks for contained
 * symbol/version pairs
 *  - Filtering: maintain a bitset to represent keys (rows) that have not been filtered out. Optionally memcpy when
 *               true bits falls below some threshold
 *   - Iterators: Iterate through all the keys in the segment (or all with true bits if filtered)
 *  - HashSet operations: maintain hashes of each row for quick set membership operations
 */
class KeySegment {
  public:
    KeySegment(SegmentInMemory&& segment, SymbolStructure symbol_structure);

    ARCTICDB_NO_MOVE_OR_COPY(KeySegment)

    // Returns AtomKeyPacked vector for SymbolStructure::SAME keys with numeric indexes, and AtomKey vector otherwise
    [[nodiscard]] std::variant<std::vector<entity::AtomKeyPacked>, std::vector<entity::AtomKey>> materialise() const;

  private:
    [[nodiscard]] bool check_symbols_all_same() const;
    [[nodiscard]] bool check_symbols_all_unique() const;

    size_t num_keys_;
    std::shared_ptr<StringPool> string_pool_;

    std::shared_ptr<Column> stream_ids_;
    std::shared_ptr<Column> version_ids_;
    std::shared_ptr<Column> creation_timestamps_;
    std::shared_ptr<Column> content_hashes_;
    std::shared_ptr<Column> start_indexes_;
    std::shared_ptr<Column> end_indexes_;
    std::shared_ptr<Column> key_types_;

    using uint8_TDT = entity::ScalarTagType<entity::DataTypeTag<entity::DataType::UINT8>>;
    using uint64_TDT = entity::ScalarTagType<entity::DataTypeTag<entity::DataType::UINT64>>;
    using int64_TDT = entity::ScalarTagType<entity::DataTypeTag<entity::DataType::INT64>>;

    SymbolStructure symbol_structure_{SymbolStructure::UNKNOWN};
    // If all the keys in this segment have the same symbol, stored here. std::nullopt otherwise
    std::optional<StreamId> symbol_;
};

} // namespace arcticdb