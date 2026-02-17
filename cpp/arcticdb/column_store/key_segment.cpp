/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/key_segment.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/column_store/string_pool.hpp>

namespace arcticdb {

StreamId stream_id_from_column_entry(uint64_t column_entry, bool is_sequence_type, const StringPool& string_pool) {
    if (is_sequence_type) {
        // String columns are stored as uint64s (offsets into the string pool), but StringPool::get_const_view expects
        // an int64 Hence the static cast
        return StringId(string_pool.get_const_view(static_cast<int64_t>(column_entry)));
    } else {
        return NumericId(column_entry);
    }
}

IndexValue index_value_from_column_entry(int64_t column_entry, bool is_sequence_type, const StringPool& string_pool) {
    if (is_sequence_type) {
        // String columns are stored as uint64s, whereas timestamp index start/end values are stored as int64s
        // (nanoseconds since epoch) We could add another if statement based on index_is_sequence_type, but as these
        // types are the same width, we can just do some fancy casting to reinterpret the value in the iterator as a
        // uint64. We then need to static cast back to int64 for the same reason as above RE StringPool::get_const_view
        return StringId(
                string_pool.get_const_view(static_cast<int64_t>(*reinterpret_cast<const uint64_t*>(&(column_entry))))
        );
    } else {
        return NumericIndex(column_entry);
    }
}

KeySegment::KeySegment(SegmentInMemory&& segment, SymbolStructure symbol_structure) :
    num_keys_(segment.row_count()),
    string_pool_(segment.string_pool_ptr()),
    symbol_structure_(symbol_structure) {
    // Needed as the column map is not initialised at read time if the segment has no rows
    segment.init_column_map();
    stream_ids_ = segment.column_ptr(static_cast<uint32_t>(pipelines::index::Fields::stream_id));
    version_ids_ = segment.column_ptr(static_cast<uint32_t>(pipelines::index::Fields::version_id));
    creation_timestamps_ = segment.column_ptr(static_cast<uint32_t>(pipelines::index::Fields::creation_ts));
    content_hashes_ = segment.column_ptr(static_cast<uint32_t>(pipelines::index::Fields::content_hash));
    start_indexes_ = segment.column_ptr(static_cast<uint32_t>(pipelines::index::Fields::start_index));
    end_indexes_ = segment.column_ptr(static_cast<uint32_t>(pipelines::index::Fields::end_index));
    key_types_ = segment.column_ptr(static_cast<uint32_t>(pipelines::index::Fields::key_type));

    switch (symbol_structure_) {
    case SymbolStructure::SAME:
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                check_symbols_all_same(),
                "Expected all symbols to be identical in KeySegment"
        );
        if (stream_ids_->row_count() != 0) {
            if (is_sequence_type(stream_ids_->type().data_type())) {
                symbol_ = std::string(string_pool_->get_const_view(stream_ids_->template reference_at<uint64_t>(0)));
            } else {
                symbol_ = safe_convert_to_numeric_id(stream_ids_->template reference_at<uint64_t>(0));
            }
        }
        break;
    case SymbolStructure::UNIQUE:
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                check_symbols_all_unique(),
                "Expected all symbols to be unique in KeySegment"
        );
    case SymbolStructure::UNKNOWN:
    default:
        break;
    }
}

std::variant<std::vector<entity::AtomKeyPacked>, std::vector<entity::AtomKey>> KeySegment::materialise() const {
    auto version_data = version_ids_->data();
    auto creation_ts_data = creation_timestamps_->data();
    auto content_hash_data = content_hashes_->data();
    auto index_start_data = start_indexes_->data();
    auto index_end_data = end_indexes_->data();
    auto key_types_data = key_types_->data();

    auto version_it = version_data.template cbegin<uint64_TDT>();
    auto creation_ts_it = creation_ts_data.template cbegin<int64_TDT>();
    auto content_hash_it = content_hash_data.template cbegin<uint64_TDT>();
    auto key_types_it = key_types_data.template cbegin<uint8_TDT>();

    if (symbol_structure_ == SymbolStructure::SAME && !is_sequence_type(start_indexes_->type().data_type())) {
        // Can return AtomKeyPacked vector, much more time and memory efficient, and most common case
        std::vector<AtomKeyPacked> res;
        res.reserve(num_keys_);
        auto index_start_it = index_start_data.template cbegin<int64_TDT>();
        auto index_end_it = index_end_data.template cbegin<int64_TDT>();
        for (size_t row_idx = 0; row_idx < num_keys_; ++row_idx,
                    ++version_it,
                    ++creation_ts_it,
                    ++content_hash_it,
                    ++index_start_it,
                    ++index_end_it,
                    ++key_types_it) {
            res.emplace_back(
                    *version_it,
                    *creation_ts_it,
                    *content_hash_it,
                    KeyType(*key_types_it),
                    *index_start_it,
                    *index_end_it
            );
        }
        return res;
    } else {
        // Fall back to returning fully materialised AtomKeys
        std::vector<AtomKey> res;
        res.reserve(num_keys_);
        auto stream_id_data = stream_ids_->data();
        auto stream_id_it = stream_id_data.template cbegin<uint64_TDT>();
        const bool stream_id_is_sequence_type = is_sequence_type(stream_ids_->type().data_type());
        const bool index_is_sequence_type = is_sequence_type(start_indexes_->type().data_type());
        auto index_start_it = index_start_data.template cbegin<int64_TDT>();
        auto index_end_it = index_start_data.template cbegin<int64_TDT>();
        for (size_t row_idx = 0; row_idx < num_keys_; ++row_idx,
                    ++stream_id_it,
                    ++version_it,
                    ++creation_ts_it,
                    ++content_hash_it,
                    ++index_start_it,
                    ++index_end_it,
                    ++key_types_it) {
            res.emplace_back(
                    symbol_.value_or(
                            stream_id_from_column_entry(*stream_id_it, stream_id_is_sequence_type, *string_pool_)
                    ),
                    *version_it,
                    *creation_ts_it,
                    *content_hash_it,
                    index_value_from_column_entry(*index_start_it, index_is_sequence_type, *string_pool_),
                    index_value_from_column_entry(*index_end_it, index_is_sequence_type, *string_pool_),
                    KeyType(*key_types_it)
            );
        }
        return res;
    }
}

bool KeySegment::check_symbols_all_same() const {
    if (stream_ids_->row_count() != 0) {
        auto data = stream_ids_->data();
        auto end_it = data.template cend<uint64_TDT>();
        auto it = data.template cbegin<uint64_TDT>();
        uint64_t value{*it};
        for (; it != end_it; ++it) {
            if (*it != value) {
                return false;
            }
        }
    }
    return true;
}

bool KeySegment::check_symbols_all_unique() const {
    if (stream_ids_->row_count() != 0) {
        auto data = stream_ids_->data();
        auto end_it = data.template cend<uint64_TDT>();
        ankerl::unordered_dense::set<uint64_t> values;
        values.reserve(stream_ids_->row_count());
        for (auto it = data.template cbegin<uint64_TDT>(); it != end_it; ++it) {
            if (!values.insert(*it).second) {
                return false;
            }
        }
    }
    return true;
}

} // namespace arcticdb