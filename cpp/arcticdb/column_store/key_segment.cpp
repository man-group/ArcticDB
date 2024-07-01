/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/key_segment.hpp>

namespace arcticdb {

KeySegment::KeySegment(SegmentInMemory&& segment, SymbolStructure symbol_structure):
            segment_(std::move(segment)),
            stream_ids_(segment_.column(*segment_.column_index("stream_id"))),
            version_ids_(segment_.column(*segment_.column_index("version_id"))),
            creation_timestamps_(segment_.column(*segment_.column_index("creation_ts"))),
            content_hashes_(segment_.column(*segment_.column_index("content_hash"))),
            start_indexes_(segment_.column(*segment_.column_index("start_index"))),
            end_indexes_(segment_.column(*segment_.column_index("end_index"))),
            key_types_(segment_.column(*segment_.column_index("key_type"))),
            symbol_structure_(symbol_structure)
{
    switch (symbol_structure_) {
        case SymbolStructure::SAME:
            debug::check<ErrorCode::E_ASSERTION_FAILURE>(check_symbols_all_same(),
                                                         "Expected all symbols to be identical in KeySegment");
            if (stream_ids_.row_count() != 0) {
                if (is_sequence_type(stream_ids_.type().data_type())) {
                    symbol_ = std::string(*segment_.string_at(0, *segment_.column_index("stream_id")));
                } else {
                    symbol_ = safe_convert_to_numeric_id(stream_ids_.template reference_at<uint64_t>(0));
                }
            }
            break;
        case SymbolStructure::UNIQUE:
            debug::check<ErrorCode::E_ASSERTION_FAILURE>(check_symbols_all_unique(),
                                                         "Expected all symbols to be unique in KeySegment");
        case SymbolStructure::UNKNOWN:
        default:
            break;
    }
}

std::variant<std::vector<AtomKeyPacked>, std::vector<AtomKey>> KeySegment::materialise() const {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(symbol_structure_ == SymbolStructure::SAME,
                                                    "KeySegment::materialise_packed only makes sense if all keys have same stream id");
    auto version_data = version_ids_.data();
    auto creation_ts_data = creation_timestamps_.data();
    auto content_hash_data = content_hashes_.data();
    auto index_start_data = start_indexes_.data();
    auto index_end_data = end_indexes_.data();
    auto key_types_data = key_types_.data();

    auto version_it = version_data.template cbegin<version_TDT>();
    auto creation_ts_it = creation_ts_data.template cbegin<creation_ts_TDT>();
    auto content_hash_it = content_hash_data.template cbegin<content_hash_TDT>();
    auto key_types_it = key_types_data.template cbegin<key_type_TDT>();

    if (is_sequence_type(start_indexes_.type().data_type()) || symbol_structure_ != SymbolStructure::SAME) {
        std::vector<AtomKey> res;
        res.reserve(segment_.row_count());
        auto stream_id_data = stream_ids_.data();
        auto stream_id_it = stream_id_data.template cbegin<stream_id_TDT>();
        const bool stream_id_is_sequence_type = is_sequence_type(stream_ids_.type().data_type());
        auto index_start_it = index_start_data.template cbegin<index_start_string_TDT>();
        auto index_end_it = index_end_data.template cbegin<index_end_string_TDT>();
        auto& string_pool = segment_.const_string_pool();
        for (size_t row_idx = 0;
             row_idx < segment_.row_count();
             ++row_idx, ++stream_id_it, ++version_it, ++creation_ts_it, ++content_hash_it, ++index_start_it, ++index_end_it, ++key_types_it) {
            res.emplace_back(
                    symbol_.value_or(
                            stream_id_is_sequence_type ? StreamId(StringId(string_pool.get_const_view(*stream_id_it))) : StreamId(NumericId(*stream_id_it))
                            ),
                    *version_it,
                    *creation_ts_it,
                    *content_hash_it,
                    *index_start_it,
                    *index_end_it,
                    KeyType(*key_types_it)
                    );
        }
        return res;
    } else {
        std::vector<AtomKeyPacked> res;
        res.reserve(segment_.row_count());
        auto index_start_it = index_start_data.template cbegin<index_start_numeric_TDT>();
        auto index_end_it = index_end_data.template cbegin<index_end_numeric_TDT>();
        for (size_t row_idx = 0;
             row_idx < segment_.row_count();
             ++row_idx, ++version_it, ++creation_ts_it, ++content_hash_it, ++index_start_it, ++index_end_it, ++key_types_it) {
            res.emplace_back(*version_it, *creation_ts_it, *content_hash_it, KeyType(*key_types_it), *index_start_it, *index_end_it);
        }
        return res;
    }
}

bool KeySegment::check_symbols_all_same() const {
    if (stream_ids_.row_count() != 0) {
        auto data = stream_ids_.data();
        auto end_it = data.template cend<stream_id_TDT>();
        auto it = data.template cbegin<stream_id_TDT>();
        uint64_t value{*it};
        for (;it != end_it; ++it) {
            if (*it != value) {
                return false;
            }
        }
    }
    return true;
}

bool KeySegment::check_symbols_all_unique() const {
    if (stream_ids_.row_count() != 0) {
        auto data = stream_ids_.data();
        auto end_it = data.template cend<stream_id_TDT>();
        ankerl::unordered_dense::set<uint64_t> values;
        values.reserve(stream_ids_.row_count());
        for (auto it = data.template cbegin<stream_id_TDT>();it != end_it; ++it) {
            if (!values.insert(*it).second) {
                return false;
            }
        }
    }
    return true;
}

} // arcticdb