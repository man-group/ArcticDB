#pragma once

#include <arcticdb/storage/coalesced/multisegment.hpp>
#include <arcticdb/storage/coalesced/coalesced_storage_common.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/stream/stream_utils.hpp>

namespace arcticdb::storage::coalesced {


class CoalescedKeyType {
    using GetSegmentFunc = folly::Function<KeySegmentPair(VariantKey)>;
    using KeyExistsFunc = folly::Function<bool(VariantKey)>;
    KeyType key_type_;
    GetSegmentFunc get_segment_;
    KeyExistsFunc key_exists_;
    std::optional<SegmentInMemory> index_;
    
    CoalescedKeyType(
        KeyType key_type,
        GetSegmentFunc&& get_segment, KeyExistsFunc key_exists) :
        key_type_(key_type),    
        get_segment_(std::move(get_segment)),
        key_exists_(std::move(key_exists)),
        index_(get_latest_index_segment()) {
    }
    
    Segment get_segment(const AtomKey& key) {
        if(!index_)
            get_latest_index_segment();

        if(!index_ || index_->empty())
            throw NoDataFoundException(fmt::format("Coalesced storage for key type {} does not exist fetching key {}", key_type_, key));

        auto time_symbol = time_symbol_from_key(key);
        const auto& index_column = index_->column(0);
        using IndexTagType = MultiSegmentHeader::TimeSymbolTag;
        auto it = std::lower_bound(index_column.begin<IndexTagType>(), index_column.end<IndexTagType>(), time_symbol.data());
        while(*it == time_symbol.data()) {
            const auto multi_seg_key = stream::read_key_row_into_builder<pipelines::index::Fields>(*index_, *it);

        }
    }
    
private:
    RefKey get_ref_key() {
        return RefKey{coalesced_id_for_key_type(key_type_), key_type_};
    }
    
    std::optional<SegmentInMemory> get_latest_index_segment() {
        const auto ref_key = get_ref_key();
        auto ref_exists = key_exists_(ref_key);
        if(!ref_exists)
            return std::nullopt;

        auto mem_seg = decode_segment(std::move(get_segment_(ref_key).segment()));
        auto index_key = stream::read_key_row(mem_seg, 0);
        index_ = decode_segment(std::move(get_segment_(index_key).segment()));
    }
};

}  //namespace arcticdb