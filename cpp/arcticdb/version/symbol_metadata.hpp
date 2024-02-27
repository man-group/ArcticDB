#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/stream/index.hpp>
#include "stream/schema.hpp"

namespace arcticdb {

static constexpr std::string_view MetadataSymbol = "__symbol__meta";

struct SymbolMetadata {
    SymbolMetadata(
            entity::timestamp update_time,
            entity::IndexValue start_index,
            entity::IndexValue end_index,
            uint64_t total_rows) :
        update_time_(update_time),
        start_index_(start_index),
        end_index_(end_index),
        total_rows_(total_rows) {
    }

    std::string __repr__() const {
        return fmt::format(
            "update_time: {}, start_index: {}, end_index: {}, total_rows: {}",
            update_time_, start_index_, end_index_, total_rows_
        );
    }

    ARCTICDB_MOVE_COPY_DEFAULT(SymbolMetadata)

    entity::timestamp update_time_;
    entity::IndexValue start_index_;
    entity::IndexValue end_index_;
    uint64_t total_rows_;
};

using MetadataMap = std::unordered_map<StreamId, std::vector<SymbolMetadata>>;

void write_symbol_metadata(
        const std::shared_ptr<Store>& store,
        entity::StreamId symbol,
        entity::IndexValue start_index,
        entity::IndexValue end_index,
        uint64_t total_rows,
        entity::timestamp update_time);

std::pair<SegmentInMemory, std::vector<AtomKey>> compact_metadata_keys(const std::shared_ptr<Store>& store);

void get_symbol_metadata_keys(
    const std::shared_ptr<Store>& store,
    MetadataMap& metadata_map,
    timestamp from_time,
    uint64_t lookback_seconds
    );

void get_symbol_metadata_from_segment(
    MetadataMap& metadata_map,
    SegmentInMemory&& segment,
    timestamp from_time,
    uint64_t lookback_seconds);


constexpr timestamp calc_start_time(
    timestamp from_time,
    uint64_t lookback_seconds
) {
    return from_time - (static_cast<timestamp>(lookback_seconds) * 1000000000);
}
} //namespace arcticdb