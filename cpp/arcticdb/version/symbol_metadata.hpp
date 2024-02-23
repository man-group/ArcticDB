#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/stream/index.hpp>
#include "stream/schema.hpp"

namespace arcticdb {

static constexpr std::string_view MetadataSymbol = "__symbol__meta";
static constexpr std::string_view MetadataIndexName = "update_time";

stream::TimeseriesIndex symbol_metadata_index() {
    return stream::TimeseriesIndex{std::string{MetadataIndexName}};
}

entity::StreamDescriptor symbol_metadata_descriptor() {
    return symbol_metadata_index().create_stream_descriptor(StreamId{std::string{MetadataSymbol}},  {
        scalar_field(DataType::UTF_DYNAMIC64, "symbol"),
        scalar_field(DataType::NANOSECONDS_UTC64, "start_index"),
        scalar_field(DataType::NANOSECONDS_UTC64, "end_index"),
        scalar_field(DataType::UINT64, "total_rows")
    });
};

enum class MetadataField : std::size_t {
    UpdateTime = 0,
    Symbol = 1,
    StartIndex = 2,
    EndIndex = 3,
    TotalRows = 4
};

constexpr size_t pos(MetadataField field) {
    return static_cast<size_t>(field);
}

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
        entity::timestamp update_time) {
    SegmentInMemory segment{StreamDescriptor{symbol}};

    store->write_sync(KeyType::METRICS, VersionId{total_rows}, symbol, start_index, end_index, std::move(segment));
}

template <class Predicate>
std::vector<AtomKey> scan_metadata_keys(const std::shared_ptr<Store>& store, const Predicate& predicate) {
    std::vector<AtomKey> output;
    store->iterate_type(KeyType::METRICS, [&predicate, &output] (const auto& vk) {
        const auto& key = to_atom(vk);
        if(predicate(key))
            output.emplace_back(key);
    });
    return output;
}

std::pair<SegmentInMemory, std::vector<AtomKey>> compact_metadata_keys(const std::shared_ptr<Store>& store) {
    using namespace arcticdb::stream;

    auto keys = scan_metadata_keys(store, [] (const auto&) { return true; });
    using AggregatorType = Aggregator<stream::TimeseriesIndex, FixedSchema, NeverSegmentPolicy>;
    SegmentInMemory output;
    AggregatorType agg{FixedSchema{symbol_metadata_descriptor(), symbol_metadata_index()}, [&output](auto&& segment) {
        output = std::forward<SegmentInMemory>(segment);
    }};

    for(const auto& key : keys) {
        agg.start_row(key.creation_ts())([&key] (auto&& rb) {
            rb.set_string(pos(MetadataField::Symbol), std::get<StringId>(key.id()));
            rb.template set_scalar<timestamp>(pos(MetadataField::StartIndex), key.start_time());
            rb.template set_scalar<timestamp>(pos(MetadataField::EndIndex), key.end_time());
            rb.template set_scalar<uint64_t>(pos(MetadataField::TotalRows), key.version_id());
        });
    }
    agg.commit();
    return std::make_pair(std::move(output), std::move(keys));
}

constexpr timestamp calc_start_time(
    timestamp from_time,
    uint64_t lookback_seconds
    ) {
    return from_time - (static_cast<timestamp>(lookback_seconds) * 1000000000);
}

void get_symbol_metadata_keys(
    const std::shared_ptr<Store>& store,
    MetadataMap& metadata_map,
    timestamp from_time,
    uint64_t lookback_seconds
    ) {
    const auto start_time = calc_start_time(from_time, lookback_seconds);
    auto journal_keys = scan_metadata_keys(store, [start_time, from_time] (const auto& vk) {
        const auto& key = to_atom(vk);
        return key.creation_ts() >= start_time && key.creation_ts() <= from_time;
    });

    for(const auto& key : journal_keys) {
        metadata_map[key.id()].emplace_back(
            key.creation_ts(),
            key.start_time(),
            key.end_time(),
            key.version_id());
    }
}

void get_symbol_metadata_from_segment(
    MetadataMap& metadata_map,
    SegmentInMemory&& segment,
    timestamp from_time,
    uint64_t lookback_seconds) {
    const auto start_time = calc_start_time(from_time, lookback_seconds);
    const auto start_point = segment.column(0).search_sorted(start_time);
    const auto end_point = segment.column(0).search_sorted(from_time, true);

    auto row = start_point;
    for(; row < end_point; ++row) {
        const auto symbol = segment.string_at(row, pos(MetadataField::Symbol)).value();
        const auto update_time = segment.scalar_at<timestamp>(row, pos(MetadataField::UpdateTime)).value();
        const auto start_index = segment.scalar_at<timestamp>(row, pos(MetadataField::StartIndex)).value();
        const auto end_index = segment.scalar_at<timestamp>(row, pos(MetadataField::EndIndex)).value();
        const auto total_rows = segment.scalar_at<timestamp>(row, pos(MetadataField::TotalRows)).value();

        metadata_map[StringId{symbol}].emplace_back(update_time, start_index, end_index, total_rows);
    }
}

} //namespace arcticdb