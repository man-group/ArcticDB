#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/version/symbol_metadata.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/util/frame_adapter.hpp>

#include <unordered_map>

namespace arcticdb {

timestamp as_time(const IndexValue &index) {
    util::check(std::holds_alternative<NumericIndex>(index), "Expected index to have numeric value");
    return static_cast<timestamp>(std::get<NumericIndex>(index));
}

std::unordered_map<entity::StreamId, SymbolMetadata> get_symbol_metadata(
    version_store::LocalVersionedEngine &engine,
    std::vector<StreamId> symbols,
    timestamp from_time,
    uint64_t lookback_seconds
) {
    MetadataMap metadata_map;
    get_symbol_metadata_keys(engine.get_store(), metadata_map, from_time, lookback_seconds);

    auto start_time = calc_start_time(from_time, lookback_seconds);
    IndexRange index_range{start_time, from_time};
    ReadQuery read_query;
    read_query.row_filter = index_range;
    VersionQuery version_query;
    version_query.set_timestamp(from_time);
    auto res = engine.read_dataframe_version_internal(
        StringId{MetadataSymbol},
        version_query,
        read_query,
        ReadOptions{});

    get_symbol_metadata_from_segment(
        metadata_map,
        std::move(res.frame_and_descriptor_.frame_),
        from_time,
        lookback_seconds);

    std::vector<StreamId> missing_symbols;
    std::unordered_map<entity::StreamId, SymbolMetadata> output;
    for (const auto &symbol : symbols) {
        if (auto it = metadata_map.find(symbol); it != metadata_map.end()) {
            util::check(!it->second.empty(), "Unexpected empty list in symbol metadata");
            if (it->second.size() == 1) {
                output.try_emplace(symbol, it->second[0]);
            } else {
                auto &metadata_vec = it->second;
                std::sort(std::begin(metadata_vec), std::end(metadata_vec), [](const auto &l, const auto &r) {
                    return l.update_time_ < r.update_time_;
                });
                auto vec_it = metadata_vec.begin();
                do {
                    output.emplace(symbol, *vec_it);
                } while (vec_it != std::end(metadata_vec) && vec_it->update_time_ <= from_time);
            }
        } else {
            missing_symbols.push_back(symbol);
        }
    }

    if (!missing_symbols.empty()) {
        std::vector<VersionQuery> version_queries;
        version_queries.resize(missing_symbols.size());
        std::for_each(std::begin(version_queries), std::end(version_queries), [from_time](auto &v) {
            v.set_timestamp(from_time);
        });

        std::shared_ptr<VersionMap> version_map;
        auto version_futures =
            batch_get_versions_async(engine.get_store(), version_map, missing_symbols, version_queries, false);
        std::vector<folly::Future<DescriptorItem>> descriptor_futures;
        for (auto &&[idx, version_fut] : folly::enumerate(version_futures)) {
            descriptor_futures.emplace_back(
                engine.get_descriptor_async(std::move(version_fut), missing_symbols[idx], version_queries[idx]));
        }
        auto descriptors = folly::collect(descriptor_futures).get();
        for (const auto &item : descriptors) {
            arcticdb::proto::descriptors::TimeSeriesDescriptor tsd;
            item.timeseries_descriptor()->UnpackTo(&tsd);
            output.try_emplace(item.symbol(),
                               item.creation_ts(),
                               as_time(*item.start_index()),
                               as_time(*item.end_index()),
                               tsd.total_rows());
        }
        return output;
    }
}

void compact_symbol_metadata(
    version_store::LocalVersionedEngine &engine) {
    auto [segment, keys] = compact_metadata_keys(engine.get_store());
    SegmentToInputFrameAdapter frame_adapter(std::move(segment));
    engine.append_internal(StringId{MetadataSymbol}, frame_adapter.input_frame_, true, false, false);
    delete_keys(engine.get_store(), std::move(keys), {});
}

} // namespace arcticdb