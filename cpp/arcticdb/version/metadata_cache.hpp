#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/version/symbol_metadata.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/util/frame_adapter.hpp>

#include <unordered_map>

namespace arcticdb {

std::unordered_map<entity::StreamId, SymbolMetadata> get_symbol_metadata(
    version_store::LocalVersionedEngine &engine,
    std::vector<StreamId> symbols,
    timestamp from_time,
    uint64_t lookback_seconds);

void compact_symbol_metadata(
    version_store::LocalVersionedEngine &engine);

} // namespace arcticdb