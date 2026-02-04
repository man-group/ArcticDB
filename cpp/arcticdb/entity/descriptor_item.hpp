/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <fmt/format.h>
#include <string>

namespace arcticdb {
struct DescriptorItem {
    DescriptorItem(
            entity::AtomKey&& key, std::optional<timestamp> start_index, std::optional<timestamp> end_index,
            std::optional<TimeseriesDescriptor> timeseries_descriptor
    ) :

        key_(std::move(key)),
        start_index_(start_index),
        end_index_(end_index),
        timeseries_descriptor_(std::move(timeseries_descriptor)) {}

    DescriptorItem() = delete;

    entity::AtomKey key_;
    std::optional<timestamp> start_index_;
    std::optional<timestamp> end_index_;
    std::optional<TimeseriesDescriptor> timeseries_descriptor_;
    std::optional<SegmentInMemory> index_segment_;

    std::string symbol() const { return fmt::format("{}", key_.id()); }
    uint64_t version() const { return key_.version_id(); }
    timestamp creation_ts() const { return key_.creation_ts(); }
    std::optional<timestamp> start_index() const { return start_index_; }
    std::optional<timestamp> end_index() const { return end_index_; }
    std::optional<TimeseriesDescriptor> timeseries_descriptor() const { return timeseries_descriptor_; }
    entity::AtomKey key() const { return key_; }
    std::optional<SegmentInMemory> index_segment() const { return index_segment_; }
};
} // namespace arcticdb