/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/index.hpp>

#include <vector>

namespace arcticdb {

// A ref key snapshot whose segment uses the legacy key row layout (pipelines::index::LegacyFields), which read_key_row
// still reads
inline void write_legacy_layout_ref_snapshot(
        const std::shared_ptr<Store>& store, const SnapshotId& snapshot_id, const std::vector<AtomKey>& keys
) {
    using pipelines::index::LegacyFields;
    const auto index_data_type = DataType::NANOSECONDS_UTC64;
    auto desc = index_descriptor(
            snapshot_id,
            stream::RowCountIndex{},
            {scalar_field(DataType::UINT64, "version_id"),
             scalar_field(DataType::ASCII_DYNAMIC64, "stream_id"),
             scalar_field(DataType::UINT64, "creation_ts"),
             scalar_field(DataType::UINT64, "content_hash"),
             scalar_field(DataType::UINT8, "index_type"),
             scalar_field(index_data_type, "start_index"),
             scalar_field(index_data_type, "end_index"),
             scalar_field(DataType::UINT8, "key_type")}
    );
    SegmentInMemory segment{StreamDescriptor{std::move(desc)}};
    for (const auto& key : keys) {
        segment.set_scalar(int(LegacyFields::version_id), key.version_id());
        segment.set_string(int(LegacyFields::stream_id), std::get<StringId>(key.id()));
        segment.set_scalar(int(LegacyFields::creation_ts), key.creation_ts());
        segment.set_scalar(int(LegacyFields::content_hash), key.content_hash());
        segment.set_scalar(int(LegacyFields::index_type), static_cast<uint8_t>(stream::get_index_value_type(key)));
        segment.set_scalar(int(LegacyFields::start_index), std::get<NumericIndex>(key.start_index()));
        segment.set_scalar(int(LegacyFields::end_index), std::get<NumericIndex>(key.end_index()));
        segment.set_scalar(int(LegacyFields::key_type), static_cast<uint8_t>(key.type()));
        segment.end_row();
    }
    store->write(KeyType::SNAPSHOT_REF, snapshot_id, std::move(segment)).get();
}

} // namespace arcticdb
