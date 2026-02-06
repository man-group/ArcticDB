/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

namespace arcticdb {
struct SchemaItem {
    SchemaItem(entity::AtomKey&& key, SegmentInMemory&& index_seg, StreamDescriptor&& desc) :
        key_(std::move(key)),
        index_seg_(std::move(index_seg)) {
        // TODO: Profile how slow this (and iteration in Python) is for wide dataframes, and consider
        //  adding Python bindings for our more efficient StreamDescriptor representation (or just the
        //  fields, probably all we need)
        // We also are currently converting protobuf to our internal repr and back again here, could avoid this by
        // refactoring
        copy_stream_descriptor_to_proto(desc, desc_);
    }

    // Needed to know which version was read for error messages if reads using this object fails
    entity::AtomKey key_;
    SegmentInMemory index_seg_;
    arcticdb::proto::descriptors::StreamDescriptor desc_;
};
} // namespace arcticdb