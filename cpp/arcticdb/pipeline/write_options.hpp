/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb {
struct WriteOptions {
    size_t column_group_size = 127;
    size_t segment_row_size = 100'000;
    size_t max_num_buckets = 150;
    bool prune_previous_version = false;
    bool de_duplication = false;
    bool snapshot_dedup = false;
    bool dynamic_schema = false;
    bool ignore_sort_order = false;
    bool bucketize_dynamic = false;
    bool sparsify_floats = false;
    bool empty_types = false;

    static WriteOptions from_proto(const arcticdb::proto::storage::VersionStoreConfig::WriteOptions & opt) {
        WriteOptions def;
        return {
            .column_group_size=opt.dynamic_schema() && !opt.bucketize_dynamic() ? std::numeric_limits<size_t>::max() : (
                opt.column_group_size() > 0 ? size_t(opt.column_group_size()) : def.column_group_size),
            .segment_row_size=opt.segment_row_size() > 0 ? size_t(opt.segment_row_size()) : def.segment_row_size,
            .max_num_buckets=opt.max_num_buckets() > 0 ? size_t(opt.max_num_buckets()) : def.max_num_buckets,
            .prune_previous_version=opt.prune_previous_version(),
            .de_duplication=opt.de_duplication(),
            .snapshot_dedup=opt.snapshot_dedup(),
            .dynamic_schema=opt.dynamic_schema(),
            .ignore_sort_order=opt.ignore_sort_order(),
            .bucketize_dynamic=opt.bucketize_dynamic(),
            .sparsify_floats=opt.sparsify_floats(),
            .empty_types=opt.empty_types()
        };
    }
};
} //namespace arcticdb