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
    static WriteOptions from_proto(const arcticdb::proto::storage::VersionStoreConfig::WriteOptions & opt){
        WriteOptions def;
        return {
                opt.dynamic_schema() && !opt.bucketize_dynamic() ? std::numeric_limits<size_t>::max() :
                    (opt.column_group_size() > 0 ? size_t(opt.column_group_size()) : def.column_group_size),
                opt.segment_row_size() > 0 ? size_t(opt.segment_row_size()): def.segment_row_size,
                opt.prune_previous_version(),
                opt.de_duplication(),
                opt.snapshot_dedup(),
                opt.dynamic_schema(),
                opt.ignore_sort_order(),
                opt.bucketize_dynamic(),
                opt.max_num_buckets() > 0 ? size_t(opt.max_num_buckets()) : def.max_num_buckets,
                opt.compact_incomplete_dedup_rows()
        };
    }

    size_t column_group_size = 127;
    size_t segment_row_size = 100'000;
    bool prune_previous_version;
    bool de_duplication;
    bool snapshot_dedup;
    bool dynamic_schema;
    bool ignore_sort_order;
    bool bucketize_dynamic;
    size_t max_num_buckets = 150;
    bool compact_incomplete_dedup_rows;
};
} //namespace arcticdb