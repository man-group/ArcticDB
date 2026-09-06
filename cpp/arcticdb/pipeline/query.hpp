/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/bitset.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/variant.hpp>
#include <vector>
#include <string>
#include <variant>

namespace arcticdb::pipelines {

namespace index { struct IndexSegmentReader; }

struct SnapshotVersionQuery {
    SnapshotId name_;
};

struct TimestampVersionQuery {
    timestamp timestamp_;
    bool iterate_snapshots_if_tombstoned;
};

struct SpecificVersionQuery {
    SignedVersionId version_id_;
    bool iterate_snapshots_if_tombstoned;
};

// Similar to IndexInformation. See comment on IndexInformation for why both are useful.
struct PreloadedIndexQuery {
    PreloadedIndexQuery(AtomKey index_key, SegmentInMemory index_seg, std::shared_ptr<Segment> column_stats_seg = {}) :
        index_key_(std::move(index_key)),
        index_seg_(std::move(index_seg)),
        column_stats_seg_(std::move(column_stats_seg)) {}

    // Key is just needed for constructing the VersionedItem to return
    const AtomKey index_key_;
    const SegmentInMemory index_seg_;
    const std::shared_ptr<Segment> column_stats_seg_;
};

using VersionQueryType = std::variant<
        std::monostate, // Represents "latest"
        SnapshotVersionQuery, TimestampVersionQuery, SpecificVersionQuery, std::shared_ptr<PreloadedIndexQuery>>;

struct VersionQuery {
    VersionQueryType content_;

    void set_snap_name(const std::string& snap_name) { content_ = SnapshotVersionQuery{snap_name}; }

    void set_timestamp(timestamp ts, bool iterate_snapshots_if_tombstoned) {
        content_ = TimestampVersionQuery{ts, iterate_snapshots_if_tombstoned};
    }

    void set_version(SignedVersionId version, bool iterate_snapshots_if_tombstoned) {
        content_ = SpecificVersionQuery{version, iterate_snapshots_if_tombstoned};
    }

    void set_preloaded_index(std::shared_ptr<PreloadedIndexQuery> preloaded_index_query) {
        content_ = preloaded_index_query;
    }
};

template<typename ContainerType>
using FilterQuery =
        folly::Function<std::unique_ptr<util::BitSet>(const ContainerType&, std::unique_ptr<util::BitSet>&&)>;

template<typename ContainerType>
using CombinedQuery = folly::Function<std::unique_ptr<util::BitSet>(const ContainerType&)>;

RowRange slice_row_range_at(const std::vector<SliceAndKey>& sk, std::size_t row);
RowRange slice_row_range_at(const index::IndexSegmentReader& isr, std::size_t row);

bool is_slice_in_row_range(const RowRange& slice_row_range, const RowRange& row_filter);
bool is_slice_in_index_range(IndexRange slice_index_range, const IndexRange& index_filter, bool is_read_operation);

template<typename ContainerType, typename IdxType>
std::unique_ptr<util::BitSet> build_bitset_for_index(
        const ContainerType& container, IndexRange rg, bool dynamic_schema, bool column_groups, bool is_read_operation,
        std::unique_ptr<util::BitSet>&& input
);

} // namespace arcticdb::pipelines

namespace fmt {
using namespace arcticdb::pipelines;

template<>
struct formatter<VersionQuery> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const VersionQuery& q, FormatContext& ctx) const {
        return arcticdb::util::variant_match(
                q.content_,
                [&ctx](const SpecificVersionQuery& s) {
                    return fmt::format_to(ctx.out(), "version {}", s.version_id_);
                },
                [&ctx](const SnapshotVersionQuery& s) { return fmt::format_to(ctx.out(), "snapshot '{}'", s.name_); },
                [&ctx](const TimestampVersionQuery& t) { return fmt::format_to(ctx.out(), "{}", t.timestamp_); },
                                             [&ctx](const std::shared_ptr<arcticdb::pipelines::PreloadedIndexQuery>& p) {
                    return fmt::format_to(ctx.out(), "PreloadedIndexQuery({})", p->index_key_);
                },
                [&ctx](const std::monostate&) { return fmt::format_to(ctx.out(), "latest"); }
        );
    }
};
} // namespace fmt
