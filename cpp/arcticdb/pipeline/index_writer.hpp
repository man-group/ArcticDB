/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/pipeline_common.hpp>

namespace arcticdb::pipelines::index {
// TODO: change the name - something like KeysSegmentWriter or KeyAggragator or  better
template<ValidIndex Index>
class IndexWriter {
    // All index segments are row-count indexed in the sense that the keys are
    // already ordered - they don't need an additional index
    using AggregatorIndexType = stream::RowCountIndex;
    using SliceAggregator = stream::Aggregator<AggregatorIndexType, stream::FixedSchema, stream::NeverSegmentPolicy>;
    using Desc = stream::IndexSliceDescriptor<AggregatorIndexType>;

  public:
    ARCTICDB_MOVE_ONLY_DEFAULT(IndexWriter)

    IndexWriter(
            std::shared_ptr<stream::StreamSink> sink, IndexPartialKey partial_key, const TimeseriesDescriptor& tsd,
            const std::optional<KeyType>& key_type = std::nullopt, bool sync = false
    ) :
        bucketize_columns_(tsd.column_groups()),
        sync_(sync),
        partial_key_(std::move(partial_key)),
        slice_descriptor_(partial_key_.id, bucketize_columns_),
        agg_(
                Desc::schema(slice_descriptor_),
                [&](auto&& segment) { on_segment(std::forward<SegmentInMemory>(segment)); },
                stream::NeverSegmentPolicy{}, slice_descriptor_
        ),
        sink_(std::move(sink)),
        key_being_committed_(folly::Future<AtomKey>::makeEmpty()),
        key_type_(key_type) {
        agg_.segment().set_timeseries_descriptor(tsd);
    }

    void add_unchecked(const arcticdb::entity::AtomKey& key, const FrameSlice& slice) {
        auto add_to_row ARCTICDB_UNUSED = [&](auto& rb) {
            rb.set_scalar(int(Fields::version_id), key.version_id());
            rb.set_scalar(int(Fields::creation_ts), key.creation_ts());
            rb.set_scalar(int(Fields::content_hash), key.content_hash());
            rb.set_scalar(int(Fields::index_type), static_cast<uint8_t>(stream::get_index_value_type(key)));

            std::visit([&rb](auto&& val) { rb.set_scalar(int(Fields::stream_id), val); }, key.id());

            // note that we don't se the start index since its presence is index type specific
            std::visit([&rb](auto&& val) { rb.set_scalar(int(Fields::end_index), val); }, key.end_index());

            rb.set_scalar(int(Fields::key_type), static_cast<char>(key.type()));

            rb.set_scalar(int(Fields::start_col), slice.col_range.first);
            rb.set_scalar(int(Fields::end_col), slice.col_range.second);
            rb.set_scalar(int(Fields::start_row), slice.row_range.first);
            rb.set_scalar(int(Fields::end_row), slice.row_range.second);

            if (bucketize_columns_) {
                util::check(
                        static_cast<bool>(slice.hash_bucket()) && static_cast<bool>(slice.num_buckets()),
                        "Found no hash bucket in an index writer with bucketizing"
                );
                rb.set_scalar(int(Fields::hash_bucket), *slice.hash_bucket());
                rb.set_scalar(int(Fields::num_buckets), *slice.num_buckets());
            }
        };

        agg_.start_row()([&](auto& rb) {
            std::visit([&rb](auto&& val) { rb.set_scalar(int(Fields::start_index), val); }, key.start_index());
            add_to_row(rb);
        });
    }

    void add(const arcticdb::entity::AtomKey& key, const FrameSlice& slice) {
        // ensure sorted by col group then row group, this is normally the case but in append scenario,
        // one will need to ensure that this holds, otherwise the assumptions in the read pipeline will be
        // broken.
        ARCTICDB_DEBUG(log::version(), "Writing key {} to the index", key);
        util::check_arg(
                !current_col_.has_value() || *current_col_ <= slice.col_range.first,
                "expected increasing column group, last col range left value {}, arg {}",
                current_col_.value_or(-1),
                slice.col_range
        );

        bool new_col_group = !current_col_.has_value() || *current_col_ < slice.col_range.first;
        bool missing_row_val = !current_row_.has_value();
        bool is_valid_col = *current_col_ == slice.col_range.first;
        bool is_valid_row = *current_row_ < slice.row_range.first;
        bool is_valid = (is_valid_col && is_valid_row);
        util::check_arg(
                missing_row_val || new_col_group || is_valid,
                "expected increasing row group, last col range left value {}, col arg {}, row left value {}, row arg "
                "{}",
                current_col_.value_or(-1),
                slice.col_range,
                current_row_.value_or(-1),
                slice.row_range
        );

        add_unchecked(key, slice);

        if (new_col_group) {
            current_col_ = slice.col_range.first;
        }
        current_row_ = slice.row_range.first;
    }

    folly::Future<arcticdb::entity::AtomKey> commit() {
        util::check_arg(!sync_, "commit() called on a IndexWriter that was not created with sync = false");
        agg_.finalize();
        return std::move(key_being_committed_);
    }

    arcticdb::entity::AtomKey commit_sync() {
        util::check_arg(sync_, "commit_sync() called on a IndexWriter that was not created with sync = true");
        agg_.finalize();
        util::check(committed_key_.has_value(), "The index writer was not able to commit a key");
        return committed_key_.value();
    }

  private:
    IndexValue segment_start(const SegmentInMemory& segment) const {
        return Index::start_value_for_keys_segment(segment);
    }

    IndexValue segment_end(const SegmentInMemory& segment) const { return Index::end_value_for_keys_segment(segment); }

    void on_segment(SegmentInMemory&& s) {
        auto seg = std::move(s);
        auto key_type = key_type_.value_or(get_key_type_for_index_stream(partial_key_.id));

        if (sync_) {
            committed_key_ = std::make_optional(to_atom(sink_->write_sync(
                    key_type,
                    partial_key_.version_id,
                    partial_key_.id,
                    segment_start(seg),
                    segment_end(seg),
                    std::move(seg)
            )));
        } else {

            key_being_committed_ = sink_->write(key_type,
                                                partial_key_.version_id,
                                                partial_key_.id,
                                                segment_start(seg),
                                                segment_end(seg),
                                                std::move(seg))
                                           .thenValue([](auto&& variant_key) { return to_atom(variant_key); });
        }
    }

    bool bucketize_columns_ = false;
    bool sync_ = false;
    IndexPartialKey partial_key_;
    stream::IndexSliceDescriptor<AggregatorIndexType> slice_descriptor_;
    SliceAggregator agg_;
    std::shared_ptr<stream::StreamSink> sink_;
    folly::Future<arcticdb::entity::AtomKey> key_being_committed_;
    std::optional<arcticdb::entity::AtomKey> committed_key_ = std::nullopt;
    std::optional<std::size_t> current_col_ = std::nullopt;
    std::optional<std::size_t> current_row_ = std::nullopt;
    std::optional<KeyType> key_type_ = std::nullopt;
};

} // namespace arcticdb::pipelines::index