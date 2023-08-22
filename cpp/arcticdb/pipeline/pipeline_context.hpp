/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/string_pool.hpp>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>

#include <boost/iterator_adaptors.hpp>

#include <memory>

namespace arcticdb::pipelines {

struct PipelineContext;

struct PipelineContextRow {
    std::shared_ptr<PipelineContext> parent_;
    size_t index_ = 0;

    PipelineContextRow(const std::shared_ptr<PipelineContext>& parent, size_t index) :
        parent_(parent),
        index_(index) { }

    PipelineContextRow() = default;

    [[nodiscard]] const StringPool& string_pool() const;
    StringPool& string_pool();
    void set_string_pool(const std::shared_ptr<StringPool>& pool);
    void allocate_string_pool();
    [[nodiscard]] const SliceAndKey& slice_and_key() const;
    SliceAndKey& slice_and_key();
    [[nodiscard]] const std::optional<util::BitSet>& get_selected_columns() const;
    bool selected_columns(size_t n) const;
    bool fetch_index();
    [[nodiscard]] const StreamDescriptor& descriptor() const;
    void set_descriptor(StreamDescriptor&& desc);
    void set_descriptor(std::shared_ptr<StreamDescriptor>&& desc);
    void set_descriptor(const std::shared_ptr<StreamDescriptor>& desc);
    void set_compacted(bool val);
    [[nodiscard]] bool compacted() const;
    [[nodiscard]] bool has_string_pool() const;
    [[nodiscard]] size_t index() const;
};

/*
 * A PipelineContext instance persists throughout the lifetime of an operation. It is instantiated high up in the
 * call stack and is then passed through the call stack allowing for convenient access to in-scope data.
 */
struct PipelineContext : public std::enable_shared_from_this<PipelineContext> {

    template <class ValueType>
    class PipelineContextIterator :  public boost::iterator_facade<PipelineContextIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
        std::shared_ptr<PipelineContext> parent_;
        size_t  index_;
    public:
        PipelineContextIterator(std::shared_ptr<PipelineContext> parent, size_t index)
            :  parent_(std::move(parent)), index_(index) { }

        template <class OtherValue>
        explicit PipelineContextIterator(const PipelineContextIterator<OtherValue>& other)
            : parent_(other.parent_), index_(other.index_){}

        template <class OtherValue>
        bool equal(const PipelineContextIterator<OtherValue>& other) const
        {
            util::check(parent_ == other.parent_, "Invalid context iterator comparison");
            return index_ == other.index_;
        }

        void increment(){ ++index_; }

        void decrement(){ --index_; }

        void advance(ptrdiff_t n){ index_ += n; }

        ValueType& dereference() const {
            row_ = PipelineContextRow{parent_, index_};
            return row_;
        }

        mutable PipelineContextRow row_;
    };

    PipelineContext() = default;

    explicit PipelineContext(StreamDescriptor desc) :
        desc_(std::move(desc)) {}

    explicit PipelineContext(SegmentInMemory& frame, const AtomKey& key);

    PipelineContext(const PipelineContext& other) = delete;
    PipelineContext& operator=(const PipelineContext& other) = delete;

    // StreamDescriptor that can mutated for modifying operations. At the end of the pipeline,
    // the modified descriptor can be written out to storage, otherwise this'll match what was read from storage.
    std::optional<StreamDescriptor> desc_;
    // If user requests strings to be returned in a different format (e.g. fixed rather than dynamic) than they were
    // written in, desc_ will be modified such that the return matches what's requested, and this'll be set to the
    // original value. It's only set in this edge case.
    std::optional<StreamDescriptor> orig_desc_;
    // Usually same as what's in desc_. For joins this can be mutated.
    StreamId stream_id_;
    VersionId version_id_ = 0;
    // Used to keep track of the total number of rows when compacting incomplete segments and
    // in sort merge
    size_t total_rows_ = 0;
    // The number of rows according to the timeseries descriptor
    size_t rows_ = 0;
    std::shared_ptr<arcticdb::proto::descriptors::NormalizationMetadata> norm_meta_;
    std::unique_ptr<arcticdb::proto::descriptors::UserDefinedMetadata> user_meta_;
    std::vector<SliceAndKey> slice_and_keys_;
    util::BitSet fetch_index_;
    std::vector<std::shared_ptr<StringPool>> string_pools_;
    std::optional<util::BitSet> selected_columns_;
    std::shared_ptr<FieldCollection> filter_columns_;
    std::vector<std::shared_ptr<StreamDescriptor>> segment_descriptors_;
    std::optional<std::unordered_set<std::string_view>> filter_columns_set_;
    std::optional<SegmentInMemory> multi_key_;
    std::optional<util::BitSet> overall_column_bitset_;
    std::vector<unsigned char> compacted_;
    std::optional<size_t> incompletes_after_;
    bool bucketize_dynamic_ = false;

    PipelineContextRow operator[](size_t num) {
        return PipelineContextRow{shared_from_this(), num};
    }

    size_t last_row() const {
        if (slice_and_keys_.empty())
            return 0;
        else{
            if (bucketize_dynamic_){
                size_t max_row = 0;
                std::for_each(slice_and_keys_.begin(), slice_and_keys_.end(), [&max_row](const auto &sk){
                    max_row = std::max(max_row, sk.slice_.row_range.second);
                });
                return max_row;
            }
            else
                return slice_and_keys_.rbegin()->slice_.row_range.second;
        }
    }

    size_t first_row() const {
        return slice_and_keys_.empty() ? 0 : slice_and_keys_.begin()->slice_.row_range.first;
    }

    size_t calc_rows() const {
        return last_row() - first_row();
    }

    const StreamDescriptor& descriptor() const {
        util::check(static_cast<bool>(desc_), "Stream descriptor not found in pipeline context");
        return *desc_;
    }

    void set_descriptor(StreamDescriptor&& desc) {
        desc_ = std::move(desc);
    }

    void set_descriptor(const StreamDescriptor& desc) {
        desc_ = desc;
    }

    void set_selected_columns(const std::vector<std::string>& columns);

    IndexRange index_range() const {
        if(slice_and_keys_.empty())
            return unspecified_range();

        return IndexRange{ slice_and_keys_.begin()->key().start_index(), slice_and_keys_.rbegin()->key().end_index() };
    }

    friend void swap(PipelineContext& left, PipelineContext& right) noexcept {
        using std::swap;

        swap(left.desc_, right.desc_);
        swap(left.slice_and_keys_, right.slice_and_keys_);
        swap(left.stream_id_, right.stream_id_);
        swap(left.version_id_, right.version_id_);
        swap(left.total_rows_, right.total_rows_);
        swap(left.norm_meta_, right.norm_meta_);
        swap(left.fetch_index_, right.fetch_index_);
        swap(left.string_pools_, right.string_pools_);
        swap(left.selected_columns_, right.selected_columns_);
        swap(left.filter_columns_, right.filter_columns_);
        swap(left.segment_descriptors_, right.segment_descriptors_);
        swap(left.filter_columns_set_, right.filter_columns_set_);
        swap(left.compacted_, right.compacted_);
    }

    using iterator = PipelineContextIterator<PipelineContextRow>;
    using const_iterator = PipelineContextIterator<const PipelineContextRow>;
    iterator begin() { return iterator{shared_from_this(), size_t(0)}; }

    iterator incompletes_begin() { return iterator{shared_from_this(), incompletes_after() }; }

    size_t incompletes_after() const { return incompletes_after_.value_or(slice_and_keys_.size());  }

    iterator end() {
        return iterator{shared_from_this(),  slice_and_keys_.size()};
    }

    bool is_in_filter_columns_set(std::string_view name) {
        return !filter_columns_set_ || filter_columns_set_.value().find(name) != filter_columns_set_.value().end();
    }

    void clear_vectors() {
        slice_and_keys_.clear();
        fetch_index_.clear();
        string_pools_.clear();
        segment_descriptors_.clear();
        compacted_.clear();
    }

    void ensure_vectors() {
        util::check(slice_and_keys_.size() == fetch_index_.size(), "Size mismatch in pipeline context index vector");
        auto size = slice_and_keys_.size();
        string_pools_.resize(size);
        segment_descriptors_.resize(size);
        compacted_.resize(size);
    }

    bool is_pickled() const {
        util::check(static_cast<bool>(norm_meta_), "No normalization metadata defined");
        return norm_meta_->input_type_case() == arcticdb::proto::descriptors::NormalizationMetadata::InputTypeCase::kMsgPackFrame;
    }
};

}