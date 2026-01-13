/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/bitset.hpp>
#include <memory>

namespace arcticdb::pipelines {
namespace ranges = std::ranges;
struct PipelineContext;

struct PipelineContextRow {
    std::shared_ptr<PipelineContext> parent_;
    size_t index_ = 0;

    PipelineContextRow(const std::shared_ptr<PipelineContext>& parent, size_t index) : parent_(parent), index_(index) {}

    PipelineContextRow() = default;

    [[nodiscard]] const StringPool& string_pool() const;
    StringPool& string_pool();
    void set_string_pool(const std::shared_ptr<StringPool>& pool);
    const std::shared_ptr<StringPool>& string_pool_ptr();
    void allocate_string_pool();
    [[nodiscard]] const SliceAndKey& slice_and_key() const;
    SliceAndKey& slice_and_key();
    [[nodiscard]] const std::optional<util::BitSet>& get_selected_columns() const;
    bool fetch_index() const;
    [[nodiscard]] const StreamDescriptor& descriptor() const;
    void set_descriptor(StreamDescriptor&& desc);
    void set_descriptor(const StreamDescriptor& desc);
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

    template<class ValueType>
    class PipelineContextIterator
        : public boost::iterator_facade<
                  PipelineContextIterator<ValueType>, ValueType, boost::random_access_traversal_tag> {
        std::shared_ptr<PipelineContext> parent_;
        size_t index_;

      public:
        PipelineContextIterator(std::shared_ptr<PipelineContext> parent, size_t index) :
            parent_(std::move(parent)),
            index_(index) {}

        template<class OtherValue>
        explicit PipelineContextIterator(const PipelineContextIterator<OtherValue>& other) :
            parent_(other.parent_),
            index_(other.index_) {}

        template<class OtherValue>
        bool equal(const PipelineContextIterator<OtherValue>& other) const {
            util::check(parent_ == other.parent_, "Invalid context iterator comparison");
            return index_ == other.index_;
        }

        void increment() { ++index_; }

        void decrement() { --index_; }

        void advance(ptrdiff_t n) { index_ += n; }

        ValueType& dereference() const {
            row_ = PipelineContextRow{parent_, index_};
            return row_;
        }

        mutable PipelineContextRow row_;
    };

    PipelineContext() = default;

    explicit PipelineContext(StreamDescriptor desc) : desc_(std::move(desc)) {}

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
    // When there are staged segments this holds the combined stream descriptor for all staged segments
    // This can be different than desc_ in case dynamic schema is used. Otherwise they must be the same.
    std::optional<StreamDescriptor> staged_descriptor_;
    StreamId stream_id_;
    VersionId version_id_ = 0;
    size_t total_rows_ = 0;
    size_t rows_ = 0;
    std::shared_ptr<arcticdb::proto::descriptors::NormalizationMetadata> norm_meta_;
    std::unique_ptr<arcticdb::proto::descriptors::UserDefinedMetadata> user_meta_;
    std::vector<SliceAndKey> slice_and_keys_;
    util::BitSet fetch_index_;
    std::vector<std::shared_ptr<StringPool>> string_pools_;
    /// Columns the user selected explicitly via the columns read option. These are the columns we must
    /// return as a result of a read operation,
    std::optional<util::BitSet> selected_columns_;
    /// All columns that must be read. This is a superset of PipelineContext::selected_columns_ and is used in cases
    /// where PipelineContext::selected_columns_ depend on other columns, e.g. when projecting a column with the
    /// QueryBuilder.
    std::optional<util::BitSet> overall_column_bitset_;
    // Stores the field descriptors for the columns in PipelineContext::selected_columns_
    std::shared_ptr<FieldCollection> filter_columns_;
    // Set of the field names in PipelineContext::filter_columns_ used for faster search
    std::optional<std::unordered_set<std::string_view>> filter_columns_set_;
    std::vector<std::shared_ptr<StreamDescriptor>> segment_descriptors_;
    std::optional<SegmentInMemory> multi_key_;
    std::vector<unsigned char> compacted_;
    std::optional<size_t> incompletes_after_;
    /// Used to override the default values of types when the NullValueReducer fills missing segments. For example, in
    /// the sum unordered aggregation and the sum resampling clause, the value must 0 even if the output type is float.
    ankerl::unordered_dense::map<std::string, Value> default_values_;
    bool bucketize_dynamic_ = false;

    PipelineContextRow operator[](size_t num) { return PipelineContextRow{shared_from_this(), num}; }

    size_t last_row() const {
        if (slice_and_keys_.empty()) {
            return 0;
        } else {
            if (bucketize_dynamic_) {
                return ranges::max(
                               slice_and_keys_, {}, [](const auto& sk) { return sk.slice_.row_range.second; }
                ).slice_.row_range.second;
            } else {
                return slice_and_keys_.rbegin()->slice_.row_range.second;
            }
        }
    }

    size_t first_row() const { return slice_and_keys_.empty() ? 0 : slice_and_keys_.begin()->slice_.row_range.first; }

    size_t calc_rows() const { return last_row() - first_row(); }

    const StreamDescriptor& descriptor() const {
        util::check(static_cast<bool>(desc_), "Stream descriptor not found in pipeline context");
        return *desc_;
    }

    void set_descriptor(StreamDescriptor&& desc) { desc_ = std::move(desc); }

    void set_descriptor(const StreamDescriptor& desc) { desc_ = desc; }

    void set_selected_columns(const std::optional<std::vector<std::string>>& columns);

    IndexRange index_range() const {
        if (slice_and_keys_.empty())
            return unspecified_range();

        return IndexRange{slice_and_keys_.begin()->key().start_index(), slice_and_keys_.rbegin()->key().end_index()};
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
        swap(left.overall_column_bitset_, right.overall_column_bitset_);
        swap(left.filter_columns_, right.filter_columns_);
        swap(left.segment_descriptors_, right.segment_descriptors_);
        swap(left.filter_columns_set_, right.filter_columns_set_);
        swap(left.compacted_, right.compacted_);
        swap(left.staged_descriptor_, right.staged_descriptor_);
    }

    using iterator = PipelineContextIterator<PipelineContextRow>;
    using const_iterator = PipelineContextIterator<const PipelineContextRow>;
    iterator begin() { return iterator{shared_from_this(), size_t(0)}; }

    iterator incompletes_begin() { return iterator{shared_from_this(), incompletes_after()}; }

    size_t incompletes_after() const { return incompletes_after_.value_or(slice_and_keys_.size()); }

    iterator end() { return iterator{shared_from_this(), slice_and_keys_.size()}; }

    bool is_in_filter_columns_set(std::string_view name) {
        return !filter_columns_set_ || filter_columns_set_->find(name) != filter_columns_set_->end();
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
        return norm_meta_->input_type_case() ==
               arcticdb::proto::descriptors::NormalizationMetadata::InputTypeCase::kMsgPackFrame;
    }

    bool only_index_columns_selected() const;
};

} // namespace arcticdb::pipelines