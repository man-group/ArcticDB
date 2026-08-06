/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/read_options.hpp>
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

        template<class OtherValue>
        ptrdiff_t distance_to(const PipelineContextIterator<OtherValue>& other) const {
            return static_cast<ptrdiff_t>(other.index_) - static_cast<ptrdiff_t>(index_);
        }

        ValueType& dereference() const {
            row_ = PipelineContextRow{parent_, index_};
            return row_;
        }

        mutable PipelineContextRow row_;
    };

    PipelineContext() = default;

    explicit PipelineContext(StreamDescriptor desc) : on_disk_descriptor_(std::move(desc)) {}

    explicit PipelineContext(SegmentInMemory& frame, const AtomKey& key);

    PipelineContext(const PipelineContext& other) = delete;
    PipelineContext& operator=(const PipelineContext& other) = delete;

    // When there are staged segments this holds the combined stream descriptor for all staged segments
    // This can be different than on_disk_descriptor_ in case dynamic schema is used. Otherwise they must be the same.
    std::optional<StreamDescriptor> staged_descriptor_;
    StreamId stream_id_;
    VersionId version_id_ = 0;
    size_t total_rows_ = 0;
    size_t rows_ = 0;
    // Used in appends with compact_data to check the data can be appended
    std::optional<IndexValue> last_existing_index_value_;
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

    /// The descriptor of the dataframe that will be presented to the user.
    const StreamDescriptor& output_descriptor() const;

    const proto::descriptors::NormalizationMetadata& output_normalization() const;

    const ankerl::unordered_dense::map<std::string, Value>& output_default_values() const;

    void set_selected_columns(const std::optional<std::vector<std::string>>& columns);

    void generate_string_coerced_descriptor(const ReadOptions& read_options);

    void generate_filtered_field_descriptors(const std::optional<std::vector<std::string>>& columns);

    IndexRange index_range() const {
        if (slice_and_keys_.empty())
            return unspecified_range();

        return IndexRange{slice_and_keys_.begin()->key().start_index(), slice_and_keys_.rbegin()->key().end_index()};
    }

    friend void swap(PipelineContext& left, PipelineContext& right) noexcept {
        using std::swap;

        swap(left.on_disk_descriptor_, right.on_disk_descriptor_);
        swap(left.slice_and_keys_, right.slice_and_keys_);
        swap(left.stream_id_, right.stream_id_);
        swap(left.version_id_, right.version_id_);
        swap(left.total_rows_, right.total_rows_);
        swap(left.tsd_, right.tsd_);
        swap(left.last_existing_index_value_, right.last_existing_index_value_);
        swap(left.fetch_index_, right.fetch_index_);
        swap(left.string_pools_, right.string_pools_);
        swap(left.selected_columns_, right.selected_columns_);
        swap(left.overall_column_bitset_, right.overall_column_bitset_);
        swap(left.filter_columns_, right.filter_columns_);
        swap(left.segment_descriptors_, right.segment_descriptors_);
        swap(left.filter_columns_set_, right.filter_columns_set_);
        swap(left.compacted_, right.compacted_);
        swap(left.staged_descriptor_, right.staged_descriptor_);
        swap(left.output_schema_, right.output_schema_);
        swap(left.string_coerced_descriptor_, right.string_coerced_descriptor_);
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
        util::check(tsd_.has_value(), "No normalization metadata defined");
        return tsd_->proto().normalization().input_type_case() ==
               arcticdb::proto::descriptors::NormalizationMetadata::InputTypeCase::kMsgPackFrame;
    }

    void set_tsd(TimeseriesDescriptor&& tsd);

    const TimeseriesDescriptor& tsd() const;

    bool has_normalization() const;

    const arcticdb::proto::descriptors::NormalizationMetadata& normalization() const;

    arcticdb::proto::descriptors::NormalizationMetadata& mutable_normalization();

    void set_normalization(arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta);

    arcticdb::proto::descriptors::NormalizationMetadata release_normalization();

    bool only_index_columns_selected() const;

    std::optional<proto::descriptors::UserDefinedMetadata> release_opt_user_defined_metadata();

    const StreamDescriptor& on_disk_descriptor() const;
    StreamDescriptor& on_disk_descriptor();
    [[nodiscard]] bool has_on_disk_descriptor() const;
    void set_on_disk_descriptor(StreamDescriptor&& desc);
    void set_on_disk_descriptor(const StreamDescriptor& desc);
    [[nodiscard]] bool are_string_fields_coerced() const;
    void set_output_schema(OutputSchema&& output_schema);

  private:
    // Carries the normalization metadata and user metadata for the pipeline. On indexed reads it is initialised from
    // the existing on-disk version (the compact path additionally relies on its descriptor, total rows and sorted
    // state being those of the existing version); on writes / incompletes / joins it is created solely to carry the
    // working normalization metadata.
    std::optional<TimeseriesDescriptor> tsd_;
    /// The descriptor of the data on disk.
    std::optional<StreamDescriptor> on_disk_descriptor_;
    /// Mutated version of the on_disk_descriptor, where all string columns respect the user defined options in
    /// ReadOptions::force_strings_to_fixed_ and ReadOptions::force_strings_to_objects_
    std::optional<StreamDescriptor> string_coerced_descriptor_;
    /// Holds the schema of the output produced by the processing pipeline. This is what will be returned to the user
    /// after the pipeline runs. Computed by applying the modify_schema methods of all clauses in order they appear in
    /// the pipeline.
    std::optional<OutputSchema> output_schema_;
};

} // namespace arcticdb::pipelines