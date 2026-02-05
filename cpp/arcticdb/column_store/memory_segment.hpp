/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/column_store/memory_segment_impl.hpp>

namespace arcticdb {

/*
 * Primary class used to interact with in-memory data. Note that a segment is not guaranteed to contain all
 * columns for a row as Arctic tiles across both the rows and the columns.
 */
class SegmentInMemory {
  public:
    using value_type = SegmentInMemoryImpl::Row;
    using Row = SegmentInMemoryImpl::Row;
    using iterator = SegmentInMemoryImpl::iterator;
    using const_iterator = SegmentInMemoryImpl::const_iterator;
    using ExtraBytesPerColumn = SegmentInMemoryImpl::ExtraBytesPerColumn;

    SegmentInMemory();

    explicit SegmentInMemory(
            const StreamDescriptor& tsd, size_t expected_column_size = 0,
            AllocationType allocation_type = AllocationType::DYNAMIC, Sparsity allow_sparse = Sparsity::NOT_PERMITTED,
            const ExtraBytesPerColumn& extra_bytes_per_column = std::nullopt
    );

    explicit SegmentInMemory(
            StreamDescriptor&& tsd, size_t expected_column_size = 0,
            AllocationType allocation_type = AllocationType::DYNAMIC, Sparsity allow_sparse = Sparsity::NOT_PERMITTED,
            const ExtraBytesPerColumn& extra_bytes_per_column = std::nullopt
    );

    friend void swap(SegmentInMemory& left, SegmentInMemory& right) noexcept;

    ARCTICDB_MOVE_COPY_DEFAULT(SegmentInMemory)

    [[nodiscard]] iterator begin();

    [[nodiscard]] iterator end();

    [[nodiscard]] iterator begin() const;

    [[nodiscard]] iterator end() const;

    void push_back(const Row& row);

    [[nodiscard]] const Field& column_descriptor(size_t col) const;

    void end_row() const;

    template<typename T>
    requires std::integral<T> || std::floating_point<T>
    void set_scalar(position_t idx, T val) {
        impl_->set_scalar(idx, val);
    }

    template<typename T>
    requires std::same_as<std::decay_t<T>, std::string>
    void set_scalar(position_t idx, const T& val) {
        impl_->set_string(idx, val);
    }

    friend bool operator==(const SegmentInMemory& left, const SegmentInMemory& right);

    [[nodiscard]] const FieldCollection& fields() const;

    [[nodiscard]] const Field& field(size_t index) const;

    [[nodiscard]] std::optional<std::size_t> column_index(std::string_view name) const;

    [[nodiscard]] std::optional<std::size_t> column_index_with_name_demangling(std::string_view name) const;

    [[nodiscard]] const TimeseriesDescriptor& index_descriptor() const;

    TimeseriesDescriptor& mutable_index_descriptor();

    [[nodiscard]] bool has_index_descriptor() const;

    void init_column_map() const;

    template<class T, template<class> class Tensor>
    requires std::integral<T> || std::floating_point<T>
    void set_array(position_t pos, Tensor<T>& val) {
        impl_->set_array(pos, val);
    }

    void set_string(position_t pos, std::string_view str);

    void set_string_at(position_t col, position_t row, const char* str, size_t size);

    void set_string_array(position_t idx, size_t string_size, size_t num_strings, char* data);

    void set_string_list(position_t idx, const std::vector<std::string>& input);

    void set_value(position_t idx, const SegmentInMemoryImpl::Location& loc);

    // pybind11 can't resolve const and non-const version of column()
    Column& column_ref(position_t idx);

    Column& column(position_t idx);

    [[nodiscard]] const Column& column(position_t idx) const;

    std::vector<std::shared_ptr<Column>>& columns();

    [[nodiscard]] const std::vector<std::shared_ptr<Column>>& columns() const;

    position_t add_column(const Field& field, size_t num_rows, AllocationType presize);

    position_t add_column(const Field& field, const std::shared_ptr<Column>& column);

    position_t add_column(FieldRef field_ref, const std::shared_ptr<Column>& column);

    position_t add_column(std::string_view name, const std::shared_ptr<Column>& column);

    position_t add_column(FieldRef field_ref, size_t num_rows, AllocationType presize);

    [[nodiscard]] size_t num_blocks() const;

    void append(const SegmentInMemory& other);

    void concatenate(SegmentInMemory&& other, bool unique_column_names = true);

    void drop_column(std::string_view name);

    template<typename T>
    std::optional<T> scalar_at(position_t row, position_t col) const {
        return impl_->scalar_at<T>(row, col);
    }

    template<typename T>
    std::optional<TensorType<T>> tensor_at(position_t row, position_t col) const {
        return impl_->tensor_at<T>(row, col);
    }

    [[nodiscard]] std::optional<std::string_view> string_at(position_t row, position_t col) const;

    std::optional<Column::StringArrayData> string_array_at(position_t row, position_t col);

    void set_timeseries_descriptor(const TimeseriesDescriptor& tsd);

    void reset_timeseries_descriptor();

    void calculate_statistics();

    [[nodiscard]] size_t num_columns() const;

    [[nodiscard]] size_t row_count() const;

    void unsparsify();

    [[nodiscard]] bool has_user_metadata() const;

    [[nodiscard]] const arcticdb::proto::descriptors::UserDefinedMetadata& user_metadata() const;

    void sparsify();

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_external_block(position_t idx, T* val, size_t size) {
        impl_->set_external_block(idx, val, size);
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_sparse_block(position_t idx, T* val, size_t rows_to_write) {
        impl_->set_sparse_block(idx, val, rows_to_write);
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset);

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset);

    void set_offset(ssize_t offset);

    [[nodiscard]] ssize_t offset() const;

    void clear();

    void end_block_write(ssize_t size);

    [[nodiscard]] size_t string_pool_size() const;

    [[nodiscard]] bool has_string_pool() const;

    [[nodiscard]] ColumnData string_pool_data() const;

    [[nodiscard]] ColumnData column_data(size_t col) const;

    [[nodiscard]] const StreamDescriptor& descriptor() const;

    StreamDescriptor& descriptor();

    [[nodiscard]] const std::shared_ptr<StreamDescriptor>& descriptor_ptr() const;

    void attach_descriptor(std::shared_ptr<StreamDescriptor> desc);

    StringPool& string_pool();

    [[nodiscard]] const StringPool& const_string_pool() const;

    [[nodiscard]] const std::shared_ptr<StringPool>& string_pool_ptr() const;

    void reset_metadata();

    void set_metadata(google::protobuf::Any&& meta);

    bool has_metadata();

    void set_row_id(ssize_t rid);

    void set_row_data(ssize_t rid);

    [[nodiscard]] const google::protobuf::Any* metadata() const;

    [[nodiscard]] bool is_index_sorted() const;

    void sort(const std::string& column);

    void sort(const std::vector<std::string>& columns);

    void sort(const std::vector<position_t>& columns);

    void sort(position_t column);

    [[nodiscard]] SegmentInMemory clone() const;

    void set_string_pool(const std::shared_ptr<StringPool>& string_pool);

    SegmentInMemory filter(util::BitSet&& filter_bitset, bool filter_down_stringpool = false, bool validate = false)
            const;

    /// @see SegmentInMemoryImpl::truncate
    [[nodiscard]] SegmentInMemory truncate(size_t start_row, size_t end_row, bool reconstruct_string_pool) const;

    [[nodiscard]] std::vector<SegmentInMemory> partition(
            const std::vector<uint8_t>& row_to_segment, const std::vector<uint64_t>& segment_counts
    ) const;

    [[nodiscard]] bool empty() const;

    [[nodiscard]] bool is_null() const;

    [[nodiscard]] size_t num_bytes() const;

    [[nodiscard]] bool compacted() const;

    void set_compacted(bool val);

    void change_schema(const StreamDescriptor& descriptor);

    SegmentInMemoryImpl* impl();

    void check_magic() const;

    // Not currently used but might be useful in the future
    void compact_blocks();

    std::shared_ptr<Column> column_ptr(position_t idx) const;

    template<typename RowType>
    RowType make_row_ref(std::size_t row_id) {
        if constexpr (std::is_same_v<RowType, Row>) {
            return RowType(impl(), row_id);
        } else {
            return RowType(row_id, *this);
        }
    }

    [[nodiscard]] bool allow_sparse() const;

    [[nodiscard]] bool is_sparse() const;

    [[nodiscard]] std::vector<SegmentInMemory> split(size_t rows, bool filter_down_stringpool = false) const;

    void drop_empty_columns();

    std::string_view string_at_offset(position_t offset_in_string_pool) const;

  private:
    explicit SegmentInMemory(std::shared_ptr<SegmentInMemoryImpl> impl) : impl_(std::move(impl)) {}

    std::shared_ptr<SegmentInMemoryImpl> impl_;
};

} // namespace arcticdb