/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/column_store/memory_segment_impl.hpp>
#include <arcticdb/entity/output_format.hpp>

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

    SegmentInMemory() :
            impl_(std::make_shared<SegmentInMemoryImpl>()) {
    }

    explicit SegmentInMemory(
        const StreamDescriptor &tsd,
        size_t expected_column_size = 0,
        AllocationType presize = AllocationType::DYNAMIC,
        Sparsity allow_sparse = Sparsity::NOT_PERMITTED,
        OutputFormat output_format = OutputFormat::NATIVE,
        DataTypeMode mode = DataTypeMode::INTERNAL) :
            impl_(std::make_shared<SegmentInMemoryImpl>(tsd, expected_column_size, presize, allow_sparse, output_format, mode)){
    }

    explicit SegmentInMemory(
        StreamDescriptor&& tsd,
        size_t expected_column_size = 0,
        AllocationType presize = AllocationType::DYNAMIC,
        Sparsity allow_sparse = Sparsity::NOT_PERMITTED,
        OutputFormat output_format = OutputFormat::NATIVE,
        DataTypeMode mode = DataTypeMode::INTERNAL) :
            impl_(std::make_shared<SegmentInMemoryImpl>(std::move(tsd), expected_column_size, presize, allow_sparse, output_format, mode)){
    }

    friend void swap(SegmentInMemory& left, SegmentInMemory& right) {
        std::swap(left.impl_, right.impl_);
    }

    ARCTICDB_MOVE_COPY_DEFAULT(SegmentInMemory)

    [[nodiscard]] auto begin() { return impl_->begin(); }

    [[nodiscard]] auto end() { return impl_->end(); }

    [[nodiscard]] auto begin() const { return impl_->begin(); }

    [[nodiscard]] auto end() const { return impl_->end(); }

    void push_back(const Row& row) {
        impl_->push_back(row);
    }

    [[nodiscard]] auto& column_descriptor(size_t col) const {
        return impl_->column_descriptor(col);
    }

    void end_row() const {
        impl_->end_row();
    }

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

    friend bool operator==(const SegmentInMemory& left, const SegmentInMemory& right) {
        return *left.impl_ == *right.impl_;
    }


    [[nodiscard]] const auto& fields() const {
        return impl_->fields();
    }

    [[nodiscard]] const auto& field(size_t index) const {
        return impl_->field(index);
    }

    [[nodiscard]] std::optional<std::size_t> column_index(std::string_view name) const {
        return impl_->column_index(name);
    }

    [[nodiscard]] std::optional<std::size_t> column_index_with_name_demangling(std::string_view name) const {
        return impl_->column_index_with_name_demangling(name);
    }

    [[nodiscard]] const TimeseriesDescriptor& index_descriptor() const {
        return impl_->index_descriptor();
    }

    TimeseriesDescriptor& mutable_index_descriptor() {
        return impl_->mutable_index_descriptor();
    }

    [[nodiscard]] bool has_index_descriptor() const {
        return impl_->has_index_descriptor();
    }

    void init_column_map() const  {
        impl_->init_column_map();
    }

    template<arithmetic_tensor TensorType>
    void set_array(position_t pos, TensorType &val) {
        impl_->set_array(pos, val);
    }

    void set_string(position_t pos, std::string_view str) {
        impl_->set_string(pos, str);
    }

    void set_string_at(position_t col, position_t row, const char* str, size_t size) {
        impl_->set_string_at(col, row, str, size);
    }

    void set_string_array(position_t idx, size_t string_size, size_t num_strings, char *data) {
        impl_->set_string_array(idx, string_size, num_strings, data);
    }

    void set_string_list(position_t idx, const std::vector<std::string> &input) {
        impl_->set_string_list(idx, input);
    }

    void set_value(position_t idx, const SegmentInMemoryImpl::Location& loc) {
        impl_->set_value(idx, loc);
    }

    //pybind11 can't resolve const and non-const version of column()
    Column &column_ref(position_t idx) {
        return impl_->column_ref(idx);
    }

    Column &column(position_t idx) {
        return impl_->column(idx);
    }

    [[nodiscard]] const Column &column(position_t idx) const {
        return impl_->column(idx);
    }

    std::vector<std::shared_ptr<Column>>& columns() {
        return impl_->columns();
    }

    [[nodiscard]] const std::vector<std::shared_ptr<Column>>& columns() const {
        return impl_->columns();
    }

    position_t add_column(const Field &field, size_t num_rows, AllocationType presize) {
        return impl_->add_column(field, num_rows, presize);
    }

    position_t add_column(const Field &field, const std::shared_ptr<Column>& column) {
        return impl_->add_column(field, column);
    }

    position_t add_column(FieldRef field_ref, const std::shared_ptr<Column>& column) {
        return impl_->add_column(field_ref, column);
    }

    position_t add_column(FieldRef field_ref, size_t num_rows, AllocationType presize) {
        return impl_->add_column(field_ref, num_rows, presize);
    }

    [[nodiscard]] size_t num_blocks() const {
        return impl_->num_blocks();
    }

    void append(const SegmentInMemory& other) {
        impl_->append(*other.impl_);
    }

    void concatenate(SegmentInMemory&& other, bool unique_column_names=true) {
        impl_->concatenate(std::move(*other.impl_), unique_column_names);
    }

    void drop_column(std::string_view name) {
        impl_->drop_column(name);
    }

    template<typename T>
    std::optional<T> scalar_at(position_t row, position_t col) const {
        return impl_->scalar_at<T>(row, col);
    }

    template<typename T>
    std::optional<TensorType<T>> tensor_at(position_t row, position_t col) const {
        return impl_->tensor_at<T>(row, col);
    }

    [[nodiscard]] std::optional<std::string_view> string_at(position_t row, position_t col) const {
        return impl_->string_at(row, col);
    }

    std::optional<Column::StringArrayData> string_array_at(position_t row, position_t col) {
        return impl_->string_array_at(row, col);
    }

    void set_timeseries_descriptor(const TimeseriesDescriptor& tsd) {
        util::check(!tsd.proto_is_null(), "Got null timeseries descriptor in set_timeseries_descriptor");
        impl_->set_timeseries_descriptor(tsd);
    }

    void reset_timeseries_descriptor() {
        impl_->reset_timeseries_descriptor();
    }

    void calculate_statistics() {
        impl_->calculate_statistics();
    }

    [[nodiscard]] size_t num_columns() const { return impl_->num_columns(); }

    [[nodiscard]] size_t row_count() const { return impl_->row_count(); }

    void unsparsify() {
        impl_->unsparsify();
    }

    [[nodiscard]] bool has_user_metadata() const {
        return impl_->has_user_metadata();
    }

    [[nodiscard]] const arcticdb::proto::descriptors::UserDefinedMetadata& user_metadata() const {
        return impl_->user_metadata();
    }

    void sparsify() {
        impl_->sparsify();
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_external_block(position_t idx, T *val, size_t size) {
        impl_->set_external_block(idx, val, size);
    }

    template<class T>
    requires std::integral<T> || std::floating_point<T>
    void set_sparse_block(position_t idx, T *val, size_t rows_to_write) {
        impl_->set_sparse_block(idx, val, rows_to_write);
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
        impl_->set_sparse_block(idx, std::move(buffer), std::move(shapes), std::move(bitset));
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset) {
        impl_->set_sparse_block(idx, std::move(buffer), std::move(bitset));
    }

    void set_offset(ssize_t offset) {
        impl_->set_offset(offset);
    }

    [[nodiscard]] ssize_t offset() const {
        return impl_->offset();
    }

    void clear() {
        impl_->clear();
    }

    void end_block_write(ssize_t size) {
        impl_->end_block_write(size);
    }

    [[nodiscard]] size_t string_pool_size() const { return impl_->string_pool_size(); }

    [[nodiscard]] bool has_string_pool() const { return impl_->has_string_pool(); }

    [[nodiscard]] ColumnData string_pool_data() const {
        return impl_->string_pool_data();
    }

    [[nodiscard]] ColumnData column_data(size_t col) const {
        return impl_->column_data(col);
    }

    [[nodiscard]] const StreamDescriptor &descriptor() const {
        return impl_->descriptor();
    }

    StreamDescriptor &descriptor() {
        return impl_->descriptor();
    }

    [[nodiscard]] const std::shared_ptr<StreamDescriptor>& descriptor_ptr() const {
        return impl_->descriptor_ptr();
    }

    void attach_descriptor(std::shared_ptr<StreamDescriptor> desc) {
        impl_->attach_descriptor(std::move(desc));
    }

    StringPool &string_pool() {
        return impl_->string_pool();
    }

    [[nodiscard]] const StringPool &const_string_pool() const {
        return impl_->string_pool();
    }

    [[nodiscard]] const std::shared_ptr<StringPool>& string_pool_ptr() const {
        return impl_->string_pool_ptr();
    }

    void reset_metadata() {
        impl_->reset_metadata();
    }

    void set_metadata(google::protobuf::Any &&meta) {
        impl_->set_metadata(std::move(meta));
    }

    bool has_metadata() {
        return impl_->has_metadata();
    }

    void set_row_id(ssize_t rid) {
        impl_->set_row_id(rid);
    }

    void set_row_data(ssize_t rid) {
        impl_->set_row_data(rid);
    }

    [[nodiscard]] const google::protobuf::Any *metadata() const {
        return impl_->metadata();
    }

    [[nodiscard]] bool is_index_sorted() const {
        return impl_->is_index_sorted();
    }

    void sort(const std::string& column) {
        impl_->sort(column);
    }

    void sort(const std::vector<std::string>& columns) {
        return impl_->sort(columns);
    }

    void sort(const std::vector<position_t>& columns) {
        return impl_->sort(columns);
    }

    void sort(position_t column) {
        impl_->sort(column);
    }

    [[nodiscard]] SegmentInMemory clone() const {
        return SegmentInMemory(std::make_shared<SegmentInMemoryImpl>(impl_->clone()));
    }

    void set_string_pool(const std::shared_ptr<StringPool>& string_pool) {
        impl_->set_string_pool(string_pool);
    }

    SegmentInMemory filter(util::BitSet&& filter_bitset,
                           bool filter_down_stringpool=false,
                           bool validate=false) const{
        return SegmentInMemory(impl_->filter(std::move(filter_bitset), filter_down_stringpool, validate));
    }

    /// @see SegmentInMemoryImpl::truncate
    [[nodiscard]] SegmentInMemory truncate(size_t start_row, size_t end_row, bool reconstruct_string_pool) const{
        return SegmentInMemory(impl_->truncate(start_row, end_row, reconstruct_string_pool));
    }

    [[nodiscard]] std::vector<SegmentInMemory> partition(const std::vector<uint8_t>& row_to_segment,
                           const std::vector<uint64_t>& segment_counts) const{
        std::vector<SegmentInMemory> res;
        auto impls = impl_->partition(row_to_segment, segment_counts);
        res.reserve(impls.size());
        for (auto&& impl: impls) {
            res.emplace_back(SegmentInMemory(std::move(impl)));
        }
        return res;
    }

    [[nodiscard]] bool empty() const {
        return is_null() || impl_->empty();
    }

    [[nodiscard]] bool is_null() const {
        return !static_cast<bool>(impl_);
    }

    [[nodiscard]] size_t num_bytes() const {
        return impl_->num_bytes();
    }

    [[nodiscard]] bool compacted() const  {
        return impl_->compacted();
    }

    void set_compacted(bool val) {
        impl_->set_compacted(val);
    }

    void change_schema(const StreamDescriptor& descriptor) {
        return impl_->change_schema(descriptor);
    }

    SegmentInMemoryImpl* impl() {
        return impl_.get();
    }

    void check_magic() const {
        impl_->check_magic();
    }

    // Not currently used but might be useful in the future
    void compact_blocks() {
        impl_->compact_blocks();
    }

    std::shared_ptr<Column> column_ptr(position_t idx) {
        return impl_->column_ptr(idx);
    }

    template <typename RowType>
    RowType make_row_ref(std::size_t row_id) {
        if constexpr (std::is_same_v<RowType, Row>) {
            return RowType(impl(), row_id);
        } else {
            return RowType(row_id, *this);
        }
    }

    [[nodiscard]] bool allow_sparse() const {
        return impl_->allow_sparse();
    }


    [[nodiscard]] bool is_sparse() const {
        return impl_->is_sparse();
    }

    [[nodiscard]] std::vector<SegmentInMemory> split(size_t rows, bool filter_down_stringpool=false) const {
        std::vector<SegmentInMemory> output;
        auto new_impls = impl_->split(rows, filter_down_stringpool);
        output.reserve(new_impls.size());
        for(const auto& impl : new_impls)
            output.emplace_back(SegmentInMemory{impl});

        return output;
    }

    void drop_empty_columns() {
        impl_->drop_empty_columns();
    }

private:
    explicit SegmentInMemory(std::shared_ptr<SegmentInMemoryImpl> impl) :
            impl_(std::move(impl)) {}

    std::shared_ptr<SegmentInMemoryImpl> impl_;
};

} //namespace arcticdb
