/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/cursor.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/column_store/memory_segment_impl.hpp>

#include <google/protobuf/message.h>
#include <google/protobuf/any.h>
#include <google/protobuf/any.pb.h>

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
        bool presize = false,
        bool allow_sparse = false) :
            impl_(std::make_shared<SegmentInMemoryImpl>(tsd, expected_column_size, presize, allow_sparse)){
    }

    explicit SegmentInMemory(
        StreamDescriptor&& tsd,
        size_t expected_column_size = 0,
        bool presize = false,
        bool allow_sparse = false) :
        impl_(std::make_shared<SegmentInMemoryImpl>(std::move(tsd), expected_column_size, presize, allow_sparse)){
    }

    friend void swap(SegmentInMemory& left, SegmentInMemory& right) {
        std::swap(left.impl_, right.impl_);
    }

    ARCTICDB_MOVE_COPY_DEFAULT(SegmentInMemory)

    auto begin() { return impl_->begin(); }

    auto end() { return impl_->end(); }

    auto begin() const { return impl_->begin(); }

    auto end() const { return impl_->end(); }

    void push_back(const Row& row) {
        impl_->push_back(row);
    }

    auto& column_descriptor(size_t col) const {
        return impl_->column_descriptor(col);
    }

    void end_row() const {
        impl_->end_row();
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_scalar(position_t idx, T val) {
        impl_->set_scalar(idx, val);
    }

    template<class T, std::enable_if_t<std::is_same_v<std::decay_t<T>, std::string>, int> = 0>
    void set_scalar(position_t idx, T val) {
        impl_->set_string(idx, val);
    }

    friend bool operator==(const SegmentInMemory& left, const SegmentInMemory& right) {
        return *left.impl_ == *right.impl_;
    }


    const auto& fields() const {
        return impl_->fields();
    }

    const auto& field(size_t index) const {
        return impl_->field(index);
    }

    std::optional<std::size_t> column_index(std::string_view name) const {
        return impl_->column_index(name);
    }

    std::shared_ptr<FieldCollection> index_fields() const {
        return impl_->index_fields();
    }

    bool has_index_fields() const {
        return impl_->has_index_fields();
    }

    TimeseriesDescriptor index_descriptor() {
        return impl_->index_descriptor();
    }

    FieldCollection&& detach_index_fields() {
        return impl_->detach_index_fields();
    }

    std::shared_ptr<arcticdb::proto::descriptors::TimeSeriesDescriptor> timeseries_proto() {
        return impl_->timeseries_proto();
    }

    void set_index_fields(std::shared_ptr<FieldCollection> fields) {
        impl_->set_index_fields(std::move(fields));
    }

    void init_column_map() const  {
        impl_->init_column_map();
    }

    template<class T, template<class> class Tensor, std::enable_if_t<
            std::is_integral_v<T> || std::is_floating_point_v<T>,
            int> = 0>
    void set_array(position_t pos, Tensor<T> &val) {
        impl_->set_array(pos, val);
    }

    template<class T, std::enable_if_t<
        std::is_integral_v<T> || std::is_floating_point_v<T>,
        int> = 0>
    void set_array(position_t pos, py::array_t<T>& val) {
        impl_->set_array(pos, val);
    }

    void set_string(position_t pos, std::string_view str) {
        impl_->set_string(pos, str);
    }

    void set_string_at(position_t col, position_t row, const char* str, size_t size) {
        impl_->set_string_at(col, row, str, size);
    }

    void set_no_string_at(position_t col, position_t row, OffsetString::offset_t placeholder) {
        impl_->set_no_string_at(col, row, placeholder);
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

    const Column &column(position_t idx) const {
        return impl_->column(idx);
    }

    std::vector<std::shared_ptr<Column>>& columns() {
        return impl_->columns();
    }

    const std::vector<std::shared_ptr<Column>>& columns() const {
        return impl_->columns();
    }

    position_t add_column(const Field &field, size_t num_rows, bool presize) {
        return impl_->add_column(field, num_rows, presize);
    }

    position_t add_column(const Field &field, const std::shared_ptr<Column>& column) {
        return impl_->add_column(field, column);
    }

    position_t add_column(FieldRef field_ref, const std::shared_ptr<Column>& column) {
        return impl_->add_column(field_ref, column);
    }

    position_t add_column(FieldRef field_ref, size_t num_rows, bool presize) {
        return impl_->add_column(field_ref, num_rows, presize);
    }

    size_t num_blocks() const {
        return impl_->num_blocks();
    }

    void append(const SegmentInMemory& other) {
        impl_->append(*other.impl_);
    }

    void concatenate(SegmentInMemory&& other, bool unique_column_names=true) {
        impl_->concatenate(std::move(*other.impl_), unique_column_names);
    }

    util::BitSet get_duplicates_bitset(SegmentInMemory& other) {
        return impl_->get_duplicates_bitset(*other.impl_);
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

    std::optional<std::string_view> string_at(position_t row, position_t col) const {
        return impl_->string_at(row, col);
    }

    std::optional<Column::StringArrayData> string_array_at(position_t row, position_t col) {
        return impl_->string_array_at(row, col);
    }

    void set_timeseries_descriptor(TimeseriesDescriptor&& tsd) {
        util::check(!tsd.proto_is_null(), "Got null timeseries descriptor in set_timeseries_descriptor");
        impl_->set_timeseries_descriptor(std::move(tsd));
    }

    size_t num_columns() const { return impl_->num_columns(); }

    size_t num_fields() const { return impl_->num_fields(); }

    size_t row_count() const { return impl_->row_count(); }

    void unsparsify() {
        impl_->unsparsify();
    }

    void sparsify() {
        impl_->sparsify();
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_external_block(position_t idx, T *val, size_t size) {
        impl_->set_external_block(idx, val, size);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_sparse_block(position_t idx, T *val, size_t rows_to_write) {
        impl_->set_sparse_block(idx, val, rows_to_write);
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
        impl_->set_sparse_block(idx, std::move(buffer), std::move(shapes), std::move(bitset));
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset) {
        impl_->set_sparse_block(idx, std::move(buffer), std::move(bitset));
    }

    void set_secondary_type(position_t idx, TypeDescriptor type) {
        impl_->set_secondary_type(idx, type);
    }

    void set_offset(ssize_t offset) {
        impl_->set_offset(offset);
    }

    ssize_t offset() const {
        return impl_->offset();
    }

    void clear() {
        impl_->clear();
    }

    void end_block_write(size_t size) {
        impl_->end_block_write(size);
    }

    size_t string_pool_size() const { return impl_->string_pool_size(); }

    bool has_string_pool() const { return impl_->has_string_pool(); }

    ColumnData string_pool_data() const {
        return impl_->string_pool_data();
    }

    ColumnData column_data(size_t col) const {
        return impl_->column_data(col);
    }

    const StreamDescriptor &descriptor() const {
        return impl_->descriptor();
    }

    StreamDescriptor &descriptor() {
        return impl_->descriptor();
    }

    const std::shared_ptr<StreamDescriptor>& descriptor_ptr() const {
        return impl_->descriptor_ptr();
    }

    void attach_descriptor(std::shared_ptr<StreamDescriptor> desc) {
        impl_->attach_descriptor(std::move(desc));
    }

    StringPool &string_pool() {
        return impl_->string_pool();
    }

    void string_pool_assign(const SegmentInMemory& other) {
        impl_->string_pool_assign(*other.impl_);
    }

    const StringPool &const_string_pool() const {
        return impl_->string_pool();
    }

    const std::shared_ptr<StringPool>& string_pool_ptr() const {
        return impl_->string_pool_ptr();
    }

    void set_metadata(google::protobuf::Any &&meta) {
        impl_->set_metadata(std::move(meta));
    }

    bool has_metadata() {
        return impl_->has_metadata();
    }

    void override_metadata(google::protobuf::Any &&meta) {
        impl_->override_metadata(std::move(meta));
    }

    ssize_t get_row_id() {
        return impl_->get_row_id();
    }

    void set_row_id(ssize_t rid) {
        impl_->set_row_id(rid);
    }

    void set_row_data(ssize_t rid) {
        impl_->set_row_data(rid);
    }

    const google::protobuf::Any *metadata() const {
        return impl_->metadata();
    }

    bool is_index_sorted() const {
        return impl_->is_index_sorted();
    }

    void sort(const std::string& column) {
        impl_->sort(column);
    }

    void sort(position_t column) {
        impl_->sort(column);
    }

    SegmentInMemory clone() {
        return SegmentInMemory(std::make_shared<SegmentInMemoryImpl>(impl_->clone()));
    }

    void set_string_pool(const std::shared_ptr<StringPool>& string_pool) {
        impl_->set_string_pool(string_pool);
    }

    SegmentInMemory filter(const util::BitSet& filter_bitset,
                           bool filter_down_stringpool=false,
                           bool validate=false) const{
        return SegmentInMemory(impl_->filter(filter_bitset, filter_down_stringpool, validate));
    }

    SegmentInMemory truncate(size_t start_row, size_t end_row) const{
        return SegmentInMemory(impl_->truncate(start_row, end_row));
    }

    std::vector<SegmentInMemory> partition(const std::vector<std::optional<uint8_t>>& row_to_segment,
                           const std::vector<uint64_t>& segment_counts) const{
        std::vector<SegmentInMemory> res;
        auto impls = impl_->partition(row_to_segment, segment_counts);
        for (auto&& impl: impls) {
            res.emplace_back(SegmentInMemory(std::move(impl)));
        }
        return res;
    }

    bool empty() const {
        return is_null() || impl_->empty();
    }

    bool is_null() const {
        return !static_cast<bool>(impl_);
    }

    size_t num_bytes() const {
        return impl_->num_bytes();
    }

    bool compacted() const  {
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

    bool allow_sparse() const {
        return impl_->allow_sparse();
    }


    bool is_sparse() const {
        return impl_->is_sparse();
    }

    [[nodiscard]] std::vector<SegmentInMemory> split(size_t rows) const {
        std::vector<SegmentInMemory> output;
        auto new_impls = impl_->split(rows);
        for(const auto& impl : new_impls)
            output.push_back(SegmentInMemory{impl});

        return output;
    }

    StreamId get_index_col_name() const{
        return impl_->get_index_col_name();
    }

private:
    explicit SegmentInMemory(std::shared_ptr<SegmentInMemoryImpl> impl) :
            impl_(std::move(impl)) {}

    std::shared_ptr<SegmentInMemoryImpl> impl_;
};

}
