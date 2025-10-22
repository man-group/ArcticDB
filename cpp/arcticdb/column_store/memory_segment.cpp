/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/column_store/column.hpp>

namespace arcticdb {

SegmentInMemory::SegmentInMemory() : impl_(std::make_shared<SegmentInMemoryImpl>()) {}

SegmentInMemory::SegmentInMemory(
        const StreamDescriptor& tsd, size_t expected_column_size, AllocationType allocation_type, Sparsity allow_sparse,
        const ExtraBytesPerColumn& extra_bytes_per_column
) :
    impl_(std::make_shared<SegmentInMemoryImpl>(
            tsd, expected_column_size, allocation_type, allow_sparse, extra_bytes_per_column
    )) {}

SegmentInMemory::SegmentInMemory(
        StreamDescriptor&& tsd, size_t expected_column_size, AllocationType allocation_type, Sparsity allow_sparse,
        const ExtraBytesPerColumn& extra_bytes_per_column
) :
    impl_(std::make_shared<SegmentInMemoryImpl>(
            std::move(tsd), expected_column_size, allocation_type, allow_sparse, extra_bytes_per_column
    )) {}

void swap(SegmentInMemory& left, SegmentInMemory& right) noexcept { std::swap(left.impl_, right.impl_); }

SegmentInMemory::iterator SegmentInMemory::begin() { return impl_->begin(); }

SegmentInMemory::iterator SegmentInMemory::end() { return impl_->end(); }

SegmentInMemory::iterator SegmentInMemory::begin() const { return impl_->begin(); }

SegmentInMemory::iterator SegmentInMemory::end() const { return impl_->end(); }

void SegmentInMemory::push_back(const Row& row) { impl_->push_back(row); }

const Field& SegmentInMemory::column_descriptor(size_t col) const { return impl_->column_descriptor(col); }

void SegmentInMemory::end_row() const { impl_->end_row(); }

bool operator==(const SegmentInMemory& left, const SegmentInMemory& right) { return *left.impl_ == *right.impl_; }

const FieldCollection& SegmentInMemory::fields() const { return impl_->fields(); }

const Field& SegmentInMemory::field(size_t index) const { return impl_->field(index); }

std::optional<std::size_t> SegmentInMemory::column_index(std::string_view name) const {
    return impl_->column_index(name);
}

std::optional<std::size_t> SegmentInMemory::column_index_with_name_demangling(std::string_view name) const {
    return impl_->column_index_with_name_demangling(name);
}

const TimeseriesDescriptor& SegmentInMemory::index_descriptor() const { return impl_->index_descriptor(); }

TimeseriesDescriptor& SegmentInMemory::mutable_index_descriptor() { return impl_->mutable_index_descriptor(); }

bool SegmentInMemory::has_index_descriptor() const { return impl_->has_index_descriptor(); }

void SegmentInMemory::init_column_map() const { impl_->init_column_map(); }

void SegmentInMemory::set_string(position_t pos, std::string_view str) { impl_->set_string(pos, str); }

void SegmentInMemory::set_string_at(position_t col, position_t row, const char* str, size_t size) {
    impl_->set_string_at(col, row, str, size);
}

void SegmentInMemory::set_string_array(position_t idx, size_t string_size, size_t num_strings, char* data) {
    impl_->set_string_array(idx, string_size, num_strings, data);
}

void SegmentInMemory::set_string_list(position_t idx, const std::vector<std::string>& input) {
    impl_->set_string_list(idx, input);
}

void SegmentInMemory::set_value(position_t idx, const SegmentInMemoryImpl::Location& loc) {
    impl_->set_value(idx, loc);
}

// pybind11 can't resolve const and non-const version of column()
Column& SegmentInMemory::column_ref(position_t idx) { return impl_->column_ref(idx); }

Column& SegmentInMemory::column(position_t idx) { return impl_->column(idx); }

const Column& SegmentInMemory::column(position_t idx) const { return impl_->column(idx); }

std::vector<std::shared_ptr<Column>>& SegmentInMemory::columns() { return impl_->columns(); }

const std::vector<std::shared_ptr<Column>>& SegmentInMemory::columns() const { return impl_->columns(); }

position_t SegmentInMemory::add_column(const Field& field, size_t num_rows, AllocationType presize) {
    return impl_->add_column(field, num_rows, presize);
}

position_t SegmentInMemory::add_column(const Field& field, const std::shared_ptr<Column>& column) {
    return impl_->add_column(field, column);
}

position_t SegmentInMemory::add_column(FieldRef field_ref, const std::shared_ptr<Column>& column) {
    return impl_->add_column(field_ref, column);
}

position_t SegmentInMemory::add_column(std::string_view name, const std::shared_ptr<Column>& column) {
    return impl_->add_column(name, column);
}

position_t SegmentInMemory::add_column(FieldRef field_ref, size_t num_rows, AllocationType presize) {
    return impl_->add_column(field_ref, num_rows, presize);
}

size_t SegmentInMemory::num_blocks() const { return impl_->num_blocks(); }

void SegmentInMemory::append(const SegmentInMemory& other) { impl_->append(*other.impl_); }

void SegmentInMemory::concatenate(SegmentInMemory&& other, bool unique_column_names) {
    impl_->concatenate(std::move(*other.impl_), unique_column_names);
}

void SegmentInMemory::drop_column(std::string_view name) { impl_->drop_column(name); }

std::optional<std::string_view> SegmentInMemory::string_at(position_t row, position_t col) const {
    return impl_->string_at(row, col);
}

std::optional<Column::StringArrayData> SegmentInMemory::string_array_at(position_t row, position_t col) {
    return impl_->string_array_at(row, col);
}

void SegmentInMemory::set_timeseries_descriptor(const TimeseriesDescriptor& tsd) {
    util::check(!tsd.proto_is_null(), "Got null timeseries descriptor in set_timeseries_descriptor");
    impl_->set_timeseries_descriptor(tsd);
}

void SegmentInMemory::reset_timeseries_descriptor() { impl_->reset_timeseries_descriptor(); }

void SegmentInMemory::calculate_statistics() { impl_->calculate_statistics(); }

size_t SegmentInMemory::num_columns() const { return impl_->num_columns(); }

size_t SegmentInMemory::row_count() const { return impl_->row_count(); }

void SegmentInMemory::unsparsify() { impl_->unsparsify(); }

bool SegmentInMemory::has_user_metadata() const { return impl_->has_user_metadata(); }

const arcticdb::proto::descriptors::UserDefinedMetadata& SegmentInMemory::user_metadata() const {
    return impl_->user_metadata();
}

void SegmentInMemory::sparsify() { impl_->sparsify(); }

void SegmentInMemory::set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
    impl_->set_sparse_block(idx, std::move(buffer), std::move(shapes), std::move(bitset));
}

void SegmentInMemory::set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset) {
    impl_->set_sparse_block(idx, std::move(buffer), std::move(bitset));
}

void SegmentInMemory::set_offset(ssize_t offset) { impl_->set_offset(offset); }

ssize_t SegmentInMemory::offset() const { return impl_->offset(); }

void SegmentInMemory::clear() { impl_->clear(); }

void SegmentInMemory::end_block_write(ssize_t size) { impl_->end_block_write(size); }

size_t SegmentInMemory::string_pool_size() const { return impl_->string_pool_size(); }

bool SegmentInMemory::has_string_pool() const { return impl_->has_string_pool(); }

ColumnData SegmentInMemory::string_pool_data() const { return impl_->string_pool_data(); }

ColumnData SegmentInMemory::column_data(size_t col) const { return impl_->column_data(col); }

const StreamDescriptor& SegmentInMemory::descriptor() const { return impl_->descriptor(); }

StreamDescriptor& SegmentInMemory::descriptor() { return impl_->descriptor(); }

const std::shared_ptr<StreamDescriptor>& SegmentInMemory::descriptor_ptr() const { return impl_->descriptor_ptr(); }

void SegmentInMemory::attach_descriptor(std::shared_ptr<StreamDescriptor> desc) {
    impl_->attach_descriptor(std::move(desc));
}

StringPool& SegmentInMemory::string_pool() { return impl_->string_pool(); }

const StringPool& SegmentInMemory::const_string_pool() const { return impl_->string_pool(); }

const std::shared_ptr<StringPool>& SegmentInMemory::string_pool_ptr() const { return impl_->string_pool_ptr(); }

void SegmentInMemory::reset_metadata() { impl_->reset_metadata(); }

void SegmentInMemory::set_metadata(google::protobuf::Any&& meta) { impl_->set_metadata(std::move(meta)); }

bool SegmentInMemory::has_metadata() { return impl_->has_metadata(); }

void SegmentInMemory::set_row_id(ssize_t rid) { impl_->set_row_id(rid); }

void SegmentInMemory::set_row_data(ssize_t rid) { impl_->set_row_data(rid); }

const google::protobuf::Any* SegmentInMemory::metadata() const { return impl_->metadata(); }

bool SegmentInMemory::is_index_sorted() const { return impl_->is_index_sorted(); }

void SegmentInMemory::sort(const std::string& column) { impl_->sort(column); }

void SegmentInMemory::sort(const std::vector<std::string>& columns) { return impl_->sort(columns); }

void SegmentInMemory::sort(const std::vector<position_t>& columns) { return impl_->sort(columns); }

void SegmentInMemory::sort(position_t column) { impl_->sort(column); }

SegmentInMemory SegmentInMemory::clone() const {
    return SegmentInMemory(std::make_shared<SegmentInMemoryImpl>(impl_->clone()));
}

void SegmentInMemory::set_string_pool(const std::shared_ptr<StringPool>& string_pool) {
    impl_->set_string_pool(string_pool);
}

SegmentInMemory SegmentInMemory::filter(util::BitSet&& filter_bitset, bool filter_down_stringpool, bool validate)
        const {
    return SegmentInMemory(impl_->filter(std::move(filter_bitset), filter_down_stringpool, validate));
}

SegmentInMemory SegmentInMemory::truncate(size_t start_row, size_t end_row, bool reconstruct_string_pool) const {
    return SegmentInMemory(impl_->truncate(start_row, end_row, reconstruct_string_pool));
}

std::vector<SegmentInMemory> SegmentInMemory::partition(
        const std::vector<uint8_t>& row_to_segment, const std::vector<uint64_t>& segment_counts
) const {
    std::vector<SegmentInMemory> res;
    auto impls = impl_->partition(row_to_segment, segment_counts);
    res.reserve(impls.size());
    for (auto&& impl : impls) {
        res.emplace_back(SegmentInMemory(std::move(impl)));
    }
    return res;
}

bool SegmentInMemory::empty() const { return is_null() || impl_->empty(); }

bool SegmentInMemory::is_null() const { return !static_cast<bool>(impl_); }

size_t SegmentInMemory::num_bytes() const { return impl_->num_bytes(); }

bool SegmentInMemory::compacted() const { return impl_->compacted(); }

void SegmentInMemory::set_compacted(bool val) { impl_->set_compacted(val); }

void SegmentInMemory::change_schema(const StreamDescriptor& descriptor) { return impl_->change_schema(descriptor); }

SegmentInMemoryImpl* SegmentInMemory::impl() { return impl_.get(); }

void SegmentInMemory::check_magic() const { impl_->check_magic(); }

// Not currently used but might be useful in the future
void SegmentInMemory::compact_blocks() { impl_->compact_blocks(); }

std::shared_ptr<Column> SegmentInMemory::column_ptr(position_t idx) const { return impl_->column_ptr(idx); }

bool SegmentInMemory::allow_sparse() const { return impl_->allow_sparse(); }

bool SegmentInMemory::is_sparse() const { return impl_->is_sparse(); }

std::vector<SegmentInMemory> SegmentInMemory::split(size_t rows, bool filter_down_stringpool) const {
    std::vector<SegmentInMemory> output;
    auto new_impls = impl_->split(rows, filter_down_stringpool);
    output.reserve(new_impls.size());
    for (const auto& impl : new_impls)
        output.emplace_back(SegmentInMemory{impl});

    return output;
}

void SegmentInMemory::drop_empty_columns() { impl_->drop_empty_columns(); }

} // namespace arcticdb
