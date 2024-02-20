/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/memory_segment_impl.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/type_utils.hpp>

#include <google/protobuf/any.h>
#include <google/protobuf/any.pb.h>


namespace arcticdb {

    SegmentInMemoryImpl::SegmentInMemoryImpl() = default;

    SegmentInMemoryImpl::SegmentInMemoryImpl( const StreamDescriptor& desc, size_t expected_column_size, bool presize, bool allow_sparse)
        : descriptor_(std::make_shared<StreamDescriptor>(StreamDescriptor{ desc.id(), desc.index() }))
        , allow_sparse_(allow_sparse)
    {
        on_descriptor_change(desc, expected_column_size, presize, allow_sparse);
    }

    SegmentInMemoryImpl::~SegmentInMemoryImpl()
    {
        ARCTICDB_TRACE(log::version(), "Destroying segment in memory");
    }


// Append any columns that exist both in this segment and in the 'other' segment onto the
// end of the column in this segment. Any columns that exist in this segment but not in the
// one being appended will be default-valued (or sparse) in this segment. Any columns in the
// segment being appended not in this one will be ignored. This means that if you want to
// append a lot of segments together into a merged segment, you need to merge the descriptors
// first and create the target segment with the merged descriptor. Could be modified in the future to
// retroactively insert and default-initialize or sparsify the new column, but that's not required
//  at the moment.
void SegmentInMemoryImpl::append(const SegmentInMemoryImpl& other) {
    if (other.row_count() == 0)
        return;

    other.init_column_map();
    for(auto col = 0u; col < num_columns(); ++col) {
        auto col_name = descriptor().field(col).name();
        auto other_col_index = other.column_index(col_name);
        if(other_col_index.has_value()) {
            ARCTICDB_DEBUG(log::version(), "Appending column {} at index {}", col_name, *other_col_index);
            auto this_type = column_unchecked(col).type();
            auto other_type =  other.column_unchecked(*other_col_index).type();
            auto opt_common_type = has_valid_common_type(this_type, other_type);
            internal::check<ErrorCode::E_INVALID_ARGUMENT>(
                    opt_common_type.has_value(),
                    "Could not append type {} to type {} for column {}, this index {}, other index {}",
                    other_type, this_type, col_name, col, *other_col_index);

            if (this_type != *opt_common_type) {
                column_unchecked(col).change_type(opt_common_type->data_type_);
            }
            if (other_type != *opt_common_type) {
                auto type_promoted_col = other.column_unchecked(*other_col_index).clone();
                type_promoted_col.change_type(opt_common_type->data_type_);
                column_unchecked(col).append(type_promoted_col, row_id_ + 1);
            } else {
                column_unchecked(col).append(other.column(*other_col_index), row_id_ + 1);
            }
        } else {
            ARCTICDB_DEBUG(log::version(), "Marking {} absent rows for column {}", other.row_count(), col_name);
            column_unchecked(col).mark_absent_rows(other.row_count());
        }
    }
    set_row_id(row_id_ + other.row_count());
}

void SegmentInMemoryImpl::generate_column_map() const {
    if (column_map_) {
        column_map_->clear();
        column_map_->set_from_descriptor(*descriptor_);
    }
}

void SegmentInMemoryImpl::create_columns(size_t old_size, size_t expected_column_size, bool presize, bool allow_sparse) {
    columns_.reserve(descriptor_->field_count());
    for (size_t i = old_size; i < size_t(descriptor_->field_count()); ++i) {
        auto type = descriptor_->fields(i).type();
        util::check(type.data_type() != DataType::UNKNOWN, "Can't create column in create_columns with unknown data type");
        columns_.emplace_back(
                std::make_shared<Column>(descriptor_->fields(i).type(), expected_column_size, presize, allow_sparse));
    }
    generate_column_map();
}

void SegmentInMemoryImpl::init_column_map() const {
    std::lock_guard lock{*column_map_mutex_};
    if(column_map_)
        return;

    column_map_ = std::make_shared<ColumnMap>(descriptor().field_count());
    column_map_->set_from_descriptor(descriptor());
}

bool SegmentInMemoryImpl::is_index_sorted() const {
    timestamp idx = 0;
    for (auto it = begin(); it != end(); ++it) {
        auto val = it->begin()->value<timestamp>();
        if (idx > val)
            return false;

        idx = val;
    }
    return true;
}

/**
 * @param descriptor
 * @return false is descriptor change is not compatible and should trigger a segment commit
 */
size_t SegmentInMemoryImpl::on_descriptor_change(const StreamDescriptor &descriptor, size_t expected_column_size, bool presize, bool allow_sparse) {
    ARCTICDB_TRACE(log::storage(), "Entering descriptor change: descriptor is currently {}, incoming descriptor '{}'",
                   *descriptor_, descriptor);

    std::size_t old_size = descriptor_->fields().size();
    *descriptor_ = descriptor;
    create_columns(old_size, expected_column_size, presize, allow_sparse);
    ARCTICDB_TRACE(log::storage(), "Descriptor change: descriptor is now {}", *descriptor_);
    return old_size;
}

std::optional<std::size_t> SegmentInMemoryImpl::column_index(std::string_view name) const {
    util::check(!name.empty(), "Cannot get index of empty column name");
    util::check(static_cast<bool>(column_map_), "Uninitialized column map");
    return column_map_->column_index(name);
}

SegmentInMemoryImpl SegmentInMemoryImpl::clone() const {
    SegmentInMemoryImpl output{};
    output.row_id_ = row_id_;
    *output.descriptor_ = descriptor_->clone();

    for(const auto& column : columns_) {
        output.columns_.push_back(std::make_shared<Column>(column->clone()));
    }

    output.string_pool_ = string_pool_->clone();
    output.offset_ = offset_;
    if(metadata_) {
        google::protobuf::Any metadata;
        metadata.CopyFrom(*metadata_);
        output.metadata_ = std::make_unique<google::protobuf::Any>(std::move(metadata));
    }
    output.allow_sparse_ = allow_sparse_;
    output.compacted_ = compacted_;
    return output;
}

void SegmentInMemoryImpl::drop_column(std::string_view name) {
    std::lock_guard lock(*column_map_mutex_);
    auto opt_column_index = column_index(name);
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(opt_column_index.has_value(), "Cannot drop column with name '{}' as it doesn't exist", name);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(*opt_column_index < columns_.size(), "Column index out of range in drop_column");
    auto it = std::begin(columns_);
    std::advance(it, *opt_column_index);
    columns_.erase(it);
    descriptor_->erase_field(*opt_column_index);
    column_map_->erase(name);
}

std::shared_ptr<SegmentInMemoryImpl> SegmentInMemoryImpl::filter(const util::BitSet& filter_bitset,
                                                   bool filter_down_stringpool,
                                                   bool validate) const {
    bool is_input_sparse = is_sparse();
    util::check(is_input_sparse || row_count() == filter_bitset.size(), "Filter input sizes do not match: {} != {}", row_count(), filter_bitset.size());
    auto num_values = filter_bitset.count();
    if(num_values == 0)
        return std::shared_ptr<SegmentInMemoryImpl>{};

    auto output = get_output_segment(num_values);
    auto output_string_pool = filter_down_stringpool ? std::make_shared<StringPool>() : string_pool_;
    // Map from offsets in the input stringpool to offsets in the output stringpool
    // Only used if filter_down_stringpool is true
    robin_hood::unordered_flat_map<entity::position_t, entity::position_t> input_to_output_offsets;
    // Prepopulate with None and NaN placeholder values to avoid an if statement in a tight loop later
    input_to_output_offsets.insert(robin_hood::pair(not_a_string(), not_a_string()));
    input_to_output_offsets.insert(robin_hood::pair(nan_placeholder(), nan_placeholder()));

    // Index is built to make rank queries faster
    std::unique_ptr<util::BitIndex> filter_idx;
    for(const auto& column : folly::enumerate(columns())) {
        (*column)->type().visit_tag([&] (auto type_desc_tag){
            using TypeDescriptorTag =  decltype(type_desc_tag);
            using DataTypeTag = typename TypeDescriptorTag::DataTypeTag;
            using RawType = typename DataTypeTag::raw_type;
            const util::BitSet* final_bitset;
            util::BitSet bitset_including_sparse;

            auto sparse_map = (*column)->opt_sparse_map();
            std::unique_ptr<util::BitIndex> sparse_idx;
            auto output_col_idx = column.index;
            if (is_input_sparse || sparse_map) {
                filter_idx = std::make_unique<util::BitIndex>();
                filter_bitset.build_rs_index(filter_idx.get());
                bitset_including_sparse = filter_bitset;
                if (sparse_map) {
                    bitset_including_sparse = bitset_including_sparse.bit_and(sparse_map.value());
                    // Index is built to make rank queries faster
                    sparse_idx = std::make_unique<util::BitIndex>();
                    sparse_map.value().build_rs_index(sparse_idx.get());
                } else {
                    bitset_including_sparse.resize((*column)->row_count());
                }
                if (bitset_including_sparse.count() == 0) {
                    // No values are set in the sparse column, skip it
                    return;
                }
                output_col_idx = output->add_column(field(column.index), bitset_including_sparse.count(), true);
                final_bitset = &bitset_including_sparse;
            } else {
                final_bitset = &filter_bitset;
            }
            auto& output_col = output->column(position_t(output_col_idx));
            if (sparse_map)
                output_col.opt_sparse_map() = std::make_optional<util::BitSet>();
            auto output_ptr = reinterpret_cast<RawType*>(output_col.ptr());
            auto input_data =  (*column)->data();

            auto bitset_iter = final_bitset->first();
            auto row_count_so_far = 0;
            // Defines the position in output sparse column where we want to write data next (only used in sparse)
            // For dense, we just do +1
            util::BitSetSizeType pos_output = 0;
            while(auto block = input_data.next<TypeDescriptorTag>()) {
                if (bitset_iter == final_bitset->end())
                    break;
                auto input_ptr = block.value().data();
                if (sparse_map) {
                    while(bitset_iter != final_bitset->end()) {
                        auto rank_in_filter = filter_bitset.rank(*bitset_iter, *filter_idx);
                        if (rank_in_filter - 1 != pos_output) {
                            // setting sparse_map - marking all rows in output as NULL until this point
                            output_col.mark_absent_rows(rank_in_filter - pos_output - 1);
                            pos_output = rank_in_filter - 1;
                        }
                        auto offset = sparse_map.value().rank(*bitset_iter, *sparse_idx) - row_count_so_far - 1;
                        auto value = *(input_ptr + offset);
                        if constexpr(is_sequence_type(DataTypeTag::data_type)) {
                            if (filter_down_stringpool) {
                                if (auto it = input_to_output_offsets.find(value);
                                        it != input_to_output_offsets.end()) {
                                    *output_ptr = it->second;
                                } else {
                                    auto str = string_pool_->get_const_view(value);
                                    auto output_string_pool_offset = output_string_pool->get(str, false).offset();
                                    *output_ptr = output_string_pool_offset;
                                    input_to_output_offsets.insert(robin_hood::pair(entity::position_t(value), std::move(output_string_pool_offset)));
                                }
                            } else {
                                *output_ptr = value;
                            }
                        } else if constexpr(is_numeric_type(DataTypeTag::data_type) || is_bool_type(DataTypeTag::data_type)){
                            *output_ptr = value;
                        } else if constexpr(is_empty_type(DataTypeTag::data_type)) {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected block in empty type column in SegmentInMemoryImpl::filter");
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected column type in SegmentInMemoryImpl::filter");
                        }
                        ++output_ptr;
                        output_col.opt_sparse_map().value()[pos_output++] = true;
                        ++bitset_iter;
                    }
                } else {
                    const auto row_count = block.value().row_count();
                    const auto bitset_end = final_bitset->end();
                    do {
                        const auto offset = *bitset_iter - row_count_so_far;
                        if (offset >= row_count)
                            break;

                        auto value = *(input_ptr + offset);
                        if constexpr(is_sequence_type(DataTypeTag::data_type)) {
                            if (filter_down_stringpool) {
                                if (auto it = input_to_output_offsets.find(value);
                                        it != input_to_output_offsets.end()) {
                                    *output_ptr = it->second;
                                } else {
                                    auto str = string_pool_->get_const_view(value);
                                    auto output_string_pool_offset = output_string_pool->get(str, false).offset();
                                    *output_ptr = output_string_pool_offset;
                                    input_to_output_offsets.insert(robin_hood::pair(entity::position_t(value), std::move(output_string_pool_offset)));
                                }
                            } else {
                                *output_ptr = value;
                            }
                        } else if constexpr(is_numeric_type(DataTypeTag::data_type) || is_bool_type(DataTypeTag::data_type)){
                            *output_ptr = value;
                        } else if constexpr(is_empty_type(DataTypeTag::data_type)) {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected block in empty type column in SegmentInMemoryImpl::filter");
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected column type in SegmentInMemoryImpl::filter");
                        }

                        ++output_ptr;
                        bitset_iter.advance();
                    } while (bitset_iter < bitset_end);
                }
                row_count_so_far += block.value().row_count();
            }
            if (sparse_map && validate)
                check_output_bitset(output_col.opt_sparse_map().value(), filter_bitset, sparse_map.value());
        });
    }
    if (num_values != 0)
        output->set_row_data(num_values - 1);

    output->set_string_pool(output_string_pool);
    output->set_compacted(compacted_);
    if (metadata_) {
        google::protobuf::Any metadata;
        metadata.CopyFrom(*metadata_);
        output->set_metadata(std::move(metadata));
    }
    return output;
}

std::shared_ptr<arcticdb::proto::descriptors::TimeSeriesDescriptor> SegmentInMemoryImpl::timeseries_proto() {
    if(!tsd_) {
        tsd_ = std::make_shared<arcticdb::proto::descriptors::TimeSeriesDescriptor>();
        metadata_->UnpackTo(tsd_.get());
    }
    return tsd_;
}

std::shared_ptr<SegmentInMemoryImpl> SegmentInMemoryImpl::get_output_segment(size_t num_values, bool pre_allocate) const {
    std::shared_ptr<SegmentInMemoryImpl> output;
    if (is_sparse()) {
        output = allocate_sparse_segment(descriptor().id(), descriptor().index());
    } else {
        if (pre_allocate)
            output = allocate_dense_segment(descriptor(), num_values);
        else
            output = allocate_dense_segment(StreamDescriptor(descriptor().id(), descriptor().index(), {}), num_values);
    }
    return output;
}

std::vector<std::shared_ptr<SegmentInMemoryImpl>> SegmentInMemoryImpl::partition(const std::vector<std::optional<uint8_t>>& row_to_segment,
                                                                   const std::vector<uint64_t>& segment_counts) const {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(!is_sparse(),
                                                        "SegmentInMemory::partition not supported with sparse columns");
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(row_count() == row_to_segment.size(),
                                                    "row_to_segment size does not match segment row count: {} != {}", row_to_segment.size(), row_count());
    std::vector<std::shared_ptr<SegmentInMemoryImpl>> output(segment_counts.size());
    if(std::all_of(segment_counts.begin(), segment_counts.end(), [](const size_t& segment_count) { return segment_count == 0; })) {
        return output;
    }

    for (const auto& segment_count: folly::enumerate(segment_counts)) {
        if (*segment_count > 0) {
            auto& seg = output.at(segment_count.index);
            seg = get_output_segment(*segment_count);
            seg->set_row_data(*segment_count - 1);
            seg->set_string_pool(string_pool_);
            seg->set_compacted(compacted_);
            if (metadata_) {
                google::protobuf::Any metadata;
                metadata.CopyFrom(*metadata_);
                seg->set_metadata(std::move(metadata));
            }
        }
    }

    for(const auto& column : folly::enumerate(columns())) {
        (*column)->type().visit_tag([&] (auto type_desc_tag){
            using TypeDescriptorTag = decltype(type_desc_tag);
            using ColumnTagType = typename TypeDescriptorTag::DataTypeTag;
            using RawType = typename ColumnTagType::raw_type;

            auto output_col_idx = column.index;
            std::vector<RawType*> output_ptrs{output.size(), nullptr};
            for (const auto& segment: folly::enumerate(output)) {
                if (static_cast<bool>(*segment)) {
                    output_ptrs.at(segment.index) = reinterpret_cast<RawType*>((*segment)->column(output_col_idx).ptr());
                }
            }

            auto input_data =  (*column)->data();
            size_t overall_idx = 0;
            while(auto block = input_data.next<TypeDescriptorTag>()) {
                auto input_ptr = reinterpret_cast<const RawType*>(block.value().data());
                for (size_t block_idx = 0; block_idx < block.value().row_count(); ++block_idx, ++input_ptr, ++overall_idx) {
                    auto opt_output_segment_idx = row_to_segment[overall_idx];
                    if (opt_output_segment_idx.has_value()) {
                        *(output_ptrs[*opt_output_segment_idx]++) = *input_ptr;
                    }
                }
            }
        });
    }
    return output;
}

bool operator==(const SegmentInMemoryImpl& left, const SegmentInMemoryImpl& right) {
    if(*left.descriptor_ != *right.descriptor_ ||
       left.offset_ != right.offset_)
        return false;

    if(left.columns_.size() != right.columns_.size())
        return false;

    for(auto col = 0u; col < left.columns_.size(); ++col) {
        const auto left_data_type = left.column(col).type().data_type();
        if (is_sequence_type(left_data_type)) {

            const auto& left_col = left.column(col);
            const auto& right_col = right.column(col);
            if(left_col.type() != right_col.type())
                return false;

            if(left_col.row_count() != right_col.row_count())
                return false;

            for(auto row = 0u; row < left_col.row_count(); ++row)
                if(left.string_at(row, col) != right.string_at(row, col))
                    return false;
        } else if (is_numeric_type(left_data_type) || is_bool_type(left_data_type)) {
            if (left.column(col) != right.column(col))
                return false;
        } else if (is_empty_type(left_data_type)) {
            if (!is_empty_type(right.column(col).type().data_type()))
                return false;
        } else {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected data type in SegmentInMemory equality check");
        }
    }
    return true;
}

// Inclusive of start_row, exclusive of end_row
std::shared_ptr<SegmentInMemoryImpl> SegmentInMemoryImpl::truncate(size_t start_row, size_t end_row) const {
    auto num_values = end_row - start_row;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            is_sparse() || (start_row < row_count() && end_row <= row_count()),
            "Truncate bounds start_row={} end_row={} outside valid range {}", start_row, end_row, row_count());

    auto output = std::make_shared<SegmentInMemoryImpl>();

    output->set_row_data(num_values - 1);
    output->set_string_pool(string_pool_);
    output->set_compacted(compacted_);
    if (metadata_) {
        google::protobuf::Any metadata;
        metadata.CopyFrom(*metadata_);
        output->set_metadata(std::move(metadata));
    }

    for(const auto&& [idx, column] : folly::enumerate(columns_)) {
        auto truncated_column = Column::truncate(column, start_row, end_row);
        output->add_column(descriptor_->field(idx), truncated_column);
    }
    output->attach_descriptor(descriptor_);
    return output;
}

// Combine 2 segments that hold different columns associated with the same rows
// If unique_column_names is true, any columns from other with names matching those in this are ignored
void SegmentInMemoryImpl::concatenate(SegmentInMemoryImpl&& other, bool unique_column_names) {
        internal::check<ErrorCode::E_INVALID_ARGUMENT>(
                row_count() == other.row_count(),
                "Cannot concatenate segments with differing row counts: {} {}",
                row_count(), other.row_count());
    for (const auto& field: folly::enumerate(other.fields())) {
        if (!unique_column_names || !column_index(field->name()).has_value()) {
            add_column(*field, other.column_ptr(field.index));
        }
    }
}

position_t SegmentInMemoryImpl::add_column(FieldRef field, size_t num_rows, bool presize) {
    util::check_arg(!field.name().empty(), "Empty name in field: {}", field);
    if(!column_map_)
        init_column_map();

    columns_.emplace_back(std::make_shared<Column>(field.type(), num_rows, presize, allow_sparse_));
    auto new_field_name = descriptor_->add_field(FieldRef{field.type(), field.name()});

    std::lock_guard<std::mutex> lock{*column_map_mutex_};
    column_map_->insert(new_field_name, descriptor_->field_count() - 1);
    return columns_.size() - 1;
}

position_t SegmentInMemoryImpl::add_column(const Field& field, size_t num_rows, bool presize) {
    return add_column(FieldRef{field.type(), field.name()}, num_rows, presize);
}

position_t SegmentInMemoryImpl::add_column(FieldRef field_ref, const std::shared_ptr<Column>& column) {
    if(!column_map_)
        init_column_map();

    columns_.emplace_back(column);
    auto new_field_name = descriptor_->add_field(field_ref);

    std::lock_guard<std::mutex> lock{*column_map_mutex_};
    column_map_->insert(new_field_name, descriptor_->field_count() - 1);
    return columns_.size() - 1;
}

position_t SegmentInMemoryImpl::add_column(const Field& field, const std::shared_ptr<Column>& column) {
    return add_column(FieldRef{field.type(), field.name()}, column);
}

void SegmentInMemoryImpl::change_schema(StreamDescriptor descriptor) {
    //util::check(vector_is_unique(descriptor.fields()), "Non-unique fields in descriptor: {}", descriptor.fields());
    init_column_map();
    std::vector<std::shared_ptr<Column>> new_columns(descriptor.field_count());
    for(auto col = 0u; col < descriptor.field_count(); ++col) {
        auto col_name = descriptor.field(col).name();
        auto col_index = column_index(col_name);
        const auto& other_type = descriptor.field(col).type();
        if(col_index) {
            auto this_type = column_unchecked(*col_index).type();
            util::check(this_type == other_type, "Could not convert type {} to type {} for column {}, this index {}, other index {}",
                        other_type, this_type, col_name, *col_index, col);
            new_columns[col] = std::move(columns_[*col_index]);
        } else {
            auto new_column = std::make_shared<Column>(other_type, row_count(), false, allow_sparse_);
            new_column->default_initialize_rows(size_t{0}, row_count(), true);
            new_columns[col] = std::move(new_column);
        }
    }
    std::swap(*descriptor_, descriptor);
    std::swap(columns_, new_columns);
    generate_column_map();
}

std::optional<std::string_view> SegmentInMemoryImpl::string_at(position_t row, position_t col) const {
    auto td = descriptor_->field(col).type();
    util::check(is_sequence_type(td.data_type()), "Not a sequence type");
    util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in string", row);
    const auto& col_ref = column(col);

    if(is_fixed_string_type(td.data_type()) && col_ref.is_inflated()) {
        auto string_size = col_ref.bytes() / row_count();
        auto ptr = col_ref.data().buffer().ptr_cast<char>(row * string_size, string_size);
        return std::string_view(ptr, string_size);
    } else {

        auto offset = col_ref.scalar_at<entity::position_t>(row);
        if (offset != std::nullopt && *offset != not_a_string() && *offset != nan_placeholder())
            return string_pool_->get_view(*offset);
        else
            return std::nullopt;
    }
}

std::vector<std::shared_ptr<SegmentInMemoryImpl>> SegmentInMemoryImpl::split(size_t rows) const {
    std::vector<std::shared_ptr<SegmentInMemoryImpl>> output;
    util::check(rows > 0, "rows supplied in SegmentInMemoryImpl.split() is non positive");
    auto total_rows = row_count();
    util::BitSetSizeType start = 0;
    for(; start < total_rows; start += rows) {
        util::BitSet bitset(total_rows);
        util::BitSetSizeType end = std::min(start + rows, total_rows);
        // set_range is close interval on [left, right]
        bitset.set_range(start, end - 1, true);
        output.emplace_back(filter(bitset));
    }
    return output;
}

size_t SegmentInMemoryImpl::num_blocks() const {
    return std::accumulate(std::begin(columns_), std::end(columns_), 0, [] (size_t n, const auto& col) {
        return n + col->num_blocks();
    });
}

std::optional<Column::StringArrayData> SegmentInMemoryImpl::string_array_at(position_t row, position_t col) {
    util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in string array", row);
    return column(col).string_array_at(row, *string_pool_);
}

size_t SegmentInMemoryImpl::num_bytes() const {
    return std::accumulate(std::begin(columns_), std::end(columns_), 0, [] (size_t n, const auto& col) {
        return n + col->bytes();
    });
}

void SegmentInMemoryImpl::sort(const std::string& column_name) {
    init_column_map();
    auto idx = column_index(std::string_view(column_name));
    util::check(static_cast<bool>(idx), "Column {} not found in sort", column_name);
    sort(idx.value());
}

void SegmentInMemoryImpl::sort(position_t idx) {
    auto& sort_col = column_unchecked(idx);
    util::check(!sort_col.is_sparse(), "Can't sort on sparse column idx {} because it is not supported yet. The user should either fill the column data or filter the empty columns out",idx);
    auto table = sort_col.type().visit_tag([&sort_col] (auto tdt) {
        using TagType = decltype(tdt);
        using RawType = typename TagType::DataTypeTag::raw_type;
        return create_jive_table<RawType>(sort_col);
    });

    auto pre_allocated_space = std::vector<uint32_t>(sort_col.row_count());
    for (auto field_col = 0u; field_col < descriptor().field_count(); ++field_col) {
        column(field_col).sort_external(table, pre_allocated_space);
    }
}

void SegmentInMemoryImpl::set_timeseries_descriptor(TimeseriesDescriptor&& tsd) {
    index_fields_ = tsd.fields_ptr();
    tsd_ = tsd.proto_ptr();
    util::check(!tsd_->has_stream_descriptor() || tsd_->stream_descriptor().has_index(), "Stream descriptor without index in set_timeseries_descriptor");
    google::protobuf::Any any;
    any.PackFrom(tsd.proto());
    set_metadata(std::move(any));
}

void SegmentInMemoryImpl::set_metadata(google::protobuf::Any&& meta) {
    util::check_arg(!metadata_, "Cannot override previously set metadata");
    if (meta.ByteSizeLong())
        metadata_ = std::make_unique<google::protobuf::Any>(std::move(meta));
}

void SegmentInMemoryImpl::override_metadata(google::protobuf::Any&& meta) {
    if (meta.ByteSizeLong())
        metadata_ = std::make_unique<google::protobuf::Any>(std::move(meta));
}

bool SegmentInMemoryImpl::has_metadata() const {
    return static_cast<bool>(metadata_);
}

const google::protobuf::Any* SegmentInMemoryImpl::metadata() const {
    return metadata_.get();
}

}
