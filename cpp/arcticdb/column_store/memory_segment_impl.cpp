/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/column_store/memory_segment_impl.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/column_store/column_map.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>

#include <google/protobuf/any.pb.h>

#include <arcticdb/stream/index.hpp>

namespace arcticdb {
namespace {
std::shared_ptr<SegmentInMemoryImpl> allocate_sparse_segment(const StreamId& id, const IndexDescriptorImpl& index) {
    return std::make_shared<SegmentInMemoryImpl>(
            StreamDescriptor{id, index}, 0, AllocationType::DYNAMIC, Sparsity::PERMITTED, std::nullopt
    );
}

std::shared_ptr<SegmentInMemoryImpl> allocate_dense_segment(const StreamDescriptor& descriptor, size_t row_count) {
    return std::make_shared<SegmentInMemoryImpl>(
            descriptor, row_count, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED, std::nullopt
    );
}

void check_output_bitset(
        const arcticdb::util::BitSet& output, const arcticdb::util::BitSet& filter,
        const arcticdb::util::BitSet& column_bitset
) {
    // TODO: Do this in O(1)
    // The logic here is that the filter bitset defines how the output bitset should look like
    // The set bits in filter decides the row ids in the output. The corresponding values in sparse_map
    // should match output bitset
    auto filter_iter = filter.first();
    arcticdb::util::BitSetSizeType output_pos = 0;
    while (filter_iter != filter.end()) {
        arcticdb::util::check_rte(
                column_bitset.test(*(filter_iter++)) == output.test(output_pos++),
                "Mismatch in output bitset in filter_segment"
        );
    }
}
} // namespace

bool SegmentInMemoryImpl::Location::has_value() const { return parent_->has_value_at(row_id_, position_t(column_id_)); }

bool SegmentInMemoryImpl::Location::operator==(const Location& other) const {
    return row_id_ == other.row_id_ && column_id_ == other.column_id_;
}

SegmentInMemoryImpl::Row::Row(SegmentInMemoryImpl* parent, ssize_t row_id_) : parent_(parent), row_id_(row_id_) {}

SegmentInMemoryImpl& SegmentInMemoryImpl::Row::segment() const { return *parent_; }

const StreamDescriptor& SegmentInMemoryImpl::Row::descriptor() const { return parent_->descriptor(); }

bool SegmentInMemoryImpl::Row::operator<(const Row& other) const {
    return entity::visit_field(parent_->field(0), [this, &other](auto type_desc_tag) {
        using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
        return parent_->scalar_at<RawType>(row_id_, 0) < other.parent_->scalar_at<RawType>(other.row_id_, 0);
    });
}

size_t SegmentInMemoryImpl::Row::row_pos() const { return row_id_; }

void swap(SegmentInMemoryImpl::Row& left, SegmentInMemoryImpl::Row& right) noexcept {
    using std::swap;

    auto a = left.begin();
    auto b = right.begin();
    for (; a != left.end(); ++a, ++b) {
        util::check(a->has_value() && b->has_value(), "Can't swap sparse column values, unsparsify first?");
        a->visit([&b](auto& val) {
            using ValType = std::decay_t<decltype(val)>;
            swap(val, b->value<ValType>());
        });
    }
}

SegmentInMemoryImpl::Location SegmentInMemoryImpl::Row::operator[](int pos) const {
    return Location{parent_, row_id_, size_t(pos)};
}

SegmentInMemoryImpl::Row::iterator SegmentInMemoryImpl::Row::begin() { return {parent_, row_id_}; }

SegmentInMemoryImpl::Row::iterator SegmentInMemoryImpl::Row::end() {
    return {parent_, row_id_, size_t(parent_->descriptor().fields().size())};
}

SegmentInMemoryImpl::Row::const_iterator SegmentInMemoryImpl::Row::begin() const { return {parent_, row_id_}; }

SegmentInMemoryImpl::Row::const_iterator SegmentInMemoryImpl::Row::end() const {
    return {parent_, row_id_, size_t(parent_->descriptor().fields().size())};
}

bool SegmentInMemoryImpl::Row::operator==(const Row& other) const {
    return row_id_ == other.row_id_ && parent_ == other.parent_;
}

void SegmentInMemoryImpl::Row::swap_parent(const Row& other) { parent_ = other.parent_; }

std::optional<std::string_view> SegmentInMemoryImpl::Row::string_at(std::size_t col) const {
    return parent_->string_at(row_id_, position_t(col));
}

SegmentInMemoryImpl::iterator SegmentInMemoryImpl::begin() { return iterator{this}; }

SegmentInMemoryImpl::iterator SegmentInMemoryImpl::end() {
    util::check(row_id_ != -1, "End iterator called with negative row id, iterator will never terminate");
    return iterator{this, row_id_ + 1};
}

SegmentInMemoryImpl::const_iterator SegmentInMemoryImpl::begin() const {
    return const_iterator{const_cast<SegmentInMemoryImpl*>(this)};
}

SegmentInMemoryImpl::const_iterator SegmentInMemoryImpl::end() const {
    util::check(row_id_ != -1, "End iterator called with negative row id, iterator will never terminate");
    return const_iterator{const_cast<SegmentInMemoryImpl*>(this), row_id_};
}

const Field& SegmentInMemoryImpl::column_descriptor(size_t col) { return (*descriptor_)[col]; }

void SegmentInMemoryImpl::end_row() { row_id_++; }

const TimeseriesDescriptor& SegmentInMemoryImpl::index_descriptor() const {
    util::check(tsd_.has_value(), "Index descriptor requested but not set");
    return *tsd_;
}

TimeseriesDescriptor& SegmentInMemoryImpl::mutable_index_descriptor() {
    util::check(tsd_.has_value(), "Index descriptor requested but not set");
    return *tsd_;
}

void SegmentInMemoryImpl::end_block_write(ssize_t size) { row_id_ += size; }

void SegmentInMemoryImpl::set_offset(ssize_t offset) { offset_ = offset; }

ssize_t SegmentInMemoryImpl::offset() const { return offset_; }

void SegmentInMemoryImpl::push_back(const Row& row) {
    for (auto it : folly::enumerate(row)) {
        it->visit([&it, that = this](const auto& val) {
            if (val)
                that->set_scalar(it.index, val.value());
        });
    }
    end_row();
}

void SegmentInMemoryImpl::set_value(position_t idx, const Location& loc) {
    loc.visit([this, idx](const auto& val) {
        if (val)
            set_scalar(idx, val.value());
    });
}

void SegmentInMemoryImpl::set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset) {
    column_unchecked(idx).set_sparse_block(std::move(buffer), std::move(bitset));
}

void SegmentInMemoryImpl::set_sparse_block(
        position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset
) {
    column_unchecked(idx).set_sparse_block(std::move(buffer), std::move(shapes), std::move(bitset));
}

void SegmentInMemoryImpl::set_string(position_t pos, std::string_view str) {
    ARCTICDB_TRACE(log::version(), "Segment setting string {} at row {} column {}", str, row_id_ + 1, pos);
    OffsetString ofstr = string_pool_->get(str);
    column_unchecked(pos).set_scalar(row_id_ + 1, ofstr.offset());
}

void SegmentInMemoryImpl::set_string_at(position_t col, position_t row, const char* str, size_t size) {
    OffsetString ofstr = string_pool_->get(str, size);
    column_unchecked(col).set_scalar(row, ofstr.offset());
}

void SegmentInMemoryImpl::set_string_array(position_t idx, size_t string_size, size_t num_strings, char* data) {
    check_column_index(idx);
    column_unchecked(idx).set_string_array(row_id_ + 1, string_size, num_strings, data, string_pool());
}

void SegmentInMemoryImpl::set_string_list(position_t idx, const std::vector<std::string>& input) {
    check_column_index(idx);
    column_unchecked(idx).set_string_list(row_id_ + 1, input, string_pool());
}

Column& SegmentInMemoryImpl::column_ref(position_t idx) { return column(idx); }

Column& SegmentInMemoryImpl::column(position_t idx) {
    check_column_index(idx);
    return column_unchecked(idx);
}

const Column& SegmentInMemoryImpl::column(position_t idx) const {
    check_column_index(idx);
    return column_unchecked(idx);
}

Column& SegmentInMemoryImpl::column_unchecked(position_t idx) { return *columns_[idx]; }

std::shared_ptr<Column> SegmentInMemoryImpl::column_ptr(position_t idx) const { return columns_[idx]; }

const Column& SegmentInMemoryImpl::column_unchecked(position_t idx) const { return *columns_[idx]; }

std::vector<std::shared_ptr<Column>>& SegmentInMemoryImpl::columns() { return columns_; }

const std::vector<std::shared_ptr<Column>>& SegmentInMemoryImpl::columns() const { return columns_; }

bool SegmentInMemoryImpl::empty() const { return row_count() <= 0 && !metadata(); }

void SegmentInMemoryImpl::unsparsify() const {
    for (const auto& column : columns_)
        column->unsparsify(row_count());
}

void SegmentInMemoryImpl::sparsify() const {
    for (const auto& column : columns_)
        column->sparsify();
}

bool SegmentInMemoryImpl::has_value_at(position_t row, position_t col) const { return column(col).has_value_at(row); }

size_t SegmentInMemoryImpl::num_columns() const { return columns_.size(); }

size_t SegmentInMemoryImpl::num_fields() const { return descriptor().field_count(); }

size_t SegmentInMemoryImpl::row_count() const { return row_id_ + 1 < 0 ? 0 : size_t(row_id_ + 1); }

void SegmentInMemoryImpl::clear() {
    columns_.clear();
    string_pool_->clear();
}

size_t SegmentInMemoryImpl::string_pool_size() const { return string_pool_->size(); }

bool SegmentInMemoryImpl::has_string_pool() const { return string_pool_size() > 0; }

const std::shared_ptr<StringPool>& SegmentInMemoryImpl::string_pool_ptr() const { return string_pool_; }

void SegmentInMemoryImpl::check_column_index(position_t idx) const {
    util::check_arg(idx < position_t(columns_.size()), "Column index {} out of bounds", idx);
}

ColumnData SegmentInMemoryImpl::string_pool_data() const {
    return ColumnData{&string_pool_->data(), &string_pool_->shapes(), string_pool_descriptor().type(), nullptr};
}

void SegmentInMemoryImpl::compact_blocks() const {
    for (const auto& column : columns_)
        column->compact_blocks();
}

const FieldCollection& SegmentInMemoryImpl::fields() const { return descriptor().fields(); }

ColumnData SegmentInMemoryImpl::column_data(size_t col) const { return columns_[col]->data(); }

const StreamDescriptor& SegmentInMemoryImpl::descriptor() const { return *descriptor_; }

StreamDescriptor& SegmentInMemoryImpl::descriptor() { return *descriptor_; }

const std::shared_ptr<StreamDescriptor>& SegmentInMemoryImpl::descriptor_ptr() const {
    util::check(static_cast<bool>(descriptor_), "Descriptor pointer is null");
    return descriptor_;
}

void SegmentInMemoryImpl::attach_descriptor(std::shared_ptr<StreamDescriptor> desc) { descriptor_ = std::move(desc); }

const Field& SegmentInMemoryImpl::field(size_t index) const { return descriptor()[index]; }

void SegmentInMemoryImpl::set_row_id(ssize_t rid) { row_id_ = rid; }

void SegmentInMemoryImpl::set_row_data(ssize_t rid) {
    set_row_id(rid);
    for (const auto& column : columns())
        column->set_row_data(row_id_);
}

StringPool& SegmentInMemoryImpl::string_pool() { return *string_pool_; } // TODO protected

bool SegmentInMemoryImpl::compacted() const { return compacted_; }

void SegmentInMemoryImpl::set_compacted(bool value) { compacted_ = value; }

void SegmentInMemoryImpl::check_magic() const { magic_.check(); }

bool SegmentInMemoryImpl::allow_sparse() const { return allow_sparse_ == Sparsity::PERMITTED; }

bool SegmentInMemoryImpl::is_sparse() const {
    // TODO: Very slow, fix this by storing it in protobuf
    return std::ranges::any_of(columns_, [](const auto& c) { return c->is_sparse(); });
}

void SegmentInMemoryImpl::set_string_pool(std::shared_ptr<StringPool> string_pool) {
    string_pool_ = std::move(string_pool);
}

bool SegmentInMemoryImpl::has_index_descriptor() const { return tsd_.has_value(); }

bool SegmentInMemoryImpl::has_user_metadata() {
    return tsd_.has_value() && !tsd_->proto_is_null() && tsd_->proto().has_user_meta();
}

const arcticdb::proto::descriptors::UserDefinedMetadata& SegmentInMemoryImpl::user_metadata() const {
    return tsd_->user_metadata();
}

SegmentInMemoryImpl::SegmentInMemoryImpl() :
    descriptor_(std::make_shared<StreamDescriptor>()),
    string_pool_(std::make_shared<StringPool>()) {}

SegmentInMemoryImpl::SegmentInMemoryImpl(
        const StreamDescriptor& desc, size_t expected_column_size, AllocationType allocation_type,
        Sparsity allow_sparse, const ExtraBytesPerColumn& extra_bytes_per_column
) :
    descriptor_(std::make_shared<StreamDescriptor>(StreamDescriptor{desc.id(), desc.index()})),
    string_pool_(std::make_shared<StringPool>()),
    allow_sparse_(allow_sparse) {
    on_descriptor_change(desc, expected_column_size, allocation_type, allow_sparse, extra_bytes_per_column);
}

SegmentInMemoryImpl::~SegmentInMemoryImpl() { ARCTICDB_TRACE(log::version(), "Destroying segment in memory"); }

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
    for (auto col = 0u; col < num_columns(); ++col) {
        auto col_name = descriptor().field(col).name();
        auto other_col_index = other.column_index(col_name);
        if (other_col_index.has_value()) {
            ARCTICDB_DEBUG(log::version(), "Appending column {} at index {}", col_name, *other_col_index);
            auto this_type = column_unchecked(col).type();
            auto other_type = other.column_unchecked(static_cast<position_t>(*other_col_index)).type();
            auto opt_common_type = has_valid_common_type(this_type, other_type);
            internal::check<ErrorCode::E_INVALID_ARGUMENT>(
                    opt_common_type.has_value(),
                    "Could not append type {} to type {} for column {}, this index {}, other index {}",
                    other_type,
                    this_type,
                    col_name,
                    col,
                    *other_col_index
            );

            if (this_type != *opt_common_type) {
                column_unchecked(col).change_type(opt_common_type->data_type_);
                descriptor_->mutable_field(col).mutable_type().data_type_ = opt_common_type->data_type_;
            }
            if (other_type != *opt_common_type) {
                auto type_promoted_col = other.column_unchecked(static_cast<position_t>(*other_col_index)).clone();
                type_promoted_col.change_type(opt_common_type->data_type_);
                column_unchecked(col).append(type_promoted_col, row_id_ + 1);
            } else {
                column_unchecked(col).append(other.column(static_cast<position_t>(*other_col_index)), row_id_ + 1);
            }
        } else {
            ARCTICDB_DEBUG(log::version(), "Marking {} absent rows for column {}", other.row_count(), col_name);
            column_unchecked(col).mark_absent_rows(other.row_count());
        }
    }
    set_row_id(ssize_t(row_id_ + other.row_count()));
}

void SegmentInMemoryImpl::generate_column_map() const {
    if (column_map_) {
        column_map_->clear();
        column_map_->set_from_descriptor(*descriptor_);
    }
}

void SegmentInMemoryImpl::create_columns(
        size_t old_size, size_t expected_column_size, AllocationType allocation_type, Sparsity allow_sparse,
        const ExtraBytesPerColumn& extra_bytes_per_column
) {
    columns_.reserve(descriptor_->field_count());
    util::check(
            !extra_bytes_per_column.has_value() || extra_bytes_per_column.value().size() == descriptor_->field_count(),
            "Size mismatch for extra_bytes_per_column"
    );
    for (size_t i = old_size; i < size_t(descriptor_->field_count()); ++i) {
        size_t extra_bytes_per_block = 0;
        if (extra_bytes_per_column.has_value()) {
            extra_bytes_per_block = extra_bytes_per_column.value()[i];
        }
        auto type = descriptor_->fields(i).type();
        util::check(
                type.data_type() != DataType::UNKNOWN, "Can't create column in create_columns with unknown data type"
        );
        if (allocation_type == AllocationType::DETACHABLE &&
            is_fixed_string_type(descriptor_->fields(i).type().data_type())) {
            // Do not use detachable blocks for fixed width string columns as they are not yet inflated and will not be
            // passed back to the Python layer "as is"
            columns_.emplace_back(std::make_shared<Column>(
                    descriptor_->fields(i).type(), expected_column_size, AllocationType::PRESIZED, allow_sparse
            ));
        } else {
            columns_.emplace_back(std::make_shared<Column>(
                    descriptor_->fields(i).type(),
                    expected_column_size,
                    allocation_type,
                    allow_sparse,
                    extra_bytes_per_block
            ));
        }
    }
    generate_column_map();
}

void SegmentInMemoryImpl::init_column_map() const {
    std::lock_guard lock{*column_map_mutex_};
    if (column_map_)
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
size_t SegmentInMemoryImpl::on_descriptor_change(
        const StreamDescriptor& descriptor, size_t expected_column_size, AllocationType allocation_type,
        Sparsity allow_sparse, const ExtraBytesPerColumn& extra_bytes_per_column
) {
    ARCTICDB_TRACE(
            log::storage(),
            "Entering descriptor change: descriptor is currently {}, incoming descriptor '{}'",
            *descriptor_,
            descriptor
    );

    std::size_t old_size = descriptor_->fields().size();
    *descriptor_ = descriptor;
    create_columns(old_size, expected_column_size, allocation_type, allow_sparse, extra_bytes_per_column);
    ARCTICDB_TRACE(log::storage(), "Descriptor change: descriptor is now {}", *descriptor_);
    return old_size;
}

std::optional<std::size_t> SegmentInMemoryImpl::column_index(std::string_view name) const {
    util::check(!name.empty(), "Cannot get index of empty column name");
    util::check(static_cast<bool>(column_map_), "Uninitialized column map");
    return column_map_->column_index(name);
}

[[nodiscard]] std::optional<std::size_t> SegmentInMemoryImpl::column_index_with_name_demangling(std::string_view name
) const {
    if (auto index = column_index(name); index)
        return index;

    const std::string multi_index_column_name = stream::mangled_name(name);
    if (auto multi_index = column_index(multi_index_column_name); multi_index)
        return multi_index;

    return std::nullopt;
}

SegmentInMemoryImpl SegmentInMemoryImpl::clone() const {
    SegmentInMemoryImpl output{};
    output.row_id_ = row_id_;
    *output.descriptor_ = descriptor_->clone();

    for (const auto& column : columns_) {
        output.columns_.push_back(std::make_shared<Column>(column->clone()));
    }

    output.string_pool_ = string_pool_->clone();
    output.offset_ = offset_;
    if (metadata_) {
        google::protobuf::Any metadata;
        metadata.CopyFrom(*metadata_);
        output.metadata_ = std::make_unique<google::protobuf::Any>(std::move(metadata));
    }
    output.allow_sparse_ = allow_sparse_;
    output.compacted_ = compacted_;
    if (tsd_)
        output.set_timeseries_descriptor(tsd_->clone());

    return output;
}

void SegmentInMemoryImpl::drop_column(std::string_view name) {
    std::lock_guard lock(*column_map_mutex_);
    auto opt_column_index = column_index(name);
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            opt_column_index.has_value(), "Cannot drop column with name '{}' as it doesn't exist", name
    );
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            *opt_column_index < columns_.size(), "Column index out of range in drop_column"
    );
    auto it = std::begin(columns_);
    std::advance(it, *opt_column_index);
    columns_.erase(it);
    descriptor_->erase_field(static_cast<position_t>(*opt_column_index));
    column_map_->erase(name);
}

std::shared_ptr<SegmentInMemoryImpl> SegmentInMemoryImpl::filter(
        util::BitSet&& filter_bitset, bool filter_down_stringpool, bool validate
) const {
    filter_bitset.resize(row_count());
    bool is_input_sparse = is_sparse();
    auto num_values = filter_bitset.count();
    if (num_values == 0)
        return std::shared_ptr<SegmentInMemoryImpl>{};

    auto output = get_output_segment(num_values);
    auto output_string_pool = filter_down_stringpool ? std::make_shared<StringPool>() : string_pool_;
    // Map from offsets in the input stringpool to offsets in the output stringpool
    // Only used if filter_down_stringpool is true
    ankerl::unordered_dense::map<entity::position_t, entity::position_t> input_to_output_offsets;
    // Prepopulate with None and NaN placeholder values to avoid an if statement in a tight loop later
    input_to_output_offsets.insert(std::make_pair(not_a_string(), not_a_string()));
    input_to_output_offsets.insert(std::make_pair(nan_placeholder(), nan_placeholder()));

    // Index is built to make rank queries faster
    std::unique_ptr<util::BitIndex> filter_idx;
    for (const auto& column : folly::enumerate(columns())) {
        (*column)->type().visit_tag([&](auto type_desc_tag) {
            using TypeDescriptorTag = decltype(type_desc_tag);
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
                output_col_idx = output->add_column(
                        field(column.index), bitset_including_sparse.count(), AllocationType::PRESIZED
                );
                final_bitset = &bitset_including_sparse;
            } else {
                final_bitset = &filter_bitset;
            }
            auto& output_col = output->column(position_t(output_col_idx));
            if (sparse_map) {
                output_col.opt_sparse_map() = std::make_optional<util::BitSet>();
                if (final_bitset->count() == 0) {
                    // No values are set in the sparse column, no more work to do
                    return;
                }
            }
            auto output_ptr = reinterpret_cast<RawType*>(output_col.ptr());
            auto input_data = (*column)->data();

            auto bitset_iter = final_bitset->first();
            auto row_count_so_far = 0;
            // Defines the position in output sparse column where we want to write data next (only used in sparse)
            // For dense, we just do +1
            util::BitSetSizeType pos_output = 0;
            while (auto block = input_data.next<TypeDescriptorTag>()) {
                if (bitset_iter == final_bitset->end())
                    break;
                auto input_ptr = block.value().data();
                if (sparse_map) {
                    while (bitset_iter != final_bitset->end()) {
                        auto rank_in_filter = filter_bitset.rank(*bitset_iter, *filter_idx);
                        if (rank_in_filter - 1 != pos_output) {
                            // setting sparse_map - marking all rows in output as NULL until this point
                            output_col.mark_absent_rows(rank_in_filter - pos_output - 1);
                            pos_output = rank_in_filter - 1;
                        }
                        auto offset = sparse_map.value().rank(*bitset_iter, *sparse_idx) - row_count_so_far - 1;
                        auto value = *(input_ptr + offset);
                        if constexpr (is_sequence_type(DataTypeTag::data_type)) {
                            if (filter_down_stringpool) {
                                if (auto it = input_to_output_offsets.find(value);
                                    it != input_to_output_offsets.end()) {
                                    *output_ptr = it->second;
                                } else {
                                    auto str = string_pool_->get_const_view(value);
                                    auto output_string_pool_offset = output_string_pool->get(str, false).offset();
                                    *output_ptr = output_string_pool_offset;
                                    input_to_output_offsets.insert(std::make_pair(
                                            entity::position_t(value), std::move(output_string_pool_offset)
                                    ));
                                }
                            } else {
                                *output_ptr = value;
                            }
                        } else if constexpr (is_numeric_type(DataTypeTag::data_type) ||
                                             is_bool_type(DataTypeTag::data_type)) {
                            *output_ptr = value;
                        } else if constexpr (is_empty_type(DataTypeTag::data_type)) {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                    "Unexpected block in empty type column in SegmentInMemoryImpl::filter"
                            );
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                    "Unexpected column type in SegmentInMemoryImpl::filter"
                            );
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
                        if constexpr (is_sequence_type(DataTypeTag::data_type)) {
                            if (filter_down_stringpool) {
                                if (auto it = input_to_output_offsets.find(value);
                                    it != input_to_output_offsets.end()) {
                                    *output_ptr = it->second;
                                } else {
                                    auto str = string_pool_->get_const_view(value);
                                    auto output_string_pool_offset = output_string_pool->get(str, false).offset();
                                    *output_ptr = output_string_pool_offset;
                                    input_to_output_offsets.insert(std::make_pair(
                                            entity::position_t(value), std::move(output_string_pool_offset)
                                    ));
                                }
                            } else {
                                *output_ptr = value;
                            }
                        } else if constexpr (is_numeric_type(DataTypeTag::data_type) ||
                                             is_bool_type(DataTypeTag::data_type)) {
                            *output_ptr = value;
                        } else if constexpr (is_empty_type(DataTypeTag::data_type)) {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                    "Unexpected block in empty type column in SegmentInMemoryImpl::filter"
                            );
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                    "Unexpected column type in SegmentInMemoryImpl::filter"
                            );
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

std::shared_ptr<SegmentInMemoryImpl> SegmentInMemoryImpl::get_output_segment(size_t num_values, bool pre_allocate)
        const {
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

std::vector<std::shared_ptr<SegmentInMemoryImpl>> SegmentInMemoryImpl::partition(
        const std::vector<uint8_t>& row_to_segment, const std::vector<uint64_t>& segment_counts
) const {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            row_count() == row_to_segment.size(),
            "row_to_segment size does not match segment row count: {} != {}",
            row_to_segment.size(),
            row_count()
    );
    std::vector<std::shared_ptr<SegmentInMemoryImpl>> output(segment_counts.size());
    if (std::all_of(segment_counts.begin(), segment_counts.end(), [](const size_t& segment_count) {
            return segment_count == 0;
        })) {
        return output;
    }

    for (const auto& segment_count : folly::enumerate(segment_counts)) {
        if (*segment_count > 0) {
            auto& seg = output.at(segment_count.index);
            seg = get_output_segment(*segment_count);
            seg->set_string_pool(string_pool_);
            seg->set_compacted(compacted_);
            if (metadata_) {
                google::protobuf::Any metadata;
                metadata.CopyFrom(*metadata_);
                seg->set_metadata(std::move(metadata));
            }
        }
    }

    for (const auto& column : folly::enumerate(columns())) {
        details::visit_type((*column)->type().data_type(), [&](auto col_tag) {
            using type_info = ScalarTypeInfo<decltype(col_tag)>;
            if ((*column)->is_sparse()) {
                std::vector<std::shared_ptr<Column>> output_columns(output.size());
                for (const auto& segment : folly::enumerate(output)) {
                    if (static_cast<bool>(*segment)) {
                        (*segment)->add_column(field(column.index), 0, AllocationType::DYNAMIC);
                        output_columns.at(segment.index) =
                                (*segment)->column_ptr(static_cast<position_t>(column.index));
                    }
                }
                for (const auto& segment_idx : folly::enumerate(row_to_segment)) {
                    if (*segment_idx != std::numeric_limits<uint8_t>::max()) {
                        auto opt_value = (*column)->scalar_at<typename type_info::RawType>(segment_idx.index);
                        if (opt_value.has_value()) {
                            output_columns[*segment_idx]->push_back(*opt_value);
                        } else {
                            output_columns[*segment_idx]->mark_absent_rows(1);
                        }
                    }
                }
            } else {
                std::vector<typename type_info::RawType*> output_ptrs{output.size(), nullptr};
                for (const auto& segment : folly::enumerate(output)) {
                    if (static_cast<bool>(*segment)) {
                        if (is_sparse()) {
                            (*segment)->add_column(
                                    field(column.index), segment_counts[segment.index], AllocationType::PRESIZED
                            );
                        }
                        output_ptrs.at(segment.index) = reinterpret_cast<typename type_info::RawType*>(
                                (*segment)->column(static_cast<position_t>(column.index)).ptr()
                        );
                    }
                }
                auto row_to_segment_it = row_to_segment.cbegin();
                arcticdb::for_each<typename type_info::TDT>(**column, [&row_to_segment_it, &output_ptrs](auto val) {
                    if (ARCTICDB_LIKELY(*row_to_segment_it != std::numeric_limits<uint8_t>::max())) {
                        *(output_ptrs[*row_to_segment_it]++) = val;
                    }
                    ++row_to_segment_it;
                });
            }
        });
    }
    for (const auto& segment_count : folly::enumerate(segment_counts)) {
        if (*segment_count > 0) {
            auto& seg = output.at(segment_count.index);
            seg->set_row_data(ssize_t(*segment_count - 1));
        }
    }
    return output;
}

bool operator==(const SegmentInMemoryImpl& left, const SegmentInMemoryImpl& right) {
    if (*left.descriptor_ != *right.descriptor_ || left.offset_ != right.offset_)
        return false;

    if (left.columns_.size() != right.columns_.size())
        return false;

    for (auto col = 0u; col < left.columns_.size(); ++col) {
        const auto left_data_type = left.column(col).type().data_type();
        if (is_sequence_type(left_data_type)) {

            const auto& left_col = left.column(col);
            const auto& right_col = right.column(col);
            if (left_col.type() != right_col.type())
                return false;

            if (left_col.row_count() != right_col.row_count())
                return false;

            for (auto row = 0u; row < left_col.row_count(); ++row)
                if (left.string_at(row, col) != right.string_at(row, col))
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

std::shared_ptr<SegmentInMemoryImpl> SegmentInMemoryImpl::truncate(
        size_t start_row, size_t end_row, bool reconstruct_string_pool
) const {
    auto num_values = end_row - start_row;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            is_sparse() || (start_row < row_count() && end_row <= row_count()),
            "Truncate bounds start_row={} end_row={} outside valid range {}",
            start_row,
            end_row,
            row_count()
    );

    auto output = std::make_shared<SegmentInMemoryImpl>();

    output->set_row_data(ssize_t(num_values - 1));
    output->set_compacted(compacted_);
    if (!reconstruct_string_pool) {
        output->set_string_pool(string_pool_);
    }
    if (metadata_) {
        google::protobuf::Any metadata;
        metadata.CopyFrom(*metadata_);
        output->set_metadata(std::move(metadata));
    }

    for (const auto&& [idx, column] : folly::enumerate(columns_)) {
        const Field& field = descriptor_->field(idx);
        std::shared_ptr<Column> truncated_column = Column::truncate(column, start_row, end_row);
        details::visit_type(column->type().data_type(), [&](auto col_tag) {
            using type_info = ScalarTypeInfo<decltype(col_tag)>;
            if constexpr (is_sequence_type(type_info::data_type)) {
                arcticdb::transform<typename type_info::TDT, typename type_info::TDT>(
                        *truncated_column,
                        *truncated_column,
                        [this, &output](auto string_pool_offset) -> typename type_info::RawType {
                            if (is_a_string(string_pool_offset)) {
                                const std::string_view string = get_string_from_pool(string_pool_offset, *string_pool_);
                                return output->string_pool().get(string).offset();
                            }
                            return string_pool_offset;
                        }
                );
            }
        });
        output->add_column(field, truncated_column);
    }
    output->attach_descriptor(descriptor_);
    return output;
}

/// @brief Combine 2 segments that hold different columns associated with the same rows
/// @param[in] unique_column_names If true, any columns from other with names matching those in this are ignored
void SegmentInMemoryImpl::concatenate(SegmentInMemoryImpl&& other, bool unique_column_names) {
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            row_count() == other.row_count(),
            "Cannot concatenate segments with differing row counts: {} {}",
            row_count(),
            other.row_count()
    );
    for (const auto& field : folly::enumerate(other.fields())) {
        if (!unique_column_names || !column_index(field->name()).has_value()) {
            add_column(*field, other.column_ptr(static_cast<position_t>(field.index)));
        }
    }
}

position_t SegmentInMemoryImpl::add_column(FieldRef field, size_t num_rows, AllocationType presize) {
    util::check_arg(!field.name().empty(), "Empty name in field: {}", field);
    if (!column_map_)
        init_column_map();

    columns_.emplace_back(std::make_shared<Column>(field.type(), num_rows, presize, allow_sparse_));
    auto new_field_name = descriptor_->add_field(FieldRef{field.type(), field.name()});

    std::lock_guard<std::mutex> lock{*column_map_mutex_};
    column_map_->insert(new_field_name, descriptor_->field_count() - 1);
    return static_cast<position_t>(columns_.size() - 1);
}

position_t SegmentInMemoryImpl::add_column(const Field& field, size_t num_rows, AllocationType presize) {
    return add_column(FieldRef{field.type(), field.name()}, num_rows, presize);
}

position_t SegmentInMemoryImpl::add_column(FieldRef field_ref, const std::shared_ptr<Column>& column) {
    if (!column_map_)
        init_column_map();

    columns_.emplace_back(column);
    auto new_field_name = descriptor_->add_field(field_ref);

    std::lock_guard<std::mutex> lock{*column_map_mutex_};
    column_map_->insert(new_field_name, descriptor_->field_count() - 1);
    return static_cast<position_t>(columns_.size() - 1);
}

position_t SegmentInMemoryImpl::add_column(const Field& field, const std::shared_ptr<Column>& column) {
    return add_column(FieldRef{field.type(), field.name()}, column);
}

position_t SegmentInMemoryImpl::add_column(std::string_view name, const std::shared_ptr<Column>& column) {
    return add_column(FieldRef{column->type(), name}, column);
}

void SegmentInMemoryImpl::change_schema(StreamDescriptor descriptor) {
    //util::check(vector_is_unique(descriptor.fields()), "Non-unique fields in descriptor: {}", descriptor.fields());
    init_column_map();
    std::vector<std::shared_ptr<Column>> new_columns(descriptor.field_count());
    for (auto col = 0u; col < descriptor.field_count(); ++col) {
        auto col_name = descriptor.field(col).name();
        auto col_index = column_index(col_name);
        const auto& other_type = descriptor.field(col).type();
        if (col_index) {
            auto this_type = column_unchecked(static_cast<position_t>(*col_index)).type();
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    this_type == other_type,
                    "Could not convert type {} to type {} for column {}, this index {}, other index {}",
                    other_type,
                    this_type,
                    col_name,
                    *col_index,
                    col
            );
            new_columns[col] = std::move(columns_[*col_index]);
        } else {
            auto new_column = std::make_shared<Column>(other_type, row_count(), AllocationType::DYNAMIC, allow_sparse_);
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

    if (is_fixed_string_type(td.data_type()) && col_ref.is_inflated()) {

        auto string_size = col_ref.bytes() / row_count();
        auto ptr = col_ref.data().buffer().ptr_cast<char>(row * string_size, string_size);
        return std::string_view(ptr, string_size);
    } else {
        const auto offset = col_ref.scalar_at<entity::position_t>(row);
        if (offset != std::nullopt && *offset != not_a_string() && *offset != nan_placeholder()) {
            return string_pool_->get_view(*offset);
        } else {
            return std::nullopt;
        }
    }
}

std::vector<std::shared_ptr<SegmentInMemoryImpl>> SegmentInMemoryImpl::split(size_t rows, bool filter_down_stringpool)
        const {
    std::vector<std::shared_ptr<SegmentInMemoryImpl>> output;
    util::check(rows > 0, "rows supplied in SegmentInMemoryImpl.split() is non positive");
    auto total_rows = row_count();
    util::BitSetSizeType start = 0;
    for (; start < total_rows; start += rows) {
        util::BitSet bitset(total_rows);
        util::BitSetSizeType end = std::min(start + rows, total_rows);
        // set_range is close interval on [left, right]
        bitset.set_range(start, end - 1, true);
        auto output_segment = filter(std::move(bitset), filter_down_stringpool);
        output_segment->set_offset(start);
        output.emplace_back(std::move(output_segment));
    }
    return output;
}

size_t SegmentInMemoryImpl::num_blocks() const {
    return std::accumulate(std::begin(columns_), std::end(columns_), 0, [](size_t n, const auto& col) {
        return n + col->num_blocks();
    });
}

std::optional<Column::StringArrayData> SegmentInMemoryImpl::string_array_at(position_t row, position_t col) {
    util::check_arg(size_t(row) < row_count(), "Segment index {} out of bounds in string array", row);
    return column(col).string_array_at(row, *string_pool_);
}

size_t SegmentInMemoryImpl::num_bytes() const {
    return std::accumulate(std::begin(columns_), std::end(columns_), 0, [](size_t n, const auto& col) {
        return n + col->bytes();
    });
}

void SegmentInMemoryImpl::sort(const std::string& column_name) {
    init_column_map();
    auto idx = column_index(std::string_view(column_name));
    schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(static_cast<bool>(idx), "Column {} not found in sort", column_name);
    sort(static_cast<position_t>(idx.value()));
}

void SegmentInMemoryImpl::sort(const std::vector<std::string>& column_names) {
    init_column_map();
    std::vector<position_t> positions;
    for (const auto& column_name : column_names) {
        auto idx = column_index(std::string_view(column_name));
        schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
                static_cast<bool>(idx), "Column {} not found in multi-sort", column_name
        );
        positions.emplace_back(static_cast<position_t>(*idx));
    }
    sort(positions);
}

void SegmentInMemoryImpl::sort(const std::vector<position_t>& positions) {
    std::vector<std::shared_ptr<Column>> columns;
    columns.reserve(positions.size());
    for (auto position : positions)
        columns.emplace_back(columns_[position]);

    auto table = create_jive_table(columns);
    auto pre_allocated_space = std::vector<uint32_t>(row_count());
    for (auto field_col = 0u; field_col < descriptor().field_count(); ++field_col) {
        column(field_col).sort_external(table, pre_allocated_space);
    }
}

void SegmentInMemoryImpl::sort(position_t idx) {
    auto& sort_col = column_unchecked(idx);
    util::check(
            !sort_col.is_sparse(),
            "Can't sort on sparse column idx {} because it is not supported yet. The user should either fill the "
            "column data or filter the empty columns out",
            idx
    );
    auto table = details::visit_scalar(sort_col.type(), [&sort_col](auto tdt) {
        return create_jive_table<decltype(tdt)>(sort_col);
    });

    auto pre_allocated_space = std::vector<uint32_t>(sort_col.row_count());
    for (auto field_col = 0u; field_col < descriptor().field_count(); ++field_col) {
        column(field_col).sort_external(table, pre_allocated_space);
    }
}

void SegmentInMemoryImpl::set_timeseries_descriptor(const TimeseriesDescriptor& tsd) { tsd_ = tsd; }

void SegmentInMemoryImpl::reset_timeseries_descriptor() { tsd_.reset(); }

void SegmentInMemoryImpl::calculate_statistics() {
    for (auto& column : columns_) {
        if (column->type().dimension() == Dimension::Dim0) {
            const auto type = column->type();
            if (is_numeric_type(type.data_type()) || is_sequence_type(type.data_type())) {
                details::visit_scalar(type, [&column](auto tdt) {
                    using TagType = std::decay_t<decltype(tdt)>;
                    column->set_statistics(generate_column_statistics<TagType>(column->data()));
                });
            }
        }
    }
}

void SegmentInMemoryImpl::reset_metadata() { metadata_.reset(); }

void SegmentInMemoryImpl::set_metadata(google::protobuf::Any&& meta) {
    util::check_arg(!metadata_, "Cannot override previously set metadata");
    if (meta.ByteSizeLong())
        metadata_ = std::make_unique<google::protobuf::Any>(std::move(meta));
}

bool SegmentInMemoryImpl::has_metadata() const { return static_cast<bool>(metadata_); }

const google::protobuf::Any* SegmentInMemoryImpl::metadata() const { return metadata_.get(); }

void SegmentInMemoryImpl::drop_empty_columns() {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            row_count() > 0, "Dropping all empty columns from an empty segment would result in removing all columns"
    );
    const StreamDescriptor& original = descriptor();
    StreamDescriptor only_non_empty_cols;
    only_non_empty_cols.set_id(original.id());
    only_non_empty_cols.set_index(descriptor().index());
    size_t field_index = 0;
    while (field_index < original.index().field_count()) {
        only_non_empty_cols.add_field(original.field(field_index++));
    }
    while (field_index < original.field_count()) {
        const Column& col = column(field_index);
        if (col.row_count() > 0) {
            only_non_empty_cols.add_field(original.field(field_index));
        }
        field_index++;
    }
    change_schema(std::move(only_non_empty_cols));
}

} // namespace arcticdb
