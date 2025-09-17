/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column.hpp>

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>

namespace arcticdb {

// N.B. this will not catch all the things that C++ considers to be narrowing conversions, because
// it doesn't take into account integral promotion, however we don't care about that for the
// purpose for which it is used in this file.
template<typename SourceType, typename TargetType>
constexpr bool is_narrowing_conversion() {
    if (sizeof(TargetType) < sizeof(SourceType))
        return true;

    if (sizeof(SourceType) == sizeof(TargetType) && std::is_integral_v<TargetType> && std::is_unsigned_v<SourceType> &&
        std::is_signed_v<TargetType>) {
        return true;
    }

    return false;
}

JiveTable create_jive_table(const std::vector<std::shared_ptr<Column>>& columns) {
    JiveTable output(columns[0]->row_count());
    std::iota(std::begin(output.orig_pos_), std::end(output.orig_pos_), 0);

    // Calls to scalar_at are expensive, so we precompute them to speed up the sort compare function.
    for (auto it = std::rbegin(columns); it != std::rend(columns); ++it) {
        auto& column = *it;
        user_input::check<ErrorCode::E_SORT_ON_SPARSE>(
                !column->is_sparse(), "Can't sort on sparse column with type {}", column->type()
        );
        details::visit_type(column->type().data_type(), [&output, &column](auto type_desc_tag) {
            using type_info = ScalarTypeInfo<decltype(type_desc_tag)>;
            auto column_data = column->data();
            auto accessor = random_accessor<typename type_info::TDT>(&column_data);
            std::stable_sort(
                    std::begin(output.orig_pos_),
                    std::end(output.orig_pos_),
                    [&](const auto& a, const auto& b) -> bool { return accessor.at(a) < accessor.at(b); }
            );
        });
        // Obtain the sorted_pos_ by reversing the orig_pos_ permutation
        for (auto i = 0u; i < output.orig_pos_.size(); ++i) {
            output.sorted_pos_[output.orig_pos_[i]] = i;
        }
    }

    return output;
}

bool operator==(const ExtraBufferIndex& lhs, const ExtraBufferIndex& rhs) {
    return (lhs.offset_bytes_ == rhs.offset_bytes_) && (lhs.type_ == rhs.type_);
}

std::size_t ExtraBufferIndexHash::operator()(const ExtraBufferIndex& index) const {
    return folly::hash::hash_combine(index.offset_bytes_, index.type_);
}

ChunkedBuffer& ExtraBufferContainer::create_buffer(
        size_t offset, ExtraBufferType type, size_t size, AllocationType allocation_type
) {
    std::lock_guard lock(mutex_);
    auto inserted = buffers_.try_emplace(ExtraBufferIndex{offset, type}, ChunkedBuffer{size, allocation_type});
    util::check(inserted.second, "Failed to insert additional chunked buffer at position {}", offset);
    return inserted.first->second;
}

void ExtraBufferContainer::set_buffer(size_t offset, ExtraBufferType type, ChunkedBuffer&& buffer) {
    std::lock_guard lock(mutex_);
    buffers_.try_emplace(ExtraBufferIndex{offset, type}, std::move(buffer));
}

ChunkedBuffer& ExtraBufferContainer::get_buffer(size_t offset, ExtraBufferType type) const {
    std::lock_guard lock(mutex_);
    auto it = buffers_.find(ExtraBufferIndex{offset, type});
    util::check(it != buffers_.end(), "Failed to find additional chunked buffer at position {}", offset);
    return const_cast<ChunkedBuffer&>(it->second);
}

bool ExtraBufferContainer::has_buffer(size_t offset, ExtraBufferType type) const {
    std::lock_guard lock(mutex_);
    auto it = buffers_.find(ExtraBufferIndex{offset, type});
    return it != buffers_.end();
}

void initialise_output_column(const Column& input_column, Column& output_column) {
    if (&input_column != &output_column) {
        size_t output_physical_rows;
        if (input_column.is_sparse()) {
            auto output_sparse_map = input_column.sparse_map();
            output_physical_rows = output_sparse_map.count();
            output_column.set_sparse_map(std::move(output_sparse_map));
        } else {
            output_physical_rows = input_column.row_count();
        }
        output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
        output_column.set_row_data(input_column.last_row());
    }
}

void initialise_output_column(
        const Column& left_input_column, const Column& right_input_column, Column& output_column
) {
    if (&left_input_column != &output_column && &right_input_column != &output_column) {
        size_t output_physical_rows;
        std::optional<size_t> output_last_row;
        if (!left_input_column.is_sparse() && !right_input_column.is_sparse()) {
            // Both dense. Could be different lengths if the data is semantically sparse, but happens to be dense in the
            // first n rows
            output_physical_rows = std::min(left_input_column.row_count(), right_input_column.row_count());
            output_last_row = std::min(left_input_column.last_row(), right_input_column.last_row());
        } else {
            // At least one sparse
            util::BitSet output_sparse_map;
            if (left_input_column.is_sparse() && right_input_column.is_sparse()) {
                output_sparse_map = (left_input_column.sparse_map() & right_input_column.sparse_map());
            } else if (left_input_column.is_sparse() && !right_input_column.is_sparse()) {
                output_sparse_map = left_input_column.sparse_map();
                // If the sparse column had more logical rows than the dense column, truncate the sparse map to the
                // length of the dense column
                if (left_input_column.last_row() > right_input_column.last_row()) {
                    output_sparse_map.resize(right_input_column.row_count());
                }
            } else if (!left_input_column.is_sparse() && right_input_column.is_sparse()) {
                output_sparse_map = right_input_column.sparse_map();
                // If the sparse column had more logical rows than the dense column, truncate the sparse map to the
                // length of the dense column
                if (left_input_column.last_row() < right_input_column.last_row()) {
                    output_sparse_map.resize(left_input_column.row_count());
                }
            }
            output_physical_rows = output_sparse_map.count();
            // Find the index of the last set bit (if any) for the output_last_row
            util::BitSetSizeType last_set_bit;
            if (output_sparse_map.find_reverse(last_set_bit)) {
                output_last_row = static_cast<size_t>(last_set_bit);
            }
            output_column.set_sparse_map(std::move(output_sparse_map));
        }
        if (output_physical_rows > 0) {
            output_column.allocate_data(output_physical_rows * get_type_size(output_column.type().data_type()));
        }
        if (output_last_row.has_value()) {
            output_column.set_row_data(*output_last_row);
        }
    }
}

void initialise_output_bitset(
        const Column& input_column, bool sparse_missing_value_output, util::BitSet& output_bitset
) {
    if (sparse_missing_value_output) {
        output_bitset = input_column.sparse_map();
        output_bitset.flip();
    }
    output_bitset.resize(input_column.last_row() + 1);
}

// Column operators
bool operator==(const Column& left, const Column& right) {
    if (left.row_count() != right.row_count())
        return false;

    if (left.type_ != right.type_)
        return false;

    return left.type_.visit_tag([&left, &right](auto l_impl) {
        using LeftType = std::decay_t<decltype(l_impl)>;
        using LeftRawType = typename LeftType::DataTypeTag::raw_type;

        return right.type_.visit_tag([&left, &right](auto r_impl) {
            using RightType = std::decay_t<decltype(r_impl)>;
            using RightRawType = typename RightType::DataTypeTag::raw_type;

            if constexpr (std::is_same_v<LeftRawType, RightRawType>) {
                for (auto i = 0u; i < left.row_count(); ++i) {
                    auto left_val = left.scalar_at<LeftRawType>(i);
                    auto right_val = right.scalar_at<RightRawType>(i);
                    if (left_val != right_val)
                        return false;
                }
                return true;
            } else {
                return false;
            }
        });
    });
}

bool operator!=(const Column& left, const Column& right) { return !(left == right); }

Column::Column() : type_(null_type_descriptor()) {}

Column::Column(TypeDescriptor type) : Column(type, 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED) {}

Column::Column(TypeDescriptor type, Sparsity allow_sparse) : Column(type, 0, AllocationType::DYNAMIC, allow_sparse) {}

Column::Column(TypeDescriptor type, Sparsity allow_sparse, ChunkedBuffer&& buffer) :
    data_(std::move(buffer)),
    type_(type),
    allow_sparse_(allow_sparse) {}

Column::Column(TypeDescriptor type, Sparsity allow_sparse, ChunkedBuffer&& buffer, Buffer&& shapes) :
    data_(std::move(buffer)),
    shapes_(std::move(shapes)),
    type_(type),
    allow_sparse_(allow_sparse) {}

Column::Column(TypeDescriptor type, size_t expected_rows, AllocationType presize, Sparsity allow_sparse) :
    data_(expected_rows * entity::internal_data_type_size(type), presize),
    type_(type),
    allow_sparse_(allow_sparse) {
    ARCTICDB_TRACE(log::inmem(), "Creating column with descriptor {}", type);
}

Column::Column(
        TypeDescriptor type, size_t expected_rows, AllocationType presize, Sparsity allow_sparse,
        OutputFormat output_format, DataTypeMode mode
) :
    data_(expected_rows * entity::data_type_size(type, output_format, mode), presize),
    type_(type),
    allow_sparse_(allow_sparse) {
    ARCTICDB_TRACE(log::inmem(), "Creating column with descriptor {}", type);
}

void Column::set_statistics(FieldStatsImpl stats) { stats_ = stats; }

bool Column::has_statistics() const { return stats_.set_; };

FieldStatsImpl Column::get_statistics() const { return stats_; }

void Column::backfill_sparse_map(ssize_t to_row) {
    ARCTICDB_TRACE(log::version(), "Backfilling sparse map to position {}", to_row);
    // Initialise the optional to an empty bitset if it has not been created yet
    auto& bitset = sparse_map();
    if (to_row >= 0) {
        bitset.set_range(0, bv_size(to_row), true);
    }
}

void Column::set_sparse_block(ChunkedBuffer&& buffer, util::BitSet&& bitset) {
    data_.buffer() = std::move(buffer);
    sparse_map_ = std::move(bitset);
}

void Column::set_sparse_block(ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
    data_.buffer() = std::move(buffer);
    shapes_.buffer() = std::move(shapes);
    sparse_map_ = std::move(bitset);
}

ChunkedBuffer&& Column::release_buffer() { return std::move(data_.buffer()); }

Buffer&& Column::release_shapes() { return std::move(shapes_.buffer()); }

std::optional<Column::StringArrayData> Column::string_array_at(position_t idx, const StringPool& string_pool) {
    util::check_arg(idx < row_count(), "String array index out of bounds in column");
    util::check_arg(type_.dimension() == Dimension::Dim1, "String array should always be one dimensional");
    if (!inflated_)
        inflate_string_arrays(string_pool);

    const shape_t* shape_ptr = shape_index(idx);
    auto num_strings = *shape_ptr;
    ssize_t string_size = offsets_[idx] / num_strings;
    return StringArrayData{
            num_strings, string_size, data_.ptr_cast<char>(bytes_offset(idx), num_strings * string_size)
    };
}

ChunkedBuffer::Iterator Column::get_iterator() const {
    return {const_cast<ChunkedBuffer*>(&data_.buffer()), get_type_size(type_.data_type())};
}

size_t Column::bytes() const { return data_.bytes(); }

ColumnData Column::data() const {
    return ColumnData(&data_.buffer(), &shapes_.buffer(), type_, sparse_map_ ? &*sparse_map_ : nullptr);
}

const uint8_t* Column::ptr() const { return data_.buffer().data(); }

uint8_t* Column::ptr() { return data_.buffer().data(); }

TypeDescriptor Column::type() const { return type_; }

size_t Column::num_blocks() const { return data_.buffer().num_blocks(); }

const shape_t* Column::shape_ptr() const { return shapes_.ptr_cast<shape_t>(0, num_shapes()); }

void Column::set_orig_type(const TypeDescriptor& desc) { orig_type_ = desc; }

bool Column::has_orig_type() const { return static_cast<bool>(orig_type_); }

const TypeDescriptor& Column::orig_type() const { return orig_type_.value(); }

void Column::compact_blocks() { data_.compact_blocks(); }

shape_t* Column::allocate_shapes(std::size_t bytes) {
    shapes_.ensure_bytes(bytes);
    return reinterpret_cast<shape_t*>(shapes_.cursor());
}

uint8_t* Column::allocate_data(std::size_t bytes) {
    util::check(bytes != 0, "Allocate data called with zero size");
    data_.ensure_bytes(bytes);
    return data_.cursor();
}

void Column::advance_data(std::size_t size) { data_.advance(position_t(size)); }

void Column::advance_shapes(std::size_t size) { shapes_.advance(position_t(size)); }

[[nodiscard]] ChunkedBuffer& Column::buffer() { return data_.buffer(); }

uint8_t* Column::bytes_at(size_t bytes, size_t required) {
    ARCTICDB_TRACE(log::inmem(), "Column returning {} bytes at position {}", required, bytes);
    return data_.bytes_at(bytes, required);
}

const uint8_t* Column::bytes_at(size_t bytes, size_t required) const { return data_.bytes_at(bytes, required); }

void Column::assert_size(size_t bytes) const { data_.buffer().assert_size(bytes); }

void Column::init_buffer() {
    std::call_once(*init_buffer_, [this]() { extra_buffers_ = std::make_unique<ExtraBufferContainer>(); });
}

ChunkedBuffer& Column::create_extra_buffer(
        size_t offset, ExtraBufferType type, size_t size, AllocationType allocation_type
) {
    init_buffer();
    return extra_buffers_->create_buffer(offset, type, size, allocation_type);
}

ChunkedBuffer& Column::get_extra_buffer(size_t offset, ExtraBufferType type) const {
    util::check(static_cast<bool>(extra_buffers_), "Extra buffer {} requested but pointer is null", offset);
    return extra_buffers_->get_buffer(offset, type);
}

void Column::set_extra_buffer(size_t offset, ExtraBufferType type, ChunkedBuffer&& buffer) {
    init_buffer();
    extra_buffers_->set_buffer(offset, type, std::move(buffer));
}

bool Column::has_extra_buffer(size_t offset, ExtraBufferType type) const {
    if (!extra_buffers_)
        return false;

    return extra_buffers_->has_buffer(offset, type);
}

// Column public methods
Column Column::clone() const {
    Column output;

    output.data_ = data_.clone();
    output.shapes_ = shapes_.clone();

    output.offsets_ = offsets_;
    output.type_ = type_;
    output.orig_type_ = orig_type_;
    output.last_logical_row_ = last_logical_row_;
    output.last_physical_row_ = last_physical_row_;
    output.inflated_ = inflated_;
    output.allow_sparse_ = allow_sparse_;
    output.sparse_map_ = sparse_map_;

    return output;
}

bool Column::empty() const { return row_count() == 0; }

bool Column::is_sparse() const {
    if (last_logical_row_ != last_physical_row_) {
        util::check(
                static_cast<bool>(sparse_map_),
                "Expected sparse map in column with logical row {} and physical row {}",
                last_logical_row_,
                last_physical_row_
        );
        return true;
    }
    return false;
}

bool Column::sparse_permitted() const { return allow_sparse_ == Sparsity::PERMITTED; }

ssize_t Column::last_row() const { return last_logical_row_; }

void Column::check_magic() const { magic_.check(); }

void Column::unsparsify(size_t num_rows) {
    if (!sparse_map_)
        return;

    type_.visit_tag([this, num_rows](auto tdt) {
        using TagType = decltype(tdt);
        using RawType = typename TagType::DataTypeTag::raw_type;
        const auto dest_bytes = num_rows * sizeof(RawType);
        auto dest = ChunkedBuffer::presized(dest_bytes);
        util::default_initialize<TagType>(dest.data(), dest_bytes);
        util::expand_dense_buffer_using_bitmap<RawType>(sparse_map_.value(), data_.buffer().data(), dest.data());
        std::swap(dest, data_.buffer());
    });
    sparse_map_ = std::nullopt;
    last_logical_row_ = last_physical_row_ = static_cast<ssize_t>(num_rows) - 1;

    ARCTICDB_DEBUG(
            log::version(),
            "Unsparsify: last_logical_row_: {} last_physical_row_: {}",
            last_logical_row_,
            last_physical_row_
    );
}

void Column::sparsify() {
    type().visit_tag([this](auto type_desc_tag) {
        using RawType = typename decltype(type_desc_tag)::DataTypeTag::raw_type;
        if constexpr (is_floating_point_type(type_desc_tag.data_type())) {
            auto raw_ptr = reinterpret_cast<const RawType*>(ptr());
            auto buffer = util::scan_floating_point_to_sparse(raw_ptr, row_count(), sparse_map());
            std::swap(data().buffer(), buffer);
            last_physical_row_ = sparse_map().count() - 1;
        }
    });
}

void Column::string_array_prologue(ssize_t row_offset, size_t num_strings) {
    util::check_arg(
            last_logical_row_ + 1 == row_offset,
            "string_array_prologue expected row {}, actual {} ",
            last_logical_row_ + 1,
            row_offset
    );
    shapes_.ensure<shape_t>();
    auto shape_cursor = reinterpret_cast<shape_t*>(shapes_.cursor());
    *shape_cursor = shape_t(num_strings);
    data_.ensure<entity::position_t>(num_strings);
}

void Column::string_array_epilogue(size_t num_strings) {
    data_.commit();
    shapes_.commit();
    update_offsets(num_strings * sizeof(entity::position_t));
    ++last_logical_row_;
}

void Column::set_string_array(
        ssize_t row_offset, size_t string_size, size_t num_strings, char* input, StringPool& string_pool
) {
    string_array_prologue(row_offset, num_strings);
    auto data_ptr = reinterpret_cast<entity::position_t*>(data_.cursor());
    for (size_t i = 0; i < num_strings; ++i) {
        auto off = string_pool.get(std::string_view(input, string_size));
        *data_ptr++ = off.offset();
        input += string_size;
    }
    string_array_epilogue(num_strings);
}

void Column::set_string_list(ssize_t row_offset, const std::vector<std::string>& input, StringPool& string_pool) {
    string_array_prologue(row_offset, input.size());
    auto data_ptr = reinterpret_cast<entity::position_t*>(data_.cursor());
    for (const auto& str : input) {
        auto off = string_pool.get(str.data());
        *data_ptr++ = off.offset();
    }
    string_array_epilogue(input.size());
}

void Column::append_sparse_map(const util::BitMagic& bv, position_t at_row) {
    auto& sm = sparse_map();

    bm::bvector<>::enumerator en = bv.first();
    bm::bvector<>::enumerator en_end = bv.end();

    while (en < en_end) {
        auto bv_index = *en;
        sm[uint32_t(at_row) + bv_index] = true;
        ++en;
    }
}

void Column::append(const Column& other, position_t at_row) {
    if (other.row_count() == 0)
        return;
    util::check(type() == other.type(), "Cannot append column type {} to column type {}", type(), other.type());
    const bool was_sparse = is_sparse();
    const bool was_empty = empty();
    util::check(last_physical_row_ + 1 == row_count(), "Row count calculation incorrect before dense append");
    util::check(!is_sparse() || row_count() == sparse_map_.value().count(), "Row count does not match bitmap count");

    const auto& blocks = other.data_.buffer().blocks();
    const auto initial_row_count = row_count();
    for (const auto& block : blocks) {
        data_.ensure<uint8_t>(block->bytes());
        block->copy_to(data_.cursor());
        data_.commit();
    }

    last_logical_row_ = at_row + other.last_logical_row_;
    last_physical_row_ += other.last_physical_row_ + 1;

    ARCTICDB_DEBUG(
            log::version(),
            "at_row: {}\tother.last_logical_row_: {}\tother.last_physical_row_: {}\tother.row_count(): {}",
            at_row,
            other.last_logical_row_,
            other.last_physical_row_,
            other.row_count()
    );
    ARCTICDB_DEBUG(
            log::version(),
            "initial_row_count: {}\tlast_logical_row_: {}\tlast_physical_row_: {}\trow_count: {}",
            initial_row_count,
            last_logical_row_,
            last_physical_row_,
            row_count()
    );

    util::check(last_physical_row_ + 1 == row_count(), "Row count calculation incorrect after dense append");

    if (at_row == initial_row_count && !other.is_sparse() && !is_sparse()) {
        util::check(
                last_logical_row_ == last_physical_row_,
                "Expected logical and physical rows to line up in append of non-sparse columns"
        );
        return;
    }

    if (!was_sparse) {
        if (!was_empty)
            backfill_sparse_map(initial_row_count - 1);
        else
            sparse_map().clear();
    }

    if (other.is_sparse()) {
        ARCTICDB_DEBUG(log::version(), "Other column is sparse, appending sparsemap");
        append_sparse_map(other.sparse_map(), at_row);
    } else {
        ARCTICDB_DEBUG(
                log::version(), "Other column is dense, setting range from {} to {}", at_row, at_row + other.row_count()
        );
        sparse_map().set_range(uint32_t(at_row), uint32_t(at_row + other.last_logical_row_), true);
    }

    util::check(
            !is_sparse() || row_count() == sparse_map_.value().count(),
            "Row count incorrect exiting append",
            row_count(),
            sparse_map().count()
    );
}

void Column::physical_sort_external(std::vector<uint32_t>&& sorted_pos) {
    size_t physical_rows = row_count();
    auto& buffer = data_.buffer();

    util::check(
            sorted_pos.size() == physical_rows,
            "Mismatch between sorted_pos size and row_count: {} != {}",
            sorted_pos.size(),
            physical_rows
    );

    type().visit_tag([&buffer, &sorted_pos, &physical_rows](auto tdt) {
        using TagType = decltype(tdt);
        using RawType = typename TagType::DataTypeTag::raw_type;

        for (auto i = 0u; i < physical_rows; ++i) {
            if (i != sorted_pos[i]) {
                auto& current = buffer.cast<RawType>(i);
                // Amortized O(1) complexity, because each iteration places an element where it's supposed to go
                // and once an element is in it's sorted position we never move it.
                while (i != sorted_pos[i]) {
                    auto move_to = sorted_pos[i];
                    std::swap(sorted_pos[i], sorted_pos[move_to]);
                    std::swap(current, buffer.cast<RawType>(move_to));
                }
            }
        }
    });
}

void Column::sort_external(const JiveTable& jive_table, std::vector<uint32_t>& pre_allocated_space) {
    auto rows = row_count();
    if (!is_sparse()) {
        auto sorted_pos = jive_table.sorted_pos_;
        physical_sort_external(std::move(sorted_pos));
    } else {
        const auto& sm = sparse_map();
        bm::bvector<>::enumerator en = sm.first();
        util::BitMagic new_map;
        // The additional allocation is of the same size as the jive table
        // and is needed for a significant speed improvement.
        // We could instead use a std::map and sacrifice some speed for smaller allocations.
        util::check(
                pre_allocated_space.size() == jive_table.sorted_pos_.size(),
                "Mismatch between provided pre_allocated_space size and jive table size: {} != {}",
                pre_allocated_space.size(),
                jive_table.sorted_pos_.size()
        );
        auto& sorted_logical_to_physical = pre_allocated_space;
        for (auto physical = 0u; physical < rows; ++physical, ++en) {
            auto logical = *en;
            auto sorted_logical = jive_table.sorted_pos_[logical];
            new_map.set(sorted_logical);
            sorted_logical_to_physical[sorted_logical] = physical;
        }

        util::check(
                new_map.count() == row_count(),
                "Mismatch between new bitmap size and row_count: {} != {}",
                new_map.count(),
                row_count()
        );

        auto physical_sort_pos = std::vector<uint32_t>(rows);
        en = new_map.first();
        for (auto sorted_physical = 0u; sorted_physical < rows; ++sorted_physical, ++en) {
            auto sorted_logical = *en;
            auto physical = sorted_logical_to_physical[sorted_logical];
            physical_sort_pos[physical] = sorted_physical;
        }

        std::swap(sparse_map_.value(), new_map);
        physical_sort_external(std::move(physical_sort_pos));
    }
}

void Column::mark_absent_rows(size_t num_rows) {
    if (sparse_permitted()) {
        if (!sparse_map_) {
            if (last_physical_row_ != -1)
                backfill_sparse_map(last_physical_row_);
            else
                (void)sparse_map();
        }
        last_logical_row_ += static_cast<ssize_t>(num_rows);
    } else {
        util::check(
                last_logical_row_ == last_physical_row_,
                "Expected logical and physical rows to be equal in non-sparse column"
        );
        default_initialize_rows(last_logical_row_ + 1, num_rows, true);
    }
}

void Column::default_initialize_rows(size_t start_pos, size_t num_rows, bool ensure_alloc) {
    default_initialize_rows(start_pos, num_rows, ensure_alloc, std::nullopt);
}

void Column::default_initialize_rows(
        size_t start_pos, size_t num_rows, bool ensure_alloc, const std::optional<Value>& default_value
) {
    if (num_rows > 0) {
        type_.visit_tag([&, this](auto tag) {
            using T = std::decay_t<decltype(tag)>;
            using RawType = typename T::DataTypeTag::raw_type;
            const auto bytes = (num_rows * sizeof(RawType));

            if (ensure_alloc) {
                data_.ensure<uint8_t>(bytes);
            }
            util::initialize<T>(data_.buffer(), start_pos * sizeof(RawType), bytes, default_value);
            if (ensure_alloc) {
                data_.commit();
            }

            last_logical_row_ += static_cast<ssize_t>(num_rows);
            last_physical_row_ += static_cast<ssize_t>(num_rows);
        });
    }
}

void Column::set_row_data(size_t row_id) {
    if (is_empty_type(type_.data_type())) {
        return;
    }
    last_logical_row_ = row_id;
    const auto last_stored_row = row_count() - 1;
    if (sparse_map_) {
        last_physical_row_ = static_cast<ssize_t>(sparse_map_->count()) - 1;
    } else if (last_logical_row_ != last_stored_row) {
        last_physical_row_ = last_stored_row;
        backfill_sparse_map(last_stored_row);
    } else {
        last_physical_row_ = last_logical_row_;
    }
    if (sparse_map_) {
        sparse_map_->resize(row_id + 1);
    }
    ARCTICDB_TRACE(
            log::version(),
            "Set row data: last_logical_row_: {}, last_physical_row_: {}",
            last_logical_row_,
            last_physical_row_
    );
}

size_t Column::get_physical_offset(size_t row) const {
    if (!is_sparse())
        return row;

    if (row == 0u)
        return 0u;

    // TODO: cache index
    auto rs = std::make_unique<bm::bvector<>::rs_index_type>();
    sparse_map().build_rs_index(rs.get());
    return sparse_map().count_to(bv_size(row - 1), *rs);
}

void Column::set_sparse_map(util::BitSet&& bitset) { sparse_map_ = std::move(bitset); }

std::optional<position_t> Column::get_physical_row(position_t row) const {
    if (row > last_logical_row_) {
        if (sparse_permitted())
            return std::nullopt;
        else
            util::raise_rte("Scalar index {} out of bounds in column of size {}", row, row_count());
    }

    util::check_arg(is_scalar(), "get_scalar requested on non-scalar column");
    if (is_sparse() && !sparse_map().get_bit(bv_size(row)))
        return std::nullopt;

    return get_physical_offset(row);
}

bool Column::has_value_at(position_t row) const { return !is_sparse() || sparse_map().get_bit(bv_size(row)); }

void Column::set_allow_sparse(Sparsity value) { allow_sparse_ = value; }

void Column::set_shapes_buffer(size_t row_count) {
    CursoredBuffer<Buffer> shapes;
    shapes.ensure<shape_t>();
    shape_t rc(row_count);
    memcpy(shapes.cursor(), &rc, sizeof(shape_t));
    shapes.commit();
    swap(shapes_, shapes);
}

// The following two methods inflate (reduplicate) numpy string arrays that are potentially multi-dimensional,
// i.e where the value is not a string but an array of strings
void Column::inflate_string_array(
        const TensorType<position_t>& string_refs, CursoredBuffer<ChunkedBuffer>& data, CursoredBuffer<Buffer>& shapes,
        boost::container::small_vector<position_t, 1>& offsets, const StringPool& string_pool
) {
    ssize_t max_size = 0;
    for (int i = 0; i < string_refs.size(); ++i)
        max_size = std::max(max_size, static_cast<ssize_t>(string_pool.get_const_view(string_refs.at(i)).size()));

    size_t data_size = static_cast<size_t>(max_size) * string_refs.size();
    data.ensure<uint8_t>(data_size);
    shapes.ensure<shape_t>();
    auto str_data = data.cursor();
    memset(str_data, 0, data_size);
    for (int i = 0; i < string_refs.size(); ++i) {
        auto str = string_pool.get_const_view(string_refs.at(i));
        memcpy(&str_data[i * max_size], str.data(), str.size());
    }

    offsets.push_back(data_size);
    shape_t s = string_refs.size();
    memcpy(shapes.cursor(), &s, sizeof(shape_t));
    data.commit();
    shapes.commit();
}

void Column::inflate_string_arrays(const StringPool& string_pool) {
    util::check_arg(is_fixed_string_type(type().data_type()), "Can only inflate fixed string array types");
    util::check_arg(type().dimension() == Dimension::Dim1, "Fixed string inflation is for array types only");

    CursoredBuffer<ChunkedBuffer> data;
    CursoredBuffer<Buffer> shapes;
    boost::container::small_vector<position_t, 1> offsets;
    for (position_t row = 0; row < row_count(); ++row) {
        auto string_refs = tensor_at<position_t>(row).value();
        inflate_string_array(string_refs, data, shapes, offsets, string_pool);
    }

    using std::swap;
    swap(shapes_, shapes);
    swap(offsets_, offsets);
    swap(data_, data);
    inflated_ = true;
}

// Used when the column has been inflated externally, i.e. because it has be done
// in a pipeline of tiled sub-segments
void Column::set_inflated(size_t inflated_count) {
    set_shapes_buffer(inflated_count);
    inflated_ = true;
}

bool Column::is_inflated() const { return inflated_; }

void Column::change_type(DataType target_type) {
    util::check(shapes_.empty(), "Can't change type on multi-dimensional column with type {}", type_);
    if (type_.data_type() == target_type)
        return;

    CursoredBuffer<ChunkedBuffer> buf;
    for (const auto& block : data_.buffer().blocks()) {
        details::visit_type(type_.data_type(), [&buf, &block, type = type_, target_type](auto&& source_dtt) {
            using source_raw_type = typename std::decay_t<decltype(source_dtt)>::raw_type;
            details::visit_type(target_type, [&buf, &block, &type, target_type](auto&& target_dtt) {
                using target_raw_type = typename std::decay_t<decltype(target_dtt)>::raw_type;

                if constexpr (!is_narrowing_conversion<source_raw_type, target_raw_type>() &&
                              !std::is_same_v<source_raw_type, bool>) {
                    auto num_values = block->bytes() / sizeof(source_raw_type);
                    buf.ensure<target_raw_type>(num_values);
                    auto src = reinterpret_cast<const source_raw_type*>(block->data());
                    auto dest = reinterpret_cast<target_raw_type*>(buf.cursor());
                    for (auto i = 0u; i < num_values; ++i)
                        dest[i] = target_raw_type(src[i]);
                } else {
                    util::raise_rte("Cannot narrow column type from {} to {}", type, target_type);
                }
            });
        });
    }
    buf.commit();
    type_ = TypeDescriptor{target_type, type_.dimension()};
    std::swap(data_, buf);
}

position_t Column::row_count() const {
    if (!is_scalar()) {
        // TODO check with strings as well
        return num_shapes() / shape_t(type_.dimension());
    }

    if (is_sequence_type(type().data_type()) && inflated_ && is_fixed_string_type(type().data_type()))
        return inflated_row_count();

    return data_.bytes() / size_t(item_size());
}

std::vector<std::shared_ptr<Column>> Column::split(const std::shared_ptr<Column>& column, size_t rows) {
    // TODO: Doesn't work the way you would expect for sparse columns - the bytes for each buffer won't be uniform
    const auto bytes = rows * get_type_size(column->type().data_type());
    auto new_buffers = ::arcticdb::split(column->data_.buffer(), bytes);
    util::check(
            bytes % get_type_size(column->type().data_type()) == 0,
            "Bytes {} is not a multiple of type size {}",
            bytes,
            column->type()
    );
    std::vector<std::shared_ptr<Column>> output;
    output.reserve(new_buffers.size());

    auto row = 0;
    for (auto& buffer : new_buffers) {
        output.push_back(std::make_shared<Column>(column->type(), column->allow_sparse_, std::move(buffer)));

        if (column->is_sparse()) {
            util::BitSet bit_subset;
            auto new_col = output.rbegin();
            const auto row_count = (*new_col)->row_count();
            bit_subset.copy_range(column->sparse_map(), row, uint32_t(row_count));
            (*new_col)->set_sparse_map(std::move(bit_subset));
            row += row_count;
        }
    }
    return output;
}

void Column::truncate_first_block(size_t start_row) {
    if (!is_sparse()) {
        auto bytes = start_row * data_type_size(type_, OutputFormat::NATIVE, DataTypeMode::INTERNAL);
        data_.buffer().truncate_first_block(bytes);
    }
}

void Column::truncate_last_block(size_t end_row) {
    if (!is_sparse()) {
        const auto column_row_count = row_count();
        util::check(
                column_row_count >= static_cast<int64_t>(end_row),
                "Cannot truncate column of length {} to row {}",
                column_row_count,
                end_row
        );
        auto bytes = (column_row_count - end_row) * data_type_size(type_, OutputFormat::NATIVE, DataTypeMode::INTERNAL);
        data_.buffer().truncate_last_block(bytes);
    }
}

void Column::truncate_single_block(size_t start_row, size_t end_row) {
    if (!is_sparse()) { // TODO: We need to also do truncation on sparse data
        const auto type_size = data_type_size(type_, OutputFormat::NATIVE, DataTypeMode::INTERNAL);
        auto start_offset = type_size * start_row;
        auto end_offset = type_size * end_row;
        data_.buffer().truncate_single_block(start_offset, end_offset);
    }
}

/// Bytes from the underlying chunked buffer to include when truncating. Inclusive of start_byte, exclusive of end_byte
[[nodiscard]] static std::pair<size_t, size_t> column_start_end_bytes(
        const Column& column, size_t start_row, size_t end_row
) {
    const size_t type_size = get_type_size(column.type().data_type());
    size_t start_byte = start_row * type_size;
    size_t end_byte = end_row * type_size;
    if (column.is_sparse()) {
        const util::BitMagic& input_sparse_map = column.sparse_map();
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                input_sparse_map.size() > 0, "Unexpected empty sparse map in Column::truncate"
        );
        // Sparse columns do not include trailing 0s in the bitset, so the relevant end_row is capped at the size of the
        // biset
        end_row = std::min(end_row, static_cast<size_t>(input_sparse_map.size()));
        // count_range is inclusive at both ends
        const size_t set_bits_before_range = start_row > 0 ? input_sparse_map.count_range(0, start_row - 1) : 0;
        const size_t set_bits_in_range = input_sparse_map.count_range(start_row, end_row - 1);
        start_byte = set_bits_before_range * type_size;
        end_byte = start_byte + (set_bits_in_range * type_size);
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            start_byte % type_size == 0, "start_byte {} is not a multiple of type size {}", start_byte, column.type()
    );
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            end_byte % type_size == 0, "start_byte {} is not a multiple of type size {}", end_byte, column.type()
    );
    return {start_byte, end_byte};
}

std::shared_ptr<Column> Column::truncate(const std::shared_ptr<Column>& column, size_t start_row, size_t end_row) {
    const auto [start_byte, end_byte] = column_start_end_bytes(*column, start_row, end_row);
    auto buffer = ::arcticdb::truncate(column->data_.buffer(), start_byte, end_byte);
    auto res = std::make_shared<Column>(column->type(), column->allow_sparse_, std::move(buffer));
    if (column->is_sparse()) {
        res->set_sparse_map(util::truncate_sparse_map(column->sparse_map(), start_row, end_row));
    }
    res->set_row_data(end_row - (start_row + 1));
    return res;
}

void Column::set_empty_array(ssize_t row_offset, int dimension_count) {
    ARCTICDB_SAMPLE(ColumnSetArray, RMTSF_Aggregate)
    magic_.check();
    util::check_arg(
            last_logical_row_ + 1 == row_offset,
            "set_array expected row {}, actual {} ",
            last_logical_row_ + 1,
            row_offset
    );
    shapes_.ensure<shape_t>(dimension_count);
    memset(shapes_.cursor(), 0, dimension_count * sizeof(shape_t));
    shapes_.commit();
    ++last_logical_row_;
}

void Column::set_type(TypeDescriptor td) { type_ = td; }

// Column private methods
position_t Column::last_offset() const { return offsets_.empty() ? 0 : *offsets_.rbegin(); }

void Column::update_offsets(size_t nbytes) { offsets_.push_back(last_offset() + nbytes); }

bool Column::is_scalar() const { return type().dimension() == Dimension(0); }

const shape_t* Column::shape_index(position_t idx) const {
    if (is_scalar())
        return nullptr;

    return shapes_.buffer().ptr_cast<shape_t>(idx * size_t(type_.dimension()) * sizeof(shape_t), sizeof(shape_t));
}

position_t Column::bytes_offset(position_t idx) const {
    regenerate_offsets();
    util::check_arg(idx < row_count(), "bytes_offset index {} out of bounds in column of size {}", idx, row_count());

    if (idx == 0)
        return 0;

    if (is_scalar())
        return scalar_offset(idx);

    util::check(size_t(idx - 1) < offsets_.size(), "Offset {} out of range, only have {}", idx - 1, offsets_.size());
    return offsets_[idx - 1];
}

position_t Column::scalar_offset(position_t idx) const { return idx * item_size(); }

size_t Column::item_size() const {
    if (is_sequence_type(type().data_type()) && inflated_ && is_fixed_string_type(type().data_type())) {
        return data_.bytes() / inflated_row_count();
    }

    return get_type_size(type().data_type());
}

size_t Column::inflated_row_count() const {
    // using the content of shapes since the item size
    // from the datatype is no longer reliable
    return *reinterpret_cast<const size_t*>(shapes_.data());
}

size_t Column::num_shapes() const { return shapes_.bytes() / sizeof(shape_t); }

void Column::set_sparse_bit_for_row(size_t sparse_location) { sparse_map()[bv_size(sparse_location)] = true; }

void Column::regenerate_offsets() const {
    if (ARCTICDB_LIKELY(is_scalar() || !offsets_.empty()))
        return;

    position_t pos = 0;
    for (position_t i = 0, j = i + position_t(type_.dimension()); j < position_t(num_shapes());
         i = j, j += position_t(type_.dimension())) {
        auto num_elements =
                position_t(std::accumulate(shape_index(i), shape_index(j), shape_t(1), std::multiplies<>()));
        auto offset = num_elements * get_type_size(type_.data_type());
        offsets_.push_back(pos + offset);
        pos += offset;
    }
}

util::BitMagic& Column::sparse_map() {
    if (!sparse_map_)
        sparse_map_ = std::make_optional<util::BitMagic>(0);

    return sparse_map_.value();
}

const util::BitMagic& Column::sparse_map() const {
    util::check(static_cast<bool>(sparse_map_), "Expected sparse map when it was not set");
    return sparse_map_.value();
}

std::optional<util::BitMagic>& Column::opt_sparse_map() { return sparse_map_; }

std::optional<util::BitMagic> Column::opt_sparse_map() const { return sparse_map_; }

} // namespace arcticdb
