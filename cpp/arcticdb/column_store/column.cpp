/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/sparse_utils.hpp>

namespace arcticdb {
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
    for(const auto& block : blocks) {
        data_.ensure<uint8_t>(block->bytes());
        block->copy_to(data_.cursor());
        data_.commit();
    }

    last_logical_row_ = at_row + other.last_logical_row_;
    last_physical_row_ += other.last_physical_row_ + 1;

    ARCTICDB_DEBUG(log::version(), "at_row: {}\tother.last_logical_row_: {}\tother.last_physical_row_: {}\tother.row_count(): {}",
        at_row, other.last_logical_row_, other.last_physical_row_, other.row_count());
    ARCTICDB_DEBUG(log::version(), "initial_row_count: {}\tlast_logical_row_: {}\tlast_physical_row_: {}\trow_count: {}",
                        initial_row_count, last_logical_row_, last_physical_row_, row_count());

    util::check(last_physical_row_ + 1 == row_count(), "Row count calculation incorrect after dense append");

    if(at_row == initial_row_count && !other.is_sparse() && !is_sparse()) {
        util::check(last_logical_row_ == last_physical_row_, "Expected logical and physical rows to line up in append of non-sparse columns");
        return;
    }

    if(!was_sparse) {
        if(!was_empty)
            backfill_sparse_map(initial_row_count - 1);
        else
            sparse_map().clear();
    }

    if(other.is_sparse()) {
        ARCTICDB_DEBUG(log::version(), "Other column is sparse, appending sparsemap");
        append_sparse_map(other.sparse_map(), at_row);
    }
    else {
        ARCTICDB_DEBUG(log::version(), "Other column is dense, setting range from {} to {}", at_row, at_row + other.row_count());
        sparse_map().set_range(uint32_t(at_row), uint32_t(at_row + other.last_logical_row_), true);
    }

    util::check(!is_sparse() || row_count() == sparse_map_.value().count(), "Row count incorrect exiting append",
                row_count(), sparse_map().count());
}

void Column::sort_external(const JiveTable& jive_table) {
    auto rows = row_count();
    if(!is_sparse()) {
        auto unsorted = jive_table.unsorted_rows_;
        auto& buffer = data_.buffer();
        type().visit_tag([&jive_table, &buffer, &unsorted] (auto tdt) {
            using TagType = decltype(tdt);
            using RawType = typename TagType::DataTypeTag::raw_type;

            auto loc = unsorted.get_first();
            auto tmp =  buffer.cast<RawType>(jive_table.orig_pos_[loc]);
            for(auto i = 0u; i < jive_table.num_unsorted_; ++i) {
                std::swap(tmp, buffer.cast<RawType>(loc));
                unsorted.set(loc, false);
                const auto next_pos = jive_table.sorted_pos_[loc];
                if(unsorted[next_pos]) {
                    loc = next_pos;
                } else {
                   loc =  unsorted.get_first();
                   tmp = buffer.cast<RawType>(jive_table.orig_pos_[loc]);
                }
            }
            util::check(!unsorted.any(), "Did not sort all possible values, still have {} unsorted", unsorted.count());
        });
    } else {
        const auto& sm = sparse_map();

        bm::bvector<>::enumerator en = sm.first();
        bm::bvector<>::enumerator en_end = sm.end();
        util::BitMagic new_map;

        while (en < en_end) {
            auto bv_index = *en;
            new_map.set(jive_table.sorted_pos_[bv_index]);
            ++en;
        }

        util::check(new_map.count() == row_count(), "Mismatch between new bitmap size and row_count: {} != {}",
            new_map.count(), row_count());

        auto new_buf = ChunkedBuffer::presized_in_blocks(data_.bytes());
        en = new_map.first();
        auto& buffer = data_.buffer();
        auto rs= std::make_unique<util::BitIndex>();
        sm.build_rs_index(rs.get());

        type().visit_tag([&jive_table, &sm, &rs, &buffer, rows, &en, &new_buf] (auto tdt) {
            using TagType = decltype(tdt);
            using RawType = typename TagType::DataTypeTag::raw_type;

            for(auto i = 0u; i < rows; ++i) {
                const auto bv_index = *en;
                const auto logical_pos = jive_table.orig_pos_[bv_index];
                const auto physical_pos = sm.count_to(logical_pos, *rs) - 1;
                new_buf.template cast<RawType>(i) = buffer.cast<RawType>(physical_pos);
                ++en;
            }
        });
        std::swap(data_.buffer(), new_buf);
        std::swap(sparse_map_.value(), new_map);
    }
}

std::vector<std::shared_ptr<Column>> Column::split(const std::shared_ptr<Column>& column, size_t rows) {
    // TODO: Doesn't work the way you would expect for sparse columns - the bytes for each buffer won't be uniform
    const auto bytes = rows * get_type_size(column->type().data_type());
    auto new_buffers = ::arcticdb::split(column->data_.buffer(), bytes);
    util::check(bytes % get_type_size(column->type().data_type()) == 0, "Bytes {} is not a multiple of type size {}", bytes, column->type());
    std::vector<std::shared_ptr<Column>> output;
    output.reserve(new_buffers.size());

    auto row = 0;
    for(auto& buffer : new_buffers) {
        output.push_back(std::make_shared<Column>(column->type(), column->allow_sparse_, std::move(buffer)));

        if(column->is_sparse()) {
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

// Produces a new column
// Inclusive of start_row, exclusive of end_row
std::shared_ptr<Column> Column::truncate(const std::shared_ptr<Column>& column, size_t start_row, size_t end_row) {
    auto type_size = get_type_size(column->type().data_type());
    // Bytes from the underlying chunked buffer to include. Inclusive of start_byte, exclusive of end_byte
    size_t start_byte;
    size_t end_byte;
    util::BitMagic input_sparse_map;
    if (column->is_sparse()) {
        input_sparse_map = column->sparse_map();
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(input_sparse_map.size() > 0, "Unexpected empty sparse map in Column::truncate");
        // Sparse columns do not include trailing 0s in the bitset, so the relevant end_row is capped at the size of the biset
        end_row = std::min(end_row, static_cast<size_t>(input_sparse_map.size()));
        // count_range is inclusive at both ends
        auto set_bits_before_range = start_row > 0 ? input_sparse_map.count_range(0, start_row - 1) : 0;
        auto set_bits_in_range = input_sparse_map.count_range(start_row, end_row - 1);
        start_byte = set_bits_before_range * type_size;
        end_byte = start_byte + (set_bits_in_range * type_size);
    } else {
        start_byte = start_row * type_size;
        end_byte = end_row * type_size;
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            start_byte % type_size == 0,
            "start_byte {} is not a multiple of type size {}", start_byte, column->type());
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            end_byte % type_size == 0,
            "start_byte {} is not a multiple of type size {}", end_byte, column->type());
    auto buffer = ::arcticdb::truncate(column->data_.buffer(), start_byte, end_byte);
    auto res = std::make_shared<Column>(column->type(), column->allow_sparse_, std::move(buffer));
    if (column->is_sparse()) {
        // The output sparse map is the slice [start_row, end_row) of the input sparse map
        // BitMagic doesn't have a method for this, so hand-roll it here
        // Ctor parameter is the size
        util::BitMagic output_sparse_map(end_row - start_row);
        util::BitSet::bulk_insert_iterator inserter(output_sparse_map);
        util::BitSetSizeType set_input_bit;
        if (start_row == 0) {
            // get_first can return 0 if no bits are set, but we checked earlier that input_sparse_map.size() > 0,
            // and we do not have sparse maps with no bits set
            set_input_bit = input_sparse_map.get_first();
            if (set_input_bit < end_row) {
                inserter = set_input_bit;
            }
        } else {
            set_input_bit = input_sparse_map.get_next(start_row - 1);
            // get_next returns 0 if no more bits are set
            if (set_input_bit != 0 && set_input_bit < end_row) {
                // Shift start_row elements to the left
                inserter = set_input_bit - start_row;
            }
        }
        do {
            set_input_bit = input_sparse_map.get_next(set_input_bit);
            if (set_input_bit != 0 && set_input_bit < end_row) {
                inserter = set_input_bit - start_row;
            }
        } while (set_input_bit != 0);
        inserter.flush();
        res->set_sparse_map(std::move(output_sparse_map));
    }
    res->set_row_data(end_row - (start_row + 1));
    return res;
}

} //namespace arcticdb