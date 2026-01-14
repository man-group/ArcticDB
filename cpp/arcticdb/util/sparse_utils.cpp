/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/sparse_utils.hpp>
#include <pybind11/pytypes.h>

namespace arcticdb::util {

util::BitSet scan_object_type_to_sparse(const PyObject* const* ptr, size_t rows_to_write) {
    util::BitSet bitset;
    auto scan_ptr = ptr;
    pybind11::none none;
    util::BitSet::bulk_insert_iterator inserter(bitset);
    for (size_t idx = 0; idx < rows_to_write; ++idx, ++scan_ptr) {
        if (*scan_ptr != none.ptr())
            inserter = bv_size(idx);
    }
    inserter.flush();
    bitset.resize(bv_size(rows_to_write));
    return bitset;
}

util::BitMagic truncate_sparse_map(const util::BitMagic& input_sparse_map, size_t start_row, size_t end_row) {
    // The output sparse map is the slice [start_row, end_row) of the input sparse map
    // BitMagic doesn't have a method for this, so hand-roll it here
    // Ctor parameter is the size
    util::BitMagic output_sparse_map(end_row - start_row);
    util::BitSetSizeType set_input_bit;
    // find returns false if there are no set bits after (inclusive) start_row
    if (input_sparse_map.find(start_row, set_input_bit)) {
        util::BitSet::bulk_insert_iterator inserter(output_sparse_map);
        while (set_input_bit < end_row) {
            inserter = set_input_bit - start_row;
            set_input_bit = input_sparse_map.get_next(set_input_bit);
            // get_next returns 0 if there are no more set bits
            if (set_input_bit == 0) {
                break;
            }
        }
        inserter.flush();
    } // else no set bits after (inclusive) start_row. Ctor for output_sparse_map used initialises all bits to zero
    return output_sparse_map;

    // The above code is equivalent to the (easier to read) implementation:
    //    for (auto en = input_sparse_map.first(); en != input_sparse_map.end(); ++en) {
    //        if (*en < start_row) {
    //            continue;
    //        } else if (*en < end_row) {
    //            inserter = *en - start_row;
    //        } else {
    //            break;
    //        }
    //    }
    // but is more efficient in the case that the first set bit is deep into input_sparse_map
}

} // namespace arcticdb::util
