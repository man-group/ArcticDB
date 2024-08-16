/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/sparse_utils.hpp>
#include <pybind11/pybind11.h>

namespace arcticdb::util {

util::BitSet scan_object_type_to_sparse(const PyObject* const* ptr,
                                        size_t rows_to_write) {
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

} // namespace arcticdb::util
