/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_python_adapters.hpp>

namespace arcticdb::python_util {

template<class T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
void column_set_array(Column& col, ssize_t row_offset, py::array_t<T>& val) {
    // Delegate to the generic template set_array method which accepts any Tensor-like type
    col.set_array(row_offset, val);
}

template<class T>
requires std::integral<T> || std::floating_point<T>
void segment_set_array(SegmentInMemory& seg, position_t pos, py::array_t<T>& val) {
    // Delegate to the generic template set_array method which accepts any Tensor-like type
    seg.set_array(pos, val);
}

// Explicit template instantiations for common types
template void column_set_array<int8_t>(Column&, ssize_t, py::array_t<int8_t>&);
template void column_set_array<int16_t>(Column&, ssize_t, py::array_t<int16_t>&);
template void column_set_array<int32_t>(Column&, ssize_t, py::array_t<int32_t>&);
template void column_set_array<int64_t>(Column&, ssize_t, py::array_t<int64_t>&);
template void column_set_array<uint8_t>(Column&, ssize_t, py::array_t<uint8_t>&);
template void column_set_array<uint16_t>(Column&, ssize_t, py::array_t<uint16_t>&);
template void column_set_array<uint32_t>(Column&, ssize_t, py::array_t<uint32_t>&);
template void column_set_array<uint64_t>(Column&, ssize_t, py::array_t<uint64_t>&);
template void column_set_array<float>(Column&, ssize_t, py::array_t<float>&);
template void column_set_array<double>(Column&, ssize_t, py::array_t<double>&);

template void segment_set_array<int8_t>(SegmentInMemory&, position_t, py::array_t<int8_t>&);
template void segment_set_array<int16_t>(SegmentInMemory&, position_t, py::array_t<int16_t>&);
template void segment_set_array<int32_t>(SegmentInMemory&, position_t, py::array_t<int32_t>&);
template void segment_set_array<int64_t>(SegmentInMemory&, position_t, py::array_t<int64_t>&);
template void segment_set_array<uint8_t>(SegmentInMemory&, position_t, py::array_t<uint8_t>&);
template void segment_set_array<uint16_t>(SegmentInMemory&, position_t, py::array_t<uint16_t>&);
template void segment_set_array<uint32_t>(SegmentInMemory&, position_t, py::array_t<uint32_t>&);
template void segment_set_array<uint64_t>(SegmentInMemory&, position_t, py::array_t<uint64_t>&);
template void segment_set_array<float>(SegmentInMemory&, position_t, py::array_t<float>&);
template void segment_set_array<double>(SegmentInMemory&, position_t, py::array_t<double>&);

} // namespace arcticdb::python_util
