/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

// Deprecated - use SegmentInMemoryImp::Row
class RowRef {
  public:
    RowRef() = default;

    RowRef(size_t row_pos, SegmentInMemory segment) : row_pos_(row_pos), segment_(std::move(segment)) {}

    template<class S>
    std::optional<S> scalar_at(std::size_t col) const {
        std::optional<S> res;
        const auto& type_desc = segment_.column_descriptor(col);
        visit_field(type_desc, [&segment = segment_, row_pos = row_pos_, col = col, &res](auto impl) {
            using T = std::decay_t<decltype(impl)>;
            using RawType = typename T::DataTypeTag::raw_type;
            if constexpr (T::DimensionTag::value == Dimension::Dim0) {
                if constexpr (T::DataTypeTag::data_type == DataType::ASCII_DYNAMIC64 ||
                              T::DataTypeTag::data_type == DataType::ASCII_FIXED64) {
                    // test only for now
                    util::raise_rte("not implemented");
                } else {
                    res = segment.scalar_at<RawType>(row_pos, col);
                }
            } else {
                util::raise_rte("Scalar method called on multidimensional column");
            }
        });
        return res;
    }

    [[nodiscard]] size_t col_count() const { return segment_.num_columns(); }

    [[nodiscard]] size_t row_pos() const { return row_pos_; }

    SegmentInMemory& segment() { return segment_; }

    [[nodiscard]] std::optional<std::string_view> string_at(std::size_t col) const {
        return segment_.string_at(row_pos_, static_cast<ssize_t>(col));
    }

  private:
    size_t row_pos_ = 0u;
    SegmentInMemory segment_;
};

inline RowRef last_row(const SegmentInMemory& segment) {
    util::check(segment.row_count() > 0, "Can't do last row on an empty segment");
    return RowRef{segment.row_count(), segment};
}

} // namespace arcticdb