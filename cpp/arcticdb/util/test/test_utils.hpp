/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <vector>
#include <numeric>
#include <random>

using namespace arcticdb;

template <typename DTT, Dimension DIM, NumericId def_tsid = 123,
          int def_field_count = 4>
StreamDescriptor create_tsd(StreamId tsid = def_tsid,
                            std::size_t field_count = def_field_count) {

  using TDT = TypeDescriptorTag<DTT, DimensionTag<DIM>>;

  auto tsd = stream_descriptor(tsid, stream::TimeseriesIndex::default_index(), {});
  for (std::size_t i = 0; i < field_count; ++i) {
    tsd.add_field(scalar_field(TDT::DataTypeTag::data_type, fmt::format("col_{}", i)));
  }
  return tsd;
}

template <typename TDT> struct TestValue {
  const Dimension dimensions = TDT::DimensionTag::value;
  using DataTypeTag = typename TDT::DataTypeTag;
  using raw_type = typename DataTypeTag::raw_type;

  std::vector<raw_type> data_;
  std::vector<shape_t> shapes_;
  mutable std::vector<stride_t> strides_;
  raw_type start_val_;

  TestValue(raw_type start_val = raw_type(), size_t num_vals = 20)
      : start_val_(start_val) {
    if (dimensions == Dimension::Dim0) {
      data_.push_back(start_val_);
      return;
    }

    constexpr int64_t itemsize = sizeof(raw_type);

    if (dimensions == Dimension::Dim1) {
      shapes_.push_back(num_vals);
      strides_.push_back(itemsize);
    } else {
      auto side = ssize_t(sqrt(num_vals));
      shapes_ = {side, side};
      strides_ = {side * itemsize, itemsize};
    }

    //        // Adjust strides to the correct size
    //        std::transform(std::begin(strides_),
    //                       std::end(strides_),
    //                       std::begin(strides_),
    //                       [&](auto x) { return x * itemsize; });

    // Fill data
    data_.resize(num_vals);
    fill_impl(dimensions, 0);
  }

  void fill_impl(Dimension dim, int pos) {
    auto shape = shapes_[size_t(dim) - 1];
    auto stride = strides_[size_t(dim) - 1] / sizeof(raw_type);

    for (size_t i = 0; i < +size_t(shape); ++i) {
      if (dim == Dimension(1)) {
        data_[pos + (i * stride)] = pos + i + start_val_;
      } else {
        fill_impl(Dimension(size_t(dim) - 1), i * stride);
      }
    }
  }

  raw_type get_scalar() const {
    util::check_arg(dimensions == Dimension::Dim0,
                    "get scalar called on non-scalar test value");
    return data_[0];
  }

  TensorType<raw_type> get_tensor() const {
    util::check_arg(dimensions != Dimension::Dim0,
                    "get tensor called on scalar test value");
    reconstruct_strides();
    return TensorType<raw_type>{
        shapes_.data(),         ssize_t(dimensions),
        DataTypeTag::data_type, get_type_size(DataTypeTag::data_type),
        data_.data(),           ssize_t(dimensions)};
  }

  bool check_tensor(TensorType<raw_type>& t) const {
    util::check_arg(dimensions != Dimension::Dim0,
                    "check tensor called on scalar test value");
    auto req = t.request();
    return check_impl(dimensions, 0, t.shape(), t.strides(),
                      reinterpret_cast<const raw_type*>(req.ptr));
  }

  bool check(const ssize_t* shapes, const ssize_t* strides,
             const raw_type* data) const {
    if (dimensions == Dimension::Dim0)
      return data_[0] == *data;

    return check_impl(dimensions, 0, shapes, strides, data);
  }

  bool check_impl(Dimension dim, int pos, const shape_t* shapes,
                  const stride_t* strides, const raw_type* data) const {
    auto shape = shapes_[size_t(dim) - 1];
    auto stride = strides_[size_t(dim) - 1] / sizeof(raw_type);
    for (int i = 0; i < +shape; ++i) {
      if (dim == Dimension(1)) {
        if (data[pos + (i * stride)] != pos + i + start_val_)
          return false;
      } else if (!check_impl(Dimension(size_t(dim) - 1), i * stride, shapes, strides,
                             data))
        return false;
    }
    return true;
  }

  void reconstruct_strides() const {
    if (strides_[0] == 0) {
      stride_t stride = sizeof(raw_type);

      for (auto i = stride_t(dimensions) - 1; i >= 0; --i) {
        strides_[i] = stride;
        stride *= shapes_[i];
      }
    }
  }
};

template <typename TDT> struct TestRow {
  using raw_type = typename TDT::DataTypeTag::raw_type;

  TestRow(timestamp ts, size_t num_columns, raw_type start_val = raw_type(),
          size_t num_vals = 20)
      : ts_(ts), starts_(num_columns), values_() {
    std::iota(std::begin(starts_), std::end(starts_), start_val);
    for (auto& s : starts_)
      values_.emplace_back(TestValue<TDT>{s, num_vals});
    auto prev_size = bitset_.size();
    bitset_.resize(num_columns + 1);
    bitset_.set_range(prev_size, bitset_.size() - 1, true);
  }

  bool check(position_t pos, TensorType<raw_type>& t) {
    return values_[pos].check_tensor(t);
  }

  const TestValue<TDT>& operator[](size_t pos) { return values_[pos]; }

  timestamp ts_;
  std::vector<raw_type> starts_;
  mutable util::BitSet bitset_;
  std::vector<TestValue<TDT>> values_;
};
