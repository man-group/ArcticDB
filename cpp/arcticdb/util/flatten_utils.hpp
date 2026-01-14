/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <pybind11/numpy.h>

namespace py = pybind11;

namespace arcticdb::util {

using namespace arcticdb::entity;

template<class T, template<class> class Tensor>
inline bool has_funky_strides(Tensor<T>& a) {
    for (ssize_t i = 0; i < a.ndim(); ++i) {
        if (a.strides(i) < 0 || a.strides(i) % a.itemsize() != 0)
            return true;
    }
    return false;
}

template<class T>
inline bool has_funky_strides(py::array_t<T>& a) {
    for (ssize_t i = 0; i < a.ndim(); ++i) {
        if (a.strides(i) < 0 || a.strides(i) % a.itemsize() != 0)
            return true;
    }
    return false;
}

template<typename RawType, typename TensorType>
inline bool is_cstyle_array(const TensorType& tensor) {
    return tensor.size() == 0 || tensor.strides(tensor.ndim() - 1) == sizeof(RawType);
}

template<typename T>
struct stride_advance_conservative {
    const T* operator()(const T* pos, stride_t stride, shape_t distance) const {
        const auto* byte = reinterpret_cast<const uint8_t*>(pos);
        byte += stride * distance;
        return reinterpret_cast<const T*>(byte);
    }
};

template<typename T>
struct stride_advance_optimistic {
    const T* operator()(const T* pos, stride_t stride, shape_t i) const { return pos + ((stride / sizeof(T)) * i); }
};

template<class T, template<class> class Tensor>
auto shape_and_strides(Tensor<T>& array, ssize_t dim) {
    auto total_dim = array.ndim();
    shape_t sh = array.shape(total_dim - size_t(dim));
    stride_t sd = array.strides(total_dim - size_t(dim));
    return std::make_pair(sh, sd);
}

template<class T>
auto shape_and_strides(py::array_t<T>& array, ssize_t dim) {
    auto total_dim = array.ndim();
    shape_t sh = array.shape(total_dim - size_t(dim));
    stride_t sd = array.strides(total_dim - size_t(dim));
    return std::make_pair(sh, sd);
}

template<class T, template<class> class Tensor, typename AdvanceFunc>
class FlattenHelperImpl {
    Tensor<T>& array_;
    AdvanceFunc advance_func_;

  public:
    explicit FlattenHelperImpl(Tensor<T>& a) : array_(a) {}

    using raw_type = T;

    void flatten(T*& dest, const T* src, ssize_t dim) const {
        auto [sh, sd] = shape_and_strides(array_, dim);

        for (shape_t i = 0; i < sh; ++i) {
            if (dim == 1) {
                *dest = *(advance_func_(src, sd, i));
                dest++;
            } else {
                flatten(dest, advance_func_(src, sd, i), dim - 1);
            }
        }
    }
};

template<class T, template<class> class Tensor>
class FlattenHelper {
    Tensor<T>& array_;

  public:
    explicit FlattenHelper(Tensor<T>& a) : array_(a) {}

    using raw_type = T;

    void flatten(T*& dest, const T* src) const {
        if (has_funky_strides(array_)) {
            FlattenHelperImpl<T, Tensor, stride_advance_conservative<T>> flh{array_};
            flh.flatten(dest, src, array_.ndim());
        } else {
            FlattenHelperImpl<T, Tensor, stride_advance_optimistic<T>> flh{array_};
            flh.flatten(dest, src, array_.ndim());
        }
    }
};

} // namespace arcticdb::util
