/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/magic_num.hpp>

// for std::accumulate
#include <numeric>

#include <pybind11/numpy.h>

namespace py = pybind11;

namespace arcticdb::entity {

inline ssize_t calc_elements(const shape_t* shape, ssize_t ndim) {
    return std::accumulate(shape, shape + ndim, ssize_t(1), std::multiplies<ssize_t>());
}

/*
 * A wrapper around a 1D or 2D tensor that provides a more convenient interface for accessing the data
 * in the tensor. This is used to pass data between the Python and C++ layers.
 *
 * This typically stores the data of the numpy array backing a column of a pandas DataFrame.
 */
struct NativeTensor {
    static constexpr int MaxDimensions = 2;
    using StrideContainer = std::array<stride_t, MaxDimensions>;

    /// @param ndim The dimension of the tensor as reported by the API ArcticDB used to read it
    /// @param expanded_dim The dimension of the tensor as ArcticDB should perceive it. E.g. in case a column tensor
    /// holds pointers to an array it will be reported to be of dim1 (the column is an array of pointers) while we think
    /// of it as a dim2 tensor (a column is an array of arrays).
    NativeTensor(
        int64_t nbytes,
        int ndim,
        const stride_t* strides,
        const shape_t* shapes,
        DataType dt,
        stride_t elsize,
        const void* ptr,
        int expanded_dim
    ) :
        nbytes_(nbytes),
        ndim_(ndim),
        dt_(dt),
        elsize_(elsize),
        ptr(ptr),
        expanded_dim_(expanded_dim){
        util::check(shapes != nullptr, "Unexpected null shapes ptr");
        if(shapes[0] == 0)
            ARCTICDB_DEBUG(log::version(), "Supplied tensor is empty");

        for (ssize_t i = 0; i < std::min(MaxDimensions, ndim); ++i)
            shapes_[i] = shapes[i];

        if(strides == nullptr) {
            strides_[ndim - 1] = static_cast<ssize_t>(get_type_size(dt_));
            if(ndim == 2)
                strides_[0] = strides_[1] * shapes_[1];
        } else {
            for (ssize_t i = 0; i < std::min(MaxDimensions, ndim); ++i)
                strides_[i] = strides[i];
        }
    }

    NativeTensor(const NativeTensor& other) :
    nbytes_(other.nbytes_),
    ndim_(other.ndim_),
    dt_(other.dt_),
    elsize_(other.elsize_),
    ptr(other.ptr),
    expanded_dim_(other.expanded_dim_){
        for (ssize_t i = 0; i < std::min(MaxDimensions, ndim_); ++i)
            shapes_[i] = other.shapes_[i];

        for(ssize_t i = 0; i < std::min(MaxDimensions, ndim_); ++i)
            strides_[i] = other.strides_[i];
    }

    friend void swap(NativeTensor& left, NativeTensor& right) {
        using std::swap;
        swap(left.magic_, right.magic_);
        swap(left.nbytes_, right.nbytes_);
        swap(left.ndim_, right.ndim_);
        swap(left.dt_, right.dt_);
        swap(left.elsize_, right.elsize_);
        swap(left.ptr, right.ptr);
        swap(left.expanded_dim_, right.expanded_dim_);
        for(ssize_t i = 0; i < MaxDimensions; ++i) {
            swap(left.shapes_[i], right.shapes_[i]);
            swap(left.strides_[i], right.strides_[i]);
        }
    }

    NativeTensor& operator=(const NativeTensor& other) {
        NativeTensor temp(other);
        swap(*this, temp);
        return *this;
    }

    [[nodiscard]] auto nbytes() const { return nbytes_; }
    [[nodiscard]] auto ndim() const { return ndim_; }
    [[nodiscard]] auto strides(size_t pos) const { return strides_[pos]; }
    [[nodiscard]] const auto* strides() const { return strides_.data(); };
    [[nodiscard]] auto shape(size_t pos) const { return shapes_[pos]; }
    [[nodiscard]] auto elsize() const { return elsize_; }
    [[nodiscard]] const auto* shape() const { return shapes_.data(); }
    [[nodiscard]] auto data_type() const { return dt_; }
    [[nodiscard]] const void* data() const { magic_.check(); return ptr; }
    [[nodiscard]] auto extent(ssize_t dim) const { return shapes_[dim] * strides_[dim]; }
    [[nodiscard]] auto expanded_dim() const { return expanded_dim_; }
    template<typename T>
    const T *ptr_cast(size_t pos) const {
        util::check(ptr != nullptr, "Unexpected null ptr in NativeTensor");
        const bool dimension_condition = ndim() == 1;
        const bool elsize_condition = elsize_ != 0;
        const bool strides_condition = (elsize_condition) && (strides_[0] % elsize_ == 0);
        util::warn(dimension_condition, "Cannot safely ptr_cast matrices in NativeTensor");
        util::warn(elsize_condition, "Cannot safely ptr_cast when elsize_ is zero in NativeTensor");
        util::warn(strides_condition,
                   "Cannot safely ptr_cast to type of size {} when strides ({}) is not a multiple of elsize ({}) in NativeTensor with dtype {}",
                   sizeof(T), strides_[0], elsize_, data_type());

        int64_t signed_pos = pos;
        if (dimension_condition && elsize_condition && strides_condition) {
            signed_pos *= strides_[0] / elsize_;
        }
        return (&(reinterpret_cast<const T *>(ptr)[signed_pos]));
    }

    // returns number of elements, not bytesize
    [[nodiscard]] ssize_t size() const {
        return calc_elements(shape(), ndim());
    }

    NativeTensor &request() { return *this; }

    util::MagicNum<'T','n','s','r'> magic_;
    int64_t nbytes_;
    int ndim_;
    StrideContainer strides_ = {};
    StrideContainer shapes_ = {};
    DataType dt_;
    stride_t elsize_;
    const void *ptr;
    /// @note: when iterating strides and shapes we should use the ndim as it is the dimension reported by the
    /// API providing the strides and shapes arrays, expanded_dim is what ArcticDB thinks of the tensor and using it
    /// can lead to out of bounds reads from strides and shapes.
    int expanded_dim_;
};

template <ssize_t> ssize_t byte_offset_impl(const stride_t* ) { return 0; }
template <ssize_t Dim = 0, typename... Ix>
ssize_t byte_offset_impl(const stride_t* strides, ssize_t i, Ix... index) {
    return i * strides[Dim] + byte_offset_impl<Dim + 1>(strides, index...);
}

//TODO is the conversion to a typed tensor really necessary for the codec part?
template<typename T>
struct TypedTensor : public NativeTensor {
    static size_t itemsize() { return sizeof(T); }

    std::array<stride_t, 2> f_style_strides() {
        std::array<stride_t, 2> strides = {};
        if(std::any_of(std::begin(shapes_),std::end(shapes_), [] (auto x) { return x == 0; })) {
            static constexpr auto default_stride = static_cast<stride_t>(itemsize());
            strides = {default_stride, default_stride};
        } else {
            auto total = itemsize();
            for (size_t i = 0; i < ndim(); ++i) {
                strides[i] = static_cast<ssize_t>(total);
                total *= shape(i);
            }
        }
        return strides;
    }

    bool is_f_style() {
        auto col_major = f_style_strides();
        return col_major == strides_;
    }

    template<typename... Ix> ssize_t byte_offset(Ix... index) const {
        return byte_offset_impl(strides(), ssize_t(index)...);
    }

    template<typename... Ix> const T& at(Ix... index) const {
        return *(static_cast<const T*>(NativeTensor::data()) + byte_offset(ssize_t(index)...) / itemsize());
    }

    /// @param expanded_dim @see NativeTensor::NativeTensor for information about the difference between ndim and expanded_dim
    TypedTensor(const shape_t* shapes, ssize_t ndim, DataType dt,  ssize_t elsize, const T* data, ssize_t expanded_dim) :
        NativeTensor(calc_elements(shapes, ndim) * itemsize(), ndim, nullptr, shapes, dt, elsize, data, expanded_dim) {
    }

    explicit TypedTensor(const NativeTensor& tensor) :
        NativeTensor(tensor)
    {
    }

    TypedTensor(const NativeTensor& tensor, ssize_t slice_num, ssize_t regular_slice_size, ssize_t nvalues) :
        NativeTensor(
            nvalues * itemsize(),
            tensor.ndim(),
            tensor.strides(),
            tensor.shape(),
            tensor.data_type(),
            tensor.elsize(),
            nullptr,
            tensor.expanded_dim()
        ) {

        ssize_t stride_offset;
        if(ndim() > 1) {
            // Check that we can evenly subdivide a matrix into n rows (otherwise we'd have to have
            // extra state to track how far along a row we were
            util::check(nvalues >= shape(0) && nvalues % shape(0) == 0,
                        "Cannot subdivide a tensor of width {} into {}-sized sections", shape(0), nvalues);

            // Adjust the column shape
            auto divisor = calc_elements(shape(), ndim()) / nvalues;
            shapes_[0] /= divisor;
            stride_offset = strides(0) * (shape(0));
        } else {
            shapes_[0] = nvalues;
            stride_offset = regular_slice_size * strides(0);
        }

        // The new column shape * the column stride tells us how far to move the data pointer from the origin

        ptr = reinterpret_cast<const uint8_t*>(tensor.data()) + (slice_num * stride_offset);
        util::check(ptr < static_cast<const uint8_t*>(tensor.ptr) + std::abs(tensor.extent(0)),
                "Tensor overflow, cannot put slice pointer at byte {} in a tensor of {} bytes",
                slice_num * stride_offset, tensor.extent(0));
    }
};
template<typename T>
py::array to_py_array(const TypedTensor<T>& tensor) {
    return py::array({tensor.shape(), tensor.shape() + tensor.ndim()}, reinterpret_cast<const T*>(tensor.data()));
}

template<typename T>
using TensorType = TypedTensor<T>;

}//namespace arcticdb
