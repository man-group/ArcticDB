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

namespace arcticdb::entity {

inline ssize_t calc_elements(const shape_t* shape, ssize_t ndim) {
    return std::accumulate(shape, shape + ndim, ssize_t(1), std::multiplies<ssize_t>());
}

struct NativeTensor {
    static constexpr ssize_t MaxDimensions = 2;
    using StrideContainer = std::array<stride_t, MaxDimensions>;

    NativeTensor(
               ssize_t nbytes,
               ssize_t ndim,
               const stride_t *strides,
               const shape_t *shapes,
               DataType dt,
               ssize_t elsize,
               const void *ptr) :
        nbytes_(nbytes),
        ndim_(ndim),
        dt_(dt),
        elsize_(elsize),
        ptr(ptr) {
        util::check(shapes != nullptr, "Unexpected null shapes ptr");
        if(shapes[0] == 0)
            ARCTICDB_DEBUG(log::version(), "Supplied tensor is empty");
        
        strides_[ndim - 1] = static_cast<ssize_t>(get_type_size(dt_));
        for(ssize_t i = 0; i < std::min(MaxDimensions, ndim); ++i)
            shapes_[i] = shapes[i];

        if(strides == nullptr) {
            strides_[ndim - 1] = static_cast<ssize_t>(get_type_size(dt_));
            if(ndim == 2)
                strides_[0] = strides_[1] * shapes_[1];
        }
        else {
            for(ssize_t i = 0; i < std::min(MaxDimensions, ndim); ++i)
                strides_[i] = strides[i];
        }
    }

    NativeTensor(const NativeTensor& other) :
    nbytes_(other.nbytes_),
    ndim_(other.ndim_),
    dt_(other.dt_),
    elsize_(other.elsize_),
    ptr(other.ptr) {
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

    [[nodiscard]] ssize_t nbytes() const { return nbytes_; }
    [[nodiscard]] ssize_t ndim() const { return ndim_; }
    [[nodiscard]] stride_t strides(size_t pos) const { return strides_[pos]; }
    [[nodiscard]] const stride_t* strides() const { return strides_.data(); };
    [[nodiscard]] shape_t shape(size_t pos) const { return shapes_[pos]; }
    [[nodiscard]] ssize_t elsize() const { return elsize_; }
    [[nodiscard]] const shape_t* shape() const { return shapes_.data(); }
    [[nodiscard]] DataType data_type() const { return dt_; }
    [[nodiscard]] const void* data() const { magic_.check(); return ptr; }
    [[nodiscard]] ssize_t extent(ssize_t dim) const { return shapes_[dim] * strides_[dim]; }

    template<typename T>
    const T *ptr_cast(size_t pos) const {
        bool dimension_condition = ndim() == 1;
        bool elsize_condition = elsize_ != 0;
        bool strides_condition = strides_[0] % elsize_ == 0;
        util::warn(dimension_condition, "Cannot safely ptr_cast matrices in NativeTensor");
        util::warn(elsize_condition, "Cannot safely ptr_cast when elsize_ is zero in NativeTensor");
        util::warn(strides_condition,
                   "Cannot safely ptr_cast to type of size {} when strides ({}) is not a multiple of elsize ({}) in NativeTensor with dtype {}",
                   sizeof(T), strides_[0], elsize_, data_type());
        ssize_t signed_pos = pos;
        if (dimension_condition && elsize_condition && strides_condition) {
            signed_pos *= strides_[0] / elsize_;
        }
        return (&(reinterpret_cast<const T *>(ptr)[signed_pos]));
    }

    // returns number of elements, not bytesize
    ssize_t size() const {
        return calc_elements(shape(), ndim());
    }

    NativeTensor &request() { return *this; }

    util::MagicNum<'T','n','s','r'> magic_;
    ssize_t nbytes_;
    ssize_t ndim_;
    StrideContainer strides_ = {};
    StrideContainer shapes_ = {};
    DataType dt_;
    ssize_t elsize_;
    const void *ptr;
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

    TypedTensor(const shape_t* shapes, ssize_t ndim, DataType dt,  ssize_t elsize, const T* data) :
        NativeTensor(calc_elements(shapes, ndim) * itemsize(), ndim, nullptr, shapes, dt, elsize, data) {
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
                    nullptr
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
        }
        else {
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