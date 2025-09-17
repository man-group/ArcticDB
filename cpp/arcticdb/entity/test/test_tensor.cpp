/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/util/flatten_utils.hpp>

auto get_f_tensor(size_t num_rows) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    auto data = std::make_shared<std::vector<data_t>>(num_rows);
    const std::array<stride_t, 2> strides = {4u, 40u};
    const std::array<shape_t, 2> shapes = {10u, 10u};
    size_t count = 0;
    for (auto i = 0u; i < shapes[0]; ++i) {
        auto row = i * (strides[0] / sizeof(data_t));
        for (auto j = 0u; j < shapes[1]; ++j) {
            auto pos = row + (j * (strides[1] / sizeof(data_t)));
            (*data)[pos] = count++;
        }
    }

    const ssize_t nbytes = data->size() * sizeof(data_t);
    const auto ndim = 2;
    const DataType dt = DataType::UINT32;

    NativeTensor tensor{
            nbytes,
            ndim,
            strides.data(),
            shapes.data(),
            dt,
            get_type_size(dt),
            static_cast<const void*>(data->data()),
            ndim
    };
    return std::make_pair(data, tensor);
}

auto get_c_tensor(size_t num_rows) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    auto data = std::make_shared<std::vector<data_t>>(num_rows);
    const std::array<stride_t, 2> strides = {40u, 4u};
    const std::array<shape_t, 2> shapes = {10u, 10u};
    for (auto i = 0u; i < num_rows; ++i) {
        (*data)[i] = i;
    }

    const ssize_t nbytes = data->size() * sizeof(data_t);
    const auto ndim = 2;
    const DataType dt = DataType::UINT32;

    NativeTensor tensor{
            nbytes,
            ndim,
            strides.data(),
            shapes.data(),
            dt,
            get_type_size(dt),
            static_cast<const void*>(data->data()),
            ndim
    };
    return std::make_pair(data, tensor);
}

TEST(ColumnMajorTensor, Flatten) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_f_tensor(num_rows);
    TypedTensor<data_t> typed_tensor{tensor};
    std::vector<data_t> output(num_rows);

    arcticdb::util::FlattenHelper f{typed_tensor};
    auto info = typed_tensor.request();

    uint32_t* ptr = output.data();
    f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

TEST(RowMajorTensor, Flatten) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_c_tensor(num_rows);
    TypedTensor<data_t> typed_tensor{tensor};
    std::vector<data_t> output(num_rows);

    arcticdb::util::FlattenHelper f{typed_tensor};
    auto info = typed_tensor.request();

    uint32_t* ptr = output.data();
    f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

TEST(ColumnMajorTensor, SubDivide) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_f_tensor(num_rows);
    std::vector<data_t> output(num_rows);

    ssize_t nvalues = 20;
    std::vector<TypedTensor<data_t>> tensors;
    ssize_t slice = 0;
    for (auto div = 0u; div < num_rows; div += nvalues) {
        TypedTensor<data_t> typed_tensor{tensor, slice++, nvalues, nvalues};
        tensors.push_back(typed_tensor);
    }

    uint32_t* ptr = output.data();
    for (auto& typed_tensor : tensors) {
        arcticdb::util::FlattenHelper f{typed_tensor};
        auto info = typed_tensor.request();
        f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));
    }

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

TEST(RowMajorTensor, SubDivide) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_c_tensor(num_rows);
    std::vector<data_t> output(num_rows);

    ssize_t nvalues = 20;
    std::vector<TypedTensor<data_t>> tensors;
    ssize_t slice = 0;
    for (auto div = 0u; div < num_rows; div += nvalues) {
        TypedTensor<data_t> typed_tensor{tensor, slice++, nvalues, nvalues};
        tensors.push_back(typed_tensor);
    }

    uint32_t* ptr = output.data();
    for (auto& typed_tensor : tensors) {
        arcticdb::util::FlattenHelper f{typed_tensor};
        auto info = typed_tensor.request();
        f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));
    }

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

auto get_sparse_array(size_t num_rows) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    auto data = std::make_shared<std::vector<data_t>>(num_rows * 2);
    const std::array<stride_t, 2> strides = {8u, 0u};
    const std::array<shape_t, 2> shapes = {100u, 0u};

    for (auto i = 0u; i < num_rows; ++i) {
        (*data)[i * 2] = i;
    }

    const ssize_t nbytes = num_rows * sizeof(data_t);
    const auto ndim = 1;
    const DataType dt = DataType::UINT32;

    NativeTensor tensor{
            nbytes,
            ndim,
            strides.data(),
            shapes.data(),
            dt,
            get_type_size(dt),
            static_cast<const void*>(data->data()),
            ndim
    };
    return std::make_pair(data, tensor);
}

TEST(SparseArray, Flatten) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_sparse_array(num_rows);
    TypedTensor<data_t> typed_tensor{tensor};
    std::vector<data_t> output(num_rows);

    arcticdb::util::FlattenHelper f{typed_tensor};
    auto info = typed_tensor.request();

    uint32_t* ptr = output.data();
    f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

TEST(SparseArray, SubDivide) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_sparse_array(num_rows);
    std::vector<data_t> output(num_rows);

    ssize_t nvalues = 20;
    std::vector<TypedTensor<data_t>> tensors;
    ssize_t slice = 0;
    for (auto div = 0u; div < num_rows; div += nvalues) {
        TypedTensor<data_t> typed_tensor{tensor, slice++, nvalues, nvalues};
        tensors.push_back(typed_tensor);
    }

    uint32_t* ptr = output.data();
    for (auto& typed_tensor : tensors) {
        arcticdb::util::FlattenHelper f{typed_tensor};
        auto info = typed_tensor.request();
        f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));
    }

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

auto get_sparse_array_funky_strides() {
    using namespace arcticdb::entity;
    const auto num_rows = 100u;
    using data_t = uint32_t;
    auto data = std::make_shared<std::vector<uint8_t>>(num_rows * 19 * sizeof(data_t));
    const std::array<stride_t, 2> strides = {19u, 0u};
    const std::array<shape_t, 2> shapes = {100u, 0u};

    for (auto i = 0u; i < num_rows; ++i) {
        *reinterpret_cast<data_t*>(&(*data)[i * 19]) = i;
    }

    const ssize_t nbytes = num_rows * sizeof(data_t);
    const auto ndim = 1;
    const DataType dt = DataType::UINT32;

    NativeTensor tensor{
            nbytes,
            ndim,
            strides.data(),
            shapes.data(),
            dt,
            get_type_size(dt),
            static_cast<const void*>(data->data()),
            ndim
    };
    return std::make_pair(data, tensor);
}

TEST(SparseArrayFunkyStrides, Flatten) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_sparse_array_funky_strides();
    TypedTensor<data_t> typed_tensor{tensor};
    std::vector<data_t> output(num_rows);

    arcticdb::util::FlattenHelper f{typed_tensor};
    auto info = typed_tensor.request();

    uint32_t* ptr = output.data();
    f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

TEST(SparseArrayFunkyStrides, SubDivide) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 100;
    auto [data, tensor] = get_sparse_array_funky_strides();
    std::vector<data_t> output(num_rows);

    ssize_t nvalues = 20;
    std::vector<TypedTensor<data_t>> tensors;
    ssize_t slice = 0;
    for (auto div = 0u; div < num_rows; div += nvalues) {
        TypedTensor<data_t> typed_tensor{tensor, slice++, nvalues, nvalues};
        tensors.push_back(typed_tensor);
    }

    uint32_t* ptr = output.data();
    for (auto& typed_tensor : tensors) {
        arcticdb::util::FlattenHelper f{typed_tensor};
        auto info = typed_tensor.request();
        f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));
    }

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}

auto get_sparse_array_uneven(size_t num_rows) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    auto data = std::make_shared<std::vector<data_t>>(num_rows * 2);
    const std::array<stride_t, 2> strides = {8u, 0u};
    const std::array<shape_t, 2> shapes = {109u, 0u};

    for (auto i = 0u; i < num_rows; ++i) {
        (*data)[i * 2] = i;
    }

    const auto nbytes = static_cast<ssize_t>(num_rows * sizeof(data_t));
    const auto ndim = 1;
    const DataType dt = DataType::UINT32;

    NativeTensor tensor{
            nbytes,
            ndim,
            strides.data(),
            shapes.data(),
            dt,
            get_type_size(dt),
            static_cast<const void*>(data->data()),
            ndim
    };
    return std::make_pair(data, tensor);
}

TEST(SparseArray, SubDivideUneven) {
    using namespace arcticdb::entity;
    using data_t = uint32_t;
    constexpr size_t num_rows = 109;
    auto [data, tensor] = get_sparse_array_uneven(num_rows);
    std::vector<data_t> output(num_rows);

    ssize_t nvalues = 20;
    std::vector<TypedTensor<data_t>> tensors;
    ssize_t slice = 0;
    auto remaining = num_rows;
    for (auto div = 0u; div < num_rows; div += nvalues, remaining -= nvalues) {
        TypedTensor<data_t> typed_tensor{tensor, slice++, nvalues, std::min(static_cast<ssize_t>(remaining), nvalues)};
        tensors.push_back(typed_tensor);
    }

    uint32_t* ptr = output.data();
    for (auto& typed_tensor : tensors) {
        arcticdb::util::FlattenHelper f{typed_tensor};
        auto info = typed_tensor.request();
        f.flatten(ptr, reinterpret_cast<const data_t*>(info.ptr));
    }

    for (uint32_t x = 0; x < num_rows; ++x) {
        ASSERT_EQ(output[x], x);
    }
}
