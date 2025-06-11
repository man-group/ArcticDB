#pragma once

#include <type_traits>
#include <typeinfo>
#include <stdexcept>
#include <new>
#include <cstdint>
#include <cstring>

#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

struct ValueContainer {
    std::aligned_storage_t<sizeof(uint64_t), alignof(uint64_t)> data_;
    const std::type_info *type_info_ = nullptr;

    template<typename T>
    void set_value(T value) {
        static_assert(sizeof(T) <= sizeof(data_), "Type too large to store");
        static_assert(std::is_trivially_copyable_v<T>, "Type must be trivially copyable");
        std::memcpy(&data_, &value, sizeof(T));
        type_info_ = &typeid(T);
    }

    template<typename T>
    T get_value() const {
        util::check(type_info_ == &typeid(T), "Unexpected type request in value container");
        T target;
        std::memcpy(&target, &data_, sizeof(T));
        return target;
    }
};

} //namespace arcticdb

