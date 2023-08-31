/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <variant>
#include <vector>

#include <folly/container/F14Map.h>
#include <pybind11/numpy.h>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preprocess.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

namespace py = pybind11;

using NumericSetType = std::variant<
        std::shared_ptr<std::unordered_set<uint8_t>>,
        std::shared_ptr<std::unordered_set<uint16_t>>,
        std::shared_ptr<std::unordered_set<uint32_t>>,
        std::shared_ptr<std::unordered_set<uint64_t>>,
        std::shared_ptr<std::unordered_set<int8_t>>,
        std::shared_ptr<std::unordered_set<int16_t>>,
        std::shared_ptr<std::unordered_set<int32_t>>,
        std::shared_ptr<std::unordered_set<int64_t>>,
        std::shared_ptr<std::unordered_set<float>>,
        std::shared_ptr<std::unordered_set<double>>>;

class ValueSet {
public:
    explicit ValueSet(std::vector<std::string>&& value_list);
    explicit ValueSet(py::array value_list);
    explicit ValueSet(NumericSetType&& value_set);

    bool empty() const;

    const entity::TypeDescriptor& base_type() const;

    template<typename T>
    std::shared_ptr<std::unordered_set<T>> get_set() {
        util::raise_rte("ValueSet::get_set called with unexpected template type");
    }

    std::shared_ptr<std::unordered_set<std::string>> get_fixed_width_string_set(size_t width);

private:
    bool empty_;
    entity::TypeDescriptor base_type_;
    NumericSetType numeric_base_set_;

    template<typename T>
    class typed_set {
    public:
        ARCTICDB_VISIBILITY_HIDDEN std::shared_ptr<std::unordered_set<T>> create(py::array value_list) {
            std::call_once(flag_, [&]{set_ = create_internal(value_list);});
            return set_;
        }

        std::shared_ptr<std::unordered_set<T>> transform(const NumericSetType& numeric_base_set) {
            std::call_once(flag_, [&]{set_ = transform_internal(numeric_base_set);});
            return set_;
        }

    private:
        std::shared_ptr<std::unordered_set<T>> set_;
        std::once_flag flag_;

        std::shared_ptr<std::unordered_set<T>> create_internal(py::array value_list) {
            auto arr = value_list.unchecked<T, 1>();
            auto set = std::make_shared<std::unordered_set<T>>();
            for (py::ssize_t i = 0; i < arr.shape(0); i++) {
                set->insert(arr(i));
            }
            return set;
        }

        std::shared_ptr<std::unordered_set<T>> transform_internal(const NumericSetType& numeric_base_set) {
            auto target_set = std::make_shared<std::unordered_set<T>>();
            util::variant_match(numeric_base_set,
            [&](const auto& source_set) {
                for (const auto& member: *source_set) {
                    target_set->insert(static_cast<T>(member));
                }
            });
            return target_set;
        }
    };

    std::shared_ptr<std::unordered_set<std::string>> typed_set_string_;
    folly::F14FastMap<size_t, std::shared_ptr<std::unordered_set<std::string>>> typed_set_fixed_width_strings_;

    typed_set<uint8_t> typed_set_uint8_t_;
    typed_set<uint16_t> typed_set_uint16_t_;
    typed_set<uint32_t> typed_set_uint32_t_;
    typed_set<uint64_t> typed_set_uint64_t_;
    typed_set<int8_t> typed_set_int8_t_;
    typed_set<int16_t> typed_set_int16_t_;
    typed_set<int32_t> typed_set_int32_t_;
    typed_set<int64_t> typed_set_int64_t_;
    typed_set<float> typed_set_float_;
    typed_set<double> typed_set_double_;
    std::unique_ptr<std::mutex> mutex_ = std::make_unique<std::mutex>();

};

template<>
inline std::shared_ptr<std::unordered_set<std::string>> ValueSet::get_set<std::string>() {
    return typed_set_string_;
}

template<>
inline std::shared_ptr<std::unordered_set<uint8_t>> ValueSet::get_set<uint8_t>() {
    return typed_set_uint8_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<uint16_t>> ValueSet::get_set<uint16_t>() {
    return typed_set_uint16_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<uint32_t>> ValueSet::get_set<uint32_t>() {
    return typed_set_uint32_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<uint64_t>> ValueSet::get_set<uint64_t>() {
    return typed_set_uint64_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<int8_t>> ValueSet::get_set<int8_t>() {
    return typed_set_int8_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<int16_t>> ValueSet::get_set<int16_t>() {
    return typed_set_int16_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<int32_t>> ValueSet::get_set<int32_t>() {
    return typed_set_int32_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<int64_t>> ValueSet::get_set<int64_t>() {
    return typed_set_int64_t_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<float>> ValueSet::get_set<float>() {
    return typed_set_float_.transform(numeric_base_set_);
}

template<>
inline std::shared_ptr<std::unordered_set<double>> ValueSet::get_set<double>() {
    return typed_set_double_.transform(numeric_base_set_);
}

}
