/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/value_set.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

    ValueSet::ValueSet(std::vector<std::string>&& value_list) {
        base_type_ = make_scalar_type(entity::DataType::UTF_DYNAMIC64);
        typed_set_string_ = std::make_shared<std::unordered_set<std::string>>();
        for (const auto& value: value_list) {
            typed_set_string_->emplace(std::move(value));
        }
        empty_ = value_list.empty();
    }

    // This currently assumes that the numpy array passed in is already of minimal type, and cannot be shrunk further
    // We could relax this assumption with additional checks involving numeric limits here
    ValueSet::ValueSet(py::array value_list) {
        if (py::isinstance<py::array_t<uint8_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S8));
            numeric_base_set_ = typed_set_uint8_t_.create(value_list);
        } else if (py::isinstance<py::array_t<uint16_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S16));
            numeric_base_set_ = typed_set_uint16_t_.create(value_list);
        } else if (py::isinstance<py::array_t<uint32_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S32));
            numeric_base_set_ = typed_set_uint32_t_.create(value_list);
        } else if (py::isinstance<py::array_t<uint64_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S64));
            numeric_base_set_ = typed_set_uint64_t_.create(value_list);
        } else if (py::isinstance<py::array_t<int8_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S8));
            numeric_base_set_ = typed_set_int8_t_.create(value_list);
        } else if (py::isinstance<py::array_t<int16_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S16));
            numeric_base_set_ = typed_set_int16_t_.create(value_list);
        } else if (py::isinstance<py::array_t<int32_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S32));
            numeric_base_set_ = typed_set_int32_t_.create(value_list);
        } else if (py::isinstance<py::array_t<int64_t>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S64));
            numeric_base_set_ = typed_set_int64_t_.create(value_list);
        } else if (py::isinstance<py::array_t<float>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::FLOAT, entity::SizeBits::S32));
            numeric_base_set_ = typed_set_float_.create(value_list);
        } else if (py::isinstance<py::array_t<double>>(value_list)) {
            base_type_ = make_scalar_type(combine_data_type(entity::ValueType::FLOAT, entity::SizeBits::S64));
            numeric_base_set_ = typed_set_double_.create(value_list);
        } else {
            util::raise_rte("Unexpected numpy array type passed to ValueSet constructor");
        }
        util::variant_match(numeric_base_set_,
        [&](const auto& numeric_base_set) {
            empty_ = numeric_base_set->empty();
        });
    }

    ValueSet::ValueSet(NumericSetType&& value_set):
    numeric_base_set_(std::move(value_set)) {
        util::variant_match(
                numeric_base_set_,
                [this](const std::shared_ptr<std::unordered_set<uint8_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S8));
                },
                [this](const std::shared_ptr<std::unordered_set<uint16_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S16));
                },
                [this](const std::shared_ptr<std::unordered_set<uint32_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S32));
                },
                [this](const std::shared_ptr<std::unordered_set<uint64_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::UINT, entity::SizeBits::S64));
                },
                [this](const std::shared_ptr<std::unordered_set<int8_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S8));
                },
                [this](const std::shared_ptr<std::unordered_set<int16_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S16));
                },
                [this](const std::shared_ptr<std::unordered_set<int32_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S32));
                },
                [this](const std::shared_ptr<std::unordered_set<int64_t>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::INT, entity::SizeBits::S64));
                },
                [this](const std::shared_ptr<std::unordered_set<float>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::FLOAT, entity::SizeBits::S32));
                },
                [this](const std::shared_ptr<std::unordered_set<double>>&) {
                    base_type_ = make_scalar_type(combine_data_type(entity::ValueType::FLOAT, entity::SizeBits::S64));
                }
                );
        util::variant_match(numeric_base_set_,
                            [&](const auto& numeric_base_set) {
                                empty_ = numeric_base_set->empty();
                            });
    }

    bool ValueSet::empty() const {
        return empty_;
    }

    const entity::TypeDescriptor& ValueSet::base_type() const {
        return base_type_;
    }

    std::shared_ptr<std::unordered_set<std::string>> ValueSet::get_fixed_width_string_set(size_t width) {
        std::lock_guard lock(*mutex_);
        auto it = typed_set_fixed_width_strings_.find(width);
        if (it != typed_set_fixed_width_strings_.end()) {
            return it->second;
        } else {
            auto fixed_width_string_set = std::make_shared<std::unordered_set<std::string>>();
            for (const auto& str: *typed_set_string_) {
                auto maybe_padded_str = ascii_to_padded_utf32(str, width);
                if (maybe_padded_str.has_value())
                    fixed_width_string_set->insert(*maybe_padded_str);
            }
            typed_set_fixed_width_strings_.insert(std::make_pair(width, std::move(fixed_width_string_set)));
            return typed_set_fixed_width_strings_.at(width);
        }
    }
}