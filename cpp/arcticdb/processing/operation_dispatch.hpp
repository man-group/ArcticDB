/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <variant>
#include <memory>
#include <type_traits>

#include <folly/futures/Future.h>

#include <arcticdb/pipeline/value.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/entity/type_conversion.hpp>

namespace arcticdb {

VariantData transform_to_placeholder(VariantData data) {
    // There is a further optimization we can do here which is to return FullResult
    // when the number of set bits is the same as the processing segments row-count,
    // however that would need to be maintained by the processing segment and
    // modified with each transformation. It would be worth it in the scenario
    // where a true result is very common, but is a premature change to make at this point.
    return util::variant_match(data,
                               [](std::shared_ptr<util::BitSet> bitset) -> VariantData {
                                   if (bitset->count() == 0) {
                                       return VariantData{EmptyResult{}};
                                   } else {
                                       return VariantData{bitset};
                                   }
                               },
                               [](auto v) -> VariantData {
                                   return VariantData{v};
                               }
    );
}

// Unary and binary boolean operations work on bitsets, full results and empty results
// A column is also a valid input, provided it is of type bool
// This conversion could be done within visit_binary_boolean with an explosion of the number of visitations to handle
// To avoid that, this function:
//  - returns util::Bitset, FullResult, and EmptyResult as provided
//  - throws if the input VariantData is a Value, a ValueSet, or a non-bool column
//  - returns a util::BitSet if the input is a bool column
VariantData transform_to_bitset(const VariantData& data) {
    return std::visit(util::overload{
        [&] (const std::shared_ptr<Value>&) -> VariantData {
            util::raise_rte("Value inputs cannot be input to boolean operations");
        },
        [&] (const std::shared_ptr<ValueSet>&) -> VariantData {
            util::raise_rte("ValueSet inputs cannot be input to boolean operations");
            },
        [&] (const ColumnWithStrings& column_with_strings) -> VariantData {
            auto output = std::make_shared<util::BitSet>(static_cast<util::BitSetSizeType>(column_with_strings.column_->row_count()));
            column_with_strings.column_->type().visit_tag([&column_with_strings, &output] (auto column_desc_tag) {
                using ColumnDescriptorType = std::decay_t<decltype(column_desc_tag)>;
                using ColumnTagType =  typename ColumnDescriptorType::DataTypeTag;
                using ColumnType =  typename ColumnTagType::raw_type;
                if constexpr (is_bool_type(ColumnTagType::data_type)) {
                    auto column_data = column_with_strings.column_->data();
                    util::BitSet::bulk_insert_iterator inserter(*output);
                    auto pos = 0u;
                    while (auto block = column_data.next<ColumnDescriptorType>()) {
                        auto ptr = reinterpret_cast<const ColumnType*>(block.value().data());
                        const auto row_count = block.value().row_count();
                        for (auto i = 0u; i < row_count; ++i, ++pos) {
                            if(*ptr++)
                                inserter = pos;
                        }
                    }
                    inserter.flush();
                } else {
                    util::raise_rte("Cannot convert column of type {} to a bitset", column_with_strings.column_->type());
                }
            });
            return output;
        },
        [](const auto& d) -> VariantData {
            // util::BitSet, FullResult, or EmptyResult
            return d;
        }
    }, data);
}

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, const std::shared_ptr<util::BitSet>& right, OperationType operation) {
    util::check(left->size() == right->size(), "BitSets of different lengths ({} and {}) in binary comparator", left->size(), right->size());
    switch(operation) {
    case OperationType::AND:
        return std::make_shared<util::BitSet>(*left & *right);
    case OperationType::OR:
        return std::make_shared<util::BitSet>(*left | *right);
    case OperationType::XOR:
        return std::make_shared<util::BitSet>(*left ^ *right);
    default:
        util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, EmptyResult, OperationType operation) {
    switch(operation) {
    case OperationType::AND:
        return EmptyResult{};
    case OperationType::OR:
        return left;
    case OperationType::XOR:
        return left;
    default:
        util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, FullResult, OperationType operation) {
    switch(operation) {
    case OperationType::AND:
        return left;
    case OperationType::OR:
        return FullResult{};
    case OperationType::XOR: {
        return std::make_shared<util::BitSet>(~(*left));
    }
    default:
        util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(EmptyResult, FullResult, OperationType operation) {
    switch(operation) {
    case OperationType::AND:
        return EmptyResult{};
    case OperationType::OR:
        return FullResult{};
    case OperationType::XOR:
        return FullResult{};
    default:
        util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(FullResult, FullResult, OperationType operation) {
    switch(operation) {
    case OperationType::AND:
        return FullResult{};
    case OperationType::OR:
        return FullResult{};
    case OperationType::XOR:
        return EmptyResult{};
    default:
        util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

VariantData binary_boolean(EmptyResult, EmptyResult, OperationType operation) {
    switch(operation) {
    case OperationType::AND:
    case OperationType::OR:
    case OperationType::XOR:
        return EmptyResult{};
    default:
        util::raise_rte("Unexpected operator in binary_boolean {}", int(operation));
    }
}

// Note that we can have fewer of these and reverse the parameters because all the operations are
// commutative, however if that were to change we would need the full set
VariantData visit_binary_boolean(const VariantData& left, const VariantData& right, OperationType operation) {
    auto left_transformed = transform_to_bitset(left);
    auto right_transformed = transform_to_bitset(right);
    return std::visit(util::overload {
        [&operation] (const std::shared_ptr<util::BitSet>& l, const std::shared_ptr<util::BitSet>& r) {
            return transform_to_placeholder(binary_boolean(l, r, operation));
        },
        [&operation] (const std::shared_ptr<util::BitSet>& l, EmptyResult r) {
            return transform_to_placeholder(binary_boolean(l, r, operation));
        },
        [&operation] (const std::shared_ptr<util::BitSet>& l, FullResult r) {
            return transform_to_placeholder(binary_boolean(l, r, operation));
        },
        [&operation] (EmptyResult l, const std::shared_ptr<util::BitSet>& r) {
            return binary_boolean(r, l, operation);
        },
        [&operation] (FullResult l, const std::shared_ptr<util::BitSet>& r) {
            return transform_to_placeholder(binary_boolean(r, l, operation));
        },
        [&operation] (FullResult l, EmptyResult r) {
            return binary_boolean(r, l, operation);
        },
        [&operation] (EmptyResult l, FullResult r) {
            return binary_boolean(l, r, operation);
        },
        [&operation] (FullResult l, FullResult r) {
            return binary_boolean(l, r, operation);
        },
        [&operation] (EmptyResult l, EmptyResult r) {
            return binary_boolean(r, l, operation);
        },
        [](const auto &, const auto&) -> VariantData {
            util::raise_rte("Value/ValueSet/non-bool column inputs not accepted to binary boolean");
        }
        }, left_transformed, right_transformed);
}

template <typename Func>
VariantData binary_membership(const ColumnWithStrings& column_with_strings, ValueSet& value_set, Func&& func) {
    auto output = std::make_shared<util::BitSet>(column_with_strings.column_->row_count());
    // If the value set is empty, then for IsInOperator, we are done
    // For the IsNotInOperator, set all bits to 1
    if (value_set.empty()) {
        if constexpr(std::is_same_v<Func, IsNotInOperator&&>)
            output->set();
    } else {
        entity::details::visit_type(column_with_strings.column_->type().data_type(),[&column_with_strings, &value_set, &func, &output] (auto column_desc_tag) {
            using ColumnTagType = std::decay_t<decltype(column_desc_tag)>;
            using ColumnType =  typename ColumnTagType::raw_type;

            entity::details::visit_type(value_set.base_type().data_type(),[&column_with_strings, &value_set, &func, &output, &column_desc_tag] (auto value_set_desc_tag) {
                using ValueSetBaseTypeTag = decltype(value_set_desc_tag);

                if constexpr(is_sequence_type(ColumnTagType::data_type) && is_sequence_type(ValueSetBaseTypeTag::data_type)) {
                    std::shared_ptr<std::unordered_set<std::string>> typed_value_set;
                    if constexpr(is_fixed_string_type(ColumnTagType::data_type)) {
                        auto width = column_with_strings.get_fixed_width_string_size();
                        if (width.has_value()) {
                            typed_value_set = value_set.get_fixed_width_string_set(*width);
                        }
                    } else {
                        typed_value_set = value_set.get_set<std::string>();
                    }
                    auto offset_set = column_with_strings.string_pool_->get_offsets_for_column(typed_value_set, *column_with_strings.column_);
                    auto column_data = column_with_strings.column_->data();

                    util::BitSet::bulk_insert_iterator inserter(*output);
                    auto pos = 0u;
                    while (auto block = column_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                        auto ptr = reinterpret_cast<const StringPool::offset_t*>(block.value().data());
                        const auto row_count = block.value().row_count();
                        for (auto i = 0u; i < row_count; ++i, ++pos) {
                            auto offset = *ptr++;
                            if(func(offset, offset_set))
                                inserter = pos;
                        }
                    }
                    inserter.flush();
                } else if constexpr (is_bool_type(ColumnTagType::data_type) && is_bool_type(ValueSetBaseTypeTag::data_type)) {
                    util::raise_rte("Binary membership not implemented for bools");
                } else if constexpr (is_numeric_type(ColumnTagType::data_type) && is_numeric_type(ValueSetBaseTypeTag::data_type)) {
                    using ValueSetBaseType =  typename decltype(value_set_desc_tag)::DataTypeTag::raw_type;

                    using WideType = typename type_arithmetic_promoted_type<ColumnType, ValueSetBaseType, std::remove_reference_t<Func>>::type;
                    auto typed_value_set = value_set.get_set<WideType>();
                    auto column_data = column_with_strings.column_->data();

                    util::BitSet::bulk_insert_iterator inserter(*output);
                    auto pos = 0u;
                    while (auto block = column_data.next<ScalarTagType<ColumnTagType>>()) {
                        auto ptr = reinterpret_cast<const ColumnType*>(block.value().data());
                        const auto row_count = block.value().row_count();
                        for (auto i = 0u; i < row_count; ++i, ++pos) {
                            if(func(static_cast<WideType>(*ptr++), *typed_value_set))
                                inserter = pos;
                        }
                    }
                    inserter.flush();
                } else {
                    util::raise_rte("Cannot check membership of {} in set of {} (possible categorical?)",
                                    column_with_strings.column_->type(), value_set.base_type());
                }
            });
        });
    }

    output->optimize();
    auto pos = 0;

    if(column_with_strings.column_->is_sparse()) {
        const auto& sparse_map = column_with_strings.column_->opt_sparse_map().value();
        util::BitSet replace(sparse_map.size());
        auto it = sparse_map.first();
        auto it_end = sparse_map.end();
        while(it < it_end) {
           replace.set(*it, output->test(pos++));
            ++it;
        }
    }

    log::version().debug("Filtered segment of size {} down to {} bits", output->size(), output->count());

    return {std::move(output)};
}



template<typename Func>
VariantData visit_binary_membership(const VariantData &left, const VariantData &right, Func &&func) {
    if (std::holds_alternative<EmptyResult>(left))
        return EmptyResult{};

    return std::visit(util::overload {
        [&] (const ColumnWithStrings& l, const std::shared_ptr<ValueSet>& r) ->VariantData  {
            return transform_to_placeholder(binary_membership<decltype(func)>(l, *r, std::forward<Func>(func)));
            },
            [](const auto &, const auto&) -> VariantData {
            util::raise_rte("Binary membership operations must be Column/ValueSet");
        }
        }, left, right);
}

template <typename Func>
VariantData binary_comparator(const Value& val, const ColumnWithStrings& column_with_strings, Func&& func) {
    auto output = std::make_shared<util::BitSet>(static_cast<util::BitSetSizeType>(column_with_strings.column_->row_count()));

    column_with_strings.column_->type().visit_tag([&val, &column_with_strings, &func, &output] (auto column_desc_tag) {
        using ColumnDescriptorType = std::decay_t<decltype(column_desc_tag)>;
        using ColumnTagType =  typename ColumnDescriptorType::DataTypeTag;
        using ColumnType =  typename ColumnTagType::raw_type;
        TypeDescriptor value_type{val.data_type_, Dimension::Dim0};
        value_type.visit_tag([&] (auto value_desc_tag ) {
            using DataTypeTag =  typename decltype(value_desc_tag)::DataTypeTag;
            if constexpr(is_sequence_type(ColumnTagType::data_type) && is_sequence_type(DataTypeTag::data_type)) {
                std::optional<std::string> utf32_string;
                std::optional<std::string_view> value_string;
                if constexpr(is_fixed_string_type(ColumnTagType::data_type)) {
                    auto width = column_with_strings.get_fixed_width_string_size();
                    if (width.has_value()) {
                        utf32_string = ascii_to_padded_utf32(std::string_view(*val.str_data(), val.len()), *width);
                        if (utf32_string.has_value()) {
                            value_string = std::string_view(*utf32_string);
                        }
                    }
                } else {
                    value_string = std::string_view(*val.str_data(), val.len());
                }
                auto value_offset = column_with_strings.string_pool_->get_offset_for_column(*value_string, *column_with_strings.column_);
                auto column_data = column_with_strings.column_->data();

                util::BitSet::bulk_insert_iterator inserter(*output);
                auto pos = 0u;
                while (auto block = column_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                    auto ptr = reinterpret_cast<const StringPool::offset_t*>(block.value().data());
                    const auto row_count = block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i, ++pos) {
                        auto offset = *ptr++;
                        if(func(offset, value_offset))
                            inserter = pos;
                    }
                }
                inserter.flush();
            } else if constexpr (is_numeric_type(ColumnTagType::data_type) && is_numeric_type(DataTypeTag::data_type)) {
                using RawType =  typename decltype(value_desc_tag)::DataTypeTag::raw_type;
                using comp = typename arcticdb::Comparable<RawType, ColumnType>;
                auto value = static_cast<typename comp::left_type>(*reinterpret_cast<const RawType*>(val.data_));
                auto column_data = column_with_strings.column_->data();

                util::BitSet::bulk_insert_iterator inserter(*output);
                auto pos = 0u;
                while (auto block = column_data.next<ColumnDescriptorType>()) {
                    auto ptr = reinterpret_cast<const ColumnType*>(block.value().data());
                    const auto row_count = block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i, ++pos) {
                        if(func(value, static_cast<typename comp::right_type>(*ptr++)))
                            inserter = pos;
                    }
                }
                inserter.flush();
            } else {
                util::raise_rte("Cannot compare {} to {} (possible categorical?)", val.type(), column_with_strings.column_->type());
            }
        });
    });
    ARCTICDB_DEBUG(log::version(), "Filtered segment of size {} down to {} bits", output->size(), output->count());

    return VariantData{std::move(output)};
}

template <typename Func>
VariantData binary_comparator(const ColumnWithStrings& left, const ColumnWithStrings& right, Func&& func) {
    util::check(left.column_->row_count() == right.column_->row_count(), "Columns with different row counts ({} and {}) in binary comparator", left.column_->row_count(), right.column_->row_count());
    auto output = std::make_shared<util::BitSet>(static_cast<util::BitSetSizeType>(left.column_->row_count()));

    left.column_->type().visit_tag([&] (auto left_desc_tag) {
        using LeftDescriptorType = std::decay_t<decltype(left_desc_tag)>;
        using LeftTagType =  typename decltype(left_desc_tag)::DataTypeTag;
        using LeftType =  typename LeftTagType::raw_type;
        right.column_->type().visit_tag([&] (auto right_desc_tag ) {
            using RightDescriptorType = std::decay_t<decltype(right_desc_tag)>;
            using RightTagType =  typename decltype(right_desc_tag)::DataTypeTag;
            using RightType =  typename RightTagType::raw_type;
            if constexpr(is_sequence_type(LeftTagType::data_type) && is_sequence_type(RightTagType::data_type)) {
                bool strip_fixed_width_trailing_nulls{false};
                // If one or both columns are fixed width strings, we need to strip trailing null characters to get intuitive results
                if constexpr (is_fixed_string_type(LeftTagType::data_type) || is_fixed_string_type(RightTagType::data_type))
                    strip_fixed_width_trailing_nulls = true;

                auto left_column_data = left.column_->data();
                auto right_column_data = right.column_->data();

                util::BitSet::bulk_insert_iterator inserter(*output);
                auto pos = 0u;
                while (auto left_block = left_column_data.next<LeftDescriptorType>()) {
                    auto right_block = right_column_data.next<RightDescriptorType>();
                    auto left_ptr = reinterpret_cast<const LeftType*>(left_block.value().data());
                    auto right_ptr = reinterpret_cast<const RightType*>(right_block.value().data());
                    const auto row_count = left_block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i, ++pos) {
                        if(func(left.string_at_offset(*left_ptr++, strip_fixed_width_trailing_nulls), right.string_at_offset(*right_ptr++, strip_fixed_width_trailing_nulls)))
                            inserter = pos;
                    }
                }
                inserter.flush();
            } else if constexpr (is_numeric_type(LeftTagType::data_type) && is_numeric_type(RightTagType::data_type)) {
                using comp = typename arcticdb::Comparable<LeftType, RightType>;

                auto left_column_data = left.column_->data();
                auto right_column_data = right.column_->data();

                util::BitSet::bulk_insert_iterator inserter(*output);
                auto pos = 0u;
                while (auto left_block = left_column_data.next<LeftDescriptorType>()) {
                    auto right_block = right_column_data.next<RightDescriptorType>();
                    auto left_ptr = reinterpret_cast<const LeftType*>(left_block.value().data());
                    auto right_ptr = reinterpret_cast<const RightType*>(right_block.value().data());
                    const auto row_count = left_block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i, ++pos) {
                        if(func(static_cast<typename comp::left_type>(*left_ptr++), static_cast<typename comp::right_type>(*right_ptr++)))
                            inserter = pos;
                    }
                }
                inserter.flush();
            } else {
                util::raise_rte("Cannot compare {} to {} (possible categorical?)", left.column_->type(), right.column_->type());
            }
        });
    });
    ARCTICDB_DEBUG(log::version(), "Filtered segment of size {} down to {} bits", output->size(), output->count());

    return VariantData{std::move(output)};
}

template <typename Func>
VariantData binary_comparator(const ColumnWithStrings& column_with_strings, const Value& val, Func&& func) {
    auto output = std::make_shared<util::BitSet>(static_cast<util::BitSetSizeType>(column_with_strings.column_->row_count()));

    column_with_strings.column_->type().visit_tag([&val, &column_with_strings, &func, &output] (auto column_desc_tag) {
        using ColumnDescriptorType = std::decay_t<decltype(column_desc_tag)>;
        using ColumnTagType =  typename ColumnDescriptorType::DataTypeTag;
        using ColumnType =  typename ColumnTagType::raw_type;
        TypeDescriptor value_type{val.data_type_, Dimension::Dim0};
        value_type.visit_tag([&] (auto value_desc_tag ) {
            using DataTypeTag =  typename decltype(value_desc_tag)::DataTypeTag;
            if constexpr(is_sequence_type(ColumnTagType::data_type) && is_sequence_type(DataTypeTag::data_type)) {
                std::optional<std::string> utf32_string;
                std::optional<std::string_view> value_string;
                if constexpr(is_fixed_string_type(ColumnTagType::data_type)) {
                    auto width = column_with_strings.get_fixed_width_string_size();
                    if (width.has_value()) {
                        utf32_string = ascii_to_padded_utf32(std::string_view(*val.str_data(), val.len()), *width);
                        if (utf32_string.has_value()) {
                            value_string = std::string_view(*utf32_string);
                        }
                    }
                } else {
                    value_string = std::string_view(*val.str_data(), val.len());
                }
                auto value_offset = column_with_strings.string_pool_->get_offset_for_column(*value_string, *column_with_strings.column_);
                auto column_data = column_with_strings.column_->data();

                util::BitSet::bulk_insert_iterator inserter(*output);
                auto pos = 0u;
                while (auto block = column_data.next<TypeDescriptorTag<ColumnTagType, DimensionTag<entity::Dimension::Dim0>>>()) {
                    auto ptr = reinterpret_cast<const StringPool::offset_t*>(block.value().data());
                    const auto row_count = block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i, ++pos) {
                        auto offset = *ptr++;
                        if(func(value_offset, offset))
                            inserter = pos;
                    }
                }
                inserter.flush();
            } else if constexpr ((is_numeric_type(ColumnTagType::data_type) && is_numeric_type(DataTypeTag::data_type)) ||
                                 (is_bool_type(ColumnTagType::data_type) && is_bool_type(DataTypeTag::data_type))) {
                using RawType =  typename decltype(value_desc_tag)::DataTypeTag::raw_type;
                using comp = typename arcticdb::Comparable<RawType, ColumnType>;
                auto value = static_cast<typename comp::left_type>(*reinterpret_cast<const RawType*>(val.data_));
                auto column_data = column_with_strings.column_->data();

                util::BitSet::bulk_insert_iterator inserter(*output);
                auto pos = 0u;
                while (auto block = column_data.next<ColumnDescriptorType>()) {
                    auto ptr = reinterpret_cast<const ColumnType*>(block.value().data());
                    const auto row_count = block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i, ++pos) {
                        if(func(static_cast<typename comp::right_type>(*ptr++), value))
                            inserter = pos;
                    }
                }
                inserter.flush();
            } else {
                util::raise_rte("Cannot compare {} to {} (possible categorical?)", column_with_strings.column_->type(), val.type());
            }
        });
    });
    ARCTICDB_DEBUG(log::version(), "Filtered segment of size {} down to {} bits", output->size(), output->count());

    return VariantData{std::move(output)};
}

template<typename Func>
VariantData visit_binary_comparator(const VariantData& left, const VariantData& right, Func&& func) {
    if(std::holds_alternative<EmptyResult>(left) || std::holds_alternative<EmptyResult>(right))
        return EmptyResult{};

    return std::visit(util::overload {
     [&func] (const ColumnWithStrings& l, const std::shared_ptr<Value>& r) ->VariantData  {
        auto result = binary_comparator<decltype(func)>(l, *r, std::forward<Func>(func));
         return transform_to_placeholder(result);
        },
        [&] (const ColumnWithStrings& l, const ColumnWithStrings& r)  ->VariantData {
        auto result = binary_comparator<decltype(func)>(l, r, std::forward<Func>(func));
        return transform_to_placeholder(result);
        },
        [&](const std::shared_ptr<Value>& l, const ColumnWithStrings& r) ->VariantData {
        auto result = binary_comparator<decltype(func)>(*l, r, std::forward<Func>(func));
            return transform_to_placeholder(result);
        },
        [&] ([[maybe_unused]] const std::shared_ptr<Value>& l, [[maybe_unused]] const std::shared_ptr<Value>& r) ->VariantData  {
        util::raise_rte("Two value inputs not accepted to binary comparators");
        },
        [](const auto &, const auto&) -> VariantData {
        util::raise_rte("Bitset/ValueSet inputs not accepted to binary comparators");
    }
    }, left, right);
}

template <typename Func>
VariantData binary_operator(const Value& left, const Value& right, Func&& func) {
    auto output = std::make_unique<Value>();

    details::visit_type(left.type().data_type(), [&](auto left_desc_tag) {
        using LTDT = TypeDescriptorTag<decltype(left_desc_tag), DimensionTag<Dimension::Dim0>>;
        using LeftRawType = typename LTDT::DataTypeTag::raw_type;
        if constexpr(!is_numeric_type(LTDT::DataTypeTag::data_type)) {
            util::raise_rte("Non-numeric type provided to binary operation: {}", left.type());
        }
        auto left_value = *reinterpret_cast<const LeftRawType*>(left.data_);

        details::visit_type(right.type().data_type(), [&](auto right_desc_tag) {
            using RTDT = TypeDescriptorTag<decltype(right_desc_tag), DimensionTag<Dimension::Dim0>>;
            using RightRawType = typename RTDT::DataTypeTag::raw_type;
            if constexpr(!is_numeric_type(RTDT::DataTypeTag::data_type)) {
                util::raise_rte("Non-numeric type provided to binary operation: {}", right.type());
            }
            auto right_value = *reinterpret_cast<const RightRawType*>(right.data_);
            using TargetType = typename type_arithmetic_promoted_type<LeftRawType, RightRawType, std::remove_reference_t<Func>>::type;
            output->data_type_ = data_type_from_raw_type<TargetType>();
            *reinterpret_cast<TargetType*>(output->data_) = func.apply(left_value, right_value);
        });
    });

    return VariantData(std::move(output));
}

template <typename Func>
VariantData binary_operator(const Value& val, const Column& col, Func&& func) {
    std::unique_ptr<Column> output;

    details::visit_type(col.type().data_type(), [&](auto right_desc_tag) {
        using RTDT = TypeDescriptorTag<decltype(right_desc_tag), DimensionTag<Dimension::Dim0>>;
        using RightRawType = typename RTDT::DataTypeTag::raw_type;
        if constexpr(!is_numeric_type(RTDT::DataTypeTag::data_type)) {
            util::raise_rte("Non-numeric type provided to binary operation: {}", col.type());
        }
        auto right_data = col.data();

        details::visit_type(val.type().data_type(), [&](auto left_desc_tag) {
            using LTDT = TypeDescriptorTag<decltype(left_desc_tag), DimensionTag<Dimension::Dim0>>;
            using LeftRawType = typename LTDT::DataTypeTag::raw_type;
            if constexpr(!is_numeric_type(LTDT::DataTypeTag::data_type)) {
                util::raise_rte("Non-numeric type provided to binary operation: {}", val.type());
            }
            auto left_value = *reinterpret_cast<const LeftRawType*>(val.data_);
            using TargetType = typename type_arithmetic_promoted_type<LeftRawType, RightRawType, std::remove_reference_t<Func>>::type;
            auto output_data_type = data_type_from_raw_type<TargetType>();
            output = std::make_unique<Column>(make_scalar_type(output_data_type), col.is_sparse());

            while(auto opt_right_block = right_data.next<RTDT>()) {
                auto right_block = opt_right_block.value();
                const auto nbytes = sizeof(TargetType) * right_block.row_count();
                auto ptr = reinterpret_cast<TargetType*>(output->allocate_data(nbytes));

                for(auto i = 0u; i < right_block.row_count(); ++i)
                    *ptr++ = func.apply(left_value, right_block[i]);

                output->advance_data(nbytes);
            }

            output->set_row_data(col.row_count() - 1);
        });
    });

    return VariantData(ColumnWithStrings(std::move(output)));
}

template <typename Func>
VariantData binary_operator(const Column& left, const Column& right, Func&& func) {
    util::check(left.row_count() == right.row_count(), "Columns with different row counts ({} and {}) in binary operator", left.row_count(), right.row_count());
    std::unique_ptr<Column> output;

    details::visit_type(left.type().data_type(), [&](auto left_desc_tag) {
        using LTDT = TypeDescriptorTag<decltype(left_desc_tag), DimensionTag<Dimension::Dim0>>;
        using LeftRawType = typename LTDT::DataTypeTag::raw_type;
        if constexpr(!is_numeric_type(LTDT::DataTypeTag::data_type)) {
            util::raise_rte("Non-numeric type provided to binary operation: {}", left.type());
        }
        auto left_data = left.data();
        auto opt_left_block =  left_data.next<LTDT>();

        details::visit_type(right.type().data_type(), [&](auto right_desc_tag) {
            using RTDT = TypeDescriptorTag<decltype(right_desc_tag), DimensionTag<Dimension::Dim0>>;
            using RightRawType = typename RTDT::DataTypeTag::raw_type;
            if constexpr(!is_numeric_type(RTDT::DataTypeTag::data_type)) {
                util::raise_rte("Non-numeric type provided to binary operation: {}", right.type());
            }
            using TargetType = typename type_arithmetic_promoted_type<LeftRawType, RightRawType, std::remove_reference_t<Func>>::type;
            auto output_data_type = data_type_from_raw_type<TargetType>();
            output = std::make_unique<Column>(make_scalar_type(output_data_type), left.is_sparse() || right.is_sparse());
            auto right_data = right.data();
            auto opt_right_block = right_data.next<RTDT>();
            while (opt_left_block && opt_right_block) {
                auto& left_block = opt_left_block.value();
                auto& right_block = opt_right_block.value();
                util::check(left_block.row_count() == right_block.row_count(), "Non-matching row counts in dense blocks: {} != {}", left_block.row_count(), right_block.row_count());
                const auto nbytes = sizeof(TargetType) * right_block.row_count();
                auto ptr = reinterpret_cast<TargetType*>(output->allocate_data(nbytes));

                for(auto i = 0u; i < right_block.row_count(); ++i)
                    *ptr++ = func.apply(left_block[i], right_block[i]);

                output->advance_data(nbytes);

                opt_left_block =  left_data.next<LTDT>();
                opt_right_block = right_data.next<RTDT>();
            }
            output->set_row_data(left.row_count() - 1);
        });
    });
    return VariantData(ColumnWithStrings(std::move(output)));
}

template <typename Func>
VariantData binary_operator(const Column& col, const Value& val, Func&& func) {
    std::unique_ptr<Column> output;

    details::visit_type(col.type().data_type(), [&](auto left_desc_tag) {
        using LTDT = TypeDescriptorTag<decltype(left_desc_tag), DimensionTag<Dimension::Dim0>>;
        using LeftRawType = typename LTDT::DataTypeTag::raw_type;
        if constexpr(!is_numeric_type(LTDT::DataTypeTag::data_type)) {
            util::raise_rte("Non-numeric type provided to binary operation: {}", col.type());
        }
        auto left_data = col.data();

        details::visit_type(val.type().data_type(), [&](auto right_desc_tag) {
            using RTDT = TypeDescriptorTag<decltype(right_desc_tag), DimensionTag<Dimension::Dim0>>;
            using RightRawType = typename RTDT::DataTypeTag::raw_type;
            if constexpr(!is_numeric_type(RTDT::DataTypeTag::data_type)) {
                util::raise_rte("Non-numeric type provided to binary operation: {}", val.type());
            }
            auto right_value = *reinterpret_cast<const RightRawType*>(val.data_);
            using TargetType = typename type_arithmetic_promoted_type<LeftRawType, RightRawType, std::remove_reference_t<Func>>::type;
            auto output_data_type = data_type_from_raw_type<TargetType>();
            output = std::make_unique<Column>(make_scalar_type(output_data_type), col.is_sparse());

            while(auto opt_left_block = left_data.next<LTDT>()) {
                auto left_block = opt_left_block.value();
                const auto nbytes = sizeof(TargetType) * left_block.row_count();
                auto ptr = reinterpret_cast<TargetType*>(output->allocate_data(nbytes));

                for(auto i = 0u; i < left_block.row_count(); ++i)
                    *ptr++ = func.apply(left_block[i], right_value);

                output->advance_data(nbytes);
            }

            output->set_row_data(col.row_count() - 1);
        });
    });

    return { ColumnWithStrings(std::move(output))};
}

template<typename Func>
VariantData visit_binary_operator(const VariantData& left, const VariantData& right, Func&& func) {
    if(std::holds_alternative<EmptyResult>(left) || std::holds_alternative<EmptyResult>(right))
        return EmptyResult{};

    return std::visit(util::overload {
        [&] (const ColumnWithStrings& l, const std::shared_ptr<Value>& r) ->VariantData  {
            return binary_operator<decltype(func)>(*(l.column_), *r, std::forward<Func>(func));
            },
            [&] (const ColumnWithStrings& l, const ColumnWithStrings& r)  ->VariantData {
            return binary_operator<decltype(func)>(*(l.column_), *(r.column_), std::forward<Func>(func));
            },
            [&](const std::shared_ptr<Value>& l, const ColumnWithStrings& r) ->VariantData {
            return binary_operator<decltype(func)>(*l, *(r.column_), std::forward<Func>(func));
            },
            [&] (const std::shared_ptr<Value>& l, const std::shared_ptr<Value>& r) -> VariantData {
            return binary_operator<decltype(func)>(*l, *r, std::forward<Func>(func));
            },
            [](const auto &, const auto&) -> VariantData {
            util::raise_rte("Bitset/ValueSet inputs not accepted to binary operators");
        }
        }, left, right);
}

VariantData dispatch_binary(const VariantData& left, const VariantData& right, OperationType operation) {
    switch(operation) {
    case OperationType::ADD:
        return visit_binary_operator(left, right, PlusOperator{});
    case OperationType::SUB:
        return visit_binary_operator(left, right, MinusOperator{});
    case OperationType::MUL:
        return visit_binary_operator(left, right, TimesOperator{});
    case OperationType::DIV:
        return visit_binary_operator(left, right, DivideOperator{});
    case OperationType::EQ:
        return visit_binary_comparator(left, right, EqualsOperator{});
    case OperationType::NE:
        return visit_binary_comparator(left, right, NotEqualsOperator{});
    case OperationType::LT:
        return visit_binary_comparator(left, right, LessThanOperator{});
    case OperationType::LE:
        return visit_binary_comparator(left, right, LessThanEqualsOperator{});
    case OperationType::GT:
        return visit_binary_comparator(left, right, GreaterThanOperator{});
    case OperationType::GE:
        return visit_binary_comparator(left, right, GreaterThanEqualsOperator{});
    case OperationType::ISIN:
        return visit_binary_membership(left, right, IsInOperator{});
    case OperationType::ISNOTIN:
        return visit_binary_membership(left, right, IsNotInOperator{});
    case OperationType::AND:
    case OperationType::OR:
    case OperationType::XOR:
        return visit_binary_boolean(left, right, operation);
    default:
        util::raise_rte("Unknown operation {}", int(operation));
    }
}

VariantData unary_boolean(const std::shared_ptr<util::BitSet>& bitset, OperationType operation) {
    switch(operation) {
    case OperationType::IDENTITY:
        return bitset;
    case OperationType::NOT:
        return std::make_shared<util::BitSet>(~(*bitset));
    default:
        util::raise_rte("Unexpected operator in unary_boolean {}", int(operation));
    }
}

VariantData unary_boolean(EmptyResult, OperationType operation) {
    switch(operation) {
    case OperationType::IDENTITY:
        return EmptyResult{};
    case OperationType::NOT:
        return FullResult{};
    default:
        util::raise_rte("Unexpected operator in unary_boolean {}", int(operation));
    }
}

VariantData unary_boolean(FullResult, OperationType operation) {
    switch(operation) {
    case OperationType::IDENTITY:
        return FullResult{};
    case OperationType::NOT:
        return EmptyResult{};
    default:
        util::raise_rte("Unexpected operator in unary_boolean {}", int(operation));
    }
}

VariantData visit_unary_boolean(const VariantData& left, OperationType operation) {
    auto data = transform_to_bitset(left);
    return std::visit(util::overload{
        [operation] (const std::shared_ptr<util::BitSet>& d) -> VariantData {
            return transform_to_placeholder(unary_boolean(d, operation));
        },
        [operation](EmptyResult d) {
            return transform_to_placeholder(unary_boolean(d, operation));
        },
        [operation](FullResult d) {
            return transform_to_placeholder(unary_boolean(d, operation));
        },
        [](const auto &) -> VariantData {
            util::raise_rte("Value/ValueSet/non-bool column inputs not accepted to unary boolean");
        }
        }, data);
}

template <typename Func>
VariantData unary_operator(const Value& val, Func&& func) {
    auto output = std::make_unique<Value>();
    output->data_type_ = val.data_type_;

    details::visit_type(val.type().data_type(), [&](auto val_desc_tag) {
        using DataTagType =  decltype(val_desc_tag);
        if constexpr (!is_numeric_type(DataTagType::data_type)) {
            util::raise_rte("Cannot perform arithmetic on {}", val.type());
        }
        using DT = TypeDescriptorTag<decltype(val_desc_tag), DimensionTag<Dimension::Dim0>>;
        using RawType = typename DT::DataTypeTag::raw_type;
        auto value = *reinterpret_cast<const RawType*>(val.data_);
        using TargetType = typename unary_arithmetic_promoted_type<RawType, std::remove_reference_t<Func>>::type;
        output->data_type_ = data_type_from_raw_type<TargetType>();
        *reinterpret_cast<TargetType*>(output->data_) = func.apply(value);
    });

    return {std::move(output)};
}

template <typename Func>
VariantData unary_operator(const Column& col, Func&& func) {
    std::unique_ptr<Column> output;

    details::visit_type(col.type().data_type(), [&](auto col_desc_tag) {
        using ColumnTagType =  decltype(col_desc_tag);
        if constexpr (!is_numeric_type(ColumnTagType::data_type)) {
            util::raise_rte("Cannot perform arithmetic on {}", col.type());
        }
        using DT = TypeDescriptorTag<decltype(col_desc_tag), DimensionTag<Dimension::Dim0>>;
        using RawType = typename DT::DataTypeTag::raw_type;
        using TargetType = typename unary_arithmetic_promoted_type<RawType, std::remove_reference_t<Func>>::type;
        auto output_data_type = data_type_from_raw_type<TargetType>();
        output = std::make_unique<Column>(make_scalar_type(output_data_type), col.is_sparse());
        auto data = col.data();
        while(auto opt_block = data.next<DT>()) {
            auto block = opt_block.value();
            const auto nbytes = sizeof(TargetType) * block.row_count();
            auto ptr = reinterpret_cast<TargetType*>(output->allocate_data(nbytes));
            for(auto idx = 0u; idx < block.row_count(); ++idx)
                *ptr++ = func.apply(block[idx]);

            output->advance_data(nbytes);
        }
        output->set_row_data(col.row_count() - 1);
    });
    return {ColumnWithStrings(std::move(output))};
}

template<typename Func>
VariantData visit_unary_operator(const VariantData& left, Func&& func) {
    return std::visit(util::overload{
        [&] (const ColumnWithStrings& l) -> VariantData {
            return unary_operator(*(l.column_), std::forward<Func>(func));
            },
        [&] (const std::shared_ptr<Value>& l) -> VariantData {
            return unary_operator(*l, std::forward<Func>(func));
        },
        [] (EmptyResult l) -> VariantData {
            return l;
        },
        [](const auto&) -> VariantData {
            util::raise_rte("Bitset/ValueSet inputs not accepted to unary operators");
        }
    }, left);
}

VariantData dispatch_unary(const VariantData& left, OperationType operation) {
switch(operation) {
    case OperationType::ABS:
        return visit_unary_operator(left, AbsOperator());
    case OperationType::NEG:
        return visit_unary_operator(left, NegOperator());
        case OperationType::IDENTITY:
        case OperationType::NOT:
        return visit_unary_boolean(left, operation);
    default:
        util::raise_rte("Unknown operation {}", int(operation));
    }
}

}
