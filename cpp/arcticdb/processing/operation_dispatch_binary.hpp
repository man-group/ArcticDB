/*
 * Copyright 2023 Man Group Operations Limited
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
#include <arcticdb/processing/operation_dispatch.hpp>
#include <arcticdb/processing/expression_node.hpp>
#include <arcticdb/entity/type_conversion.hpp>

namespace arcticdb {

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, const std::shared_ptr<util::BitSet>& right, OperationType operation);

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, EmptyResult, OperationType operation);

VariantData binary_boolean(const std::shared_ptr<util::BitSet>& left, FullResult, OperationType operation);

VariantData binary_boolean(EmptyResult, FullResult, OperationType operation);

VariantData binary_boolean(FullResult, FullResult, OperationType operation);

VariantData binary_boolean(EmptyResult, EmptyResult, OperationType operation);

// Note that we can have fewer of these and reverse the parameters because all the operations are
// commutative, however if that were to change we would need the full set
VariantData visit_binary_boolean(const VariantData& left, const VariantData& right, OperationType operation);

template <typename Func>
VariantData binary_membership(const ColumnWithStrings& column_with_strings, ValueSet& value_set, Func&& func) {
    if (is_empty_type(column_with_strings.column_->type().data_type())) {
        if constexpr(std::is_same_v<Func, IsInOperator&&>) {
            return EmptyResult{};
        } else if constexpr(std::is_same_v<Func, IsNotInOperator&&>) {
            return FullResult{};
        } else {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected operator passed to binary_membership");
        }
    }
    auto output = std::make_shared<util::BitSet>(column_with_strings.column_->row_count());
    // If the value set is empty, then for IsInOperator, we are done
    // For the IsNotInOperator, set all bits to 1
    if (value_set.empty()) {
        if constexpr(std::is_same_v<Func, IsNotInOperator&&>)
            output->set();
    } else {
        entity::details::visit_type(column_with_strings.column_->type().data_type(),[&column_with_strings, &value_set, &func, &output] (auto column_desc_tag) {
            using ColumnTagType = typename std::decay_t<decltype(column_desc_tag)>;
            using ColumnType = typename ColumnTagType::raw_type;

            entity::details::visit_type(value_set.base_type().data_type(), [&] (auto value_set_desc_tag) {
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
                            if constexpr (MembershipOperator::needs_uint64_special_handling<ColumnType, ValueSetBaseType>) {
                                // Avoid narrowing conversion on *ptr:
                                if (func(*ptr++, *typed_value_set, UInt64SpecialHandlingTag{}))
                                    inserter = pos;
                            } else {
                                if (func(static_cast<WideType>(*ptr++), *typed_value_set))
                                    inserter = pos;
                            }
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
            return transform_to_placeholder(binary_membership<decltype(func)>(l, *r, std::forward<decltype(func)>(func)));
            },
            [](const auto &, const auto&) -> VariantData {
            util::raise_rte("Binary membership operations must be Column/ValueSet");
        }
        }, left, right);
}

template <typename Func>
VariantData binary_comparator(const Value& val, const ColumnWithStrings& column_with_strings, Func&& func) {
    if (is_empty_type(column_with_strings.column_->type().data_type())) {
        return EmptyResult{};
    }
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
    if (is_empty_type(left.column_->type().data_type()) || is_empty_type(right.column_->type().data_type())) {
        return EmptyResult{};
    }
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
            } else if constexpr ((is_numeric_type(LeftTagType::data_type) && is_numeric_type(RightTagType::data_type)) ||
                                 (is_bool_type(LeftTagType::data_type) && is_bool_type(RightTagType::data_type))) {
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
    if (is_empty_type(column_with_strings.column_->type().data_type())) {
        return EmptyResult{};
    }
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
                using RawType = typename decltype(value_desc_tag)::DataTypeTag::raw_type;
                using comp = typename arcticdb::Comparable<RawType, ColumnType>;
                auto value = static_cast<typename comp::left_type>(*reinterpret_cast<const RawType *>(val.data_));
                auto column_data = column_with_strings.column_->data();

                util::BitSet::bulk_insert_iterator inserter(*output);
                auto pos = 0u;
                while (auto block = column_data.next<ColumnDescriptorType>()) {
                    auto ptr = reinterpret_cast<const ColumnType *>(block.value().data());
                    const auto row_count = block.value().row_count();
                    for (auto i = 0u; i < row_count; ++i, ++pos) {
                        if (func(static_cast<typename comp::right_type>(*ptr++), value))
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
        auto result = binary_comparator<decltype(func)>(l, *r, std::forward<decltype(func)>(func));
         return transform_to_placeholder(result);
        },
        [&] (const ColumnWithStrings& l, const ColumnWithStrings& r)  ->VariantData {
        auto result = binary_comparator<decltype(func)>(l, r, std::forward<decltype(func)>(func));
        return transform_to_placeholder(result);
        },
        [&](const std::shared_ptr<Value>& l, const ColumnWithStrings& r) ->VariantData {
        auto result = binary_comparator<decltype(func)>(*l, r, std::forward<decltype(func)>(func));
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
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.type().data_type()),
            "Empty column provided to binary operator");
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
            using TargetType = typename type_arithmetic_promoted_type<LeftRawType, RightRawType, std::remove_reference_t<decltype(func)>>::type;
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
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(left.type().data_type()) && !is_empty_type(right.type().data_type()),
            "Empty column provided to binary operator");
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
            using TargetType = typename type_arithmetic_promoted_type<LeftRawType, RightRawType, std::remove_reference_t<decltype(func)>>::type;
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
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.type().data_type()),
            "Empty column provided to binary operator");
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
            using TargetType = typename type_arithmetic_promoted_type<LeftRawType, RightRawType, std::remove_reference_t<decltype(func)>>::type;
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
            return binary_operator<decltype(func)>(*(l.column_), *r, std::forward<decltype(func)>(func));
            },
            [&] (const ColumnWithStrings& l, const ColumnWithStrings& r)  ->VariantData {
            return binary_operator<decltype(func)>(*(l.column_), *(r.column_), std::forward<decltype(func)>(func));
            },
            [&](const std::shared_ptr<Value>& l, const ColumnWithStrings& r) ->VariantData {
            return binary_operator<decltype(func)>(*l, *(r.column_), std::forward<decltype(func)>(func));
            },
            [&] (const std::shared_ptr<Value>& l, const std::shared_ptr<Value>& r) -> VariantData {
            return binary_operator<decltype(func)>(*l, *r, std::forward<decltype(func)>(func));
            },
            [](const auto &, const auto&) -> VariantData {
            util::raise_rte("Bitset/ValueSet inputs not accepted to binary operators");
        }
        }, left, right);
}

VariantData dispatch_binary(const VariantData& left, const VariantData& right, OperationType operation);

// instantiated in operation_dispatch_binary_operator.cpp to reduce compilation memory use
extern template
VariantData visit_binary_operator<arcticdb::PlusOperator>(const VariantData&, const VariantData&, PlusOperator&&);
extern template
VariantData visit_binary_operator<arcticdb::MinusOperator>(const VariantData&, const VariantData&, MinusOperator&&);
extern template
VariantData visit_binary_operator<arcticdb::TimesOperator>(const VariantData&, const VariantData&, TimesOperator&&);
extern template
VariantData visit_binary_operator<arcticdb::DivideOperator>(const VariantData&, const VariantData&, DivideOperator&&);

// instantiated in operation_dispatch_binary_comparator.cpp to reduce compilation memory use
extern template
VariantData visit_binary_comparator<EqualsOperator>(const VariantData&, const VariantData&, EqualsOperator&&);
extern template
VariantData visit_binary_comparator<NotEqualsOperator>(const VariantData&, const VariantData&, NotEqualsOperator&&);
extern template
VariantData visit_binary_comparator<LessThanOperator>(const VariantData&, const VariantData&, LessThanOperator&&);
extern template
VariantData visit_binary_comparator<LessThanEqualsOperator>(const VariantData&, const VariantData&, LessThanEqualsOperator&&);
extern template
VariantData visit_binary_comparator<GreaterThanOperator>(const VariantData&, const VariantData&, GreaterThanOperator&&);
extern template
VariantData visit_binary_comparator<GreaterThanEqualsOperator>(const VariantData&, const VariantData&, GreaterThanEqualsOperator&&);

}
