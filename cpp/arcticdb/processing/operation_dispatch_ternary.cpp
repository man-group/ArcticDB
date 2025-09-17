/*
 * Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <memory>

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/operation_dispatch.hpp>
#include <arcticdb/processing/operation_dispatch_ternary.hpp>
#include <arcticdb/processing/ternary_utils.hpp>

namespace arcticdb {

template<bool arguments_reversed = false>
inline std::string ternary_operation_column_name(std::string_view left, std::string_view right) {
    if constexpr (arguments_reversed) {
        return fmt::format("(COND ? {} : {})", right, left);
    } else {
        return fmt::format("(COND ? {} : {})", left, right);
    }
}

template<bool arguments_reversed = false>
inline std::string ternary_operation_with_types_to_string(
        std::string_view left, const TypeDescriptor& type_left, std::string_view right, const TypeDescriptor& type_right
) {
    if constexpr (arguments_reversed) {
        return fmt::format(
                "{} ({}) : {} ({})",
                right,
                get_user_friendly_type_string(type_right),
                left,
                get_user_friendly_type_string(type_left)
        );
    } else {
        return fmt::format(
                "{} ({}) : {} ({})",
                left,
                get_user_friendly_type_string(type_left),
                right,
                get_user_friendly_type_string(type_right)
        );
    }
}

// This handles the filter case where we are choosing between two filters e.g.
// lazy_df = lazy_df[where(lazy_df["col1"] < 0, lazy_df["col2"] < 0, lazy_df["col3"] < 0)]
VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& left, const util::BitSet& right) {
    util::BitSet output_bitset;
    auto output_size = condition.size();
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            left.size() == output_size && right.size() == output_size, "Mismatching bitset sizes"
    );
    output_bitset = (condition & left) | (~condition & right);
    output_bitset.resize(output_size);
    return output_bitset;
}

// This handles the filter case where we select based on another filter if condition is true, or a fixed bool value
// otherwise e.g. lazy_df = lazy_df[where(lazy_df["col1"] < 0, lazy_df["col2"] < 0, True)] arguments_reversed refers to
// a case like lazy_df = lazy_df[where(lazy_df["col1"] < 0, True, lazy_df["col2"] < 0)]
template<bool arguments_reversed>
VariantData ternary_operator(const util::BitSet& condition, const util::BitSet& input_bitset, bool value) {
    util::BitSet output_bitset;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            input_bitset.size() == condition.size(), "Mismatching bitset sizes"
    );
    if constexpr (arguments_reversed) {
        if (value) {
            output_bitset = condition | input_bitset;
        } else {
            output_bitset = ~condition & input_bitset;
        }
    } else {
        if (value) {
            output_bitset = ~condition | input_bitset;
        } else {
            output_bitset = condition & input_bitset;
        }
    }
    output_bitset.resize(condition.size());
    return VariantData{std::move(output_bitset)};
}

// This handles the projection case where we produce a new column by selecting values from two input columns e.g.
// lazy_df["new_col"] = where(lazy_df["col1"] < 0, lazy_df["col2"], lazy_df["col3"] + lazy_df["col4"])
VariantData ternary_operator(
        const util::BitSet& condition, const ColumnWithStrings& left, const ColumnWithStrings& right
) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(left.column_->type().data_type()) && !is_empty_type(right.column_->type().data_type()),
            "Empty column provided to ternary operator"
    );
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;

    details::visit_type(left.column_->type().data_type(), [&](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.column_->type().data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr (is_sequence_type(left_type_info::data_type) && is_sequence_type(right_type_info::data_type)) {
                if constexpr (left_type_info::data_type == right_type_info::data_type &&
                              is_dynamic_string_type(left_type_info::data_type)) {
                    output_column =
                            std::make_unique<Column>(make_scalar_type(DataType::UTF_DYNAMIC64), Sparsity::PERMITTED);
                    // If both columns came from the same segment in storage, and therefore have the same string pool,
                    // then this is MUCH faster, as we do not need to create a new string pool, we can just work with
                    // offsets
                    if (left.string_pool_ == right.string_pool_) {
                        string_pool = left.string_pool_;
                        ternary_transform<
                                typename left_type_info::TDT,
                                typename right_type_info::TDT,
                                typename left_type_info::TDT>(
                                condition,
                                *(left.column_),
                                *(right.column_),
                                *output_column,
                                [](bool cond, entity::position_t left_val, entity::position_t right_val) ->
                                typename entity::position_t { return cond ? left_val : right_val; }
                        );
                    } else {
                        string_pool = std::make_shared<StringPool>();
                        ternary_transform<
                                typename left_type_info::TDT,
                                typename right_type_info::TDT,
                                typename left_type_info::TDT>(
                                condition,
                                *(left.column_),
                                *(right.column_),
                                *output_column,
                                [&string_pool, &left, &right](
                                        bool cond, entity::position_t left_val, entity::position_t right_val
                                ) -> typename entity::position_t {
                                    if (cond) {
                                        // This is a bit faster than ColumnWithStrings::string_at_offset in this case as
                                        // none of the additional checks/code are useful
                                        if (is_a_string(left_val)) {
                                            return string_pool->get(left.string_pool_->get_const_view(left_val))
                                                    .offset();
                                        } else {
                                            return left_val;
                                        }
                                    } else {
                                        if (is_a_string(right_val)) {
                                            return string_pool->get(right.string_pool_->get_const_view(right_val))
                                                    .offset();
                                        } else {
                                            return right_val;
                                        }
                                    }
                                }
                        );
                    }
                } else {
                    // Fixed width string columns
                    schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                            "Ternary operator does not support fixed width string columns '{}' and '{}'",
                            left.column_name_,
                            right.column_name_
                    );
                }
            } else if constexpr ((is_numeric_type(left_type_info::data_type) &&
                                  is_numeric_type(right_type_info::data_type)) ||
                                 (is_bool_type(left_type_info::data_type) && is_bool_type(right_type_info::data_type)
                                 )) {
                using TargetType = typename ternary_operation_promoted_type<
                        typename left_type_info::RawType,
                        typename right_type_info::RawType>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                ternary_transform<
                        typename left_type_info::TDT,
                        typename right_type_info::TDT,
                        ScalarTagType<DataTypeTag<output_data_type>>>(
                        condition,
                        *(left.column_),
                        *(right.column_),
                        *output_column,
                        [](bool condition, TargetType left_val, TargetType right_val) {
                            return condition ? left_val : right_val;
                        }
                );
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        "Invalid ternary operator arguments {}",
                        ternary_operation_with_types_to_string(
                                left.column_name_, left.column_->type(), right.column_name_, right.column_->type()
                        )
                );
            }
        });
    });
    return {ColumnWithStrings(
            std::move(output_column), string_pool, ternary_operation_column_name(left.column_name_, right.column_name_)
    )};
}

// This handles the projection case where we produce a new column by selecting values from an input column where
// condition is true, or a fixed value otherwise e.g. lazy_df["new_col"] = where(lazy_df["col1"] < 0, lazy_df["col2"],
// 5) arguments_reversed refers to a case like lazy_df["new_col"] = where(lazy_df["col1"] < 0, 5, lazy_df["col2"])
template<bool arguments_reversed>
VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& col, const Value& val) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()), "Empty column provided to ternary operator"
    );
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;
    std::string value_string;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        details::visit_type(val.data_type(), [&](auto val_tag) {
            using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
            if constexpr (is_sequence_type(col_type_info::data_type) && is_sequence_type(val_type_info::data_type)) {
                if constexpr (is_dynamic_string_type(col_type_info::data_type)) {
                    output_column =
                            std::make_unique<Column>(make_scalar_type(DataType::UTF_DYNAMIC64), Sparsity::PERMITTED);
                    // It would be nice if we could just reuse the input column's string pool, and insert the value into
                    // it In experiments this is x7-x40 times faster than the current approach, depending on the unique
                    // count of strings in this column
                    // Unfortunately, if the value string is already in the column's string pool, this breaks some
                    // downstream processes, such as SegmentInMemoryImpl::filter when filter_down_stringpool is true,
                    // which rely on uniqueness of values in the string pool
                    // As there is no efficient way to check if a string is in the pool, this optimisation cannot be
                    // applied
                    string_pool = std::make_shared<StringPool>();
                    value_string = std::string(*val.str_data(), val.len());
                    // Put the value string in the string pool as it will probably be needed, and so that we can just
                    // work with offsets in the lambda passed to ternary_transform
                    auto value_offset = string_pool->get(value_string).offset();
                    ternary_transform<typename col_type_info::TDT, typename col_type_info::TDT, arguments_reversed>(
                            condition,
                            *(col.column_),
                            value_offset,
                            *output_column,
                            [&string_pool, &col](bool cond, entity::position_t left_val, entity::position_t right_val)
                                    -> entity::position_t {
                                if (cond) {
                                    // This is a bit faster than ColumnWithStrings::string_at_offset in this case as
                                    // none of the additional checks/code are useful
                                    if (is_a_string(left_val)) {
                                        return string_pool->get(col.string_pool_->get_const_view(left_val)).offset();
                                    } else {
                                        return left_val;
                                    }
                                } else {
                                    return right_val;
                                }
                            }
                    );
                } else {
                    // Fixed width string column
                    schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                            "Ternary operator does not support fixed width string columns '{}'", col.column_name_
                    );
                }
            } else if constexpr ((is_numeric_type(col_type_info::data_type) && is_numeric_type(val_type_info::data_type)
                                 ) ||
                                 (is_bool_type(col_type_info::data_type) && is_bool_type(val_type_info::data_type))) {
                using TargetType = typename ternary_operation_promoted_type<
                        typename col_type_info::RawType,
                        typename val_type_info::RawType>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                auto value = static_cast<TargetType>(val.get<typename val_type_info::RawType>());
                value_string = fmt::format("{}", value);
                ternary_transform<
                        typename col_type_info::TDT,
                        ScalarTagType<DataTypeTag<output_data_type>>,
                        arguments_reversed>(
                        condition,
                        *(col.column_),
                        value,
                        *output_column,
                        [](bool condition, TargetType left_val, TargetType right_val) {
                            return condition ? left_val : right_val;
                        }
                );
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        "Invalid ternary operator arguments {}",
                        ternary_operation_with_types_to_string<arguments_reversed>(
                                col.column_name_,
                                col.column_->type(),
                                val.to_string<typename val_type_info::RawType>(),
                                val.descriptor()
                        )
                );
            }
        });
    });
    return {ColumnWithStrings(
            std::move(output_column),
            string_pool,
            ternary_operation_column_name<arguments_reversed>(col.column_name_, value_string)
    )};
}

// This handles the projection case where we produce a new column by selecting values from two input columns e.g.
// lazy_df["new_col"] = where(lazy_df["col1"] < 0, lazy_df["col2"], lazy_df["col3"])
// but we have dynamic schema enabled, and one of the columns is missing from some row slices
// arguments_reversed refers to which column is missing from the row slice. In the example above:
// - true:  col2 is missing
// - false: col3 is missing
template<bool arguments_reversed>
VariantData ternary_operator(const util::BitSet& condition, const ColumnWithStrings& col, EmptyResult) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !is_empty_type(col.column_->type().data_type()), "Empty column provided to ternary operator"
    );
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;

    details::visit_type(col.column_->type().data_type(), [&](auto col_tag) {
        using col_type_info = ScalarTypeInfo<decltype(col_tag)>;
        if constexpr (is_dynamic_string_type(col_type_info::data_type) || is_numeric_type(col_type_info::data_type) ||
                      is_bool_type(col_type_info::data_type)) {
            if constexpr (is_dynamic_string_type(col_type_info::data_type)) {
                // We do not need to construct a new string pool, as all the strings in the output column come from the
                // input column
                string_pool = col.string_pool_;
            }
            output_column = std::make_unique<Column>(col.column_->type(), Sparsity::PERMITTED);
            ternary_transform<typename col_type_info::TDT, arguments_reversed>(
                    condition,
                    *col.column_,
                    EmptyResult{},
                    *output_column,
                    [](typename col_type_info::RawType val) { return val; }
            );
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Invalid ternary operator arguments {}",
                    ternary_operation_with_types_to_string<arguments_reversed>(
                            col.column_name_, col.column_->type(), "", {}
                    )
            );
        }
    });
    return {ColumnWithStrings(
            std::move(output_column),
            string_pool,
            ternary_operation_column_name<arguments_reversed>(col.column_name_, "")
    )};
}

template VariantData ternary_operator<true>(const util::BitSet& condition, const ColumnWithStrings& col, EmptyResult);
template VariantData ternary_operator<false>(const util::BitSet& condition, const ColumnWithStrings& col, EmptyResult);

// This handles the projection case where we produce a new column by selecting from 2 fixed values e.g.
// lazy_df["new_col"] = where(lazy_df["col1"] < 0, 5, 10)
// Note that this requires condition.size() to be equal to the number of rows in the row-slice being processed, as we
// have no other way to know how large to make the output column
// This is enforced, both here and in other filtering operations, by calling resize on bitsets produced, as many
// operations (such as flip()) can change the size of a bitset
VariantData ternary_operator(const util::BitSet& condition, const Value& left, const Value& right) {
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;
    std::string left_string;
    std::string right_string;

    details::visit_type(left.data_type(), [&](auto left_tag) {
        using left_type_info = ScalarTypeInfo<decltype(left_tag)>;
        details::visit_type(right.data_type(), [&](auto right_tag) {
            using right_type_info = ScalarTypeInfo<decltype(right_tag)>;
            if constexpr (is_sequence_type(left_type_info::data_type) && is_sequence_type(right_type_info::data_type)) {
                if constexpr (left_type_info::data_type == right_type_info::data_type &&
                              is_dynamic_string_type(left_type_info::data_type)) {
                    output_column =
                            std::make_unique<Column>(make_scalar_type(left_type_info::data_type), Sparsity::PERMITTED);
                    string_pool = std::make_shared<StringPool>();
                    left_string = std::string(*left.str_data(), left.len());
                    right_string = std::string(*right.str_data(), right.len());
                    // Put both possible strings in the pool for performance, it's possible one will be redundant if
                    // condition is all true or all false
                    auto left_offset = string_pool->get(left_string, false).offset();
                    auto right_offset = string_pool->get(right_string, false).offset();
                    ternary_transform<typename left_type_info::TDT>(
                            condition, left_offset, right_offset, *output_column
                    );
                } else {
                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                            "Unexpected fixed-width string value in ternary operator"
                    );
                }
            } else if constexpr ((is_numeric_type(left_type_info::data_type) &&
                                  is_numeric_type(right_type_info::data_type)) ||
                                 (is_bool_type(left_type_info::data_type) && is_bool_type(right_type_info::data_type)
                                 )) {
                using TargetType = typename ternary_operation_promoted_type<
                        typename left_type_info::RawType,
                        typename right_type_info::RawType>::type;
                constexpr auto output_data_type = data_type_from_raw_type<TargetType>();
                output_column = std::make_unique<Column>(make_scalar_type(output_data_type), Sparsity::PERMITTED);
                auto left_value = static_cast<TargetType>(left.get<typename left_type_info::RawType>());
                auto right_value = static_cast<TargetType>(right.get<typename right_type_info::RawType>());
                left_string = fmt::format("{}", left_value);
                right_string = fmt::format("{}", right_value);
                ternary_transform<ScalarTagType<DataTypeTag<output_data_type>>>(
                        condition, left_value, right_value, *output_column
                );
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        "Invalid ternary operator arguments {}",
                        ternary_operation_with_types_to_string(
                                left.to_string<typename right_type_info::RawType>(),
                                left.descriptor(),
                                right.to_string<typename right_type_info::RawType>(),
                                right.descriptor()
                        )
                );
            }
        });
    });
    return {ColumnWithStrings(
            std::move(output_column), string_pool, ternary_operation_column_name(left_string, right_string)
    )};
}

// This handles the projection case where we produce a new column by selecting values from an input column where
// condition is true, or a fixed value otherwise e.g. lazy_df["new_col"] = where(lazy_df["col1"] < 0, lazy_df["col2"],
// 5) but we have dynamic schema enabled, and the column is missing from some row slices arguments_reversed refers to a
// case like lazy_df["new_col"] = where(lazy_df["col1"] < 0, 5, lazy_df["col2"])
template<bool arguments_reversed>
VariantData ternary_operator(const util::BitSet& condition, const Value& val, EmptyResult) {
    std::unique_ptr<Column> output_column;
    std::shared_ptr<StringPool> string_pool;
    std::string value_string;

    details::visit_type(val.data_type(), [&](auto val_tag) {
        using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
        if constexpr (is_dynamic_string_type(val_type_info::data_type)) {
            output_column = std::make_unique<Column>(val.descriptor(), Sparsity::PERMITTED);
            string_pool = std::make_shared<StringPool>();
            value_string = std::string(*val.str_data(), val.len());
            auto offset_string = string_pool->get(value_string);
            ternary_transform<typename val_type_info::TDT, arguments_reversed>(
                    condition, offset_string.offset(), EmptyResult{}, *output_column
            );
        } else if constexpr (is_numeric_type(val_type_info::data_type) || is_bool_type(val_type_info::data_type)) {
            using TargetType = val_type_info::RawType;
            output_column = std::make_unique<Column>(val.descriptor(), Sparsity::PERMITTED);
            auto value = static_cast<TargetType>(val.get<typename val_type_info::RawType>());
            value_string = fmt::format("{}", value);
            ternary_transform<typename val_type_info::TDT, arguments_reversed>(
                    condition, value, EmptyResult{}, *output_column
            );
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Invalid ternary operator arguments {}",
                    ternary_operation_with_types_to_string<arguments_reversed>(
                            val.to_string<typename val_type_info::RawType>(), val.descriptor(), "", {}
                    )
            );
        }
    });
    return {ColumnWithStrings(
            std::move(output_column), string_pool, ternary_operation_column_name<arguments_reversed>(value_string, "")
    )};
}

template VariantData ternary_operator<true>(const util::BitSet& condition, const Value& val, EmptyResult);
template VariantData ternary_operator<false>(const util::BitSet& condition, const Value& val, EmptyResult);

// This handles the filter case where we select based on two fixed bool values. This could be produced from Python like:
// lazy_df = lazy_df[where(lazy_df["col1"] < 0, True, False)]
// although this is dumb, as the same effect could be achieved with:
// lazy_df = lazy_df[lazy_df["col1"] < 0]
// It is actually here to cope with cases where input processing produced FullResult or EmptyResult, which are then
// combined with a fixed bool value
VariantData ternary_operator(const util::BitSet& condition, bool left, bool right) {
    util::BitSet output_bitset;
    if (left && right) {
        if (!condition.empty()) {
            output_bitset.set_range(0, condition.size() - 1);
        }
    } else if (left) {
        // right is false
        output_bitset = condition;
    } else if (right) {
        // left is false
        output_bitset = ~condition;
    } // else left and right are false, the resize below will correctly initialise all bits to zero
    output_bitset.resize(condition.size());
    return output_bitset;
}

VariantData visit_ternary_operator(const VariantData& condition, const VariantData& left, const VariantData& right) {
    if (std::holds_alternative<FullResult>(condition)) {
        return left;
    } else if (std::holds_alternative<EmptyResult>(condition)) {
        return right;
    }
    auto transformed_condition = transform_to_bitset(condition);
    // transformed_condition is a bitset. transform_to_bitset throws if provided a Value or ValueSet. If it is a column,
    // it throws if the column type is not bool, and converts bool columns to a bitset, and full/empty results were
    // handled above
    auto c = std::get<util::BitSet>(std::move(transformed_condition));
    // Of the possible types in VariantData, all except ValueSet can make sense here. Using the ordering:
    // BitSet
    // ColumnWithStrings
    // Value
    // FullResult
    // EmptyResult
    // All such combinations, including the reversing of arguments, are handled below
    return std::visit(
            util::overload{
                    [&c](const util::BitSet& l, const util::BitSet& r) -> VariantData {
                        auto result = ternary_operator(c, l, r);
                        return transform_to_placeholder(result);
                    },
                    [&c](const util::BitSet& l, const ColumnWithStrings& r) -> VariantData {
                        auto bitset = std::get<util::BitSet>(transform_to_bitset(r));
                        auto result = ternary_operator(c, l, bitset);
                        return transform_to_placeholder(result);
                    },
                    [&c](const ColumnWithStrings& l, const util::BitSet& r) -> VariantData {
                        auto bitset = std::get<util::BitSet>(transform_to_bitset(l));
                        auto result = ternary_operator(c, bitset, r);
                        return transform_to_placeholder(result);
                    },
                    [&c](const util::BitSet& l, const std::shared_ptr<Value>& r) -> VariantData {
                        // This operator needs to resolve to a filter, which we can do with a boolean value, but doesn't
                        // make sense for numeric or string values without being opinionated on the truthiness of
                        // numbers/strings
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                is_bool_type(r->data_type()),
                                "Ternary operator expected bool value, received {}",
                                get_user_friendly_type_string(r->descriptor())
                        );
                        auto result = ternary_operator(c, l, r->get<bool>());
                        return transform_to_placeholder(result);
                    },
                    [&c](const std::shared_ptr<Value>& l, const util::BitSet& r) -> VariantData {
                        // This operator needs to resolve to a filter, which we can do with a boolean value, but doesn't
                        // make sense for numeric or string values without being opinionated on the truthiness of
                        // numbers/strings
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                is_bool_type(l->data_type()),
                                "Ternary operator expected bool value, received {}",
                                get_user_friendly_type_string(l->descriptor())
                        );
                        auto result = ternary_operator<true>(c, r, l->get<bool>());
                        return transform_to_placeholder(result);
                    },
                    [&c](const util::BitSet& l, FullResult) -> VariantData {
                        auto result = ternary_operator(c, l, true);
                        return transform_to_placeholder(result);
                    },
                    [&c](FullResult, const util::BitSet& r) -> VariantData {
                        auto result = ternary_operator<true>(c, r, true);
                        return transform_to_placeholder(result);
                    },
                    [&c](const util::BitSet& l, EmptyResult) -> VariantData {
                        auto result = ternary_operator(c, l, false);
                        return transform_to_placeholder(result);
                    },
                    [&c](EmptyResult, const util::BitSet& r) -> VariantData {
                        auto result = ternary_operator<true>(c, r, false);
                        return transform_to_placeholder(result);
                    },
                    [&c](const ColumnWithStrings& l, const ColumnWithStrings& r) -> VariantData {
                        auto result = ternary_operator(c, l, r);
                        return transform_to_placeholder(result);
                    },
                    [&c](const ColumnWithStrings& l, const std::shared_ptr<Value>& r) -> VariantData {
                        auto result = ternary_operator(c, l, *r);
                        return transform_to_placeholder(result);
                    },
                    [&c](const std::shared_ptr<Value>& l, const ColumnWithStrings& r) -> VariantData {
                        auto result = ternary_operator<true>(c, r, *l);
                        return transform_to_placeholder(result);
                    },
                    [&c](const ColumnWithStrings& l, FullResult) -> VariantData {
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                is_bool_type(l.column_->type().data_type()),
                                "Ternary operator cannot combine column '{}' of type {} with a FullResult."
                                " This can be caused by dynamic schema when a row-slice has a column necessary for "
                                "computing"
                                " the ternary operator result missing.",
                                l.column_name_,
                                get_user_friendly_type_string(l.column_->type())
                        );
                        auto bitset = std::get<util::BitSet>(transform_to_bitset(l));
                        auto result = ternary_operator(c, bitset, true);
                        return transform_to_placeholder(result);
                    },
                    [&c](FullResult, const ColumnWithStrings& r) -> VariantData {
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                is_bool_type(r.column_->type().data_type()),
                                "Ternary operator cannot combine column '{}' of type {} with a FullResult."
                                " This can be caused by dynamic schema when a row-slice has a column necessary for "
                                "computing"
                                " the ternary operator result missing.",
                                r.column_name_,
                                get_user_friendly_type_string(r.column_->type())
                        );
                        auto bitset = std::get<util::BitSet>(transform_to_bitset(r));
                        auto result = ternary_operator<true>(c, bitset, true);
                        return transform_to_placeholder(result);
                    },
                    [&c](const ColumnWithStrings& l, const EmptyResult& r) -> VariantData {
                        auto result = ternary_operator(c, l, r);
                        return transform_to_placeholder(result);
                    },
                    [&c](const EmptyResult& l, const ColumnWithStrings& r) -> VariantData {
                        auto result = ternary_operator<true>(c, r, l);
                        return transform_to_placeholder(result);
                    },
                    [&c](const std::shared_ptr<Value>& l, const std::shared_ptr<Value>& r) -> VariantData {
                        auto result = ternary_operator(c, *l, *r);
                        return transform_to_placeholder(result);
                    },
                    [&c](const std::shared_ptr<Value>& l, FullResult) -> VariantData {
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                is_bool_type(l->data_type()),
                                "Ternary operator expected bool value, received {}",
                                get_user_friendly_type_string(l->descriptor())
                        );
                        auto value = l->get<bool>();
                        auto result = ternary_operator(c, value, true);
                        return transform_to_placeholder(result);
                    },
                    [&c](FullResult, const std::shared_ptr<Value>& r) -> VariantData {
                        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                                is_bool_type(r->data_type()),
                                "Ternary operator expected bool value, received {}",
                                get_user_friendly_type_string(r->descriptor())
                        );
                        auto value = r->get<bool>();
                        auto result = ternary_operator(c, true, value);
                        return transform_to_placeholder(result);
                    },
                    [&c](const std::shared_ptr<Value>& l, const EmptyResult& r) -> VariantData {
                        auto result = ternary_operator(c, *l, r);
                        return transform_to_placeholder(result);
                    },
                    [&c](const EmptyResult& l, const std::shared_ptr<Value>& r) -> VariantData {
                        auto result = ternary_operator<true>(c, *r, l);
                        return transform_to_placeholder(result);
                    },
                    [](FullResult, FullResult) -> VariantData { return FullResult{}; },
                    [&c](FullResult, EmptyResult) -> VariantData { return c; },
                    [&c](EmptyResult, FullResult) -> VariantData {
                        auto res = c;
                        res.flip();
                        res.resize(c.size());
                        return res;
                    },
                    [](EmptyResult, EmptyResult) -> VariantData { return EmptyResult{}; },
                    [](const auto&, const auto&) -> VariantData {
                        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Invalid input types to ternary operator"
                        );
                        return EmptyResult{};
                    }
            },
            left,
            right
    );
}

VariantData dispatch_ternary(
        const VariantData& condition, const VariantData& left, const VariantData& right, OperationType operation
) {
    switch (operation) {
    case OperationType::TERNARY:
        return visit_ternary_operator(condition, left, right);
    default:
        util::raise_rte("Unknown operation {}", int(operation));
    }
}

} // namespace arcticdb
