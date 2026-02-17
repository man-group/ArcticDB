/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <type_traits>
#include <string_view>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/error_code.hpp>
#include <ankerl/unordered_dense.h>

template<typename Aggregator>
consteval bool is_static_schema() {
    if constexpr (std::is_same_v<typename Aggregator::SchemaPolicy, arcticdb::stream::DynamicSchema>) {
        return false;
    } else if constexpr (std::is_same_v<typename Aggregator::SchemaPolicy, arcticdb::stream::FixedSchema>) {
        return true;
    } else {
        static_assert(sizeof(Aggregator) == 0, "Unknown schema type");
    }
}

template<typename Aggregator>
consteval bool is_dynamic_schema() {
    return !is_static_schema<Aggregator>();
}

namespace arcticdb::stream {
template<typename AggregatorType, typename QueueType>
void do_merge(QueueType& input_streams, AggregatorType& agg, bool add_symbol_column) {
    using IndexType = typename AggregatorType::IndexType;
    while (!input_streams.empty() && input_streams.top()->seg_.row_count() == 0) {
        input_streams.pop_top();
    }

    // NaT is definied as std::numeric_limits<int64_t>::min(), if there are any NaT values they will be on the top of
    // the queue
    if (!input_streams.empty()) {
        const auto& next = input_streams.top();
        const auto index_value = std::get<timestamp>(
                *pipelines::index::index_value_from_row(next->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0)
        );
        sorting::check<ErrorCode::E_UNSORTED_DATA>(index_value != NaT, "NaT values are not allowed in the index");
    }

    [[maybe_unused]] const ankerl::unordered_dense::map<std::string_view, size_t> field_name_to_index = [&]() {
        ankerl::unordered_dense::map<std::string_view, size_t> res;
        if constexpr (is_dynamic_schema<AggregatorType>()) {
            const StreamDescriptor& desc = agg.descriptor();
            for (size_t field_idx = 0; field_idx < desc.field_count(); ++field_idx) {
                const Field& field = desc.field(field_idx);
                res[field.name()] = field_idx;
            }
        }
        return res;
    }();

    while (!input_streams.empty()) {
        auto next = input_streams.pop_top();
        if (next->seg_.row_count() == 0) {
            continue;
        }
        const auto index_value =
                *pipelines::index::index_value_from_row(next->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0);
        agg.start_row(index_value)([&](auto& rb) {
            if (add_symbol_column)
                rb.set_scalar_by_name(
                        "symbol", std::string_view(std::get<StringId>(next->id())), DataType::UTF_DYNAMIC64
                );

            auto val = next->row().begin();
            std::advance(val, IndexType::field_count());
            for (; val != next->row().end(); ++val) {
                val->visit_field([&](const auto& opt_v, std::string_view name, auto row_field_descriptor_tag) {
                    if (opt_v) {
                        if constexpr (is_static_schema<AggregatorType>()) {
                            rb.set_scalar_by_name(name, opt_v.value(), row_field_descriptor_tag.data_type());
                        } else {
                            const TypeDescriptor& final_type =
                                    agg.descriptor().field(field_name_to_index.find(name)->second).type();
                            details::visit_type(final_type.data_type(), [&](auto merged_descriptor_type) {
                                using merged_type_info = ScalarTypeInfo<decltype(merged_descriptor_type)>;
                                using RowFieldDescriptorTagType =
                                        typename std::decay_t<decltype(row_field_descriptor_tag)>;
                                using RowFieldDescriptorTagDataType = typename RowFieldDescriptorTagType::DataTypeTag;
                                using row_type_info = ScalarTypeInfo<RowFieldDescriptorTagDataType>;
                                // At this point all staged descriptors were merged using merge_descriptors, and it
                                // ensured that all staged descriptors are either the same or are convertible to the
                                // stream descriptor in the aggregator.
                                if constexpr (merged_type_info::data_type == row_type_info::data_type) {
                                    rb.set_scalar_by_name(name, opt_v.value(), merged_type_info::data_type);
                                } else if constexpr (std::is_convertible_v<
                                                             decltype(*opt_v),
                                                             typename merged_type_info::RawType>) {
                                    rb.set_scalar_by_name(
                                            name,
                                            static_cast<typename merged_type_info::RawType>(*opt_v),
                                            merged_type_info::data_type
                                    );
                                } else {
                                    schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                                            "Cannot convert {} to {}",
                                            merged_type_info::TDT::type_descriptor(),
                                            row_type_info::TDT::type_descriptor()
                                    );
                                }
                            });
                        }
                    } else {
                        if constexpr (is_sequence_type(row_field_descriptor_tag.data_type())) {
                            // When the value is std::nullopt this means we're dealing with sparse data. For string
                            // values this means that the values in the string pool are placeholders either for NaN or
                            // None. We write the placeholder into the column, otherwise if the whole column contains
                            // None/NaN the encoding throws an exception see Monday tickets: 9712849046 and 9712629170
                            using TDT = decltype(row_field_descriptor_tag);
                            using RawType = typename TDT::DataTypeTag::raw_type;
                            const RawType& raw_value = val->template value<RawType>();
                            DEBUG_CHECK(
                                    ErrorCode::E_ASSERTION_FAILURE,
                                    nan_placeholder() == raw_value || not_a_string() == raw_value,
                                    "Expected NaN or None placeholders to represent missing value."
                            );
                            rb.set_scalar_by_name(name, raw_value, row_field_descriptor_tag.data_type());
                        }
                    }
                });
            }
        });

        if (next->advance())
            input_streams.emplace(std::move(next));
    }
    agg.commit();
}
} // namespace arcticdb::stream
