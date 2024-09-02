/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <type_traits>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb::stream {
template<typename IndexType, typename AggregatorType, typename QueueType>
void do_merge(
    QueueType& input_streams,
    AggregatorType& agg,
    bool add_symbol_column
    ) {
    while (!input_streams.empty() && input_streams.top()->row().parent_->row_count() == 0) {
        input_streams.pop_top();
    }

    // NaT is definied as std::numeric_limits<int64_t>::min(), if there are any NaT values they will be on the top of the queue
    if (!input_streams.empty()) {
        const auto& next = input_streams.top();
        const auto index_value =
            std::get<timestamp>(*pipelines::index::index_value_from_row(next->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0));
        sorting::check<ErrorCode::E_UNSORTED_DATA>(index_value != NaT, "NaT values are not allowed in the index");
    }

    while (!input_streams.empty()) {
        auto next = input_streams.pop_top();

        debug::check<ErrorCode::E_ASSERTION_FAILURE>(next->row().parent_->row_count() > 0, "Empty segments are not allowed here");
        agg.start_row(pipelines::index::index_value_from_row(next->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0).value()) ([&next, add_symbol_column](auto &rb) {
            if(add_symbol_column)
                rb.set_scalar_by_name("symbol", std::string_view(std::get<StringId>(next->id())), DataType::UTF_DYNAMIC64);

            auto val = next->row().begin();
            std::advance(val, IndexType::field_count());
            for(; val != next->row().end(); ++val) {
                val->visit_field([&rb] (const auto& opt_v, std::string_view name, const TypeDescriptor& type_desc) {
                    if (opt_v) {
                        const StreamDescriptor& descriptor = rb.descriptor();
                        const size_t field_idx = *descriptor.find_field(name);
                        const Field& field = descriptor.field(field_idx);
                        if (type_desc == field.type()) {
                            rb.set_scalar_by_name(name, *opt_v, type_desc.data_type());
                        } else {
                            const auto common_type = has_valid_common_type(type_desc, field.type());
                            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                                common_type,
                                "No valid common type between staged segments for column {}. Mismatched types are {} "
                                "and {}",
                                name,
                                type_desc,
                                field.type()
                            );
                            common_type->visit_tag([&](auto type_desc_tag) {
                                using RawType = decltype(type_desc_tag)::DataTypeTag::raw_type;
                                if constexpr (std::is_convertible_v<RawType, std::decay_t<decltype(*opt_v)>>) {
                                    using RawType = decltype(type_desc_tag)::DataTypeTag::raw_type;
                                    const auto cast_value = static_cast<RawType>(*opt_v);
                                    rb.set_scalar_by_name(name, cast_value, type_desc.data_type());
                                } else {
                                    rb.set_scalar_by_name(name, *opt_v, type_desc.data_type());
                                }
                            });
                        }
                    }
                });
            }
        });

        if(next->advance())
            input_streams.emplace(std::move(next));
    }
    agg.commit();
}
} //namespace arcticdb::stream
