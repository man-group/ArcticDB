/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/util/constants.hpp>

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
                    if(opt_v)
                        rb.set_scalar_by_name(name, opt_v.value(), type_desc.data_type());
                });
            }
        });

        if(next->advance())
            input_streams.emplace(std::move(next));
    }
    agg.commit();
}
} //namespace arcticdb::stream
