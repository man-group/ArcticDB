/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <variant>

#include <folly/futures/Future.h>
#include <folly/container/F14Set.h>
#include <boost/noncopyable.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <pybind11/pybind11.h>

#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/stream/stream_source.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/util/constructors.hpp>
#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/python_output_frame.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_options.hpp>

namespace arcticdb::pipelines {

template<class ContainerType>
std::optional<CombinedQuery<ContainerType>> combine_filter_functions(std::vector<FilterQuery<ContainerType>>& filters) {
    if(filters.empty())
        return std::nullopt;

    return [&](const ContainerType &container) mutable {
        auto filter = filters.begin();
        std::unique_ptr<util::BitSet> orig;
        auto bitset = (*filter)(container, std::move(orig));
        for(++filter; filter!=filters.end(); ++filter) {
            bitset = (*filter)(container, std::move(bitset));
        }
        return bitset;
    };
}

inline SliceAndKey get_row(const index::IndexSegmentReader& isr, size_t row) {
    return isr.row(row);
}

template<class C>
void foreach_active_bit(const util::BitSet &bs, C &&visitor) {
    for (auto r = bs.first(); r != bs.end(); r++) {
        visitor(*r);
    }
}

template<typename ContainerType>
std::vector<SliceAndKey> filter_index(const ContainerType &container, std::optional<CombinedQuery<ContainerType>> &&query) {
    ARCTICDB_SAMPLE_DEFAULT(FilterIndex)
    std::vector<SliceAndKey> output{};
    if (container.size()> 0) {
        if(query) {
            auto row_bitset = (*query)(container);
            ARCTICDB_DEBUG(log::version(), "Row bitset has {} bits set of {}", row_bitset->count(), row_bitset->size());
            output.reserve(row_bitset->count());
            foreach_active_bit(*row_bitset, [&](auto r) {
                output.push_back(get_row(container, r));
            });
        } else {
            output.reserve(container.size());
            for(auto i = 0u; i < container.size(); ++i) {
                output.push_back(get_row(container, i));
            }
        }
    }
    ARCTICDB_DEBUG(log::version(), "Returning filtered output with {} data keys", output.size());
    return output;
}

inline util::BitSet build_column_bitset(const StreamDescriptor::Proto &desc, const folly::F14FastSet<std::string_view>& columns) {
    util::BitSet col_bitset(static_cast<util::BitSetSizeType>(desc.fields().size()));
    for (std::size_t c = 0; c < static_cast<std::size_t>(desc.fields().size()); ++c) {
        auto& f = desc.fields(static_cast<int>(c));
        col_bitset[c] = c < desc.index().field_count() || columns.find(f.name()) != columns.end();
    }

    ARCTICDB_DEBUG(log::version(), "{} columns set", col_bitset.count());
    return col_bitset;
}

inline util::BitSet build_column_bitset(const StreamDescriptor::Proto&desc, const std::vector<std::string>& columns) {
    folly::F14FastSet<std::string_view> col_set{columns.begin(), columns.end()};
    return build_column_bitset(desc, col_set);
}

inline bool contains_index_column(const std::vector<std::string>& columns, const StreamDescriptor::Proto& desc) {
    return desc.index().field_count() == 0
        || std::find(std::begin(columns), std::end(columns), desc.fields(0).name())
            != std::end(columns);
}

inline auto add_index_column(const std::vector<std::string>& columns, const StreamDescriptor::Proto& desc) {
    std::vector<std::string> columns_with_index{columns};
    columns_with_index.push_back(desc.fields(0).name());
    return columns_with_index;
}

inline std::optional<util::BitSet> requested_column_bitset_including_index(const StreamDescriptor::Proto& desc, const std::vector<std::string>& columns) {
    // Add the index column if it's not there
    if (!columns.empty()) {
        if(!contains_index_column(columns, desc)) {
            ARCTICDB_DEBUG(log::version(), "Specified columns missing index column");
            return build_column_bitset(desc, add_index_column(columns, desc));
        }
        else
            return build_column_bitset(desc, columns);
    }
    return std::nullopt;
}

inline std::optional<util::BitSet> clause_column_bitset(const StreamDescriptor::Proto& desc,
                                                        const std::vector<std::shared_ptr<Clause>>& clauses) {
    folly::F14FastSet<std::string_view> column_set;
    for (const auto& clause: clauses) {
        auto opt_columns = clause->clause_info().input_columns_;
        if (opt_columns.has_value()) {
            for (const auto& column: *clause->clause_info().input_columns_) {
                column_set.insert(std::string_view(column));
            }
        }
    }
    if (!column_set.empty()) {
        return build_column_bitset(desc, column_set);
    } else {
        return std::nullopt;
    }
}

// Returns std::nullopt if all columns are required, which is the case if requested_columns is std::nullopt
// Otherwise augment the requested_columns bitset with columns that are required by any of the clauses
inline std::optional<util::BitSet> overall_column_bitset(const StreamDescriptor::Proto& desc,
                                                         const std::vector<std::shared_ptr<Clause>>& clauses,
                                                         const std::optional<util::BitSet>& requested_columns) {
    // std::all_of returns true if the range is empty
    auto clauses_can_combine_with_column_selection = std::all_of(clauses.begin(), clauses.end(),
                                                                 [](const std::shared_ptr<Clause>& clause){
        return clause->clause_info().can_combine_with_column_selection_;
    });
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !requested_columns.has_value() || clauses_can_combine_with_column_selection,
            "Cannot combine provided clauses with column selection");

    if (clauses_can_combine_with_column_selection) {
        if (requested_columns.has_value()) {
            auto clause_bitset = clause_column_bitset(desc, clauses);
            return clause_bitset.has_value() ? *requested_columns | *clause_bitset : requested_columns;
        } else {
            return std::nullopt;
        }
    } else {
        // clauses_can_combine_with_column_selection is false implies requested_columns.has_value() is false by the previous check
        return clause_column_bitset(desc, clauses);
    }
}

inline void generate_filtered_field_descriptors(PipelineContext& context, const std::vector<std::string>& columns) {
    if (!columns.empty()) {
        std::unordered_set<std::string_view> column_set{std::begin(columns), std::end(columns)};
        
        context.filter_columns_ = std::make_shared<FieldCollection>();
        const auto& desc = context.descriptor();
        ARCTICDB_DEBUG(log::version(), "Context descriptor: {}", desc);
        for(const auto& field : desc.fields()) {
            if(column_set.find(field.name()) != column_set.end())
                context.filter_columns_->add_field(field.type(), field.name());
        }

        context.filter_columns_set_ = std::unordered_set<std::string_view>{};
        for(const auto& field : *context.filter_columns_)
            context.filter_columns_set_->insert(field.name());
    }
}

inline void generate_filtered_field_descriptors(std::shared_ptr<PipelineContext>& context, const std::vector<std::string>& columns) {
    generate_filtered_field_descriptors(*context, columns);
}

template<class ContainerType>
inline std::vector<FilterQuery<ContainerType>> get_column_bitset_and_query_functions(
    const ReadQuery& query,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    bool dynamic_schema,
    bool column_groups) {
    using namespace arcticdb::pipelines::index;

    if(!dynamic_schema || column_groups) {
        pipeline_context->set_selected_columns(query.columns);
        pipeline_context->overall_column_bitset_ = overall_column_bitset(pipeline_context->descriptor().proto(),
                                                                         query.clauses_,
                                                                         pipeline_context->selected_columns_);
    }
    return build_read_query_filters<ContainerType>(pipeline_context->overall_column_bitset_, pipeline_context, query.row_filter, dynamic_schema, column_groups);
}

} // arcticdb::pipelines
