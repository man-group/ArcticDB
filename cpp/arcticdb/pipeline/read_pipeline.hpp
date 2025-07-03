/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <variant>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/util/bitset.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/pandas_output_frame.hpp>
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
        auto bitset = (*filter)(container, std::unique_ptr<util::BitSet>{});
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
    for (auto r = bs.first(); r != bs.end(); ++r) {
        visitor(*r);
    }
}

inline std::vector<SliceAndKey> filter_index(
    const index::IndexSegmentReader& index_segment_reader,
    std::optional<CombinedQuery<index::IndexSegmentReader>> &&query
) {
    ARCTICDB_SAMPLE_DEFAULT(FilterIndex)
    std::vector<SliceAndKey> output{};
    if (!index_segment_reader.empty()) {
        if(query) {
            auto row_bitset = (*query)(index_segment_reader);
            ARCTICDB_DEBUG(log::version(), "Row bitset has {} bits set of {}", row_bitset->count(), row_bitset->size());
            output.reserve(row_bitset->count());
            foreach_active_bit(*row_bitset, [&](auto r) {
                output.emplace_back(get_row(index_segment_reader, r));
            });
        } else {
            output.reserve(index_segment_reader.size());
            for(auto i = 0u; i < index_segment_reader.size(); ++i) {
                output.emplace_back(get_row(index_segment_reader, i));
            }
        }
    }
    ARCTICDB_DEBUG(log::version(), "Returning filtered output with {} data keys", output.size());
    return output;
}

inline util::BitSet build_column_bitset(const StreamDescriptor& desc, const folly::F14FastSet<std::string_view>& columns) {
    util::BitSet col_bitset(static_cast<util::BitSetSizeType>(desc.fields().size()));
    for (std::size_t c = 0; c < static_cast<std::size_t>(desc.fields().size()); ++c) {
        auto& f = desc.fields(static_cast<int>(c));
        col_bitset[c] = c < desc.index().field_count() || columns.find(f.name()) != columns.end();
    }

    ARCTICDB_DEBUG(log::version(), "{} columns set", col_bitset.count());
    return col_bitset;
}

inline util::BitSet build_column_bitset(const StreamDescriptor& desc, const std::vector<std::string>& columns) {
    folly::F14FastSet<std::string_view> col_set{columns.begin(), columns.end()};
    return build_column_bitset(desc, col_set);
}

inline auto add_index_column(const std::vector<std::string>& columns, const StreamDescriptor& desc) {
    std::vector<std::string> columns_with_index{columns};
    columns_with_index.emplace_back(desc.fields(0).name());
    return columns_with_index;
}

inline bool contains_index_column(const std::vector<std::string>& columns, const StreamDescriptor& desc) {
    return desc.index().field_count() == 0
        || std::find(std::begin(columns), std::end(columns), desc.fields(0).name())
            != std::end(columns);
}

inline std::optional<util::BitSet> requested_column_bitset_including_index(const StreamDescriptor& desc, const std::optional<std::vector<std::string>>& columns) {
    // Add the index column if it's not there
    if (columns.has_value()) {
        if(!contains_index_column(*columns, desc)) {
            ARCTICDB_DEBUG(log::version(), "Specified columns missing index column");
            return build_column_bitset(desc, add_index_column(*columns, desc));
        } else {
            return build_column_bitset(desc, *columns);
        }
    }
    return std::nullopt;
}

// Returns std::nullopt if all columns are required, which is the case if requested_columns is std::nullopt
// Otherwise augment the requested_columns bitset with columns that are required by any of the clauses
std::optional<util::BitSet> overall_column_bitset(
        const StreamDescriptor& desc,
        const std::vector<std::shared_ptr<Clause>>& clauses,
        const std::optional<util::BitSet>& requested_columns);

inline void generate_filtered_field_descriptors(PipelineContext& context, const std::optional<std::vector<std::string>>& columns) {
    if (columns.has_value()) {
        const ankerl::unordered_dense::set<std::string_view> column_set{std::begin(*columns), std::end(*columns)};
        
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

inline void generate_filtered_field_descriptors(std::shared_ptr<PipelineContext>& context, const std::optional<std::vector<std::string>>& columns) {
    generate_filtered_field_descriptors(*context, columns);
}

inline void get_column_bitset_in_context(
    const ReadQuery& query,
    const std::shared_ptr<PipelineContext>& pipeline_context) {
    pipeline_context->set_selected_columns(query.columns);
    pipeline_context->overall_column_bitset_ = overall_column_bitset(pipeline_context->descriptor(),
                                                                     query.clauses_,
                                                                     pipeline_context->selected_columns_);
}

template<class ContainerType>
std::vector<FilterQuery<ContainerType>> get_column_bitset_and_query_functions(
    const ReadQuery& query,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    bool dynamic_schema,
    bool column_groups) {
    using namespace arcticdb::pipelines::index;
    if(!dynamic_schema || column_groups) {
        get_column_bitset_in_context(query, pipeline_context);
    }
    return build_read_query_filters<ContainerType>(pipeline_context, query.row_filter, dynamic_schema, column_groups);
}

} // arcticdb::pipelines
