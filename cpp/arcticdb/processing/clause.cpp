/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <vector>
#include <variant>
#include <arcticdb/processing/processing_segment.hpp>
#include <folly/Poly.h>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/pipeline/value_set.hpp>
#include <arcticdb/util/third_party/emilib_map.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

namespace arcticdb {

[[nodiscard]] Composite<ProcessingSegment> FilterClause::process(std::shared_ptr<Store> store,
    Composite<ProcessingSegment>&& p) const
{
    auto procs = std::move(p);
    Composite<ProcessingSegment> output;
    procs.broadcast([&store, &execution_context = execution_context_, &output](auto& proc) {
        proc.set_execution_context(execution_context);
        auto variant_data = proc.get(execution_context->root_node_name_, store);
        util::variant_match(
            variant_data,
            [&proc, &output, &store](const std::shared_ptr<util::BitSet>& bitset) {
                proc.apply_filter(*bitset, store);
                output.push_back(std::move(proc));
            },
            [](EmptyResult) { log::version().debug("Filter returned empty result"); },
            [&output, &proc](FullResult) { output.push_back(std::move(proc)); },
            [](const auto&) { util::raise_rte("Expected bitset from filter clause"); });
    });
    return output;
}

[[nodiscard]] Composite<ProcessingSegment> ProjectClause::process(std::shared_ptr<Store> store,
    Composite<ProcessingSegment>&& p) const
{
    auto procs = std::move(p);
    Composite<ProcessingSegment> output;
    procs.broadcast([&store, &execution_context = execution_context_, &output, that = this](auto& proc) {
        proc.set_execution_context(execution_context);
        auto variant_data = proc.get(execution_context->root_node_name_, store);
        util::variant_match(
            variant_data,
            [&proc, &output, &store, &execution_context, &that](ColumnWithStrings& col) {
                const auto data_type = col.column_->type().data_type();
                const std::string_view name = that->column_name_;

                std::call_once(*that->add_column_, [context = execution_context, &data_type, &name]() {
                    std::scoped_lock lock{*context->column_mutex_};
                    context->output_descriptor_->add_scalar_field(data_type, name);
                });
                auto& slice_and_keys = proc.data();
                auto& last = *slice_and_keys.rbegin();
                last.segment(store).add_column(scalar_field_proto(data_type, name), col.column_);
                ++last.slice().col_range.second;
                output.push_back(std::move(proc));
            },
            [&proc, &output, &execution_context](const EmptyResult&) {
                if (execution_context->dynamic_schema())
                    output.push_back(std::move(proc));
                else
                    util::raise_rte("Cannot project from empty column with static schema");
            },
            [](const auto&) { util::raise_rte("Expected bitset from filter clause"); });
    });
    return output;
}

class GroupingMap {
    using NumericMapType = std::variant<std::monostate,
        std::shared_ptr<std::unordered_map<bool, size_t>>,
        std::shared_ptr<std::unordered_map<uint8_t, size_t>>,
        std::shared_ptr<std::unordered_map<uint16_t, size_t>>,
        std::shared_ptr<std::unordered_map<uint32_t, size_t>>,
        std::shared_ptr<std::unordered_map<uint64_t, size_t>>,
        std::shared_ptr<std::unordered_map<int8_t, size_t>>,
        std::shared_ptr<std::unordered_map<int16_t, size_t>>,
        std::shared_ptr<std::unordered_map<int32_t, size_t>>,
        std::shared_ptr<std::unordered_map<int64_t, size_t>>,
        std::shared_ptr<std::unordered_map<float, size_t>>,
        std::shared_ptr<std::unordered_map<double, size_t>>>;

    NumericMapType map_;

public:
    size_t size() const
    {
        return util::variant_match(
            map_,
            [](const std::monostate&) { return size_t(0); },
            [](const auto& other) { return other->size(); });
    }

    template<typename T>
    std::shared_ptr<std::unordered_map<T, size_t>> get()
    {
        return util::variant_match(
            map_,
            [that = this](const std::monostate&) {
                that->map_ = std::make_shared<std::unordered_map<T, size_t>>();
                return std::get<std::shared_ptr<std::unordered_map<T, size_t>>>(that->map_);
            },
            [](const std::shared_ptr<std::unordered_map<T, size_t>>& ptr) { return ptr; },
            [](const auto&) -> std::shared_ptr<std::unordered_map<T, size_t>> {
                util::raise_rte("Grouping column type change");
            });
    }
};

std::vector<Composite<ProcessingSegment>> single_partition(std::vector<Composite<ProcessingSegment>>&& comps)
{
    std::vector<Composite<ProcessingSegment>> v;
    v.push_back(merge_composites_shallow(std::move(comps)));
    return v;
}

[[nodiscard]] Composite<ProcessingSegment> AggregationClause::process(std::shared_ptr<Store> store,
    Composite<ProcessingSegment>&& p) const
{
    std::call_once(*reset_descriptor_, [context = execution_context_]() {
        std::scoped_lock lock{*context->column_mutex_};
        context->orig_output_descriptor_ = context->output_descriptor_;
        context->output_descriptor_ = empty_descriptor();
    });
    SegmentInMemory seg{empty_descriptor()};
    auto procs = std::move(p);
    size_t num_unique = 0;
    std::vector<Aggregation> aggregators;
    auto desc = execution_context_->orig_output_descriptor_;
    for (const auto& agg : aggregation_operators_) {
        auto agg_construct = agg.construct();
        const auto& agg_field_pos = desc->find_field(agg_construct.get_input_column_name().value);
        util::check(agg_field_pos.has_value(),
            "Field {} not found in aggregation",
            agg_construct.get_input_column_name().value);
        auto agg_field = desc->field(agg_field_pos.value());
        agg_construct.set_data_type(data_type_from_proto(agg_field.type_desc()));
        aggregators.emplace_back(agg_construct);
    }

    auto offset = 0;
    auto grouping_column_name = execution_context_->root_node_name_.value;
    auto string_pool = std::make_shared<StringPool>();
    DataType grouping_data_type;
    GroupingMap grouping_map;
    procs.broadcast([&store,
                        &num_unique,
                        &execution_context = execution_context_,
                        &grouping_data_type,
                        &grouping_map,
                        &offset,
                        &aggregators,
                        &string_pool,
                        &grouping_column_name](auto& proc) {
        proc.set_execution_context(execution_context);
        //TODO this is a hack, ideally the grouping should be able to be an expression
        auto partitioning_column = proc.get(ColumnName(grouping_column_name), store);
        if (std::holds_alternative<ColumnWithStrings>(partitioning_column)) {
            ColumnWithStrings col = std::get<ColumnWithStrings>(partitioning_column);
            entity::details::visit_type(col.column_->type().data_type(),
                [&proc_ = proc,
                    &grouping_map,
                    &offset,
                    &aggregators,
                    &string_pool,
                    &col,
                    &num_unique,
                    &store,
                    &grouping_data_type](auto data_type_tag) {
                    using DataTypeTagType = decltype(data_type_tag);
                    using RawType = typename DataTypeTagType::raw_type;
                    constexpr auto data_type = DataTypeTagType::data_type;
                    grouping_data_type = data_type;
                    std::vector<size_t> row_to_group;
                    row_to_group.reserve(col.column_->row_count());
                    auto input_data = col.column_->data();
                    while (auto block = input_data.next<ScalarTagType<DataTypeTagType>>()) {
                        const auto row_count = block->row_count();
                        auto ptr = block->data();
                        for (size_t i = 0; i < row_count; ++i, ++ptr) {
                            RawType val;
                            if constexpr (is_sequence_type(data_type)) {
                                std::optional<std::string_view> str = col.string_at_offset(*ptr);
                                if (str.has_value()) {
                                    val = string_pool->get(*str, true).offset();
                                } else {
                                    val = *ptr;
                                }
                            } else {
                                val = *ptr;
                            }
                            auto hashes = grouping_map.get<RawType>();
                            if (auto it = hashes->find(val); it == hashes->end()) {
                                row_to_group.push_back(offset);
                                hashes->try_emplace(val, offset++);
                            } else {
                                row_to_group.push_back(it->second);
                            }
                        }
                    }

                    num_unique = offset;
                    util::check(num_unique != 0, "Got zero unique values");
                    for (Aggregation& agg : aggregators) {
                        auto input_column = proc_.get(agg.get_input_column_name(), store);
                        std::optional<ColumnWithStrings> opt_input_column;
                        if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                            opt_input_column.emplace(std::get<ColumnWithStrings>(input_column));
                        }
                        agg.aggregate(opt_input_column, row_to_group, num_unique);
                    }
                });
        } else {
            util::raise_rte("Expected single column from expression");
        }
    });

    auto index_pos =
        seg.add_column(scalar_field_proto(grouping_data_type, grouping_column_name), grouping_map.size(), true);
    execution_context_->check_output_column(grouping_column_name, grouping_data_type);
    entity::details::visit_type(grouping_data_type, [&seg, &grouping_map, index_pos](auto data_type_tag) {
        using DataTypeTagType = decltype(data_type_tag);
        using RawType = typename DataTypeTagType::raw_type;
        auto hashes = grouping_map.get<RawType>();
        auto index_ptr = reinterpret_cast<RawType*>(seg.column(index_pos).ptr());
        std::vector<std::pair<RawType, size_t>> elements;
        for (const auto& hash : *hashes)
            elements.push_back(hash);

        std::sort(std::begin(elements),
            std::end(elements),
            [](const std::pair<RawType, size_t>& l, const std::pair<RawType, size_t>& r) {
                return l.second < r.second;
            });

        for (const auto& element : elements)
            *index_ptr++ = element.first;
    });

    for (auto& agg : aggregators) {
        auto data_type = agg.finalize(seg, execution_context_->dynamic_schema(), num_unique);
        if (data_type)
            execution_context_->check_output_column(agg.get_output_column_name().value, data_type.value());
    }

    seg.set_string_pool(string_pool);
    seg.set_row_id(num_unique - 1);
    std::call_once(*set_name_index_, [context = execution_context_, grouping_column_name]() {
        std::scoped_lock lock{*context->name_index_mutex_};
        auto norm_desc = context->get_norm_meta_descriptor();
        norm_desc->mutable_df()->mutable_common()->mutable_index()->set_name(grouping_column_name);
        norm_desc->mutable_df()->mutable_common()->mutable_index()->clear_fake_name();
        norm_desc->mutable_df()->mutable_common()->mutable_index()->set_is_not_range_index(true);
    });
    return Composite{ProcessingSegment{std::move(seg)}};
}

template<typename IndexType, typename DensityPolicy, typename QueueType, typename Comparator, typename StreamId>
void merge_impl(Composite<ProcessingSegment>& ret,
    QueueType& input_streams,
    bool add_symbol_column,
    StreamId stream_id,
    const arcticdb::pipelines::RowRange row_range,
    const arcticdb::pipelines::ColRange col_range,
    IndexType index,
    std::shared_ptr<ExecutionContext> execution_context)
{
    using namespace arcticdb::pipelines;
    auto num_segment_rows = ConfigsMap::instance()->get_int("Merge.SegmentSize", 100000);
    using SegmentationPolicy = stream::RowCountSegmentPolicy;
    SegmentationPolicy segmentation_policy{static_cast<size_t>(num_segment_rows)};

    auto func = [&ret, &row_range, &col_range](auto&& segment) {
        ret.push_back(ProcessingSegment{std::forward<SegmentInMemory>(segment), FrameSlice{col_range, row_range}});
    };

    using AggregatorType = stream::Aggregator<IndexType, stream::DynamicSchema, SegmentationPolicy, DensityPolicy>;
    auto fields = execution_context->output_descriptor_->fields();
    StreamDescriptor::FieldsCollection new_fields{};
    auto field_name = fields[0].name();
    auto new_field = new_fields.Add();
    new_field->set_name(field_name.data(), field_name.size());
    new_field->CopyFrom(fields[0]);

    auto index_desc = index_descriptor(stream_id, index, new_fields);
    auto desc = StreamDescriptor{index_desc};

    AggregatorType agg{stream::DynamicSchema{desc, index},
        std::move(func),
        std::move(segmentation_policy),
        desc,
        std::nullopt};

    stream::do_merge<IndexType, SliceAndKeyWrapper, AggregatorType, decltype(input_streams)>(input_streams,
        agg,
        add_symbol_column);
}

// MergeClause receives a list of DataFrames as input and merge them into a single one where all
// the rows are sorted by time stamp
[[nodiscard]] Composite<ProcessingSegment> MergeClause::process(std::shared_ptr<Store> store,
    Composite<ProcessingSegment>&& p) const
{
    using namespace arcticdb::pipelines;
    auto procs = std::move(p);

    auto compare = [](const std::unique_ptr<SliceAndKeyWrapper>& left,
                       const std::unique_ptr<SliceAndKeyWrapper>& right) {
        const auto left_index = pipelines::index::index_value_from_row(left->row(), IndexDescriptor::TIMESTAMP, 0);
        const auto right_index = pipelines::index::index_value_from_row(right->row(), IndexDescriptor::TIMESTAMP, 0);
        return left_index > right_index;
    };

    movable_priority_queue<std::unique_ptr<SliceAndKeyWrapper>,
        std::vector<std::unique_ptr<SliceAndKeyWrapper>>,
        decltype(compare)>
        input_streams{compare};

    size_t min_start_row = std::numeric_limits<size_t>::max();
    size_t max_end_row = 0;
    size_t min_start_col = std::numeric_limits<size_t>::max();
    size_t max_end_col = 0;
    procs.broadcast([&input_streams, &store, &min_start_row, &max_end_row, &min_start_col, &max_end_col](auto&& proc) {
        auto slice_and_keys = proc.release_data();
        for (auto&& slice_and_key : slice_and_keys) {
            size_t start_row = slice_and_key.slice().row_range.start();
            min_start_row = start_row < min_start_row ? start_row : min_start_row;
            size_t end_row = slice_and_key.slice().row_range.end();
            max_end_row = end_row > max_end_row ? end_row : max_end_row;
            size_t start_col = slice_and_key.slice().col_range.start();
            min_start_col = start_col < min_start_col ? start_col : min_start_col;
            size_t end_col = slice_and_key.slice().col_range.end();
            max_end_col = end_col > max_end_col ? end_col : max_end_col;
            input_streams.push(
                std::make_unique<SliceAndKeyWrapper>(std::forward<pipelines::SliceAndKey>(slice_and_key), store));
        }
    });
    const RowRange row_range{min_start_row, max_end_row};
    const ColRange col_range{min_start_col, max_end_col};
    Composite<ProcessingSegment> ret;
    std::visit(
        [&ret,
            &input_streams,
            add_symbol_column = add_symbol_column_,
            &comp = compare,
            stream_id = stream_id_,
            &row_range,
            &col_range,
            execution_context = execution_context_](auto idx, auto density) {
            merge_impl<decltype(idx), decltype(density), decltype(input_streams), decltype(comp), decltype(stream_id)>(
                ret,
                input_streams,
                add_symbol_column,
                stream_id,
                row_range,
                col_range,
                idx,
                execution_context);
        },
        index_,
        density_policy_);

    return ret;
}

[[nodiscard]] Composite<ProcessingSegment> RemoveColumnPartitioningClause::process(std::shared_ptr<Store> store,
    Composite<ProcessingSegment>&& p) const
{
    using namespace arcticdb::pipelines;
    auto procs = std::move(p);
    Composite<ProcessingSegment> output;
    auto index = stream::index_type_from_descriptor(execution_context_->output_descriptor_.value());
    util::variant_match(
        index,
        [&](const stream::TimeseriesIndex&) {
            size_t num_index_columns = stream::TimeseriesIndex::field_count();
            procs.broadcast([&store, &output, &num_index_columns](ProcessingSegment& proc) {
                SegmentInMemory new_segment{empty_descriptor()};
                new_segment.set_row_id(proc.data()[0].segment_->get_row_id());
                size_t min_start_row = std::numeric_limits<size_t>::max();
                size_t max_end_row = 0;
                size_t min_start_col = std::numeric_limits<size_t>::max();
                size_t max_end_col = 0;
                for (auto& slice_and_key : proc.data()) {
                    size_t start_row = slice_and_key.slice().row_range.start();
                    min_start_row = start_row < min_start_row ? start_row : min_start_row;
                    size_t end_row = slice_and_key.slice().row_range.end();
                    max_end_row = end_row > max_end_row ? end_row : max_end_row;
                    size_t start_col = slice_and_key.slice().col_range.start();
                    min_start_col = start_col < min_start_col ? start_col : min_start_col;
                    size_t end_col = slice_and_key.slice().col_range.end();
                    max_end_col = end_col > max_end_col ? end_col : max_end_col;
                    size_t column_idx = 0;
                    for (auto field : folly::enumerate(slice_and_key.segment(store).descriptor().fields())) {
                        const auto column_name = field->name();
                        auto column = proc.get(ColumnName(column_name), store);
                        if (std::holds_alternative<ColumnWithStrings>(column)) {
                            ColumnWithStrings column_strings = std::get<ColumnWithStrings>(column);
                            const auto data_type = column_strings.column_->type().data_type();
                            if (((start_col - num_index_columns) == 0) || column_idx >= num_index_columns) {
                                new_segment.add_column(scalar_field_proto(data_type, column_name),
                                    slice_and_key.segment(store).column_ptr(field.index));
                            }
                        } else {
                            util::raise_rte("Expected single column from expression");
                        }
                        column_idx++;
                    }
                }
                const RowRange row_range{min_start_row, max_end_row};
                const ColRange col_range{min_start_col, max_end_col};
                output.push_back(ProcessingSegment{std::move(new_segment), FrameSlice{col_range, row_range}});
            });
        },
        [&](const auto&) { util::raise_rte("Not supported index type for sort merge implementation"); });
    return output;
}

} // namespace arcticdb