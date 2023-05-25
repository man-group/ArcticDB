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
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/value_set.hpp>

#include <arcticdb/util/third_party/emilib_set.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>

namespace arcticdb {

[[nodiscard]] Composite<ProcessingSegment>
ColumnStatsGenerationClause::process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
    auto procs = std::move(p);
    std::vector<ColumnStatsAggregatorData> aggregators_data;
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            static_cast<bool>(column_stats_aggregators_),
            "ColumnStatsGenerationClause::process does not make sense with no aggregators");
    for (const auto &agg : *column_stats_aggregators_){
        aggregators_data.emplace_back(agg.get_aggregator_data());
    }

    emilib::HashSet<IndexValue> start_indexes;
    emilib::HashSet<IndexValue> end_indexes;

    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            !procs.empty(),
            "ColumnStatsGenerationClause::process does not make sense with no processing segments");
    std::optional<bool> dynamic_schema;
    procs.broadcast(
            [&store, &start_indexes, &end_indexes, &dynamic_schema, &aggregators_data, that=this](
                    auto &proc) {
                if (dynamic_schema.has_value()) {
                    util::check(*dynamic_schema == proc.dynamic_schema_, "All ProcessingSegments should agree on whether dynamic schema is true or false");
                } else {
                    dynamic_schema = proc.dynamic_schema_;
                }
                for (const auto& slice_and_key: proc.data_) {
                    start_indexes.insert(slice_and_key.key_->start_index());
                    end_indexes.insert(slice_and_key.key_->end_index());
                }
                for (auto agg_data : folly::enumerate(aggregators_data)) {
                    auto input_column_name = that->column_stats_aggregators_->at(agg_data.index).get_input_column_name();
                    auto input_column = proc.get(input_column_name, store);
                    if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                        auto input_column_with_strings = std::get<ColumnWithStrings>(input_column);
                        agg_data->aggregate(input_column_with_strings);
                    } else {
                        if(!proc.dynamic_schema_)
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                    "Unable to resolve column denoted by aggregation operator: '{}'",
                                    input_column_name);
                    }
                }
            });

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            start_indexes.size() == 1 && end_indexes.size() == 1,
            "Expected all data segments in one processing segment to have same start and end indexes");
    auto start_index = *start_indexes.begin();
    auto end_index = *end_indexes.begin();
    schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
            std::holds_alternative<NumericIndex>(start_index) && std::holds_alternative<NumericIndex>(end_index),
            "Cannot build column stats over string-indexed symbol"
            );
    auto start_index_col = std::make_shared<Column>(make_scalar_type(DataType::MICROS_UTC64), true);
    auto end_index_col = std::make_shared<Column>(make_scalar_type(DataType::MICROS_UTC64), true);
    start_index_col->template push_back<NumericIndex>(std::get<NumericIndex>(start_index));
    end_index_col->template push_back<NumericIndex>(std::get<NumericIndex>(end_index));
    start_index_col->set_row_data(0);
    end_index_col->set_row_data(0);

    SegmentInMemory seg;
    seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
    seg.add_column(scalar_field_proto(DataType::MICROS_UTC64, start_index_column_name), start_index_col);
    seg.add_column(scalar_field_proto(DataType::MICROS_UTC64, end_index_column_name), end_index_col);
    for (const auto& agg_data: folly::enumerate(aggregators_data)) {
       seg.concatenate(agg_data->finalize(column_stats_aggregators_->at(agg_data.index).get_output_column_names()));
    }
    seg.set_row_id(0);
    util::check(dynamic_schema.has_value(), "Cannot proceed with processing pipeline without knowing dynamic schema value");
    return Composite{ProcessingSegment{std::move(seg), *dynamic_schema}};
}

[[nodiscard]] Composite<ProcessingSegment>
FilterClause::process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
    auto procs = std::move(p);
    Composite<ProcessingSegment> output;
    procs.broadcast([&optimisation=optimisation_, &store, &expression_context = expression_context_, &output](auto &proc) {
        proc.set_expression_context(expression_context);
        auto variant_data = proc.get(expression_context->root_node_name_, store);
        util::variant_match(variant_data,
                            [&optimisation, &proc, &output, &store](const std::shared_ptr<util::BitSet> &bitset) {
                                proc.apply_filter(*bitset, store, optimisation);
                                output.push_back(std::move(proc));
                            },
                            [](EmptyResult) {
                               log::version().debug("Filter returned empty result");
                            },
                            [&output, &proc](FullResult) {
                                output.push_back(std::move(proc));
                            },
                            [](const auto &) {
                                util::raise_rte("Expected bitset from filter clause");
                            });
    });
    return output;
}

[[nodiscard]] std::string FilterClause::to_string() const {
    return expression_context_ ? fmt::format("WHERE {}", expression_context_->root_node_name_.value) : "";
}

[[nodiscard]] Composite<ProcessingSegment> ProjectClause::process(std::shared_ptr<Store> store,
                                                                  Composite<ProcessingSegment> &&p) const {
    auto procs = std::move(p);
    Composite<ProcessingSegment> output;
    procs.broadcast([&store, &expression_context = expression_context_, &output, that = this](auto &proc) {
        proc.set_expression_context(expression_context);
        auto variant_data = proc.get(expression_context->root_node_name_, store);
        util::variant_match(variant_data,
                            [&proc, &output, &store, &that](ColumnWithStrings &col) {

                                const auto data_type = col.column_->type().data_type();
                                const std::string_view name = that->output_column_;

                                auto &slice_and_keys = proc.data();
                                auto &last = *slice_and_keys.rbegin();
                                last.segment(store).add_column(scalar_field_proto(data_type, name), col.column_);
                                ++last.slice().col_range.second;
                                output.push_back(std::move(proc));
                            },
                            [&proc, &output](const EmptyResult&) {
                                if(proc.dynamic_schema_)
                                    output.push_back(std::move(proc));
                                else
                                    util::raise_rte("Cannot project from empty column with static schema");
                            },
                            [](const auto &) {
                                util::raise_rte("Expected column from projection clause");
                            });
    });
    return output;
}

[[nodiscard]] std::string ProjectClause::to_string() const {
    return expression_context_ ? fmt::format("PROJECT Column[\"{}\"] = {}", output_column_, expression_context_->root_node_name_.value) : "";
}

class GroupingMap {
    using NumericMapType = std::variant<
        std::monostate,
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
    size_t size() const {
        return util::variant_match(map_,
                                   [](const std::monostate &) {
                                       return size_t(0);
                                   },
                                   [](const auto &other) {
                                       return other->size();
                                   });
    }

    template<typename T>
    std::shared_ptr<std::unordered_map<T, size_t>> get() {
        return util::variant_match(map_,
                                   [that = this](const std::monostate &) {
                                       that->map_ = std::make_shared<std::unordered_map<T, size_t>>();
                                       return std::get<std::shared_ptr<std::unordered_map<T, size_t>>>(that->map_);
                                   },
                                   [](const std::shared_ptr<std::unordered_map<T, size_t>> &ptr) {
                                       return ptr;
                                   },
                                   [](const auto &) -> std::shared_ptr<std::unordered_map<T, size_t>> {
                                       util::raise_rte("Grouping column type change");
                                   });
    }
};

std::vector<Composite<ProcessingSegment>> single_partition(std::vector<Composite<ProcessingSegment>> &&comps) {
    std::vector<Composite<ProcessingSegment>> v;
    v.push_back(merge_composites_shallow(std::move(comps)));
    return v;
}

[[nodiscard]] Composite<ProcessingSegment>
AggregationClause::process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
    auto procs = std::move(p);
    std::vector<GroupingAggregatorData> aggregators_data;
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            !aggregators_.empty(),
            "AggregationClause::process does not make sense with no aggregators");
    for (const auto &agg: aggregators_){
        aggregators_data.emplace_back(agg.get_aggregator_data());
    }

    // Work out the common type between the processing segments for the columns being aggregated
    procs.broadcast([&store, &aggregators_data, &aggregators=aggregators_](auto& proc) {
        for (auto agg_data: folly::enumerate(aggregators_data)) {
            auto input_column_name = aggregators.at(agg_data.index).get_input_column_name();
            auto input_column = proc.get(input_column_name, store);
            std::optional<ColumnWithStrings> opt_input_column;
            if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                agg_data->add_data_type(std::get<ColumnWithStrings>(input_column).column_->type().data_type());
            }
        }
    });

    size_t num_unique = 0;
    auto offset = 0;
    auto string_pool = std::make_shared<StringPool>();
    DataType grouping_data_type;
    GroupingMap grouping_map;
    std::optional<bool> dynamic_schema;
    procs.broadcast(
        [&store, &num_unique, &dynamic_schema,
        &grouping_data_type, &grouping_map, &offset, &aggregators_data, &string_pool, that=this](
            auto &proc) {
            if (dynamic_schema.has_value()) {
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                        *dynamic_schema == proc.dynamic_schema_,
                        "All ProcessingSegments should agree on whether dynamic schema is true or false");
            } else {
                dynamic_schema = proc.dynamic_schema_;
            }
            auto partitioning_column = proc.get(ColumnName(that->grouping_column_), store);
            if (std::holds_alternative<ColumnWithStrings>(partitioning_column)) {
                ColumnWithStrings col = std::get<ColumnWithStrings>(partitioning_column);
                entity::details::visit_type(col.column_->type().data_type(),
                                            [&proc_=proc, &grouping_map, &offset, &aggregators_data, &string_pool, &col,
                                             &num_unique, &store, &grouping_data_type, that](auto data_type_tag) {
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
                                                        if constexpr(is_sequence_type(data_type)) {
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
                                                for (auto agg_data: folly::enumerate(aggregators_data)) {
                                                    auto input_column_name = that->aggregators_.at(agg_data.index).get_input_column_name();
                                                    auto input_column = proc_.get(input_column_name, store);
                                                    std::optional<ColumnWithStrings> opt_input_column;
                                                    if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                                                        opt_input_column.emplace(std::get<ColumnWithStrings>(input_column));
                                                    }
                                                    agg_data->aggregate(opt_input_column, row_to_group, num_unique);
                                                }
                                            });
            } else {
                util::raise_rte("Expected single column from expression");
            }
        });

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            dynamic_schema.has_value(),
            "Cannot proceed with processing pipeline without knowing dynamic schema value");

    SegmentInMemory seg;
    auto index_col = std::make_shared<Column>(make_scalar_type(grouping_data_type), grouping_map.size(), true, false);
    auto index_pos = seg.add_column(scalar_field_proto(grouping_data_type, grouping_column_), index_col);
    seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));

    entity::details::visit_type(grouping_data_type, [&seg, &grouping_map, index_pos](auto data_type_tag) {
        using DataTypeTagType = decltype(data_type_tag);
        using RawType = typename DataTypeTagType::raw_type;
        auto hashes = grouping_map.get<RawType>();
        auto index_ptr = reinterpret_cast<RawType *>(seg.column(index_pos).ptr());
        std::vector<std::pair<RawType, size_t>> elements;
        for (const auto &hash : *hashes)
            elements.push_back(hash);

        std::sort(std::begin(elements),
                  std::end(elements),
                  [](const std::pair<RawType, size_t> &l, const std::pair<RawType, size_t> &r) {
                      return l.second < r.second;
                  });

        for (const auto &element : elements)
            *index_ptr++ = element.first;
    });
    index_col->set_row_data(grouping_map.size() - 1);

    for (auto agg_data: folly::enumerate(aggregators_data)) {
        seg.concatenate(agg_data->finalize(aggregators_.at(agg_data.index).get_output_column_name(), *dynamic_schema, num_unique));
    }

    seg.set_string_pool(string_pool);
    seg.set_row_id(num_unique - 1);
    return Composite{ProcessingSegment{std::move(seg), *dynamic_schema}};
}

[[nodiscard]] std::string AggregationClause::to_string() const {
    return fmt::format("AGGREGATE {}", aggregation_map_);
}

template<typename IndexType, typename DensityPolicy, typename QueueType, typename Comparator, typename StreamId>
void merge_impl(
        Composite<ProcessingSegment> &ret,
        QueueType &input_streams,
        bool add_symbol_column,
        StreamId stream_id,
        const arcticdb::pipelines::RowRange row_range,
        const arcticdb::pipelines::ColRange col_range,
        IndexType index,
        const StreamDescriptor& stream_descriptor,
        bool dynamic_schema
) {
    using namespace arcticdb::pipelines;
    auto num_segment_rows = ConfigsMap::instance()->get_int("Merge.SegmentSize", 100000);
    using SegmentationPolicy = stream::RowCountSegmentPolicy;
    SegmentationPolicy segmentation_policy{static_cast<size_t>(num_segment_rows)};

    auto func = [&ret, &row_range, &col_range, dynamic_schema](auto &&segment) {
        ret.push_back(ProcessingSegment{std::forward<SegmentInMemory>(segment), FrameSlice{col_range, row_range}, dynamic_schema});
    };
    
    using AggregatorType = stream::Aggregator<IndexType, stream::DynamicSchema, SegmentationPolicy, DensityPolicy>;
    auto fields = stream_descriptor.fields();
    StreamDescriptor::FieldsCollection new_fields{};
    auto field_name = fields[0].name();
    auto new_field = new_fields.Add();
    new_field->set_name(field_name.data(), field_name.size());
    new_field->CopyFrom(fields[0]);
    
    auto index_desc = index_descriptor(stream_id, index, new_fields);
    auto desc = StreamDescriptor{index_desc};

    AggregatorType agg{
            stream::DynamicSchema{desc, index},
            std::move(func), std::move(segmentation_policy), desc, std::nullopt
    };


    stream::do_merge<IndexType, SliceAndKeyWrapper, AggregatorType, decltype(input_streams)>(
        input_streams, agg, add_symbol_column);
}

// MergeClause receives a list of DataFrames as input and merge them into a single one where all 
// the rows are sorted by time stamp
[[nodiscard]] Composite<ProcessingSegment>
MergeClause::process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
        using namespace arcticdb::pipelines;
        auto procs = std::move(p);

        auto compare =
                [](const std::unique_ptr<SliceAndKeyWrapper> &left,
                   const std::unique_ptr<SliceAndKeyWrapper> &right) {
                    const auto left_index = pipelines::index::index_value_from_row(left->row(),
                                                                                   IndexDescriptor::TIMESTAMP, 0);
                    const auto right_index = pipelines::index::index_value_from_row(right->row(),
                                                                                    IndexDescriptor::TIMESTAMP, 0);
                    return left_index > right_index;
                };

        movable_priority_queue<std::unique_ptr<SliceAndKeyWrapper>, std::vector<std::unique_ptr<SliceAndKeyWrapper>>, decltype(compare)> input_streams{
                compare};

        size_t min_start_row = std::numeric_limits<size_t>::max();
        size_t max_end_row = 0;
        size_t min_start_col = std::numeric_limits<size_t>::max();
        size_t max_end_col = 0;
        std::optional<bool> dynamic_schema;
        procs.broadcast([&input_streams, &store, &min_start_row, &max_end_row, &min_start_col, &max_end_col, &dynamic_schema](auto &&proc) {
            if (dynamic_schema.has_value()) {
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                        *dynamic_schema == proc.dynamic_schema_,
                        "All ProcessingSegments should agree on whether dynamic schema is true or false");
            } else {
                dynamic_schema = proc.dynamic_schema_;
            }
            auto slice_and_keys = proc.release_data();
            for (auto &&slice_and_key: slice_and_keys) {
                size_t start_row = slice_and_key.slice().row_range.start();
                min_start_row = start_row < min_start_row ? start_row : min_start_row;
                size_t end_row = slice_and_key.slice().row_range.end();
                max_end_row = end_row > max_end_row ? end_row : max_end_row;
                size_t start_col = slice_and_key.slice().col_range.start();
                min_start_col = start_col < min_start_col ? start_col : min_start_col;
                size_t end_col = slice_and_key.slice().col_range.end();
                max_end_col = end_col > max_end_col ? end_col : max_end_col;
                input_streams.push(
                        std::make_unique<SliceAndKeyWrapper>(std::forward<pipelines::SliceAndKey>(slice_and_key),
                                                             store));
            }
        });
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                dynamic_schema.has_value(),
                "Cannot proceed with processing pipeline without knowing dynamic schema value");
        const RowRange row_range{min_start_row, max_end_row};
        const ColRange col_range{min_start_col, max_end_col};
        Composite<ProcessingSegment> ret;
        std::visit(
                [&ret, &input_streams, add_symbol_column = add_symbol_column_, &comp = compare, stream_id = stream_id_, &row_range, &col_range, &stream_descriptor = stream_descriptor_, &dynamic_schema](auto idx,
                                                                                                auto density) {
                    merge_impl<decltype(idx), decltype(density), decltype(input_streams), decltype(comp), decltype(stream_id)>(ret,
                                                                                                          input_streams,
                                                                                                          add_symbol_column,
                                                                                                          stream_id,
                                                                                                          row_range,
                                                                                                          col_range,
                                                                                                          idx,
                                                                                                          stream_descriptor,
                                                                                                          *dynamic_schema);
                }, index_, density_policy_);

        return ret;
    }

[[nodiscard]] Composite<ProcessingSegment>
RemoveColumnPartitioningClause::process(std::shared_ptr<Store> store, Composite<ProcessingSegment> &&p) const {
        using namespace arcticdb::pipelines;
        auto procs = std::move(p);
        Composite<ProcessingSegment> output;
        procs.broadcast([&store, &output](ProcessingSegment &proc) {
            size_t min_start_row = std::numeric_limits<size_t>::max();
            size_t max_end_row = 0;
            size_t min_start_col = std::numeric_limits<size_t>::max();
            size_t max_end_col = 0;
            std::optional<SegmentInMemory> output_seg;
            for (auto& slice_and_key: proc.data()) {
                min_start_row = std::min(min_start_row, slice_and_key.slice().row_range.start());
                max_end_row = std::max(max_end_row, slice_and_key.slice().row_range.end());
                min_start_col = std::min(min_start_col, slice_and_key.slice().col_range.start());
                max_end_col = std::max(max_end_col, slice_and_key.slice().col_range.end());
                auto segment = std::move(slice_and_key.segment(store));
                if (output_seg.has_value()) {
                    stream::merge_string_columns(segment, output_seg->string_pool_ptr(), false);
                    output_seg->concatenate(std::move(segment), true);
                } else {
                    output_seg = std::make_optional<SegmentInMemory>(std::move(segment));
                }
            }
            if (output_seg.has_value()) {
                const RowRange row_range{min_start_row, max_end_row};
                const ColRange col_range{min_start_col, max_end_col};
                output.push_back(ProcessingSegment{std::move(*output_seg), FrameSlice{col_range, row_range}, proc.dynamic_schema_});
            }
        });
        return output;
}

}