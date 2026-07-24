/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <vector>
#include <variant>

#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/column_store/segment_reslicer.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/stream/merge.hpp>

#include <arcticdb/processing/clause.hpp>

#include <arcticdb/pipeline/column_name_resolution.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/util/test/random_throw.hpp>
#include <ankerl/unordered_dense.h>
#include <arcticdb/util/movable_priority_queue.hpp>
#include <arcticdb/stream/merge_utils.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/stream_sink.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/util/collection_utils.hpp>

#include <ranges>

namespace arcticdb {

namespace ranges = std::ranges;
using namespace pipelines;

class GroupingMap {
    using NumericMapType = std::variant<
            std::monostate, std::shared_ptr<ankerl::unordered_dense::map<bool, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<uint8_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<uint16_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<uint32_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<uint64_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<int8_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<int16_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<int32_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<int64_t, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<float, size_t>>,
            std::shared_ptr<ankerl::unordered_dense::map<double, size_t>>>;

    NumericMapType map_;

  public:
    size_t size() const {
        return util::variant_match(
                map_, [](const std::monostate&) { return size_t(0); }, [](const auto& other) { return other->size(); }
        );
    }

    template<typename T>
    std::shared_ptr<ankerl::unordered_dense::map<T, size_t>> get() {
        ARCTICDB_DEBUG_THROW(5)
        return util::variant_match(
                map_,
                [that = this](const std::monostate&) {
                    that->map_ = std::make_shared<ankerl::unordered_dense::map<T, size_t>>();
                    return std::get<std::shared_ptr<ankerl::unordered_dense::map<T, size_t>>>(that->map_);
                },
                [](const std::shared_ptr<ankerl::unordered_dense::map<T, size_t>>& ptr) { return ptr; },
                [](const auto&) -> std::shared_ptr<ankerl::unordered_dense::map<T, size_t>> {
                    schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                            "GroupBy does not support the grouping column type changing with dynamic schema"
                    );
                }
        );
    }
};

struct SegmentWrapper {
    SegmentInMemory seg_;
    SegmentInMemory::iterator it_;
    const StreamId id_;

    explicit SegmentWrapper(SegmentInMemory&& seg) :
        seg_(std::move(seg)),
        it_(seg_.begin()),
        id_(seg_.descriptor().id()) {}

    bool advance() { return ++it_ != seg_.end(); }

    SegmentInMemory::Row& row() { return *it_; }

    const StreamId& id() const { return id_; }
};

OutputSchema modify_schema(OutputSchema&& schema, const std::vector<std::shared_ptr<Clause>>& clauses) {
    for (const auto& clause : clauses) {
        schema = clause->modify_schema(std::move(schema));
    }
    return schema;
}

std::vector<EntityId> PassthroughClause::process(std::vector<EntityId>&& entity_ids) const {
    return std::move(entity_ids);
}

std::vector<EntityId> FilterClause::process(std::vector<EntityId>&& entity_ids) const {
    ARCTICDB_SAMPLE(FilterClause, 0)
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    proc.set_expression_context(expression_context_);
    ARCTICDB_RUNTIME_DEBUG(
            log::memory(), "Doing filter {} for entity ids {}", expression_context_->root_->label_, entity_ids
    );
    auto variant_data = expression_context_->root_->compute(proc);
    std::vector<EntityId> output;
    util::variant_match(
            variant_data,
            [&proc, &output, this](util::BitSet& bitset) {
                if (bitset.count() > 0) {
                    proc.apply_filter(std::move(bitset), optimisation_);
                    output = push_entities(*component_manager_, std::move(proc));
                } else {
                    log::memory().debug("Filter returned empty result");
                }
            },
            [](EmptyResult) { log::memory().debug("Filter returned empty result"); },
            [&output, &proc, this](FullResult) { output = push_entities(*component_manager_, std::move(proc)); },
            [](const auto&) { util::raise_rte("Expected bitset from filter clause"); }
    );
    return output;
}

OutputSchema FilterClause::modify_schema(OutputSchema&& output_schema) const {
    check_column_presence(output_schema, clause_info_.input_columns_, "Filter");
    std::variant<BitSetTag, DataType> return_type = expression_context_->root_->compute(output_schema.column_types());
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            std::holds_alternative<BitSetTag>(return_type), "FilterClause AST would produce a column, not a bitset"
    );
    return output_schema;
}

std::string FilterClause::to_string() const {
    return expression_context_ ? fmt::format("WHERE {}", expression_context_->root_->label_) : "";
}

std::vector<EntityId> ProjectClause::process(std::vector<EntityId>&& entity_ids) const {
    ARCTICDB_DEBUG_THROW(5)
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    proc.set_expression_context(expression_context_);
    auto variant_data = expression_context_->root_->compute(proc);
    std::vector<EntityId> output;
    util::variant_match(
            variant_data,
            [&proc, &output, this](ColumnWithStrings& col) {
                add_column(proc, col);
                output = push_entities(*component_manager_, std::move(proc));
            },
            [&proc, &output, this](const std::shared_ptr<Value>& val) {
                // It is possible for the AST to produce a Value, either through use of apply with a raw
                // value, or through use of the ternary operator
                // Turn this Value into a dense column where all of the entries are the same as this value,
                // of the same length as the other segments in this processing unit
                auto rows = proc.segments_->back()->row_count();
                auto output_column = std::make_unique<Column>(val->descriptor(), Sparsity::PERMITTED);
                auto output_bytes = rows * get_type_size(output_column->type().data_type());
                output_column->allocate_data(output_bytes);
                output_column->set_row_data(rows);
                auto string_pool = std::make_shared<StringPool>();
                details::visit_type(val->data_type(), [&](auto val_tag) {
                    using val_type_info = ScalarTypeInfo<decltype(val_tag)>;
                    if constexpr (is_dynamic_string_type(val_type_info::data_type)) {
                        using TargetType = val_type_info::RawType;
                        const auto offset = string_pool->get(*val->str_data(), val->len()).offset();
                        auto data = output_column->ptr_cast<TargetType>(0, output_bytes);
                        std::fill_n(data, rows, offset);
                    } else if constexpr (is_numeric_type(val_type_info::data_type) ||
                                         is_bool_type(val_type_info::data_type)) {
                        using TargetType = val_type_info::RawType;
                        auto value = static_cast<TargetType>(val->get<typename val_type_info::RawType>());
                        auto data = output_column->ptr_cast<TargetType>(0, output_bytes);
                        std::fill_n(data, rows, value);
                    } else {
                        util::raise_rte("Unexpected Value type in ProjectClause: {}", val->data_type());
                    }
                });
                ColumnWithStrings col(std::move(output_column), string_pool, "");
                add_column(proc, col);
                output = push_entities(*component_manager_, std::move(proc));
            },
            [&proc, &output, this](const EmptyResult&) {
                if (expression_context_->dynamic_schema_)
                    output = push_entities(*component_manager_, std::move(proc));
                else
                    util::raise_rte("Cannot project from empty column with static schema");
            },
            [](const auto&) { util::raise_rte("Expected column from projection clause"); }
    );
    return output;
}

OutputSchema ProjectClause::modify_schema(OutputSchema&& output_schema) const {
    check_column_presence(output_schema, clause_info_.input_columns_, "Project");
    const auto& root = *expression_context_->root_;
    if (root.is_value()) {
        const auto& value = std::get<std::shared_ptr<Value>>(std::get<ExpressionNode::Leaf>(root.kind_));
        output_schema.add_field(output_column_, value->descriptor().data_type());
    } else {
        std::variant<BitSetTag, DataType> return_type = root.compute(output_schema.column_types());
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                std::holds_alternative<DataType>(return_type), "ProjectClause AST would produce a bitset, not a column"
        );
        output_schema.add_field(output_column_, std::get<DataType>(return_type));
    }
    return output_schema;
}

[[nodiscard]] std::string ProjectClause::to_string() const {
    return expression_context_
                   ? fmt::format("PROJECT Column[\"{}\"] = {}", output_column_, expression_context_->root_->label_)
                   : "";
}

void ProjectClause::add_column(ProcessingUnit& proc, const ColumnWithStrings& col) const {
    auto& last_segment = *proc.segments_->back();

    auto seg = std::make_shared<SegmentInMemory>();
    // Add in the same index fields as the last existing segment in proc
    seg->descriptor().set_index(last_segment.descriptor().index());
    for (uint32_t idx = 0; idx < last_segment.descriptor().index().field_count(); ++idx) {
        seg->add_column(last_segment.field(idx), last_segment.column_ptr(idx));
    }
    // Add the column with its string pool and set the segment row data
    seg->add_column(scalar_field(col.column_->type().data_type(), output_column_), col.column_);
    seg->set_string_pool(col.string_pool_);
    seg->set_row_data(last_segment.row_count() - 1);

    // Calculate the row range and col range for the new segment
    auto row_range = std::make_shared<RowRange>(*proc.row_ranges_->back());
    auto last_col_idx = proc.col_ranges_->back()->second;
    auto col_range = std::make_shared<ColRange>(last_col_idx, last_col_idx + 1);

    proc.segments_->emplace_back(std::move(seg));
    proc.row_ranges_->emplace_back(std::move(row_range));
    proc.col_ranges_->emplace_back(std::move(col_range));
}

AggregationClause::AggregationClause(
        const std::string& grouping_column, const std::vector<NamedAggregator>& named_aggregators
) :
    grouping_column_(grouping_column) {
    ARCTICDB_DEBUG_THROW(5)

    clause_info_.input_structure_ = ProcessingStructure::HASH_BUCKETED;
    clause_info_.can_combine_with_column_selection_ = false;
    clause_info_.index_ = NewIndex(grouping_column_);
    clause_info_.input_columns_ = {grouping_column_};
    str_ = "AGGREGATE {";
    for (const auto& named_aggregator : named_aggregators) {
        str_.append(fmt::format(
                "{}: ({}, {}), ",
                named_aggregator.output_column_name_,
                named_aggregator.input_column_name_,
                named_aggregator.aggregation_operator_
        ));
        clause_info_.input_columns_.insert(named_aggregator.input_column_name_);
        auto typed_input_column_name = ColumnName(named_aggregator.input_column_name_);
        auto typed_output_column_name = ColumnName(named_aggregator.output_column_name_);
        if (named_aggregator.aggregation_operator_ == "sum") {
            aggregators_.emplace_back(SumAggregatorUnsorted(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "mean") {
            aggregators_.emplace_back(MeanAggregatorUnsorted(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "max") {
            aggregators_.emplace_back(MaxAggregatorUnsorted(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "min") {
            aggregators_.emplace_back(MinAggregatorUnsorted(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "count") {
            aggregators_.emplace_back(CountAggregatorUnsorted(typed_input_column_name, typed_output_column_name));
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Unknown aggregation operator provided: {}", named_aggregator.aggregation_operator_
            );
        }
    }
    str_.append("}");
}

std::vector<std::vector<EntityId>> AggregationClause::structure_for_processing(
        std::vector<std::vector<EntityId>>&& entity_ids_vec
) {
    // Some could be empty, so actual number may be lower
    auto max_num_buckets = ConfigsMap::instance()->get_int(
            "Partition.NumBuckets", async::TaskScheduler::instance()->cpu_thread_count()
    );
    max_num_buckets = std::min(max_num_buckets, static_cast<int64_t>(std::numeric_limits<bucket_id>::max()));
    // Preallocate results with expected sizes, erase later if any are empty
    std::vector<std::vector<EntityId>> res(max_num_buckets);
    // With an even distribution, expect each element of res to have entity_ids_vec.size() elements
    for (auto& res_element : res) {
        res_element.reserve(entity_ids_vec.size());
    }
    ARCTICDB_DEBUG_THROW(5)
    // Experimentation shows flattening the entities into a single vector and a single call to
    // component_manager_->get is faster than not flattening and making multiple calls
    auto entity_ids = util::flatten_vectors(std::move(entity_ids_vec));
    auto [buckets] = component_manager_->get_entities<bucket_id>(entity_ids);
    for (auto [idx, entity_id] : folly::enumerate(entity_ids)) {
        res[buckets[idx]].emplace_back(entity_id);
    }
    // Get rid of any empty buckets
    std::erase_if(res, [](const std::vector<EntityId>& entity_ids) { return entity_ids.empty(); });
    return res;
}

std::vector<EntityId> AggregationClause::process(std::vector<EntityId>&& entity_ids) const {
    ARCTICDB_DEBUG_THROW(5)
    ARCTICDB_SAMPLE(AggregationClause, 0)
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    auto row_slices = split_by_row_slice(std::move(proc));

    // Sort procs following row range descending order, as we are going to iterate through them backwards
    // front() is UB if vector is empty. Should be non-empty by construction, but exception > UB
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            ranges::all_of(
                    row_slices,
                    [](const auto& proc) { return proc.row_ranges_.has_value() && !proc.row_ranges_->empty(); }
            ),
            "Unexpected empty row_ranges_ in AggregationClause::process"
    );
    ranges::sort(row_slices, [](const auto& left, const auto& right) {
        return left.row_ranges_->front()->start() > right.row_ranges_->front()->start();
    });

    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            !aggregators_.empty(), "AggregationClause::process does not make sense with no aggregators"
    );
    std::vector<GroupingAggregatorData> aggregators_data;
    aggregators_data.reserve(aggregators_.size());
    ranges::transform(aggregators_, std::back_inserter(aggregators_data), [&](const auto& agg) {
        return agg.get_aggregator_data();
    });
    // Work out the common type between the processing units for the columns being aggregated
    for (auto& row_slice : row_slices) {
        for (auto agg_data : folly::enumerate(aggregators_data)) {
            // Check that segments row ranges are the same
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    ranges::all_of(
                            *row_slice.row_ranges_,
                            [&](const auto& row_range) {
                                return row_range->start() == row_slice.row_ranges_->at(0)->start();
                            }
                    ),
                    "Expected all data segments in one processing unit to have the same row ranges"
            );

            auto input_column_name = aggregators_.at(agg_data.index).get_input_column_name();
            auto input_column = row_slice.get(input_column_name);
            if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                agg_data->add_data_type(std::get<ColumnWithStrings>(input_column).column_->type().data_type());
            }
        }
    }

    size_t num_unique{0};
    size_t next_group_id{0};
    auto string_pool = std::make_shared<StringPool>();
    DataType grouping_data_type;
    GroupingMap grouping_map;
    // Iterating backwards as we are going to erase from this vector as we go along
    // This is to spread out deallocation of the input segments
    auto it = row_slices.rbegin();
    while (it != row_slices.rend()) {
        auto& row_slice = *it;
        auto partitioning_column = row_slice.get(ColumnName(grouping_column_));
        if (std::holds_alternative<ColumnWithStrings>(partitioning_column)) {
            ColumnWithStrings col = std::get<ColumnWithStrings>(partitioning_column);
            details::visit_type(col.column_->type().data_type(), [&, this](auto data_type_tag) {
                using col_type_info = ScalarTypeInfo<decltype(data_type_tag)>;
                grouping_data_type = col_type_info::data_type;
                // Faster to initialise to zero (missing value group) and use raw ptr than repeated calls to
                // emplace_back
                std::vector<size_t> row_to_group(col.column_->last_row() + 1, 0);
                size_t* row_to_group_ptr = row_to_group.data();
                auto hash_to_group = grouping_map.get<typename col_type_info::RawType>();
                // For string grouping columns, keep a local map within this ProcessingUnit
                // from offsets to groups, to avoid needless calls to col.string_at_offset and
                // string_pool->get
                // This could be slower in cases where there aren't many repeats in string
                // grouping columns. Maybe track hit ratio of finds and stop using it if it is
                // too low?
                // Tested with 100,000,000 row dataframe with 100,000 unique values in the grouping column. Timings:
                // 11.14 seconds without caching
                // 11.01 seconds with caching
                // Not worth worrying about right now
                ankerl::unordered_dense::map<typename col_type_info::RawType, size_t> offset_to_group;

                const bool is_sparse = col.column_->is_sparse();
                if (is_sparse && next_group_id == 0) {
                    // We use 0 for the missing value group id
                    ++next_group_id;
                }
                ssize_t previous_value_index = 0;

                arcticdb::for_each_enumerated<typename col_type_info::TDT>(
                        *col.column_,
                        [&] ARCTICDB_LAMBDA_INLINE(auto enumerating_it) {
                            typename col_type_info::RawType val;
                            if constexpr (is_sequence_type(col_type_info::data_type)) {
                                auto offset = enumerating_it.value();
                                if (auto it = offset_to_group.find(offset); it != offset_to_group.end()) {
                                    val = it->second;
                                } else {
                                    std::optional<std::string_view> str = col.string_at_offset(offset);
                                    if (str.has_value()) {
                                        val = string_pool->get(*str, true).offset();
                                    } else {
                                        val = offset;
                                    }
                                    typename col_type_info::RawType val_copy(val);
                                    offset_to_group.emplace(offset, val_copy);
                                }
                            } else {
                                val = enumerating_it.value();
                            }

                            if (is_sparse) {
                                for (auto j = previous_value_index; j != enumerating_it.idx(); ++j) {
                                    static constexpr size_t missing_value_group_id = 0;
                                    *row_to_group_ptr++ = missing_value_group_id;
                                }
                                previous_value_index = enumerating_it.idx() + 1;
                            }

                            if (auto it = hash_to_group->find(val); it == hash_to_group->end()) {
                                *row_to_group_ptr++ = next_group_id;
                                auto group_id = next_group_id++;
                                hash_to_group->emplace(val, group_id);
                            } else {
                                *row_to_group_ptr++ = it->second;
                            }
                        }
                );

                num_unique = next_group_id;
                util::check(num_unique != 0, "Got zero unique values");
                for (auto agg_data : folly::enumerate(aggregators_data)) {
                    auto input_column_name = aggregators_.at(agg_data.index).get_input_column_name();
                    auto input_column = row_slice.get(input_column_name);
                    std::optional<ColumnWithStrings> opt_input_column;
                    if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                        auto column_with_strings = std::get<ColumnWithStrings>(input_column);
                        // Empty columns don't contribute to aggregations
                        if (!is_empty_type(column_with_strings.column_->type().data_type())) {
                            opt_input_column.emplace(std::move(column_with_strings));
                        }
                    }
                    if (opt_input_column) {
                        // The column is missing from the segment. Do not perform any aggregation and leave it to
                        // the NullValueReducer to take care of the default values.
                        agg_data->aggregate(*opt_input_column, row_to_group, num_unique);
                    }
                }
            });
        } else {
            util::raise_rte("Expected single column from expression");
        }
        it = static_cast<decltype(row_slices)::reverse_iterator>((row_slices.erase(std::next(it).base())));
    }
    SegmentInMemory seg;
    auto index_col = std::make_shared<Column>(
            make_scalar_type(grouping_data_type), grouping_map.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED
    );

    seg.add_column(scalar_field(grouping_data_type, grouping_column_), index_col);
    seg.descriptor().set_index(IndexDescriptorImpl(IndexDescriptorImpl::Type::ROWCOUNT, 0));

    details::visit_type(grouping_data_type, [&grouping_map, &index_col](auto data_type_tag) {
        using col_type_info = ScalarTypeInfo<decltype(data_type_tag)>;
        auto hashes = grouping_map.get<typename col_type_info::RawType>();
        std::vector<std::pair<typename col_type_info::RawType, size_t>> elements;
        for (const auto& hash : *hashes)
            elements.emplace_back(hash.first, hash.second);

        ranges::sort(
                elements,
                [](const std::pair<typename col_type_info::RawType, size_t>& l,
                   const std::pair<typename col_type_info::RawType, size_t>& r) { return l.second < r.second; }
        );

        auto column_data = index_col->data();
        std::transform(
                elements.cbegin(),
                elements.cend(),
                column_data.begin<typename col_type_info::TDT>(),
                [](const auto& element) { return element.first; }
        );
    });
    index_col->set_row_data(grouping_map.size() - 1);

    for (auto agg_data : folly::enumerate(aggregators_data)) {
        seg.concatenate(agg_data->finalize(
                aggregators_.at(agg_data.index).get_output_column_name(), processing_config_.dynamic_schema_, num_unique
        ));
    }

    seg.set_string_pool(string_pool);
    seg.set_row_id(num_unique - 1);
    return push_entities(*component_manager_, ProcessingUnit(std::move(seg)));
}

OutputSchema AggregationClause::modify_schema(OutputSchema&& output_schema) const {
    check_column_presence(output_schema, clause_info_.input_columns_, "Aggregation");
    output_schema.clear_default_values();
    const auto& input_stream_desc = output_schema.stream_descriptor();
    StreamDescriptor stream_desc(input_stream_desc.id());
    stream_desc.add_field(input_stream_desc.field(*input_stream_desc.find_field(grouping_column_)));
    stream_desc.set_index({IndexDescriptorImpl::Type::ROWCOUNT, 0});

    for (const auto& agg : aggregators_) {
        const auto& input_column_name = agg.get_input_column_name().value;
        const auto& output_column_name = agg.get_output_column_name().value;
        const auto& input_column_type = output_schema.column_types()[input_column_name];
        auto agg_data = agg.get_aggregator_data();
        agg_data.add_data_type(input_column_type);
        const DataType output_column_type = agg_data.get_output_data_type();
        stream_desc.add_scalar_field(output_column_type, output_column_name);
        const std::optional<Value>& default_value = agg_data.get_default_value();
        if (default_value) {
            output_schema.set_default_value_for_column(output_column_name, *default_value);
        }
    }

    output_schema.set_stream_descriptor(std::move(stream_desc));
    auto mutable_index = output_schema.norm_metadata_.mutable_df()->mutable_common()->mutable_index();
    mutable_index->set_name(grouping_column_);
    mutable_index->clear_fake_name();
    mutable_index->set_is_physically_stored(true);
    return output_schema;
}

[[nodiscard]] std::string AggregationClause::to_string() const { return str_; }

[[nodiscard]] std::vector<EntityId> RemoveColumnPartitioningClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    size_t min_start_row = std::numeric_limits<size_t>::max();
    size_t max_end_row = 0;
    size_t min_start_col = std::numeric_limits<size_t>::max();
    size_t max_end_col = 0;
    std::optional<SegmentInMemory> output_seg;
    for (auto&& [idx, segment] : folly::enumerate(proc.segments_.value())) {
        min_start_row = std::min(min_start_row, proc.row_ranges_->at(idx)->start());
        max_end_row = std::max(max_end_row, proc.row_ranges_->at(idx)->end());
        min_start_col = std::min(min_start_col, proc.col_ranges_->at(idx)->start());
        max_end_col = std::max(max_end_col, proc.col_ranges_->at(idx)->end());
        if (output_seg.has_value()) {
            merge_string_columns(*segment, output_seg->string_pool_ptr(), false);
            output_seg->concatenate(std::move(*segment), true);
        } else {
            output_seg = std::make_optional<SegmentInMemory>(std::move(*segment));
        }
    }
    std::vector<EntityId> output;
    if (output_seg.has_value()) {
        output = push_entities(
                *component_manager_,
                ProcessingUnit(
                        std::move(*output_seg),
                        RowRange{min_start_row, max_end_row},
                        ColRange{min_start_col, max_end_col}
                )
        );
    }
    return output;
}

std::vector<EntityId> SplitClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    std::vector<EntityId> ret;
    for (auto&& [idx, seg] : folly::enumerate(proc.segments_.value())) {
        auto split_segs = seg->split(rows_);
        size_t start_row = proc.row_ranges_->at(idx)->start();
        size_t end_row = 0;
        for (auto&& split_seg : split_segs) {
            end_row = start_row + split_seg.row_count();
            auto new_entity_ids = push_entities(
                    *component_manager_,
                    ProcessingUnit(
                            std::move(split_seg), RowRange(start_row, end_row), std::move(*proc.col_ranges_->at(idx))
                    )
            );
            ret.insert(ret.end(), new_entity_ids.begin(), new_entity_ids.end());
            start_row = end_row;
        }
    }
    return ret;
}

std::vector<EntityId> SortClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    for (auto& seg : proc.segments_.value()) {
        // This modifies the segment in place, which goes against the ECS principle of all entities being immutable
        // Only used by SortMerge right now and so this is fine, although it would not generalise well
        seg->sort(column_);
    }
    return push_entities(*component_manager_, std::move(proc));
}

template<typename IndexType, typename DensityPolicy, typename QueueType, bool dynamic_schema>
void merge_impl(
        std::shared_ptr<ComponentManager> component_manager, std::vector<std::vector<EntityId>>& ret,
        QueueType& input_streams, bool add_symbol_column, const RowRange& row_range, const ColRange& col_range,
        IndexType index, const StreamDescriptor& stream_descriptor
) {
    const auto num_segment_rows = ConfigsMap::instance()->get_int("Merge.SegmentSize", 100000);
    using SegmentationPolicy = stream::RowCountSegmentPolicy;
    SegmentationPolicy segmentation_policy{static_cast<size_t>(num_segment_rows)};

    auto commit_callback = [&component_manager, &ret, &col_range, start_row = row_range.first](SegmentInMemory&& segment
                           ) mutable {
        const size_t end_row = start_row + segment.row_count();
        ret.emplace_back(push_entities(
                *component_manager, ProcessingUnit{std::move(segment), RowRange{start_row, end_row}, col_range}
        ));
        start_row = end_row;
    };

    using Schema = std::conditional_t<dynamic_schema, stream::DynamicSchema, stream::FixedSchema>;
    using AggregatorType = stream::Aggregator<IndexType, Schema, SegmentationPolicy, DensityPolicy>;

    AggregatorType agg{
            Schema{stream_descriptor, index},
            std::move(commit_callback),
            std::move(segmentation_policy),
            stream_descriptor,
            std::nullopt
    };

    stream::do_merge(input_streams, agg, add_symbol_column);
}

MergeClause::MergeClause(
        stream::Index index, const stream::VariantColumnPolicy& density_policy, const StreamId& stream_id,
        const StreamDescriptor& stream_descriptor, bool dynamic_schema
) :
    index_(std::move(index)),
    density_policy_(density_policy),
    stream_id_(stream_id),
    stream_descriptor_(stream_descriptor),
    dynamic_schema_(dynamic_schema) {
    clause_info_.input_structure_ = ProcessingStructure::ALL;
    clause_info_.output_structure_ = ProcessingStructure::ALL;
}

void MergeClause::set_processing_config(const ProcessingConfig&) {}

void MergeClause::set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
    component_manager_ = std::move(component_manager);
}

const ClauseInfo& MergeClause::clause_info() const { return clause_info_; }

OutputSchema MergeClause::modify_schema(OutputSchema&& output_schema) const {
    check_is_timeseries(output_schema.stream_descriptor(), "Merge");
    return output_schema;
}

std::vector<std::vector<EntityId>> MergeClause::structure_for_processing(
        std::vector<std::vector<EntityId>>&& entity_ids_vec
) {

    // TODO this is a hack because we don't currently have a way to
    // specify any particular input shape unless a clause is the
    // first one and can use structure_for_processing. Ideally
    // merging should be parallel like resampling
    auto entity_ids = util::flatten_vectors(std::move(entity_ids_vec));
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );

    auto compare = [](const std::unique_ptr<SegmentWrapper>& left, const std::unique_ptr<SegmentWrapper>& right) {
        if (left->seg_.row_count() == 0) {
            return false;
        } else if (right->seg_.row_count() == 0) {
            return true;
        }
        const auto left_index = index::index_value_from_row(left->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0);
        const auto right_index = index::index_value_from_row(right->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0);
        return left_index > right_index;
    };

    movable_priority_queue<
            std::unique_ptr<SegmentWrapper>,
            std::vector<std::unique_ptr<SegmentWrapper>>,
            decltype(compare)>
            input_streams{compare};

    size_t min_start_row = std::numeric_limits<size_t>::max();
    size_t max_end_row = 0;
    size_t min_start_col = std::numeric_limits<size_t>::max();
    size_t max_end_col = 0;
    for (auto&& [idx, segment] : folly::enumerate(proc.segments_.value())) {
        size_t start_row = proc.row_ranges_->at(idx)->start();
        size_t end_row = proc.row_ranges_->at(idx)->end();
        min_start_row = std::min(start_row, min_start_row);
        max_end_row = std::max(end_row, max_end_row);

        size_t start_col = proc.col_ranges_->at(idx)->start();
        size_t end_col = proc.col_ranges_->at(idx)->end();
        min_start_col = std::min(start_col, min_start_col);
        max_end_col = std::max(end_col, max_end_col);

        input_streams.push(std::make_unique<SegmentWrapper>(std::move(*segment)));
    }
    const RowRange row_range{min_start_row, max_end_row};
    const ColRange col_range{min_start_col, max_end_col};
    std::vector<std::vector<EntityId>> ret;
    std::visit(
            [this, &ret, &input_streams, stream_id = stream_id_, &row_range, &col_range](auto idx, auto density) {
                if (dynamic_schema_) {
                    merge_impl<decltype(idx), decltype(density), decltype(input_streams), true>(
                            component_manager_,
                            ret,
                            input_streams,
                            add_symbol_column_,
                            row_range,
                            col_range,
                            idx,
                            stream_descriptor_
                    );
                } else {
                    merge_impl<decltype(idx), decltype(density), decltype(input_streams), false>(
                            component_manager_,
                            ret,
                            input_streams,
                            add_symbol_column_,
                            row_range,
                            col_range,
                            idx,
                            stream_descriptor_
                    );
                }
            },
            index_,
            density_policy_
    );
    return ret;
}

// MergeClause receives a list of DataFrames as input and merge them into a single one where all
// the rows are sorted by time stamp
std::vector<EntityId> MergeClause::process(std::vector<EntityId>&& entity_ids) const { return std::move(entity_ids); }

std::vector<EntityId> ColumnStatsGenerationClause::process(std::vector<EntityId>&& entity_ids) const {
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            !entity_ids.empty(), "ColumnStatsGenerationClause::process does not make sense with no processing units"
    );
    auto proc = gather_entities<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>,
            std::shared_ptr<AtomKey>>(*component_manager_, std::move(entity_ids));
    std::vector<ColumnStatsAggregatorData> aggregators_data;
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            static_cast<bool>(column_stats_aggregators_),
            "ColumnStatsGenerationClause::process does not make sense with no aggregators"
    );
    for (const auto& agg : *column_stats_aggregators_) {
        aggregators_data.emplace_back(agg.get_aggregator_data());
    }

    ankerl::unordered_dense::set<IndexValue> start_indexes;
    ankerl::unordered_dense::set<IndexValue> end_indexes;

    for (const auto& key : proc.atom_keys_.value()) {
        start_indexes.insert(key->start_index());
        end_indexes.insert(key->end_index());
    }
    for (auto agg_data : folly::enumerate(aggregators_data)) {
        auto input_column_name = column_stats_aggregators_->at(agg_data.index).get_input_column_name();
        auto input_column = proc.get(input_column_name);
        if (std::holds_alternative<ColumnWithStrings>(input_column)) {
            auto input_column_with_strings = std::get<ColumnWithStrings>(input_column);
            agg_data->aggregate(input_column_with_strings);
        } else {
            if (!processing_config_.dynamic_schema_)
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                        "Unable to resolve column denoted by aggregation operator: '{}'", input_column_name
                );
        }
    }

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            start_indexes.size() == 1 && end_indexes.size() == 1,
            "Expected all data segments in one processing unit to have same start and end indexes"
    );
    auto start_index = *start_indexes.begin();
    auto end_index = *end_indexes.begin();
    schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
            std::holds_alternative<NumericIndex>(start_index) && std::holds_alternative<NumericIndex>(end_index),
            "Cannot build column stats over string-indexed symbol"
    );
    auto start_index_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), Sparsity::PERMITTED);
    auto end_index_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), Sparsity::PERMITTED);
    start_index_col->template push_back<NumericIndex>(std::get<NumericIndex>(start_index));
    end_index_col->template push_back<NumericIndex>(std::get<NumericIndex>(end_index));
    start_index_col->set_row_data(0);
    end_index_col->set_row_data(0);

    SegmentInMemory seg;
    seg.descriptor().set_index(IndexDescriptorImpl(IndexDescriptorImpl::Type::ROWCOUNT, 0));
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, start_index_column_name), start_index_col);
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, end_index_column_name), end_index_col);
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader merged_header;
    for (const auto& agg_data : folly::enumerate(aggregators_data)) {
        auto finalized = agg_data->finalize(column_stats_aggregators_->at(agg_data.index).get_output_column_names());
        auto offset_base = seg.descriptor().field_count();
        util::check(finalized.metadata(), "Expect finalized to have metadata, ColumnStatsGenerationClause::process");
        arcticc::pb2::column_stats_pb2::ColumnStatsHeader sub_header;
        bool unpacked = finalized.metadata()->UnpackTo(&sub_header);
        util::check(unpacked, "Could not unpack meta to a ColumnStatsHeader in ColumnStatsGenerationClause#process");
        for (const auto& [data_col_offset, entry_list] : sub_header.stats_by_column()) {
            auto& merged_entry_list = (*merged_header.mutable_stats_by_column())[data_col_offset];
            for (const auto& entry : entry_list.entries()) {
                auto* new_entry = merged_entry_list.add_entries();
                new_entry->set_stats_seg_offset(entry.stats_seg_offset() + offset_base);
                new_entry->set_type(entry.type());
            }
        }
        seg.concatenate(std::move(finalized));
    }
    google::protobuf::Any any;
    bool packed = any.PackFrom(merged_header);
    util::check(packed, "Failed to pack merged_header into Any in ColumnStatsGenerationClause#process");
    seg.set_metadata(std::move(any));
    seg.set_row_id(0);
    return push_entities(*component_manager_, ProcessingUnit(std::move(seg)));
}

std::vector<std::vector<size_t>> RowRangeClause::structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys) {
    const auto row_range_filter = RowRange{start_, end_};
    std::erase_if(ranges_and_keys, [&](const RangesAndKey& ranges_and_key) {
        return !is_slice_in_row_range(ranges_and_key.row_range(), row_range_filter);
    });
    return structure_by_row_slice(ranges_and_keys);
}

std::vector<std::vector<EntityId>> RowRangeClause::structure_for_processing(
        std::vector<std::vector<EntityId>>&& entity_ids_vec
) {
    auto entity_ids = util::flatten_vectors(std::move(entity_ids_vec));
    if (entity_ids.empty()) {
        return {};
    }
    auto [segments, old_row_ranges, col_ranges] = component_manager_->get_entities<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>>(entity_ids);

    // Map from old row ranges to new ones
    std::map<RowRange, RowRange> row_range_mapping;
    for (const auto& row_range : old_row_ranges) {
        // Value is same as key initially
        row_range_mapping.insert({*row_range, *row_range});
    }
    bool first_range{true};
    size_t prev_range_end{0};
    for (auto& [old_range, new_range] : row_range_mapping) {
        if (first_range) {
            // Make the first row-range start from zero
            new_range.first = 0;
            new_range.second = old_range.diff();
            first_range = false;
        } else {
            new_range.first = prev_range_end;
            new_range.second = new_range.first + old_range.diff();
        }
        prev_range_end = new_range.second;
    }

    calculate_start_and_end(prev_range_end);

    std::vector<std::shared_ptr<RowRange>> new_row_ranges;
    new_row_ranges.reserve(old_row_ranges.size());
    std::vector<RangesAndEntity> ranges_and_entities;
    ranges_and_entities.reserve(entity_ids.size());
    for (size_t idx = 0; idx < entity_ids.size(); ++idx) {
        auto new_row_range = std::make_shared<RowRange>(row_range_mapping.at(*old_row_ranges[idx]));
        ranges_and_entities.emplace_back(entity_ids[idx], new_row_range, col_ranges[idx]);
        new_row_ranges.emplace_back(std::move(new_row_range));
    }

    component_manager_->replace_entities<std::shared_ptr<RowRange>>(entity_ids, new_row_ranges);

    auto new_structure_offsets = structure_by_row_slice(ranges_and_entities);
    return offsets_to_entity_ids(new_structure_offsets, ranges_and_entities);
}

std::vector<EntityId> RowRangeClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    std::vector<EntityId> output;
    // The processing unit represents one row slice by construction, so just look at the first
    auto row_range = proc.row_ranges_->at(0);
    if (start_ >= row_range->end() || end_ <= row_range->start()) {
        // No rows from this processing unit are required
        return {};
    } else if ((start_ > row_range->start() && start_ < row_range->end()) ||
               (end_ > row_range->start() && end_ < row_range->end())) {
        // Zero-indexed within this slice
        size_t start_row{0};
        size_t end_row{row_range->diff()};
        if (start_ > row_range->start() && start_ < row_range->end()) {
            start_row = start_ - row_range->start();
        }
        if (end_ > row_range->start() && end_ < row_range->end()) {
            end_row = end_ - (row_range->start());
        }
        proc.truncate(start_row, end_row);
    } // else all rows in this segment are required, do nothing
    return push_entities(*component_manager_, std::move(proc));
}

void RowRangeClause::set_processing_config(const ProcessingConfig& processing_config) {
    calculate_start_and_end(processing_config.total_rows_);
}

std::string RowRangeClause::to_string() const {
    if (row_range_type_ == RowRangeType::RANGE) {
        if (user_provided_start_ && user_provided_end_) {
            return fmt::format("ROWRANGE: RANGE, start={}, end={}", user_provided_start_, user_provided_end_);
        } else if (user_provided_start_) {
            return fmt::format("ROWRANGE: RANGE, start={}, end=OPEN", user_provided_start_);
        } else if (user_provided_end_) {
            return fmt::format("ROWRANGE: RANGE, start=OPEN, end={}", user_provided_end_);
        } else {
            return fmt::format("ROWRANGE: RANGE, start=OPEN, end=OPEN");
        }
    }

    return fmt::format("ROWRANGE: {}, n={}", row_range_type_ == RowRangeType::HEAD ? "HEAD" : "TAIL", n_);
}

void RowRangeClause::calculate_start_and_end(size_t total_rows) {
    auto signed_total_rows = static_cast<int64_t>(total_rows);
    switch (row_range_type_) {
    case RowRangeType::HEAD:
        if (n_ >= 0) {
            start_ = 0;
            end_ = std::min(n_, signed_total_rows);
        } else {
            start_ = 0;
            end_ = std::max(static_cast<int64_t>(0), signed_total_rows + n_);
        }
        break;
    case RowRangeType::TAIL:
        if (n_ >= 0) {
            start_ = std::max(static_cast<int64_t>(0), signed_total_rows - n_);
            end_ = signed_total_rows;
        } else {
            start_ = std::min(-n_, signed_total_rows);
            end_ = signed_total_rows;
        }
        break;
    case RowRangeType::RANGE:
        // Wrap around negative indices.
        start_ =
                (user_provided_start_ >= 0
                         ? std::min(user_provided_start_, signed_total_rows)
                         : std::max(signed_total_rows + user_provided_start_, static_cast<int64_t>(0)));
        end_ = (user_provided_end_ >= 0 ? std::min(user_provided_end_, signed_total_rows)
                                        : std::max(signed_total_rows + user_provided_end_, static_cast<int64_t>(0)));
        break;

    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "Unrecognised RowRangeType {}", static_cast<uint8_t>(row_range_type_)
        );
    }
}

std::vector<std::vector<size_t>> DateRangeClause::structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys) {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            processing_config_.index_type_ == IndexDescriptor::Type::TIMESTAMP,
            "Cannot use date range with non-timestamp indexed data"
    );
    const auto index_filter = IndexRange(start_, end_);
    std::erase_if(ranges_and_keys, [&](const RangesAndKey& ranges_and_key) {
        const auto slice_index_range = IndexRange(ranges_and_key.key_.time_range());
        return !is_slice_in_index_range(slice_index_range, index_filter, true);
    });
    return structure_by_row_slice(ranges_and_keys);
}

std::vector<EntityId> DateRangeClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty() || start_ > end_) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, std::move(entity_ids)
    );
    std::vector<EntityId> output;
    // We are only interested in the index, which is in every SegmentInMemory in proc.segments_, so just use the
    // first
    auto row_range = proc.row_ranges_->at(0);
    auto start_index = std::get<timestamp>(stream::TimeseriesIndex::start_value_for_segment(*proc.segments_->at(0)));
    auto end_index = std::get<timestamp>(stream::TimeseriesIndex::end_value_for_segment(*proc.segments_->at(0)));
    if (start_index > end_ || end_index < start_) {
        // No rows from this processing unit are required
        return {};
    } else if ((start_ > start_index && start_ <= end_index) || (end_ >= start_index && end_ <= end_index)) {
        size_t start_row{0};
        size_t end_row{row_range->diff()};
        if (start_ > start_index && start_ <= end_index) {
            start_row = lower_bound_idx<timestamp>(*proc.segments_->at(0)->column_ptr(0), start_);
        }
        if (end_ >= start_index && end_ <= end_index) {
            end_row = upper_bound_idx<timestamp>(*proc.segments_->at(0)->column_ptr(0), end_);
        }
        proc.truncate(start_row, end_row);
    } // else all rows in the processing unit are required, do nothing
    return push_entities(*component_manager_, std::move(proc));
}

void DateRangeClause::set_processing_config(const ProcessingConfig& processing_config) {
    processing_config_ = processing_config;
}

OutputSchema DateRangeClause::modify_schema(OutputSchema&& output_schema) const {
    check_is_timeseries(output_schema.stream_descriptor(), "DateRange");
    return output_schema;
}

std::string DateRangeClause::to_string() const { return fmt::format("DATE RANGE {} - {}", start_, end_); }

ConcatClause::ConcatClause(JoinType join_type) {
    clause_info_.input_structure_ = ProcessingStructure::MULTI_SYMBOL;
    clause_info_.multi_symbol_ = true;
    join_type_ = join_type;
}

std::vector<std::vector<EntityId>> ConcatClause::structure_for_processing(
        std::vector<std::vector<EntityId>>&& entity_ids_vec
) {
    // Similar logic to RowRangeClause::structure_for_processing but as input row ranges come from multiple symbols
    // it is slightly different
    std::vector<RangesAndEntity> ranges_and_entities;
    std::vector<std::shared_ptr<RowRange>> new_row_ranges;
    bool first_range{true};
    size_t prev_range_end{0};
    for (const auto& entity_ids : entity_ids_vec) {
        auto [old_row_ranges, col_ranges] =
                component_manager_->get_entities<std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(entity_ids);
        // Map from old row ranges WITHIN THIS SYMBOL to new ones
        std::map<RowRange, RowRange> row_range_mapping;
        for (const auto& row_range : old_row_ranges) {
            // Value is same as key initially
            row_range_mapping.insert({*row_range, *row_range});
        }
        for (auto& [old_range, new_range] : row_range_mapping) {
            if (first_range) {
                // Make the first row-range start from zero
                new_range.first = 0;
                new_range.second = old_range.diff();
                first_range = false;
            } else {
                new_range.first = prev_range_end;
                new_range.second = new_range.first + old_range.diff();
            }
            prev_range_end = new_range.second;
        }

        for (size_t idx = 0; idx < entity_ids.size(); ++idx) {
            auto new_row_range = std::make_shared<RowRange>(row_range_mapping.at(*old_row_ranges[idx]));
            ranges_and_entities.emplace_back(entity_ids[idx], new_row_range, col_ranges[idx]);
            new_row_ranges.emplace_back(std::move(new_row_range));
        }
    }
    component_manager_->replace_entities<std::shared_ptr<RowRange>>(
            util::flatten_vectors(std::move(entity_ids_vec)), new_row_ranges
    );
    auto new_structure_offsets = structure_by_row_slice(ranges_and_entities);
    return offsets_to_entity_ids(new_structure_offsets, ranges_and_entities);
}

std::vector<EntityId> ConcatClause::process(std::vector<EntityId>&& entity_ids) const { return std::move(entity_ids); }

OutputSchema ConcatClause::join_schemas(std::vector<OutputSchema>&& input_schemas) const {
    util::check(!input_schemas.empty(), "Cannot join empty list of schemas");
    auto [stream_desc, norm_meta] = join_indexes(input_schemas);
    join_type_ == JoinType::INNER ? inner_join(stream_desc, input_schemas) : outer_join(stream_desc, input_schemas);
    return {std::move(stream_desc), std::move(norm_meta)};
}

std::string ConcatClause::to_string() const { return "CONCAT"; }

WriteClause::WriteClause(
        const IndexPartialKey& index_partial_key, std::shared_ptr<DeDupMap> dedup_map, std::shared_ptr<Store> store,
        ProcessingStructure input_processing_structure
) :
    index_partial_key_(index_partial_key),
    dedup_map_(std::move(dedup_map)),
    store_(std::move(store)) {
    clause_info_.input_structure_ = input_processing_structure;
    clause_info_.output_structure_ = clause_info_.input_structure_;
    clause_info_.can_combine_with_column_selection_ = false;
}

std::vector<std::vector<size_t>> WriteClause::structure_for_processing(std::vector<RangesAndKey>&) {
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("WriteClause should never be first in the pipeline");
}

std::vector<std::vector<EntityId>> WriteClause::structure_for_processing(std::vector<std::vector<EntityId>>&&) {
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("WriteClause should never restructure entities");
}

std::vector<EntityId> WriteClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    const auto proc =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager_, std::move(entity_ids)
            );

    std::vector<std::shared_ptr<folly::Future<SliceAndKey>>> data_segments_to_write;
    data_segments_to_write.reserve(proc.segments_->size());

    for (size_t i = 0; i < proc.segments_->size(); ++i) {
        const SegmentInMemory& segment = *(*proc.segments_)[i];
        const RowRange& row_range = *(*proc.row_ranges_)[i];
        const ColRange& col_range = *(*proc.col_ranges_)[i];
        stream::PartialKey partial_key = create_partial_key(segment, row_range);
        data_segments_to_write.push_back(
                std::make_shared<folly::Future<SliceAndKey>>(store_->compress_and_schedule_async_write(
                        std::make_tuple(std::move(partial_key), segment, FrameSlice(col_range, row_range)), dedup_map_
                ))
        );
    }
    return component_manager_->add_entities(std::move(data_segments_to_write));
}

stream::PartialKey WriteClause::create_partial_key(const SegmentInMemory& segment, const RowRange& row_range) const {
    if (segment.descriptor().index().type() == IndexDescriptor::Type::ROWCOUNT) {
        return stream::PartialKey{
                .key_type = KeyType::TABLE_DATA,
                .version_id = index_partial_key_.version_id,
                .stream_id = index_partial_key_.id,
                .start_index = static_cast<timestamp>(row_range.first),
                .end_index = static_cast<timestamp>(row_range.second)
        };
    } else if (segment.descriptor().index().type() == IndexDescriptor::Type::TIMESTAMP) {
        const timestamp start_ts = std::get<timestamp>(stream::TimeseriesIndex::start_value_for_segment(segment));
        const timestamp end_ts =
                std::get<timestamp>(end_index_generator(stream::TimeseriesIndex::end_value_for_segment(segment)));
        return stream::PartialKey{
                .key_type = KeyType::TABLE_DATA,
                .version_id = index_partial_key_.version_id,
                .stream_id = index_partial_key_.id,
                .start_index = start_ts,
                .end_index = end_ts
        };
    }
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unknown index encountered in WriteClause");
}

const ClauseInfo& WriteClause::clause_info() const { return clause_info_; }

void WriteClause::set_processing_config(const ProcessingConfig&) {}

void WriteClause::set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
    component_manager_ = std::move(component_manager);
}

OutputSchema WriteClause::modify_schema(OutputSchema&& output_schema) const { return output_schema; }

OutputSchema WriteClause::join_schemas(std::vector<OutputSchema>&&) const {
    util::raise_rte("WriteClause::join_schemas should never be called");
}

std::string WriteClause::to_string() const { return "Write"; }

} // namespace arcticdb
