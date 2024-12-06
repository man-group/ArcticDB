/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <vector>
#include <variant>

#include <arcticdb/processing/processing_unit.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/stream/merge.hpp>

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <ankerl/unordered_dense.h>
#include <ranges>

namespace rng = std::ranges;

namespace arcticdb {

using namespace pipelines;

class GroupingMap {
    using NumericMapType = std::variant<
            std::monostate,
            std::shared_ptr<ankerl::unordered_dense::map<bool, size_t>>,
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
        return util::variant_match(map_,
                                   [](const std::monostate &) {
                                       return size_t(0);
                                   },
                                   [](const auto &other) {
                                       return other->size();
                                   });
    }

    template<typename T>
    std::shared_ptr<ankerl::unordered_dense::map<T, size_t>> get() {
        return util::variant_match(map_,
                                   [that = this](const std::monostate &) {
                                       that->map_ = std::make_shared<ankerl::unordered_dense::map<T, size_t>>();
                                       return std::get<std::shared_ptr<ankerl::unordered_dense::map<T, size_t>>>(that->map_);
                                   },
                                   [](const std::shared_ptr<ankerl::unordered_dense::map<T, size_t>> &ptr) {
                                       return ptr;
                                   },
                                   [](const auto &) -> std::shared_ptr<ankerl::unordered_dense::map<T, size_t>> {
                                       schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                                               "GroupBy does not support the grouping column type changing with dynamic schema");
                                   });
    }
};

struct SegmentWrapper {
    SegmentInMemory seg_;
    SegmentInMemory::iterator it_;
    const StreamId id_;

    explicit SegmentWrapper(SegmentInMemory&& seg) :
            seg_(std::move(seg)),
            it_(seg_.begin()),
            id_(seg_.descriptor().id()) {
    }

    bool advance() {
        return ++it_ != seg_.end();
    }

    SegmentInMemory::Row &row() {
        return *it_;
    }

    const StreamId &id() const {
        return id_;
    }
};

std::vector<EntityId> PassthroughClause::process(std::vector<EntityId>&& entity_ids) const {
    return std::move(entity_ids);
}

std::vector<EntityId> FilterClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
    proc.set_expression_context(expression_context_);
    auto variant_data = proc.get(expression_context_->root_node_name_);
    std::vector<EntityId> output;
    util::variant_match(variant_data,
                        [&proc, &output, this](util::BitSet &bitset) {
                            if (bitset.count() > 0) {
                                proc.apply_filter(std::move(bitset), optimisation_);
                                output = push_entities(*component_manager_, std::move(proc));
                            } else {
                                log::version().debug("Filter returned empty result");
                            }
                        },
                        [](EmptyResult) {
                            log::version().debug("Filter returned empty result");
                        },
                        [&output, &proc, this](FullResult) {
                            output = push_entities(*component_manager_, std::move(proc));
                        },
                        [](const auto &) {
                            util::raise_rte("Expected bitset from filter clause");
                        });
    return output;
}

std::string FilterClause::to_string() const {
    return expression_context_ ? fmt::format("WHERE {}", expression_context_->root_node_name_.value) : "";
}

std::vector<EntityId> ProjectClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
    proc.set_expression_context(expression_context_);
    auto variant_data = proc.get(expression_context_->root_node_name_);
    std::vector<EntityId> output;
    util::variant_match(variant_data,
                        [&proc, &output, this](ColumnWithStrings &col) {

                            const auto data_type = col.column_->type().data_type();
                            const std::string_view name = output_column_;

                            proc.segments_->back()->add_column(scalar_field(data_type, name), col.column_);
                            auto new_col_range = std::make_shared<ColRange>(*proc.col_ranges_->back());
                            ++new_col_range->second;
                            proc.col_ranges_->back() = std::move(new_col_range);
                            output = push_entities(*component_manager_, std::move(proc));
                        },
                        [&proc, &output, this](const EmptyResult &) {
                            if (expression_context_->dynamic_schema_)
                                output = push_entities(*component_manager_, std::move(proc));
                            else
                                util::raise_rte("Cannot project from empty column with static schema");
                        },
                        [](const auto &) {
                            util::raise_rte("Expected column from projection clause");
                        });
    return output;
}

[[nodiscard]] std::string ProjectClause::to_string() const {
    return expression_context_ ? fmt::format("PROJECT Column[\"{}\"] = {}", output_column_, expression_context_->root_node_name_.value) : "";
}

AggregationClause::AggregationClause(const std::string& grouping_column,
                                     const std::vector<NamedAggregator>& named_aggregators):
        grouping_column_(grouping_column) {
    clause_info_.input_structure_ = ProcessingStructure::HASH_BUCKETED;
    clause_info_.can_combine_with_column_selection_ = false;
    clause_info_.index_ = NewIndex(grouping_column_);
    clause_info_.input_columns_ = std::make_optional<std::unordered_set<std::string>>({grouping_column_});
    clause_info_.modifies_output_descriptor_ = true;
    str_ = "AGGREGATE {";
    for (const auto& named_aggregator: named_aggregators) {
        str_.append(fmt::format("{}: ({}, {}), ",
                                named_aggregator.output_column_name_,
                                named_aggregator.input_column_name_,
                                named_aggregator.aggregation_operator_));
        clause_info_.input_columns_->insert(named_aggregator.input_column_name_);
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
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Unknown aggregation operator provided: {}", named_aggregator.aggregation_operator_);
        }
    }
    str_.append("}");
}

std::vector<std::vector<EntityId>> AggregationClause::structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
    schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
            std::any_of(entity_ids_vec.cbegin(), entity_ids_vec.cend(), [](const std::vector<EntityId>& entity_ids) {
                return !entity_ids.empty();
            }),
            "Grouping column {} does not exist or is empty", grouping_column_
    );
    // Some could be empty, so actual number may be lower
    auto max_num_buckets = ConfigsMap::instance()->get_int("Partition.NumBuckets",
                                                           async::TaskScheduler::instance()->cpu_thread_count());
    max_num_buckets = std::min(max_num_buckets, static_cast<int64_t>(std::numeric_limits<bucket_id>::max()));
    // Preallocate results with expected sizes, erase later if any are empty
    std::vector<std::vector<EntityId>> res(max_num_buckets);
    // With an even distribution, expect each element of res to have entity_ids_vec.size() elements
    for (auto& res_element: res) {
        res_element.reserve(entity_ids_vec.size());
    }
    // Experimentation shows flattening the entities into a single vector and a single call to
    // component_manager_->get is faster than not flattening and making multiple calls
    auto entity_ids = flatten_entities(std::move(entity_ids_vec));
    auto [buckets] = component_manager_->get_entities<bucket_id>(entity_ids, false);
    for (auto [idx, entity_id]: folly::enumerate(entity_ids)) {
        res[buckets[idx]].emplace_back(entity_id);
    }
    // Get rid of any empty buckets
    std::erase_if(res, [](const std::vector<EntityId>& entity_ids) { return entity_ids.empty(); });
    return res;
}

std::vector<EntityId> AggregationClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
    auto row_slices = split_by_row_slice(std::move(proc));

    // Sort procs following row range descending order, as we are going to iterate through them backwards
    std::sort(std::begin(row_slices), std::end(row_slices),
              [](const auto& left, const auto& right) {
                  return left.row_ranges_->at(0)->start() > right.row_ranges_->at(0)->start();
    });

    std::vector<GroupingAggregatorData> aggregators_data;
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            !aggregators_.empty(),
            "AggregationClause::process does not make sense with no aggregators");
    for (const auto &agg: aggregators_){
        aggregators_data.emplace_back(agg.get_aggregator_data());
    }

    // Work out the common type between the processing units for the columns being aggregated
    for (auto& row_slice: row_slices) {
        for (auto agg_data: folly::enumerate(aggregators_data)) {
            // Check that segments row ranges are the same
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                std::all_of(row_slice.row_ranges_->begin(), row_slice.row_ranges_->end(), [&] (const auto& row_range) {return row_range->start() == row_slice.row_ranges_->at(0)->start();}),
                "Expected all data segments in one processing unit to have the same row ranges");

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
    while(it != row_slices.rend()) {
        auto& row_slice = *it;
        auto partitioning_column = row_slice.get(ColumnName(grouping_column_));
        if (std::holds_alternative<ColumnWithStrings>(partitioning_column)) {
            ColumnWithStrings col = std::get<ColumnWithStrings>(partitioning_column);
            details::visit_type(
                col.column_->type().data_type(),
                [&row_slice, &grouping_map, &next_group_id, &aggregators_data, &string_pool, &col,
                        &num_unique, &grouping_data_type, this](auto data_type_tag) {
                    using col_type_info = ScalarTypeInfo<decltype(data_type_tag)>;
                    grouping_data_type = col_type_info::data_type;
                    // Faster to initialise to zero (missing value group) and use raw ptr than repeated calls to emplace_back
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

                    Column::for_each_enumerated<typename col_type_info::TDT>(
                            *col.column_,
                            [&](auto enumerating_it) {
                                typename col_type_info::RawType val;
                                if constexpr (is_sequence_type(col_type_info::data_type)) {
                                    auto offset = enumerating_it.value();
                                    if (auto it = offset_to_group.find(offset); it !=
                                                                                offset_to_group.end()) {
                                        val = it->second;
                                    } else {
                                        std::optional<std::string_view> str = col.string_at_offset(
                                                offset);
                                        if (str.has_value()) {
                                            val = string_pool->get(*str, true).offset();
                                        } else {
                                            val = offset;
                                        }
                                        typename col_type_info::RawType val_copy(val);
                                        offset_to_group.insert(
                                                std::make_pair<typename col_type_info::RawType, size_t>(
                                                        std::forward<typename col_type_info::RawType>(
                                                                offset),
                                                        std::forward<typename col_type_info::RawType>(
                                                                val_copy)));
                                    }
                                } else {
                                    val = enumerating_it.value();
                                }

                                if (is_sparse) {
                                    constexpr size_t missing_value_group_id = 0;
                                    for (auto j = previous_value_index;
                                         j != enumerating_it.idx(); ++j) {
                                        *row_to_group_ptr++ = missing_value_group_id;
                                    }
                                    previous_value_index = enumerating_it.idx() + 1;
                                }

                                if (auto it = hash_to_group->find(val); it ==
                                                                        hash_to_group->end()) {
                                    *row_to_group_ptr++ = next_group_id;
                                    auto group_id = next_group_id++;
                                    hash_to_group->insert(
                                            std::make_pair<typename col_type_info::RawType, size_t>(
                                                    std::forward<typename col_type_info::RawType>(
                                                            val),
                                                    std::forward<typename col_type_info::RawType>(
                                                            group_id)));
                                } else {
                                    *row_to_group_ptr++ = it->second;
                                }
                            }
                    );

                    num_unique = next_group_id;
                    util::check(num_unique != 0, "Got zero unique values");
                    for (auto agg_data: folly::enumerate(aggregators_data)) {
                        auto input_column_name = aggregators_.at(
                                agg_data.index).get_input_column_name();
                        auto input_column = row_slice.get(input_column_name);
                        std::optional<ColumnWithStrings> opt_input_column;
                        if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                            auto column_with_strings = std::get<ColumnWithStrings>(input_column);
                            // Empty columns don't contribute to aggregations
                            if (!is_empty_type(column_with_strings.column_->type().data_type())) {
                                opt_input_column.emplace(std::move(column_with_strings));
                            }
                        }
                        agg_data->aggregate(opt_input_column, row_to_group, num_unique);
                    }
                });
        } else {
            util::raise_rte("Expected single column from expression");
        }
        it = static_cast<decltype(row_slices)::reverse_iterator>((row_slices.erase(std::next(it).base())));
    }
    SegmentInMemory seg;
    auto index_col = std::make_shared<Column>(make_scalar_type(grouping_data_type), grouping_map.size(), AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);

    seg.add_column(scalar_field(grouping_data_type, grouping_column_), index_col);
    seg.descriptor().set_index(IndexDescriptorImpl(0, IndexDescriptorImpl::Type::ROWCOUNT));

    details::visit_type(grouping_data_type, [&grouping_map, &index_col](auto data_type_tag) {
        using col_type_info = ScalarTypeInfo<decltype(data_type_tag)>;
        auto hashes = grouping_map.get<typename col_type_info::RawType>();
        std::vector<std::pair<typename col_type_info::RawType, size_t>> elements;
        for (const auto &hash : *hashes)
            elements.emplace_back(std::make_pair(hash.first, hash.second));

        std::sort(std::begin(elements),
                  std::end(elements),
                  [](const std::pair<typename col_type_info::RawType, size_t> &l, const std::pair<typename col_type_info::RawType, size_t> &r) {
                      return l.second < r.second;
                  });

        auto column_data = index_col->data();
        std::transform(elements.cbegin(), elements.cend(), column_data.begin<typename col_type_info::TDT>(), [](const auto& element) {
            return element.first;
        });
    });
    index_col->set_row_data(grouping_map.size() - 1);

    for (auto agg_data: folly::enumerate(aggregators_data)) {
        seg.concatenate(agg_data->finalize(aggregators_.at(agg_data.index).get_output_column_name(), processing_config_.dynamic_schema_, num_unique));
    }

    seg.set_string_pool(string_pool);
    seg.set_row_id(num_unique - 1);
    return push_entities(*component_manager_, ProcessingUnit(std::move(seg)));
}

[[nodiscard]] std::string AggregationClause::to_string() const {
    return str_;
}

template<ResampleBoundary closed_boundary>
ResampleClause<closed_boundary>::ResampleClause(std::string rule,
    ResampleBoundary label_boundary,
    BucketGeneratorT&& generate_bucket_boundaries,
    timestamp offset,
    ResampleOrigin origin) :
    rule_(std::move(rule)),
    label_boundary_(label_boundary),
    generate_bucket_boundaries_(std::move(generate_bucket_boundaries)),
    offset_(offset),
    origin_(std::move(origin)) {
    clause_info_.input_structure_ = ProcessingStructure::TIME_BUCKETED;
    clause_info_.can_combine_with_column_selection_ = false;
    clause_info_.modifies_output_descriptor_ = true;
    clause_info_.index_ = KeepCurrentTopLevelIndex();
}

template<ResampleBoundary closed_boundary>
const ClauseInfo& ResampleClause<closed_boundary>::clause_info() const {
    return clause_info_;
}

template<ResampleBoundary closed_boundary>
void ResampleClause<closed_boundary>::set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
    component_manager_ = std::move(component_manager);
}

template<ResampleBoundary closed_boundary>
std::string ResampleClause<closed_boundary>::rule() const {
    return rule_;
}

template<ResampleBoundary closed_boundary>
void ResampleClause<closed_boundary>::set_date_range(timestamp date_range_start, timestamp date_range_end) {
    // Start and end need to read the first and last segments of the date range. At the moment buckets are set up before
    // reading and processing the data.
    constexpr static std::array unsupported_origin{ "start", "end", "start_day", "end_day" };
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
        util::variant_match(origin_,
            [&](const std::string& origin) { return rng::none_of(unsupported_origin, [&](std::string_view el) { return el == origin; }); },
            [](const auto&) { return true;}
        ),
        "Resampling origins {} are not supported in conjunction with date range", unsupported_origin
    );
    date_range_.emplace(date_range_start, date_range_end);
}

template<ResampleBoundary closed_boundary>
void ResampleClause<closed_boundary>::set_aggregations(const std::vector<NamedAggregator>& named_aggregators) {
    clause_info_.input_columns_ = std::make_optional<std::unordered_set<std::string>>();
    str_ = fmt::format("RESAMPLE({}) | AGGREGATE {{", rule());
    for (const auto& named_aggregator: named_aggregators) {
        str_.append(fmt::format("{}: ({}, {}), ",
                                named_aggregator.output_column_name_,
                                named_aggregator.input_column_name_,
                                named_aggregator.aggregation_operator_));
        clause_info_.input_columns_->insert(named_aggregator.input_column_name_);
        auto typed_input_column_name = ColumnName(named_aggregator.input_column_name_);
        auto typed_output_column_name = ColumnName(named_aggregator.output_column_name_);
        if (named_aggregator.aggregation_operator_ == "sum") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::SUM, closed_boundary>(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "mean") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::MEAN, closed_boundary>(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "min") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::MIN, closed_boundary>(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "max") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::MAX, closed_boundary>(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "first") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::FIRST, closed_boundary>(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "last") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::LAST, closed_boundary>(typed_input_column_name, typed_output_column_name));
        } else if (named_aggregator.aggregation_operator_ == "count") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::COUNT, closed_boundary>(typed_input_column_name, typed_output_column_name));
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Unknown aggregation operator provided to resample: {}", named_aggregator.aggregation_operator_);
        }
    }
    str_.append("}");
}

template<ResampleBoundary closed_boundary>
void ResampleClause<closed_boundary>::set_processing_config(const ProcessingConfig& processing_config) {
    processing_config_ = processing_config;
}

template<ResampleBoundary closed_boundary>
std::vector<std::vector<size_t>> ResampleClause<closed_boundary>::structure_for_processing(
        std::vector<RangesAndKey>& ranges_and_keys) {
    if (ranges_and_keys.empty()) {
        return {};
    }
    
    const TimestampRange index_range = std::accumulate(
        std::next(ranges_and_keys.begin()),
        ranges_and_keys.end(),
        TimestampRange{ ranges_and_keys.begin()->start_time(), ranges_and_keys.begin()->end_time() },
        [](const TimestampRange& rng, const RangesAndKey& el) { return TimestampRange{std::min(rng.first, el.start_time()), std::max(rng.second, el.end_time())};});

    if (date_range_.has_value()) {
        date_range_->first = std::max(date_range_->first, index_range.first);
        date_range_->second = std::min(date_range_->second, index_range.second);
    } else {
        date_range_ = index_range;
    }

    bucket_boundaries_ = generate_bucket_boundaries_(date_range_->first, date_range_->second, rule_, closed_boundary, offset_, origin_);
    if (bucket_boundaries_.size() < 2) {
        return {};
    }
    debug::check<ErrorCode::E_ASSERTION_FAILURE>(rng::is_sorted(bucket_boundaries_),
                                                 "Resampling expects provided bucket boundaries to be strictly monotonically increasing");
    return structure_by_time_bucket<closed_boundary>(ranges_and_keys, bucket_boundaries_);
}

template<ResampleBoundary closed_boundary>
std::vector<std::vector<EntityId>> ResampleClause<closed_boundary>::structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
    auto entity_ids = flatten_entities(std::move(entity_ids_vec));
    if (entity_ids.empty()) {
        return {};
    }
    auto [segments, row_ranges, col_ranges] = component_manager_->get_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(entity_ids, false);
    std::vector<RangesAndEntity> ranges_and_entities;
    ranges_and_entities.reserve(entity_ids.size());
    timestamp min_start_ts{std::numeric_limits<timestamp>::max()};
    timestamp max_end_ts{std::numeric_limits<timestamp>::min()};
    for (size_t idx=0; idx<entity_ids.size(); ++idx) {
        auto start_ts = std::get<timestamp>(stream::TimeseriesIndex::start_value_for_segment(*segments[idx]));
        auto end_ts = std::get<timestamp>(stream::TimeseriesIndex::end_value_for_segment(*segments[idx]));
        min_start_ts = std::min(min_start_ts, start_ts);
        max_end_ts = std::max(max_end_ts, end_ts);
        ranges_and_entities.emplace_back(entity_ids[idx], row_ranges[idx], col_ranges[idx], std::make_optional<TimestampRange>(start_ts, end_ts));
    }

    date_range_ = std::make_optional<TimestampRange>(min_start_ts, max_end_ts);
    bucket_boundaries_ = generate_bucket_boundaries_(date_range_->first, date_range_->second, rule_, closed_boundary, offset_, origin_);
    if (bucket_boundaries_.size() < 2) {
        return {};
    }
    debug::check<ErrorCode::E_ASSERTION_FAILURE>(rng::is_sorted(bucket_boundaries_),
                                                 "Resampling expects provided bucket boundaries to be strictly monotonically increasing");

    auto new_structure_offsets = structure_by_time_bucket<closed_boundary>(ranges_and_entities, bucket_boundaries_);

    std::vector<EntityFetchCount> expected_fetch_counts(ranges_and_entities.size(), 0);
    for (const auto& list: new_structure_offsets) {
        for (auto idx: list) {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    idx < expected_fetch_counts.size(),
                    "Index {} in new_structure_offsets out of bounds >{}", idx, expected_fetch_counts.size() - 1);
            expected_fetch_counts[idx]++;
        }
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            rng::all_of(expected_fetch_counts, [](EntityFetchCount fetch_count) {
                return fetch_count == 1 || fetch_count == 2;
            }),
            "ResampleClause::structure_for_processing: invalid expected entity fetch count (should be 1 or 2)"
            );
    std::vector<EntityId> entities_to_be_fetched_twice;
    for (auto&& [idx, ranges_and_entity]: folly::enumerate(ranges_and_entities)) {
        if (expected_fetch_counts[idx] == 2) {
            entities_to_be_fetched_twice.emplace_back(ranges_and_entity.id_);
        }
    }
    component_manager_->replace_entities<EntityFetchCount>(entities_to_be_fetched_twice, EntityFetchCount(2));
    return offsets_to_entity_ids(new_structure_offsets, ranges_and_entities);
}

template<ResampleBoundary closed_boundary>
std::vector<EntityId> ResampleClause<closed_boundary>::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>, EntityFetchCount>(*component_manager_, std::move(entity_ids));
    auto row_slices = split_by_row_slice(std::move(proc));
    // If the entity fetch counts for the entities in the first row slice are 2, the first bucket overlapping this row
    // slice is being computed by the call to process dealing with the row slices above these. Otherwise, this call
    // should do it
    const auto& front_slice = row_slices.front();
    bool responsible_for_first_overlapping_bucket = front_slice.entity_fetch_count_->at(0) == 1;
    // Find the iterators into bucket_boundaries_ of the start of the first and the end of the last bucket this call to process is
    // responsible for calculating
    // All segments in a given row slice contain the same index column, so just grab info from the first one
    const auto& index_column_name = front_slice.segments_->at(0)->field(0).name();
    const auto& first_row_slice_index_col = front_slice.segments_->at(0)->column(0);
    // Resampling only makes sense for timestamp indexes
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(is_time_type(first_row_slice_index_col.type().data_type()),
                                                    "Cannot resample data with index column of non-timestamp type");
    auto first_ts = first_row_slice_index_col.scalar_at<timestamp>(0).value();
    // We can use the last timestamp from the first row-slice's index column, as by construction (structure_for_processing) the bucket covering
    // this value will cover the remaining index values this call is responsible for
    auto last_ts = first_row_slice_index_col.scalar_at<timestamp>(first_row_slice_index_col.row_count() - 1).value();
    auto bucket_boundaries = generate_bucket_boundaries(first_ts, last_ts, responsible_for_first_overlapping_bucket);
    std::vector<std::shared_ptr<Column>> input_index_columns;
    input_index_columns.reserve(row_slices.size());
    for (const auto& row_slice: row_slices) {
        input_index_columns.emplace_back(row_slice.segments_->at(0)->column_ptr(0));
    }
    auto output_index_column = generate_output_index_column(input_index_columns, bucket_boundaries);
    // Bucket boundaries can be wider than the date range specified by the user, narrow the first and last buckets here if necessary
    bucket_boundaries.front() = std::max(bucket_boundaries.front(), date_range_->first - (closed_boundary == ResampleBoundary::RIGHT ? 1 : 0));
    bucket_boundaries.back() = std::min(bucket_boundaries.back(), date_range_->second + (closed_boundary == ResampleBoundary::LEFT ? 1 : 0));
    SegmentInMemory seg;
    RowRange output_row_range(row_slices.front().row_ranges_->at(0)->start(),
                              row_slices.front().row_ranges_->at(0)->start() + output_index_column->row_count());
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, index_column_name), output_index_column);
    seg.descriptor().set_index(IndexDescriptorImpl(1, IndexDescriptor::Type::TIMESTAMP));
    auto& string_pool = seg.string_pool();
    for (const auto& aggregator: aggregators_) {
        std::vector<std::optional<ColumnWithStrings>> input_agg_columns;
        input_agg_columns.reserve(row_slices.size());
        for (auto& row_slice: row_slices) {
            auto variant_data = row_slice.get(aggregator.get_input_column_name());
            util::variant_match(variant_data,
                                [&input_agg_columns](const ColumnWithStrings& column_with_strings) {
                                    input_agg_columns.emplace_back(column_with_strings);
                                },
                                [&input_agg_columns](const EmptyResult&) {
                                    // Dynamic schema, missing column from this row-slice
                                    // Not currently supported, but will be, hence the argument to aggregate being a vector of optionals
                                    input_agg_columns.emplace_back();
                                },
                                [](const auto&) {
                                    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected return type from ProcessingUnit::get, expected column-like");
                                }
            );
        }
        auto aggregated_column = std::make_shared<Column>(aggregator.aggregate(input_index_columns, input_agg_columns, bucket_boundaries, *output_index_column, string_pool));
        seg.add_column(scalar_field(aggregated_column->type().data_type(), aggregator.get_output_column_name().value), aggregated_column);
    }
    seg.set_row_data(output_index_column->row_count() - 1);
    return push_entities(*component_manager_, ProcessingUnit(std::move(seg), std::move(output_row_range)));
}

template<ResampleBoundary closed_boundary>
[[nodiscard]] std::string ResampleClause<closed_boundary>::to_string() const {
    return str_;
}

template<ResampleBoundary closed_boundary>
std::vector<timestamp> ResampleClause<closed_boundary>::generate_bucket_boundaries(timestamp first_ts,
                                                                                   timestamp last_ts,
                                                                                   bool responsible_for_first_overlapping_bucket) const {
    auto first_it = std::lower_bound(bucket_boundaries_.begin(), bucket_boundaries_.end(), first_ts,
                                     [](timestamp boundary, timestamp first_ts) {
                                         if constexpr(closed_boundary == ResampleBoundary::LEFT) {
                                             return boundary <= first_ts;
                                         } else {
                                             // closed_boundary == ResampleBoundary::RIGHT
                                             return boundary < first_ts;
                                         }
                                     });
    if (responsible_for_first_overlapping_bucket && first_it != bucket_boundaries_.begin()) {
        --first_it;
    }
    auto last_it = std::upper_bound(first_it, bucket_boundaries_.end(), last_ts,
                                    [](timestamp last_ts, timestamp boundary) {
                                        if constexpr(closed_boundary == ResampleBoundary::LEFT) {
                                            return last_ts < boundary;
                                        } else {
                                            // closed_boundary == ResampleBoundary::RIGHT
                                            return last_ts <= boundary;
                                        }
                                    });
    if (last_it != bucket_boundaries_.end()) {
        ++last_it;
    }
    std::vector<timestamp> bucket_boundaries(first_it, last_it);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(bucket_boundaries.size() >= 2,
                                                    "Always expect at least bucket boundaries in ResampleClause::generate_bucket_boundaries");
    return bucket_boundaries;
}

template<ResampleBoundary closed_boundary>
std::shared_ptr<Column> ResampleClause<closed_boundary>::generate_output_index_column(const std::vector<std::shared_ptr<Column>>& input_index_columns,
                                                                                      const std::vector<timestamp>& bucket_boundaries) const {
    constexpr auto data_type = DataType::NANOSECONDS_UTC64;
    using IndexTDT = ScalarTagType<DataTypeTag<data_type>>;

    const auto max_index_column_bytes = (bucket_boundaries.size() - 1) * get_type_size(data_type);
    auto output_index_column = std::make_shared<Column>(TypeDescriptor(data_type, Dimension::Dim0),
                                                        Sparsity::NOT_PERMITTED,
                                                        ChunkedBuffer::presized_in_blocks(max_index_column_bytes));
    auto output_index_column_data = output_index_column->data();
    auto output_index_column_it = output_index_column_data.template begin<IndexTDT>();
    size_t output_index_column_row_count{0};

    auto bucket_end_it = std::next(bucket_boundaries.cbegin());
    Bucket<closed_boundary> current_bucket{*std::prev(bucket_end_it), *bucket_end_it};
    bool current_bucket_added_to_index{false};
    // Only include buckets that have at least one index value in range
    for (const auto& input_index_column: input_index_columns) {
        auto index_column_data = input_index_column->data();
        const auto cend = index_column_data.cend<IndexTDT>();
        auto it = index_column_data.cbegin<IndexTDT>();
        // In case the passed date_range does not span the whole segment we need to skip the index values
        // which are before the date range start.
        while (it != cend && *it < date_range_->first) {
            ++it;
        }
        for (;it != cend && *it <= date_range_->second; ++it) {
            if (ARCTICDB_LIKELY(current_bucket.contains(*it))) {
                if (ARCTICDB_UNLIKELY(!current_bucket_added_to_index)) {
                    *output_index_column_it++ = label_boundary_ == ResampleBoundary::LEFT ? *std::prev(bucket_end_it) : *bucket_end_it;
                    ++output_index_column_row_count;
                    current_bucket_added_to_index = true;
                }
            } else {
                advance_boundary_past_value<closed_boundary>(bucket_boundaries, bucket_end_it, *it);
                if (ARCTICDB_UNLIKELY(bucket_end_it == bucket_boundaries.end())) {
                    break;
                } else {
                    current_bucket.set_boundaries(*std::prev(bucket_end_it), *bucket_end_it);
                    current_bucket_added_to_index = false;
                    if (ARCTICDB_LIKELY(current_bucket.contains(*it))) {
                        *output_index_column_it++ = label_boundary_ == ResampleBoundary::LEFT ? *std::prev(bucket_end_it) : *bucket_end_it;
                        ++output_index_column_row_count;
                        current_bucket_added_to_index = true;
                    }
                }
            }
        }
    }
    const auto actual_index_column_bytes = output_index_column_row_count * get_type_size(data_type);
    output_index_column->buffer().trim(actual_index_column_bytes);
    output_index_column->set_row_data(output_index_column_row_count - 1);
    return output_index_column;
}

template struct ResampleClause<ResampleBoundary::LEFT>;
template struct ResampleClause<ResampleBoundary::RIGHT>;

[[nodiscard]] std::vector<EntityId> RemoveColumnPartitioningClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
    size_t min_start_row = std::numeric_limits<size_t>::max();
    size_t max_end_row = 0;
    size_t min_start_col = std::numeric_limits<size_t>::max();
    size_t max_end_col = 0;
    std::optional<SegmentInMemory> output_seg;
    for (auto&& [idx, segment]: folly::enumerate(proc.segments_.value())) {
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
        output = push_entities(*component_manager_,
                               ProcessingUnit(std::move(*output_seg),
                               RowRange{min_start_row, max_end_row},
                               ColRange{min_start_col, max_end_col}));
    }
    return output;
}

std::vector<EntityId> SplitClause::process(std::vector<EntityId>&& entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
    std::vector<EntityId> ret;
    for (auto&& [idx, seg]: folly::enumerate(proc.segments_.value())) {
        auto split_segs = seg->split(rows_);
        size_t start_row = proc.row_ranges_->at(idx)->start();
        size_t end_row = 0;
        for (auto&& split_seg : split_segs) {
            end_row = start_row + split_seg.row_count();
            auto new_entity_ids = push_entities(*component_manager_,
                                                ProcessingUnit(std::move(split_seg),
                                                RowRange(start_row, end_row),
                                                std::move(*proc.col_ranges_->at(idx))));
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
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
    for (auto& seg: proc.segments_.value()) {
        // This modifies the segment in place, which goes against the ECS principle of all entities being immutable
        // Only used by SortMerge right now and so this is fine, although it would not generalise well
        seg->sort(column_);
    }
    return push_entities(*component_manager_, std::move(proc));
}

template<typename IndexType, typename DensityPolicy, typename QueueType, bool dynamic_schema>
void merge_impl(
        std::shared_ptr<ComponentManager> component_manager,
        std::vector<std::vector<EntityId>>& ret,
        QueueType &input_streams,
        bool add_symbol_column,
        const RowRange& row_range,
        const ColRange& col_range,
        IndexType index,
        const StreamDescriptor& stream_descriptor) {
    auto num_segment_rows = ConfigsMap::instance()->get_int("Merge.SegmentSize", 100000);
    using SegmentationPolicy = stream::RowCountSegmentPolicy;
    SegmentationPolicy segmentation_policy{static_cast<size_t>(num_segment_rows)};

    auto func = [&component_manager, &ret, &col_range, start_row = row_range.first](auto&& segment) mutable {
        const size_t end_row = start_row + segment.row_count();
        ret.emplace_back(push_entities(*component_manager, ProcessingUnit{std::forward<decltype(segment)>(segment), RowRange{start_row, end_row}, col_range}));
        start_row = end_row;
    };

    using Schema = std::conditional_t<dynamic_schema, stream::DynamicSchema, stream::FixedSchema>;
    using AggregatorType = stream::Aggregator<IndexType, Schema, SegmentationPolicy, DensityPolicy>;

    AggregatorType agg{
        Schema{stream_descriptor, index},
        std::move(func),
        std::move(segmentation_policy),
        stream_descriptor,
        std::nullopt
    };

    stream::do_merge<IndexType, AggregatorType, decltype(input_streams)>(input_streams, agg, add_symbol_column);
}

MergeClause::MergeClause(
        stream::Index index,
        const stream::VariantColumnPolicy &density_policy,
        const StreamId& stream_id,
        const StreamDescriptor& stream_descriptor,
        bool dynamic_schema) :
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

const ClauseInfo& MergeClause::clause_info() const {
    return clause_info_;
}

std::vector<std::vector<EntityId>> MergeClause::structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {

    // TODO this is a hack because we don't currently have a way to
    // specify any particular input shape unless a clause is the
    // first one and can use structure_for_processing. Ideally
    // merging should be parallel like resampling
    auto entity_ids = flatten_entities(std::move(entity_ids_vec));
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));

    auto compare =
            [](const std::unique_ptr<SegmentWrapper> &left,
               const std::unique_ptr<SegmentWrapper> &right) {
                if (left->seg_.row_count() == 0) {
                    return false;
                } else if (right->seg_.row_count() == 0) {
                    return true;
                }
                const auto left_index = index::index_value_from_row(left->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0);
                const auto right_index = index::index_value_from_row(right->row(), IndexDescriptorImpl::Type::TIMESTAMP, 0);
                return left_index > right_index;
            };

    movable_priority_queue<std::unique_ptr<SegmentWrapper>, std::vector<std::unique_ptr<SegmentWrapper>>, decltype(compare)> input_streams{
            compare};

    size_t min_start_row = std::numeric_limits<size_t>::max();
    size_t max_end_row = 0;
    size_t min_start_col = std::numeric_limits<size_t>::max();
    size_t max_end_col = 0;
    for (auto&& [idx, segment]: folly::enumerate(proc.segments_.value())) {
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
    std::visit([this, &ret, &input_streams, stream_id=stream_id_, &row_range, &col_range](auto idx, auto density) {
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
        }, index_, density_policy_);
    return ret;
}

// MergeClause receives a list of DataFrames as input and merge them into a single one where all
// the rows are sorted by time stamp
std::vector<EntityId> MergeClause::process(std::vector<EntityId>&& entity_ids) const {
    return std::move(entity_ids);
}


std::vector<EntityId> ColumnStatsGenerationClause::process(std::vector<EntityId>&& entity_ids) const {
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            !entity_ids.empty(),
            "ColumnStatsGenerationClause::process does not make sense with no processing units");
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>, std::shared_ptr<AtomKey>>(*component_manager_, std::move(entity_ids));
    std::vector<ColumnStatsAggregatorData> aggregators_data;
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            static_cast<bool>(column_stats_aggregators_),
            "ColumnStatsGenerationClause::process does not make sense with no aggregators");
    for (const auto &agg : *column_stats_aggregators_){
        aggregators_data.emplace_back(agg.get_aggregator_data());
    }

    ankerl::unordered_dense::set<IndexValue> start_indexes;
    ankerl::unordered_dense::set<IndexValue> end_indexes;

    for (const auto& key: proc.atom_keys_.value()) {
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
                    "Unable to resolve column denoted by aggregation operator: '{}'",
                    input_column_name);
        }
    }

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            start_indexes.size() == 1 && end_indexes.size() == 1,
            "Expected all data segments in one processing unit to have same start and end indexes");
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
    seg.descriptor().set_index(IndexDescriptorImpl(0, IndexDescriptorImpl::Type::ROWCOUNT));
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, start_index_column_name), start_index_col);
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, end_index_column_name), end_index_col);
    for (const auto& agg_data: folly::enumerate(aggregators_data)) {
        seg.concatenate(agg_data->finalize(column_stats_aggregators_->at(agg_data.index).get_output_column_names()));
    }
    seg.set_row_id(0);
    return push_entities(*component_manager_, ProcessingUnit(std::move(seg)));
}

std::vector<std::vector<size_t>> RowRangeClause::structure_for_processing(
        std::vector<RangesAndKey>& ranges_and_keys) {
    ranges_and_keys.erase(std::remove_if(ranges_and_keys.begin(), ranges_and_keys.end(), [this](const RangesAndKey& ranges_and_key) {
        return ranges_and_key.row_range_.start() >= end_ || ranges_and_key.row_range_.end() <= start_;
    }), ranges_and_keys.end());
    return structure_by_row_slice(ranges_and_keys);
}

std::vector<std::vector<EntityId>> RowRangeClause::structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
    auto entity_ids = flatten_entities(std::move(entity_ids_vec));
    if (entity_ids.empty()) {
        return {};
    }
    auto [segments, old_row_ranges, col_ranges] = component_manager_->get_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(entity_ids, false);

    // Map from old row ranges to new ones
    std::map<RowRange, RowRange> row_range_mapping;
    for (const auto& row_range: old_row_ranges) {
        // Value is same as key initially
        row_range_mapping.insert({*row_range, *row_range});
    }
    bool first_range{true};
    size_t prev_range_end{0};
    for (auto& [old_range, new_range]: row_range_mapping) {
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
    for (size_t idx=0; idx<entity_ids.size(); ++idx) {
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
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
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
        return fmt::format("ROWRANGE: RANGE, start={}, end ={}", start_, end_);
    }

    return fmt::format("ROWRANGE: {}, n={}", row_range_type_ == RowRangeType::HEAD ? "HEAD" : "TAIL", n_);
}

void RowRangeClause::calculate_start_and_end(size_t total_rows) {
    auto signed_total_rows = static_cast<int64_t>(total_rows);
    switch(row_range_type_) {
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
            start_ = (
                    user_provided_start_ >= 0 ?
                    std::min(user_provided_start_, signed_total_rows) :
                    std::max(signed_total_rows + user_provided_start_, static_cast<int64_t>(0))
            );
            end_ = (
                    user_provided_end_ >= 0 ?
                    std::min(user_provided_end_, signed_total_rows) :
                    std::max(signed_total_rows + user_provided_end_, static_cast<int64_t>(0))
            );
            break;

        default:
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unrecognised RowRangeType {}", static_cast<uint8_t>(row_range_type_));
    }
}

std::vector<std::vector<size_t>> DateRangeClause::structure_for_processing(
        std::vector<RangesAndKey>& ranges_and_keys) {
    ranges_and_keys.erase(std::remove_if(ranges_and_keys.begin(), ranges_and_keys.end(), [this](const RangesAndKey& ranges_and_key) {
        auto [start_index, end_index] = ranges_and_key.key_.time_range();
        return start_index > end_ || end_index <= start_;
    }), ranges_and_keys.end());
    return structure_by_row_slice(ranges_and_keys);
}

std::vector<EntityId> DateRangeClause::process(std::vector<EntityId> &&entity_ids) const {
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
    std::vector<EntityId> output;
    // We are only interested in the index, which is in every SegmentInMemory in proc.segments_, so just use the first
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
            start_row = proc.segments_->at(0)->column_ptr(0)->search_sorted<timestamp>(start_);
        }
        if (end_ >= start_index && end_ <= end_index) {
            end_row = proc.segments_->at(0)->column_ptr(0)->search_sorted<timestamp>(end_, true);
        }
        proc.truncate(start_row, end_row);
    } // else all rows in the processing unit are required, do nothing
    return push_entities(*component_manager_, std::move(proc));
}

std::string DateRangeClause::to_string() const {
    return fmt::format("DATE RANGE {} - {}", start_, end_);
}

}
