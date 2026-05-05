/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/collection_utils.hpp>
#include <arcticdb/util/test/random_throw.hpp>

namespace arcticdb {

namespace ranges = std::ranges;
using namespace pipelines;

template<ResampleBoundary closed_boundary>
ResampleClause<closed_boundary>::ResampleClause(
        std::string rule, ResampleBoundary label_boundary, BucketGeneratorT&& generate_bucket_boundaries,
        timestamp offset, ResampleOrigin origin
) :
    rule_(std::move(rule)),
    label_boundary_(label_boundary),
    generate_bucket_boundaries_(std::move(generate_bucket_boundaries)),
    offset_(offset),
    origin_(std::move(origin)) {
    clause_info_.input_structure_ = ProcessingStructure::TIME_BUCKETED;
    clause_info_.can_combine_with_column_selection_ = false;
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
OutputSchema ResampleClause<closed_boundary>::modify_schema(OutputSchema&& output_schema) const {
    check_is_timeseries(output_schema.stream_descriptor(), "Resample");
    output_schema.clear_default_values();
    check_column_presence(output_schema, *clause_info_.input_columns_, "Resample");
    const auto& input_stream_desc = output_schema.stream_descriptor();
    StreamDescriptor stream_desc(input_stream_desc.id());
    stream_desc.add_field(input_stream_desc.field(0));
    stream_desc.set_index(IndexDescriptorImpl(IndexDescriptor::Type::TIMESTAMP, 1));

    for (const auto& agg : aggregators_) {
        const auto& input_column_name = agg.get_input_column_name().value;
        const auto& output_column_name = agg.get_output_column_name().value;
        const auto& input_column_type = output_schema.column_types()[input_column_name];
        agg.check_aggregator_supported_with_data_type(input_column_type);
        auto output_column_type = agg.generate_output_data_type(input_column_type);
        stream_desc.add_scalar_field(output_column_type, output_column_name);
        const std::optional<Value>& default_value = agg.get_default_value(input_column_type);
        if (default_value) {
            output_schema.set_default_value_for_column(output_column_name, *default_value);
        }
    }
    output_schema.set_stream_descriptor(std::move(stream_desc));

    if (output_schema.norm_metadata_.df().common().has_multi_index()) {
        const auto& multi_index = output_schema.norm_metadata_.mutable_df()->mutable_common()->multi_index();
        auto name = multi_index.name();
        auto tz = multi_index.tz();
        bool fake_name{false};
        for (auto pos : multi_index.fake_field_pos()) {
            if (pos == 0) {
                fake_name = true;
                break;
            }
        }
        auto mutable_index = output_schema.norm_metadata_.mutable_df()->mutable_common()->mutable_index();
        mutable_index->set_tz(tz);
        mutable_index->set_is_physically_stored(true);
        mutable_index->set_name(name);
        mutable_index->set_fake_name(fake_name);
    }
    return output_schema;
}

template<ResampleBoundary closed_boundary>
std::string ResampleClause<closed_boundary>::rule() const {
    return rule_;
}

template<ResampleBoundary closed_boundary>
void ResampleClause<closed_boundary>::set_date_range(timestamp date_range_start, timestamp date_range_end) {
    // Start and end need to read the first and last segments of the date range. At the moment buckets are set up
    // before reading and processing the data.
    constexpr static std::array unsupported_origin{"start", "end", "start_day", "end_day"};
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            util::variant_match(
                    origin_,
                    [&](const std::string& origin) {
                        return ranges::none_of(unsupported_origin, [&](std::string_view el) { return el == origin; });
                    },
                    [](const auto&) { return true; }
            ),
            "Resampling origins {} are not supported in conjunction with date range",
            unsupported_origin
    );
    date_range_.emplace(date_range_start, date_range_end);
}

template<ResampleBoundary closed_boundary>
void ResampleClause<closed_boundary>::set_aggregations(const std::vector<NamedAggregator>& named_aggregators) {
    clause_info_.input_columns_ = std::make_optional<std::unordered_set<std::string>>();
    str_ = fmt::format("RESAMPLE({}) | AGGREGATE {{", rule());
    for (const auto& named_aggregator : named_aggregators) {
        str_.append(fmt::format(
                "{}: ({}, {}), ",
                named_aggregator.output_column_name_,
                named_aggregator.input_column_name_,
                named_aggregator.aggregation_operator_
        ));
        clause_info_.input_columns_->insert(named_aggregator.input_column_name_);
        auto typed_input_column_name = ColumnName(named_aggregator.input_column_name_);
        auto typed_output_column_name = ColumnName(named_aggregator.output_column_name_);
        if (named_aggregator.aggregation_operator_ == "sum") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::SUM, closed_boundary>(
                    typed_input_column_name, typed_output_column_name
            ));
        } else if (named_aggregator.aggregation_operator_ == "mean") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::MEAN, closed_boundary>(
                    typed_input_column_name, typed_output_column_name
            ));
        } else if (named_aggregator.aggregation_operator_ == "min") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::MIN, closed_boundary>(
                    typed_input_column_name, typed_output_column_name
            ));
        } else if (named_aggregator.aggregation_operator_ == "max") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::MAX, closed_boundary>(
                    typed_input_column_name, typed_output_column_name
            ));
        } else if (named_aggregator.aggregation_operator_ == "first") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::FIRST, closed_boundary>(
                    typed_input_column_name, typed_output_column_name
            ));
        } else if (named_aggregator.aggregation_operator_ == "last") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::LAST, closed_boundary>(
                    typed_input_column_name, typed_output_column_name
            ));
        } else if (named_aggregator.aggregation_operator_ == "count") {
            aggregators_.emplace_back(SortedAggregator<AggregationOperator::COUNT, closed_boundary>(
                    typed_input_column_name, typed_output_column_name
            ));
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Unknown aggregation operator provided to resample: {}", named_aggregator.aggregation_operator_
            );
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
        std::vector<RangesAndKey>& ranges_and_keys
) {
    ARCTICDB_RUNTIME_DEBUG(log::memory(), "ResampleClause: structure for processing 1");
    if (ranges_and_keys.empty()) {
        return {};
    }
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            processing_config_.index_type_ == IndexDescriptor::Type::TIMESTAMP,
            "Cannot resample non-timestamp indexed data"
    );

    // Iterate over ranges_and_keys and create a pair with first element equal to the smallest start time and second
    // element equal to the largest end time.
    const TimestampRange index_range = std::accumulate(
            std::next(ranges_and_keys.begin()),
            ranges_and_keys.end(),
            TimestampRange{ranges_and_keys.begin()->start_time(), ranges_and_keys.begin()->end_time()},
            [](const TimestampRange& rng, const RangesAndKey& el) {
                return TimestampRange{std::min(rng.first, el.start_time()), std::max(rng.second, el.end_time())};
            }
    );

    if (date_range_.has_value()) {
        date_range_->first = std::max(date_range_->first, index_range.first);
        date_range_->second = std::min(date_range_->second, index_range.second);
    } else {
        date_range_ = index_range;
    }

    bucket_boundaries_ = generate_bucket_boundaries_(
            date_range_->first, date_range_->second, rule_, closed_boundary, offset_, origin_
    );
    if (bucket_boundaries_.size() < 2) {
        ranges_and_keys.clear();
        return {};
    }
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            ranges::is_sorted(bucket_boundaries_),
            "Resampling expects provided bucket boundaries to be strictly monotonically increasing"
    );
    return structure_by_time_bucket<closed_boundary>(ranges_and_keys, bucket_boundaries_);
}

template<ResampleBoundary closed_boundary>
std::vector<std::vector<EntityId>> ResampleClause<closed_boundary>::structure_for_processing(
        std::vector<std::vector<EntityId>>&& entity_ids_vec
) {
    auto entity_ids = util::flatten_vectors(std::move(entity_ids_vec));
    if (entity_ids.empty()) {
        return {};
    }
    ARCTICDB_RUNTIME_DEBUG(log::memory(), "ResampleClause: structure for processing 2");
    auto [segments, row_ranges, col_ranges] = component_manager_->get_entities<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>>(entity_ids);
    std::vector<RangesAndEntity> ranges_and_entities;
    ranges_and_entities.reserve(entity_ids.size());
    timestamp min_start_ts{std::numeric_limits<timestamp>::max()};
    timestamp max_end_ts{std::numeric_limits<timestamp>::min()};
    for (size_t idx = 0; idx < entity_ids.size(); ++idx) {
        auto start_ts = std::get<timestamp>(stream::TimeseriesIndex::start_value_for_segment(*segments[idx]));
        auto end_ts = std::get<timestamp>(stream::TimeseriesIndex::end_value_for_segment(*segments[idx]));
        min_start_ts = std::min(min_start_ts, start_ts);
        max_end_ts = std::max(max_end_ts, end_ts);
        ranges_and_entities.emplace_back(
                entity_ids[idx], row_ranges[idx], col_ranges[idx], std::make_optional<TimestampRange>(start_ts, end_ts)
        );
    }

    date_range_ = std::make_optional<TimestampRange>(min_start_ts, max_end_ts);
    bucket_boundaries_ = generate_bucket_boundaries_(
            date_range_->first, date_range_->second, rule_, closed_boundary, offset_, origin_
    );
    if (bucket_boundaries_.size() < 2) {
        return {};
    }
    ARCTICDB_DEBUG_CHECK(
            ErrorCode::E_ASSERTION_FAILURE,
            ranges::is_sorted(bucket_boundaries_),
            "Resampling expects provided bucket boundaries to be strictly monotonically increasing"
    );

    auto new_structure_offsets = structure_by_time_bucket<closed_boundary>(ranges_and_entities, bucket_boundaries_);

    std::vector<EntityFetchCount> expected_fetch_counts(ranges_and_entities.size(), 0);
    for (const auto& list : new_structure_offsets) {
        for (auto idx : list) {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    idx < expected_fetch_counts.size(),
                    "Index {} in new_structure_offsets out of bounds >{}",
                    idx,
                    expected_fetch_counts.size() - 1
            );
            expected_fetch_counts[idx]++;
        }
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            ranges::all_of(
                    expected_fetch_counts,
                    [](EntityFetchCount fetch_count) { return fetch_count == 1 || fetch_count == 2; }
            ),
            "ResampleClause::structure_for_processing: invalid expected entity fetch count (should be 1 or 2)"
    );
    std::vector<EntityId> entities_to_be_fetched_twice;
    for (auto&& [idx, ranges_and_entity] : folly::enumerate(ranges_and_entities)) {
        if (expected_fetch_counts[idx] == 2) {
            entities_to_be_fetched_twice.emplace_back(ranges_and_entity.id_);
        }
    }
    component_manager_->replace_entities<EntityFetchCount>(entities_to_be_fetched_twice, EntityFetchCount(2));
    return offsets_to_entity_ids(new_structure_offsets, ranges_and_entities);
}

template<ResampleBoundary closed_boundary>
std::vector<EntityId> ResampleClause<closed_boundary>::process(std::vector<EntityId>&& entity_ids) const {
    ARCTICDB_SAMPLE(ResampleClause, 0)
    if (entity_ids.empty()) {
        return {};
    }
    auto proc = gather_entities<
            std::shared_ptr<SegmentInMemory>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>,
            EntityFetchCount>(*component_manager_, std::move(entity_ids));
    ARCTICDB_RUNTIME_DEBUG(log::memory(), "ResampleClause: processing entities {}", entity_ids);
    auto row_slices = split_by_row_slice(std::move(proc));
    // If the entity fetch counts for the entities in the first row slice are 2, the first bucket overlapping this
    // row slice is being computed by the call to process dealing with the row slices above these. Otherwise, this
    // call should do it
    const auto& front_slice = row_slices.front();
    const bool responsible_for_first_overlapping_bucket = front_slice.entity_fetch_count_->at(0) == 1;
    // Find the iterators into bucket_boundaries_ of the start of the first and the end of the last bucket this call
    // to process is responsible for calculating All segments in a given row slice contain the same index column, so
    // just grab info from the first one
    const auto& index_column_name = front_slice.segments_->at(0)->field(0).name();
    const auto& first_row_slice_index_col = front_slice.segments_->at(0)->column(0);
    // Resampling only makes sense for timestamp indexes
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            is_time_type(first_row_slice_index_col.type().data_type()),
            "Cannot resample data with index column of non-timestamp type"
    );
    const auto first_ts = first_row_slice_index_col.template scalar_at<timestamp>(0).value();
    // If there is only one row slice, then the last index value of interest is just the last index value for this
    // row slice. If there is more than one, then the first index value from the second row slice must be used to
    // calculate the buckets of interest, due to an old bug in update. See
    // test_compatibility.py::test_compat_resample_updated_data for details
    const auto last_ts =
            row_slices.size() == 1
                    ? first_row_slice_index_col.template scalar_at<timestamp>(first_row_slice_index_col.row_count() - 1)
                              .value()
                    : row_slices.back().segments_->at(0)->column(0).template scalar_at<timestamp>(0).value();
    auto bucket_boundaries = generate_bucket_boundaries(first_ts, last_ts, responsible_for_first_overlapping_bucket);
    if (bucket_boundaries.size() < 2) {
        return {};
    }
    std::vector<std::shared_ptr<Column>> input_index_columns;
    input_index_columns.reserve(row_slices.size());
    for (const auto& row_slice : row_slices) {
        input_index_columns.emplace_back(row_slice.segments_->at(0)->column_ptr(0));
    }
    const auto output_index_column = generate_output_index_column(input_index_columns, bucket_boundaries);
    // Bucket boundaries can be wider than the date range specified by the user, narrow the first and last buckets
    // here if necessary
    bucket_boundaries.front(
    ) = std::max(bucket_boundaries.front(), date_range_->first - (closed_boundary == ResampleBoundary::RIGHT ? 1 : 0));
    bucket_boundaries.back(
    ) = std::min(bucket_boundaries.back(), date_range_->second + (closed_boundary == ResampleBoundary::LEFT ? 1 : 0));
    SegmentInMemory seg;
    RowRange output_row_range(
            row_slices.front().row_ranges_->at(0)->start(),
            row_slices.front().row_ranges_->at(0)->start() + output_index_column->row_count()
    );
    ColRange output_col_range(1, aggregators_.size() + 1);
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, index_column_name), output_index_column);
    seg.descriptor().set_index(IndexDescriptorImpl(IndexDescriptor::Type::TIMESTAMP, 1));
    auto& string_pool = seg.string_pool();

    ARCTICDB_DEBUG_THROW(5)
    for (const auto& aggregator : aggregators_) {
        std::vector<std::optional<ColumnWithStrings>> input_agg_columns;
        input_agg_columns.reserve(row_slices.size());
        for (auto& row_slice : row_slices) {
            auto variant_data = row_slice.get(aggregator.get_input_column_name());
            util::variant_match(
                    variant_data,
                    [&input_agg_columns](const ColumnWithStrings& column_with_strings) {
                        input_agg_columns.emplace_back(column_with_strings);
                    },
                    [&input_agg_columns](const EmptyResult&) {
                        // Dynamic schema, missing column from this row-slice
                        // Not currently supported, but will be, hence the argument to aggregate being a vector of
                        // optionals
                        input_agg_columns.emplace_back();
                    },
                    [](const auto&) {
                        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                "Unexpected return type from ProcessingUnit::get, expected column-like"
                        );
                    }
            );
        }
        std::optional<Column> aggregated = aggregator.aggregate(
                input_index_columns,
                input_agg_columns,
                bucket_boundaries,
                *output_index_column,
                string_pool,
                label_boundary_
        );
        if (aggregated) {
            seg.add_column(
                    scalar_field(aggregated->type().data_type(), aggregator.get_output_column_name().value),
                    std::make_shared<Column>(std::move(aggregated).value())
            );
        }
    }
    seg.set_row_data(output_index_column->row_count() - 1);
    return push_entities(
            *component_manager_,
            ProcessingUnit(std::move(seg), std::move(output_row_range), std::move(output_col_range))
    );
}

template<ResampleBoundary closed_boundary>
[[nodiscard]] std::string ResampleClause<closed_boundary>::to_string() const {
    return str_;
}

template<ResampleBoundary closed_boundary>
std::vector<timestamp> ResampleClause<closed_boundary>::generate_bucket_boundaries(
        timestamp first_ts, timestamp last_ts, bool responsible_for_first_overlapping_bucket
) const {
    auto first_it = std::lower_bound(
            bucket_boundaries_.begin(),
            bucket_boundaries_.end(),
            first_ts,
            [](timestamp boundary, timestamp first_ts) {
                if constexpr (closed_boundary == ResampleBoundary::LEFT) {
                    return boundary <= first_ts;
                } else {
                    // closed_boundary == ResampleBoundary::RIGHT
                    return boundary < first_ts;
                }
            }
    );
    if (responsible_for_first_overlapping_bucket && first_it != bucket_boundaries_.begin()) {
        --first_it;
    }
    auto last_it =
            std::upper_bound(first_it, bucket_boundaries_.end(), last_ts, [](timestamp last_ts, timestamp boundary) {
                if constexpr (closed_boundary == ResampleBoundary::LEFT) {
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
    // There used to be a check here that there was at least one bucket to process. However, this is not always the
    // case for data written by old versions of Arctic using update. See
    // test_compatibility.py::test_compat_resample_updated_data for more explanation
    return bucket_boundaries;
}

template<ResampleBoundary closed_boundary>
std::shared_ptr<Column> ResampleClause<closed_boundary>::generate_output_index_column(
        const std::vector<std::shared_ptr<Column>>& input_index_columns, const std::vector<timestamp>& bucket_boundaries
) const {
    constexpr auto data_type = DataType::NANOSECONDS_UTC64;
    using IndexTDT = ScalarTagType<DataTypeTag<data_type>>;

    const auto max_index_column_bytes = (bucket_boundaries.size() - 1) * get_type_size(data_type);
    auto output_index_column = std::make_shared<Column>(
            TypeDescriptor(data_type, Dimension::Dim0),
            Sparsity::NOT_PERMITTED,
            ChunkedBuffer::presized_in_blocks(max_index_column_bytes)
    );
    auto output_index_column_data = output_index_column->data();
    auto output_index_column_it = output_index_column_data.template begin<IndexTDT>();
    size_t output_index_column_row_count{0};

    auto bucket_end_it = std::next(bucket_boundaries.cbegin());
    Bucket<closed_boundary> current_bucket{*std::prev(bucket_end_it), *bucket_end_it};
    bool current_bucket_added_to_index{false};
    // Only include buckets that have at least one index value in range
    for (const auto& input_index_column : input_index_columns) {
        auto index_column_data = input_index_column->data();
        const auto cend = index_column_data.cend<IndexTDT>();
        auto it = index_column_data.cbegin<IndexTDT>();
        // In case the passed date_range does not span the whole segment we need to skip the index values
        // which are before the date range start.
        while (it != cend && *it < date_range_->first) {
            ++it;
        }
        for (; it != cend && *it <= date_range_->second; ++it) {
            if (ARCTICDB_LIKELY(current_bucket.contains(*it))) {
                if (ARCTICDB_UNLIKELY(!current_bucket_added_to_index)) {
                    *output_index_column_it++ =
                            label_boundary_ == ResampleBoundary::LEFT ? *std::prev(bucket_end_it) : *bucket_end_it;
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
                        *output_index_column_it++ =
                                label_boundary_ == ResampleBoundary::LEFT ? *std::prev(bucket_end_it) : *bucket_end_it;
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

} // namespace arcticdb
