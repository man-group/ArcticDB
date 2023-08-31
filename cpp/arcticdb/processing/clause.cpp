/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <vector>
#include <variant>
#include <arcticdb/processing/processing_unit.hpp>
#include <folly/Poly.h>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/value_set.hpp>

#include <arcticdb/util/third_party/emilib_set.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>

namespace arcticdb {

std::vector<Composite<SliceAndKey>> structure_by_row_slice(std::vector<SliceAndKey>& slice_and_keys, size_t start_from) {
    std::sort(std::begin(slice_and_keys), std::end(slice_and_keys), [] (const SliceAndKey& left, const SliceAndKey& right) {
        return std::tie(left.slice().row_range.first, left.slice().col_range.first) < std::tie(right.slice().row_range.first, right.slice().col_range.first);
    });

    std::vector<Composite<SliceAndKey>> rows;
    auto sk_it = std::begin(slice_and_keys);
    std::advance(sk_it, start_from);
    while(sk_it != std::end(slice_and_keys)) {
        pipelines::RowRange row_range{sk_it->slice().row_range};
        auto sk = Composite{std::move(*sk_it)};
        // Iterate through all SliceAndKeys that contain data for the same RowRange - i.e., iterate along column segments
        // for same row group
        while(++sk_it != std::end(slice_and_keys) && sk_it->slice().row_range == row_range) {
            sk.push_back(std::move(*sk_it));
        }

        util::check(!sk.empty(), "Should not push empty slice/key pairs to the pipeline");
        rows.emplace_back(std::move(sk));
    }
    return rows;
}

std::vector<Composite<SliceAndKey>> structure_by_column_slice(std::vector<SliceAndKey>& slice_and_keys) {
    std::sort(std::begin(slice_and_keys), std::end(slice_and_keys), [] (const SliceAndKey& left, const SliceAndKey& right) {
        return std::tie(left.slice().col_range.first, left.slice().row_range.first) < std::tie(right.slice().col_range.first, right.slice().row_range.first);
    });

    std::vector<Composite<SliceAndKey>> cols;
    auto sk_it = std::begin(slice_and_keys);
    while(sk_it != std::end(slice_and_keys)) {
        pipelines::ColRange col_range{sk_it->slice().col_range};
        auto sk = Composite{std::move(*sk_it)};
        // Iterate through all SliceAndKeys that contain data for the same ColRange - i.e., iterate along row segments
        // for same column group
        while(++sk_it != std::end(slice_and_keys) && sk_it->slice().col_range == col_range) {
            sk.push_back(std::move(*sk_it));
        }

        util::check(!sk.empty(), "Should not push empty slice/key pairs to the pipeline");
        cols.emplace_back(std::move(sk));
    }
    return cols;
}

std::vector<Composite<ProcessingUnit>> single_partition(std::vector<Composite<ProcessingUnit>> &&comps) {
    std::vector<Composite<ProcessingUnit>> v;
    v.push_back(merge_composites_shallow(std::move(comps)));
    return v;
}

class GroupingMap {
    using NumericMapType = std::variant<
            std::monostate,
            std::shared_ptr<emilib::HashMap<bool, size_t>>,
            std::shared_ptr<emilib::HashMap<uint8_t, size_t>>,
            std::shared_ptr<emilib::HashMap<uint16_t, size_t>>,
            std::shared_ptr<emilib::HashMap<uint32_t, size_t>>,
            std::shared_ptr<emilib::HashMap<uint64_t, size_t>>,
            std::shared_ptr<emilib::HashMap<int8_t, size_t>>,
            std::shared_ptr<emilib::HashMap<int16_t, size_t>>,
            std::shared_ptr<emilib::HashMap<int32_t, size_t>>,
            std::shared_ptr<emilib::HashMap<int64_t, size_t>>,
            std::shared_ptr<emilib::HashMap<float, size_t>>,
            std::shared_ptr<emilib::HashMap<double, size_t>>>;

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
    std::shared_ptr<emilib::HashMap<T, size_t>> get() {
        return util::variant_match(map_,
                                   [that = this](const std::monostate &) {
                                       that->map_ = std::make_shared<emilib::HashMap<T, size_t>>();
                                       return std::get<std::shared_ptr<emilib::HashMap<T, size_t>>>(that->map_);
                                   },
                                   [](const std::shared_ptr<emilib::HashMap<T, size_t>> &ptr) {
                                       return ptr;
                                   },
                                   [](const auto &) -> std::shared_ptr<emilib::HashMap<T, size_t>> {
                                       schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                                               "GroupBy does not support the grouping column type changing with dynamic schema");
                                   });
    }
};

struct SliceAndKeyWrapper {
    pipelines::SliceAndKey seg_;
    std::shared_ptr<Store> store_;
    SegmentInMemory::iterator it_;
    const StreamId id_;

    explicit SliceAndKeyWrapper(pipelines::SliceAndKey &&seg, std::shared_ptr<Store> store) :
            seg_(std::move(seg)),
            store_(std::move(store)),
            it_(seg_.segment(store_).begin()),
            id_(seg_.segment(store_).descriptor().id()) {
    }

    bool advance() {
        return ++it_ != seg_.segment(store_).end();
    }

    SegmentInMemory::Row &row() {
        return *it_;
    }

    const StreamId &id() const {
        return id_;
    }
};

Composite<ProcessingUnit> PassthroughClause::process(ARCTICDB_UNUSED const std::shared_ptr<Store> &store,
                                                     Composite<ProcessingUnit> &&p) const {
    auto procs = std::move(p);
    return procs;
}

Composite<ProcessingUnit> FilterClause::process(
        std::shared_ptr<Store> store,
        Composite<ProcessingUnit> &&p
        ) const {
    auto procs = std::move(p);
    Composite<ProcessingUnit> output;
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

std::string FilterClause::to_string() const {
    return expression_context_ ? fmt::format("WHERE {}", expression_context_->root_node_name_.value) : "";
}

Composite<ProcessingUnit> ProjectClause::process(std::shared_ptr<Store> store,
                                                 Composite<ProcessingUnit> &&p) const {
    auto procs = std::move(p);
    Composite<ProcessingUnit> output;
    procs.broadcast([&store, &expression_context = expression_context_, &output, that = this](auto &proc) {
        proc.set_expression_context(expression_context);
        auto variant_data = proc.get(expression_context->root_node_name_, store);
        util::variant_match(variant_data,
                            [&proc, &output, &store, &that](ColumnWithStrings &col) {

                                const auto data_type = col.column_->type().data_type();
                                const std::string_view name = that->output_column_;

                                auto &slice_and_keys = proc.data();
                                auto &last = *slice_and_keys.rbegin();
                                last.segment(store).add_column(scalar_field(data_type, name), col.column_);
                                ++last.slice().col_range.second;
                                output.push_back(std::move(proc));
                            },
                            [&proc, &output, &expression_context](const EmptyResult&) {
                                if(expression_context->dynamic_schema_)
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

AggregationClause::AggregationClause(const std::string& grouping_column,
                                     const std::unordered_map<std::string,
                                     std::string>& aggregations):
        grouping_column_(grouping_column),
        aggregation_map_(aggregations) {
    clause_info_.can_combine_with_column_selection_ = false;
    clause_info_.new_index_ = grouping_column_;
    clause_info_.input_columns_ = std::make_optional<std::unordered_set<std::string>>({grouping_column_});
    clause_info_.modifies_output_descriptor_ = true;
    for (const auto& [column_name, aggregation_operator]: aggregations) {
        auto [_, inserted] = clause_info_.input_columns_->insert(column_name);
        user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(inserted,
                                                              "Cannot perform two aggregations over the same column: {}",
                                                              column_name);
        auto typed_column_name = ColumnName(column_name);
        if (aggregation_operator == "sum") {
            aggregators_.emplace_back(SumAggregator(typed_column_name, typed_column_name));
        } else if (aggregation_operator == "mean") {
            aggregators_.emplace_back(MeanAggregator(typed_column_name, typed_column_name));
        } else if (aggregation_operator == "max") {
            aggregators_.emplace_back(MaxOrMinAggregator(typed_column_name, typed_column_name, Extremum::MAX));
        } else if (aggregation_operator == "min") {
            aggregators_.emplace_back(MaxOrMinAggregator(typed_column_name, typed_column_name, Extremum::MIN));
        } else {
            user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Unknown aggregation operator provided: {}", aggregation_operator);
        }
    }
}

Composite<ProcessingUnit> AggregationClause::process(std::shared_ptr<Store> store,
                                                     Composite<ProcessingUnit> &&p) const {
    auto procs = std::move(p);
    std::vector<GroupingAggregatorData> aggregators_data;
    internal::check<ErrorCode::E_INVALID_ARGUMENT>(
            !aggregators_.empty(),
            "AggregationClause::process does not make sense with no aggregators");
    for (const auto &agg: aggregators_){
        aggregators_data.emplace_back(agg.get_aggregator_data());
    }

    // Work out the common type between the processing units for the columns being aggregated
    procs.broadcast([&store, &aggregators_data, &aggregators=aggregators_](auto& proc) {
        for (auto agg_data: folly::enumerate(aggregators_data)) {
            auto input_column_name = aggregators.at(agg_data.index).get_input_column_name();
            auto input_column = proc.get(input_column_name, store);
            if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                agg_data->add_data_type(std::get<ColumnWithStrings>(input_column).column_->type().data_type());
            }
        }
    });

    size_t num_unique{0};
    size_t next_group_id{0};
    auto string_pool = std::make_shared<StringPool>();
    DataType grouping_data_type;
    GroupingMap grouping_map;
    procs.broadcast(
        [&store, &num_unique,
        &grouping_data_type, &grouping_map, &next_group_id, &aggregators_data, &string_pool, that=this](
            auto &proc) {
            auto partitioning_column = proc.get(ColumnName(that->grouping_column_), store);
            if (std::holds_alternative<ColumnWithStrings>(partitioning_column)) {
                ColumnWithStrings col = std::get<ColumnWithStrings>(partitioning_column);
                entity::details::visit_type(col.column_->type().data_type(),
                                            [&proc_=proc, &grouping_map, &next_group_id, &aggregators_data, &string_pool, &col,
                                             &num_unique, &store, &grouping_data_type, that](auto data_type_tag) {
                                                using DataTypeTagType = decltype(data_type_tag);
                                                using RawType = typename DataTypeTagType::raw_type;
                                                constexpr auto data_type = DataTypeTagType::data_type;
                                                grouping_data_type = data_type;
                                                std::vector<size_t> row_to_group;
                                                row_to_group.reserve(col.column_->row_count());
                                                auto input_data = col.column_->data();
                                                auto hash_to_group = grouping_map.get<RawType>();
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
                                                emilib::HashMap<RawType, size_t> offset_to_group;
                                                while (auto block = input_data.next<ScalarTagType<DataTypeTagType>>()) {
                                                    const auto row_count = block->row_count();
                                                    auto ptr = block->data();
                                                    for (size_t i = 0; i < row_count; ++i, ++ptr) {
                                                        RawType val;
                                                        if constexpr(is_sequence_type(data_type)) {
                                                            auto offset = *ptr;
                                                            if (auto it = offset_to_group.find(offset); it != offset_to_group.end()) {
                                                                val = it->second;
                                                            } else {
                                                                std::optional<std::string_view> str = col.string_at_offset(offset);
                                                                if (str.has_value()) {
                                                                    val = string_pool->get(*str, true).offset();
                                                                } else {
                                                                    val = offset;
                                                                }
                                                                RawType val_copy(val);
                                                                offset_to_group.insert_unique(std::move(offset), std::move(val_copy));
                                                            }
                                                        } else {
                                                            val = *ptr;
                                                        }
                                                        if (auto it = hash_to_group->find(val); it == hash_to_group->end()) {
                                                            row_to_group.emplace_back(next_group_id);
                                                            auto group_id = next_group_id++;
                                                            hash_to_group->insert_unique(std::move(val), std::move(group_id));
                                                        } else {
                                                            row_to_group.emplace_back(it->second);
                                                        }
                                                    }
                                                }

                                                num_unique = next_group_id;
                                                util::check(num_unique != 0, "Got zero unique values");
                                                for (auto agg_data: folly::enumerate(aggregators_data)) {
                                                    auto input_column_name = that->aggregators_.at(agg_data.index).get_input_column_name();
                                                    auto input_column = proc_.get(input_column_name, store);
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
        });

    SegmentInMemory seg;
    auto index_col = std::make_shared<Column>(make_scalar_type(grouping_data_type), grouping_map.size(), true, false);
    auto index_pos = seg.add_column(scalar_field(grouping_data_type, grouping_column_), index_col);
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
        seg.concatenate(agg_data->finalize(aggregators_.at(agg_data.index).get_output_column_name(), processing_config_.dynamic_schema_, num_unique));
    }

    seg.set_string_pool(string_pool);
    seg.set_row_id(num_unique - 1);
    return Composite{ProcessingUnit{std::move(seg)}};
}

[[nodiscard]] std::string AggregationClause::to_string() const {
    return fmt::format("AGGREGATE {}", aggregation_map_);
}

[[nodiscard]] Composite<ProcessingUnit> RemoveColumnPartitioningClause::process(std::shared_ptr<Store> store,
                                                                                Composite<ProcessingUnit> &&p) const {
    using namespace arcticdb::pipelines;
    auto procs = std::move(p);
    Composite<ProcessingUnit> output;
    procs.broadcast([&store, &output](ProcessingUnit &proc) {
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
            output.push_back(ProcessingUnit{std::move(*output_seg), FrameSlice{col_range, row_range}});
        }
    });
    return output;
}

Composite<ProcessingUnit> SplitClause::process(std::shared_ptr<Store> store,
                                               Composite<ProcessingUnit> &&procs) const {
    using namespace arcticdb::pipelines;

    auto proc_composite = std::move(procs);
    Composite<ProcessingUnit> ret;
    proc_composite.broadcast([&store, rows = rows_, &ret](auto &&p) {
        auto proc = std::forward<decltype(p)>(p);
        auto slice_and_keys = proc.data();
        for (auto &slice_and_key: slice_and_keys) {
            auto split_segs = slice_and_key.segment(store).split(rows);
            const ColRange col_range{slice_and_key.slice().col_range};
            size_t start_row = slice_and_key.slice().row_range.start();
            size_t end_row = 0;
            for (auto &item : split_segs) {
                end_row = start_row + item.row_count();
                const RowRange row_range{start_row, end_row};
                ret.push_back(ProcessingUnit{std::move(item), FrameSlice{col_range, row_range}});
                start_row = end_row;
            }
        }
    });
    return ret;
}

Composite<ProcessingUnit> SortClause::process(std::shared_ptr<Store> store,
                                              Composite<ProcessingUnit> &&p) const {
    auto procs = std::move(p);
    procs.broadcast([&store, &column = column_](auto &proc) {
        auto slice_and_keys = proc.data();
        for (auto &slice_and_key: slice_and_keys) {
            slice_and_key.segment(store).sort(column);
        }
    });
    return procs;
}

template<typename IndexType, typename DensityPolicy, typename QueueType, typename Comparator, typename StreamId>
void merge_impl(
        Composite<ProcessingUnit> &ret,
        QueueType &input_streams,
        bool add_symbol_column,
        StreamId stream_id,
        const arcticdb::pipelines::RowRange row_range,
        const arcticdb::pipelines::ColRange col_range,
        IndexType index,
        const StreamDescriptor& stream_descriptor) {
    using namespace arcticdb::pipelines;
    auto num_segment_rows = ConfigsMap::instance()->get_int("Merge.SegmentSize", 100000);
    using SegmentationPolicy = stream::RowCountSegmentPolicy;
    SegmentationPolicy segmentation_policy{static_cast<size_t>(num_segment_rows)};

    auto func = [&ret, &row_range, &col_range](auto &&segment) {
        ret.push_back(ProcessingUnit{std::forward<SegmentInMemory>(segment), FrameSlice{col_range, row_range}});
    };
    
    using AggregatorType = stream::Aggregator<IndexType, stream::DynamicSchema, SegmentationPolicy, DensityPolicy>;
    const auto& fields = stream_descriptor.fields();
    FieldCollection new_fields{};
    (void)new_fields.add(fields[0].ref());
    
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
Composite<ProcessingUnit> MergeClause::process(std::shared_ptr<Store> store,
                                               Composite<ProcessingUnit> &&p) const {
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
    procs.broadcast([&input_streams, &store, &min_start_row, &max_end_row, &min_start_col, &max_end_col](auto &&proc) {
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
    const RowRange row_range{min_start_row, max_end_row};
    const ColRange col_range{min_start_col, max_end_col};
    Composite<ProcessingUnit> ret;
    std::visit(
            [&ret, &input_streams, add_symbol_column = add_symbol_column_, &comp = compare, stream_id = stream_id_, &row_range, &col_range, &stream_descriptor = stream_descriptor_](auto idx,
                                                                                            auto density) {
                merge_impl<decltype(idx), decltype(density), decltype(input_streams), decltype(comp), decltype(stream_id)>(ret,
                                                                                                      input_streams,
                                                                                                      add_symbol_column,
                                                                                                      stream_id,
                                                                                                      row_range,
                                                                                                      col_range,
                                                                                                      idx,
                                                                                                      stream_descriptor);
            }, index_, density_policy_);

    return ret;
}

std::optional<std::vector<Composite<ProcessingUnit>>> MergeClause::repartition(
        std::vector<Composite<ProcessingUnit>> &&comps) const {
    std::vector<Composite<ProcessingUnit>> v;
    v.push_back(merge_composites_shallow(std::move(comps)));
    return v;
}

Composite<ProcessingUnit> ColumnStatsGenerationClause::process(std::shared_ptr<Store> store,
                                                               Composite<ProcessingUnit> &&p) const {
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
            "ColumnStatsGenerationClause::process does not make sense with no processing units");
    procs.broadcast(
            [&store, &start_indexes, &end_indexes, &aggregators_data, that=this](
                    auto &proc) {
                for (const auto& slice_and_key: proc.data_) {
                    start_indexes.insert(slice_and_key.key_->start_index());
                    end_indexes.insert(slice_and_key.key_->end_index());
                }
                for (auto agg_data : folly::enumerate(aggregators_data)) {
                    auto
                        input_column_name = that->column_stats_aggregators_->at(agg_data.index).get_input_column_name();
                    auto input_column = proc.get(input_column_name, store);
                    if (std::holds_alternative<ColumnWithStrings>(input_column)) {
                        auto input_column_with_strings = std::get<ColumnWithStrings>(input_column);
                        agg_data->aggregate(input_column_with_strings);
                    } else {
                        if (!that->processing_config_.dynamic_schema_)
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                                "Unable to resolve column denoted by aggregation operator: '{}'",
                                input_column_name);
                    }
                }
            });

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            start_indexes.size() == 1 && end_indexes.size() == 1,
            "Expected all data segments in one processing unit to have same start and end indexes");
    auto start_index = *start_indexes.begin();
    auto end_index = *end_indexes.begin();
    schema::check<ErrorCode::E_UNSUPPORTED_INDEX_TYPE>(
            std::holds_alternative<NumericIndex>(start_index) && std::holds_alternative<NumericIndex>(end_index),
            "Cannot build column stats over string-indexed symbol"
    );
    auto start_index_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), true);
    auto end_index_col = std::make_shared<Column>(make_scalar_type(DataType::NANOSECONDS_UTC64), true);
    start_index_col->template push_back<NumericIndex>(std::get<NumericIndex>(start_index));
    end_index_col->template push_back<NumericIndex>(std::get<NumericIndex>(end_index));
    start_index_col->set_row_data(0);
    end_index_col->set_row_data(0);

    SegmentInMemory seg;
    seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, start_index_column_name), start_index_col);
    seg.add_column(scalar_field(DataType::NANOSECONDS_UTC64, end_index_column_name), end_index_col);
    for (const auto& agg_data: folly::enumerate(aggregators_data)) {
        seg.concatenate(agg_data->finalize(column_stats_aggregators_->at(agg_data.index).get_output_column_names()));
    }
    seg.set_row_id(0);
    return Composite{ProcessingUnit{std::move(seg)}};
}

std::vector<Composite<SliceAndKey>> RowRangeClause::structure_for_processing(
        std::vector<SliceAndKey>& slice_and_keys, ARCTICDB_UNUSED size_t start_from) const {
    slice_and_keys.erase(std::remove_if(slice_and_keys.begin(), slice_and_keys.end(), [this](const SliceAndKey& slice_and_key) {
        return slice_and_key.slice_.row_range.start() >= end_ || slice_and_key.slice_.row_range.end() <= start_;
    }), slice_and_keys.end());
    return structure_by_column_slice(slice_and_keys);
}

Composite<ProcessingUnit> RowRangeClause::process(std::shared_ptr<Store> store,
                                                  Composite<ProcessingUnit> &&p) const {
    auto procs = std::move(p);
    procs.broadcast([&store, this](ProcessingUnit &proc) {
        for (auto& slice_and_key: proc.data()) {
            auto row_range = slice_and_key.slice_.row_range;
            if ((start_ > row_range.start() && start_ < row_range.end()) ||
                (end_ > row_range.start() && end_ < row_range.end())) {
                // Zero-indexed within this slice
                size_t start_row{0};
                size_t end_row{row_range.diff()};
                if (start_ > row_range.start() && start_ < row_range.end()) {
                    start_row = start_ - row_range.start();
                }
                if (end_ > row_range.start() && end_ < row_range.end()) {
                    end_row = end_ - (row_range.start());
                }
                auto seg = truncate_segment(slice_and_key.segment(store), start_row, end_row);
                slice_and_key.slice_.adjust_rows(seg.row_count());
                slice_and_key.slice_.adjust_columns(seg.descriptor().field_count() - seg.descriptor().index().field_count());
                slice_and_key.segment_ = std::move(seg);
            } // else all rows in the slice and key are required, do nothing
        }
    });
    return procs;
}

void RowRangeClause::set_processing_config(const ProcessingConfig& processing_config) {
    auto total_rows = static_cast<int64_t>(processing_config.total_rows_);
    switch(row_range_type_) {
        case RowRangeType::HEAD:
            if (n_ >= 0) {
                end_ = std::min(n_, total_rows);
            } else {
                end_ = std::max(static_cast<int64_t>(0), total_rows + n_);
            }
            break;
        case RowRangeType::TAIL:
            if (n_ >= 0) {
                start_ = std::max(static_cast<int64_t>(0), total_rows - n_);
                end_ = total_rows;
            } else {
                start_ = std::min(-n_, total_rows);
                end_ = total_rows;
            }
            break;
        default:
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unrecognised RowRangeType {}", static_cast<uint8_t>(row_range_type_));
    }
}

std::string RowRangeClause::to_string() const {
    return fmt::format("{} {}", row_range_type_ == RowRangeType::HEAD ? "HEAD" : "TAIL", n_);
}

std::vector<Composite<SliceAndKey>> DateRangeClause::structure_for_processing(
        std::vector<SliceAndKey>& slice_and_keys, size_t start_from) const {
    slice_and_keys.erase(std::remove_if(slice_and_keys.begin(), slice_and_keys.end(), [this](const SliceAndKey& slice_and_key) {
        auto [start_index, end_index] = slice_and_key.key().time_range();
        return start_index > end_ || end_index <= start_;
    }), slice_and_keys.end());
    return structure_by_row_slice(slice_and_keys, start_from);
}

Composite<ProcessingUnit> DateRangeClause::process(ARCTICDB_UNUSED std::shared_ptr<Store> store,
                                                   Composite<ProcessingUnit> &&p) const {
    auto procs = std::move(p);
    procs.broadcast([&store, this](ProcessingUnit &proc) {
        // We are only interested in the index, which is in every SegmentInMemory in proc.data(), so just use the first
        auto slice_and_key = proc.data()[0];
        auto row_range = slice_and_key.slice_.row_range;
        auto [start_index, end_index] = slice_and_key.key().time_range();
        if ((start_ > start_index && start_ < end_index) || (end_ >= start_index && end_ < end_index)) {
            size_t start_row{0};
            size_t end_row{row_range.diff()};
            if (start_ > start_index && start_ < end_index) {
                start_row = slice_and_key.segment(store).column_ptr(0)->search_sorted<timestamp>(start_);
            }
            if (end_ >= start_index && end_ < end_index) {
                end_row = slice_and_key.segment(store).column_ptr(0)->search_sorted<timestamp>(end_, true);
            }
            proc.truncate(start_row, end_row, store);
        } // else all rows in the processing unit are required, do nothing
    });
    return procs;
}

std::string DateRangeClause::to_string() const {
    return fmt::format("DATE RANGE {} - {}", start_, end_);
}

}