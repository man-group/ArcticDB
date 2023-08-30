/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/bitset.hpp>
#include <arcticdb/entity/index_range.hpp>
#include <arcticdb/processing/expression_context.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/python_output_frame.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/simple_string_hash.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>

#include <algorithm>
#include <vector>
#include <string>
#include <variant>

namespace arcticdb::pipelines {

using FilterRange = std::variant<std::monostate, IndexRange, RowRange>;

struct SignedRowRange {
    int64_t start_;
    int64_t end_;
};

struct ReadQuery {
    mutable std::vector<std::string> columns; // empty <=> all columns
    std::optional<SignedRowRange> row_range;
    FilterRange row_filter; // no filter by default
    std::vector<std::shared_ptr<Clause>> clauses_;

    ReadQuery() = default;

    explicit ReadQuery(std::vector<std::shared_ptr<Clause>>&& clauses):
            clauses_(std::move(clauses)) {
    }

    void add_clauses(std::vector<std::shared_ptr<Clause>>& clauses) {
        clauses_ = clauses;
    }

    void calculate_row_filter(int64_t total_rows) {
        if (row_range.has_value()) {
            size_t start = row_range->start_ >= 0 ?
                           std::min(row_range->start_, total_rows) :
                           std::max(total_rows + row_range->start_,
                                    static_cast<int64_t>(0));
            size_t end = row_range->end_ >= 0 ?
                         std::min(row_range->end_, total_rows) :
                         std::max(total_rows + row_range->end_, static_cast<int64_t>(0));
            row_filter = RowRange(start, end);
        }
    }
};

struct SnapshotVersionQuery {
    std::string name_;
};

struct TimestampVersionQuery {
    timestamp timestamp_;
};

struct SpecificVersionQuery {
    SignedVersionId version_id_;
};

using VersionQueryType = std::variant<
        std::monostate, // Represents "latest"
        SnapshotVersionQuery,
        TimestampVersionQuery,
        SpecificVersionQuery
        >;

struct VersionQuery {
    VersionQueryType content_;
    std::optional<bool> skip_compat_;
    std::optional<bool> iterate_on_failure_;

    void set_snap_name(const std::string& snap_name) {
        content_ = SnapshotVersionQuery{snap_name};
    }

    void set_timestamp(timestamp ts) {
        content_ = TimestampVersionQuery{ts};
    }

    void set_version(SignedVersionId version) {
        content_ = SpecificVersionQuery{version};
    }

    void set_skip_compat(const std::optional<bool>& skip_compat) {
        skip_compat_ = skip_compat;
    }

    void set_iterate_on_failure(const std::optional<bool>& iterate_on_failure) {
        iterate_on_failure_ = iterate_on_failure;
    }
};

template<typename ContainerType>
using FilterQuery = folly::Function<std::unique_ptr<util::BitSet>(const ContainerType &, std::unique_ptr<util::BitSet>&&)>;

template<typename ContainerType>
using CombinedQuery = folly::Function<std::unique_ptr<util::BitSet>(const ContainerType&)>;

inline bool is_column_selected(size_t start_col, size_t end_col, const util::BitSet& sc) {
    if (start_col == 0) {
        auto col = sc.get_first();
        return col < end_col;
    } else {
        auto col = sc.get_next(start_col - 1ULL);
        return col != 0 && col < end_col;
    }
}

inline FilterQuery<index::IndexSegmentReader> create_static_col_filter(util::BitSet &&selected_columns) {
    return [sc = std::move(selected_columns)](const index::IndexSegmentReader &isr, std::unique_ptr<util::BitSet>&& input) mutable {
        auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(isr.size()));
        auto start_col = isr.column(index::Fields::start_col).begin<stream::SliceTypeDescriptorTag>();
        auto end_col = isr.column(index::Fields::end_col).begin<stream::SliceTypeDescriptorTag>();

        if (input) {
            bm::bvector<>::enumerator en = input->first();
            bm::bvector<>::enumerator en_end = input->end();
            size_t pos{0};

            while (en < en_end) {
                const auto dist = *en - pos;
                pos = *en;
                std::advance(start_col, dist);
                std::advance(end_col, dist);
                (*res)[*en] = is_column_selected(*start_col, *end_col, sc);
                ++en;
            }

        } else {
            for (std::size_t r = 0, end = isr.size(); r < end; ++r) {
                (*res)[r] = is_column_selected(*start_col, *end_col, sc);
                ++start_col;
                ++end_col;
            }
        }
        ARCTICDB_DEBUG(log::version(), "Column filter has {} bits set", res->count());
        return res;
    };
}

inline FilterQuery<index::IndexSegmentReader> create_dynamic_col_filter(util::BitSet &&selected_columns, const std::shared_ptr<PipelineContext>& pipeline_context) {
    return [sc = std::move(selected_columns), pipeline_context](const index::IndexSegmentReader &isr, std::unique_ptr<util::BitSet>&& input) mutable {
        auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(sc.size()));
        util::check(isr.bucketize_dynamic(), "Expected column group in index segment reader dynamic column filter");
        auto hash_bucket = isr.column(index::Fields::hash_bucket).begin<stream::SliceTypeDescriptorTag>();
        auto num_buckets = isr.column(index::Fields::num_buckets).begin<stream::SliceTypeDescriptorTag>();

        bm::bvector<>::enumerator col = sc.first();
        bm::bvector<>::enumerator col_end = sc.end();
        std::unordered_set<size_t> cols_hashes;
        while (col < col_end) {
            // we use raw_hashes for each col
            // A FrameSlice stores (hash_bucket, total_buckets) at the time of writing that slice
            // so a column will exist inside a slice iff col_hash % total_buckets == hash_bucket
            cols_hashes.insert(bucketize(pipeline_context->desc_->field(*col).name(), std::nullopt));
            ++col;
        }

        if (input) {
            bm::bvector<>::enumerator en = input->first();
            bm::bvector<>::enumerator en_end = input->end();
            size_t pos{0};

            while (en < en_end) {
                const auto dist = *en - pos;
                pos = *en;
                std::advance(hash_bucket, dist);
                std::advance(num_buckets, dist);
                (*res)[*en] = std::find_if(cols_hashes.begin(), cols_hashes.end(),
                                        [&num_buckets, &hash_bucket](auto col_hash){
                                            return (col_hash % *num_buckets) == (*hash_bucket);
                                        }) != cols_hashes.end();
                ++en;
            }

        } else {
            for (std::size_t r = 0, end = isr.size(); r < end; ++r) {
                (*res)[r] = std::find_if(cols_hashes.begin(), cols_hashes.end(),
                                      [&num_buckets, &hash_bucket](auto col_hash){
                                          return (col_hash % *num_buckets) == (*hash_bucket);
                                      }) != cols_hashes.end();
                ++hash_bucket;
                ++num_buckets;
            }
        }
        ARCTICDB_DEBUG(log::version(), "Dynamic column filter has {} bits set", res->count());
        return res;
    };
}

inline std::size_t start_row(const index::IndexSegmentReader &isr, std::size_t row) {
    return isr.column(index::Fields::start_row).scalar_at<std::size_t>(row).value();
}

inline std::size_t start_row(const std::vector<SliceAndKey> &sk, std::size_t row) {
    return sk[row].slice_.row_range.first;
}

inline std::size_t end_row(const index::IndexSegmentReader &isr, std::size_t row) {
    return isr.column(index::Fields::end_row).scalar_at<std::size_t>(row).value();
}

inline std::size_t end_row(const std::vector<SliceAndKey> &sk, std::size_t row) {
    return sk[row].slice_.row_range.second;
}

template<typename ContainerType>
inline FilterQuery<ContainerType> create_row_filter(RowRange &&range) {
    return [rg = std::move(range)](const ContainerType &container, std::unique_ptr<util::BitSet>&& input) mutable {
        auto res = std::make_unique<util::BitSet>(static_cast<util::BitSetSizeType>(container.size()));
        for (std::size_t r = 0, end = container.size(); r < end; ++r) {
            bool included = start_row(container, r) < rg.second && end_row(container, r) > rg.first;
            ARCTICDB_DEBUG(log::version(), "Row {} is {} range {}", r, included ? "inside" : "outside", rg);
            (*res)[r] = included;
        }

        if(input)
            *res &= *input;

        ARCTICDB_DEBUG(log::version(), "Row filter has {} bits set", res->count());
        return res;
    };
}

IndexValue start_index(const std::vector<SliceAndKey> &sk, std::size_t row);

IndexValue start_index(const index::IndexSegmentReader &isr, std::size_t row);

IndexValue end_index(const index::IndexSegmentReader &isr, std::size_t row);

IndexValue end_index(const std::vector<SliceAndKey> &sk, std::size_t row);

template <typename RawType>
bool range_intersects(RawType a_start, RawType a_end, RawType b_start, RawType b_end) {
    return a_start <= b_end && a_end >= b_start;
}

template<typename ContainerType, typename IdxType>
std::unique_ptr<util::BitSet> build_bitset_for_index(
        const ContainerType& container,
        IndexRange rg,
        bool dynamic_schema,
        bool column_groups,
        std::unique_ptr<util::BitSet>&& input);

template<typename ContainerType>
inline FilterQuery<ContainerType> create_index_filter(const IndexRange &range, bool dynamic_schema, bool column_groups) {
    static_assert(std::is_same_v<ContainerType, index::IndexSegmentReader>);
    return [rg = range, dynamic_schema, column_groups](const ContainerType &container, std::unique_ptr<util::BitSet>&& input) mutable {
        auto index_type = container.seg().template scalar_at<uint8_t>(0u, int(index::Fields::index_type));

        switch (index_type.value()) {
        case IndexDescriptor::TIMESTAMP: {
            return build_bitset_for_index<ContainerType, TimeseriesIndex>(container,
                                                                          rg,
                                                                          dynamic_schema,
                                                                          column_groups,
                                                                          std::move(input));
        }
        case IndexDescriptor::STRING: {
            return build_bitset_for_index<ContainerType, TableIndex>(container, rg, dynamic_schema, column_groups, std::move(input));
        }
        default:util::raise_rte("Unknown index type {} in create_index_filter", uint32_t(index_type.value()));
        }
    };
}

template<typename ContainerType>
inline std::vector<FilterQuery<ContainerType>> build_read_query_filters(
    std::optional<util::BitSet>& col_bitset,
    const std::shared_ptr<PipelineContext>& pipeline_context,
    const FilterRange &range,
    bool dynamic_schema,
    bool column_groups) {
    using namespace arcticdb::pipelines;
    std::vector<FilterQuery<ContainerType>> queries;

    util::variant_match(range,
                        [&](const RowRange &row_range) {
                            queries.push_back(
                                    create_row_filter<ContainerType>(RowRange{row_range.first, row_range.second}));
                        },
                        [&](const IndexRange &index_range) {
                            if (index_range.specified_) {
                                queries.push_back(create_index_filter<ContainerType>(index_range, dynamic_schema, column_groups));
                            }
                        },
                        [](const auto &) {}
    );

    if(col_bitset) {
        util::check(!dynamic_schema || column_groups, "Did not expect a column bitset with dynamic schema");

        if(column_groups)
            queries.push_back(create_dynamic_col_filter(util::BitSet(col_bitset.value()), pipeline_context));
        else
            queries.push_back(create_static_col_filter(util::BitSet(col_bitset.value())));
    }

    return queries;
}

struct UpdateQuery {
    FilterRange row_filter; // no filter by default
};

template<typename ContainerType>
inline std::vector<FilterQuery<ContainerType>> build_update_query_filters(
        const FilterRange &range,
        const stream::Index& index,
        const IndexRange& index_range,
        bool dynamic_schema,
        bool column_groups
) {
    // If a range was supplied, construct a query based on the type of the supplied range, otherwise create a query
    // based on the index type of the incoming update frame. All three types must match, i.e. the index type of the frame to
    // be appended to, the type of the frame being appended, and the specified range, if supplied.
    std::vector<FilterQuery<ContainerType>> queries;
    util::variant_match(range,
                        [&](const  RowRange &row_range) {
                            util::check(std::holds_alternative<stream::RowCountIndex>(index), "Cannot partition by row count when a timeseries-indexed frame was supplied");
                            queries.push_back(
                                    create_row_filter<ContainerType>(RowRange{row_range.first, row_range.second}));
                        },
                        [&](const IndexRange &index_range) {
                            util::check(std::holds_alternative<stream::TimeseriesIndex>(index), "Cannot partition by time when a rowcount-indexed frame was supplied");
                            queries.push_back(create_index_filter<ContainerType>(IndexRange{index_range}, dynamic_schema, column_groups));
                        },
                        [&](const auto &) {
                            util::variant_match(index,
                                                [&](const stream::TimeseriesIndex &) {
                                                    queries.push_back(create_index_filter<ContainerType>(IndexRange{index_range}, dynamic_schema, column_groups));
                                                },
                                                [&](const stream::RowCountIndex &) {
                                                    RowRange row_range{std::get<NumericId>(index_range.start_), std::get<NumericIndex>(index_range.end_)};
                                                    queries.push_back(create_row_filter<ContainerType>(std::move(row_range)));
                                                },
                                                [&](const auto &) {
                                                });
                        });

    return queries;
}

inline FilterRange get_query_index_range(
        const stream::Index& index,
        const IndexRange& index_range) {
        if(std::holds_alternative<stream::TimeseriesIndex>(index))
               return index_range;
        else
               return RowRange{std::get<NumericIndex>(index_range.start_), std::get<NumericIndex>(index_range.end_)};
}

inline std::vector<SliceAndKey> strictly_before(const FilterRange &range, const std::vector<SliceAndKey> &input) {
    std::vector<SliceAndKey> output;
    util::variant_match(range,
                        [&](const RowRange &row_range) {
                            std::copy_if(std::begin(input), std::end(input), std::back_inserter(output),
                                         [&](const auto &sk) {
                                             return sk.slice_.row_range.second < row_range.first;
                                         });
                        },
                        [&](const IndexRange &index_range) {
                            std::copy_if(std::begin(input), std::end(input), std::back_inserter(output),
                                         [&](const auto &sk) {
                                             return sk.key().index_range().end_ < index_range.start_;
                                         });
                        },
                        [&](const auto &) {
                            util::raise_rte("Expected specified range ");
                        });
    return output;
}

inline std::vector<SliceAndKey> strictly_after(const FilterRange &range, const std::vector<SliceAndKey> &input) {
    std::vector<SliceAndKey> output;
    util::variant_match(range,
                        [&input, &output](const RowRange &row_range) {
                            std::copy_if(std::begin(input), std::end(input), std::back_inserter(output),
                                         [&](const auto &sk) {
                                             return sk.slice_.row_range.first > row_range.second;
                                         });
                        },
                        [&input, &output](const IndexRange &index_range) {
                            std::copy_if(std::begin(input), std::end(input), std::back_inserter(output),
                                         [&](const auto &sk) {
                                             return sk.key().index_range().start_ > index_range.end_;
                                         });
                        },
                        [](const auto &) {
                            util::raise_rte("Expected specified range ");
                        });
    return output;
}

} //namespace arcticdb::pipelines

namespace fmt {
using namespace arcticdb::pipelines;

template<>
struct formatter<VersionQuery> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const VersionQuery& q, FormatContext& ctx) const {
        return arcticdb::util::variant_match(q.content_,
                [&ctx](const SpecificVersionQuery& s) { return format_to(ctx.out(), "version {}", s.version_id_); },
                [&ctx](const SnapshotVersionQuery& s) { return format_to(ctx.out(), "snapshot '{}'", s.name_); },
                [&ctx](const TimestampVersionQuery& t) { return format_to(ctx.out(), "{}", t.timestamp_); },
                [&ctx](const std::monostate&) { return format_to(ctx.out(), "latest"); });
    }
};
}
