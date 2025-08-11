/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/constants.hpp>

#include <memory>

namespace arcticdb::stream {

namespace {
    template <class Container, class F, std::size_t... Is>
    void compile_for_(Container cont, F func, std::index_sequence<Is...>) {
        (func(cont.at(Is)), ...);
    }
}

struct AggregationStats {
    size_t nbytes = 0;
    size_t count = 0;
    size_t total_rows_ = 0;
    timestamp last_active_time_ = util::SysClock::coarse_nanos_since_epoch();

    void reset();

    void update(size_t num_bytes);

    void update_many(size_t rows, size_t num_bytes );

    inline void update_rows(size_t rows) { total_rows_ += rows; }

    [[nodiscard]] inline size_t total_rows() const { return total_rows_; }
};

class RowCountSegmentPolicy {
  public:
    static constexpr std::size_t default_max_row_count_ = 100000;

    RowCountSegmentPolicy() = default;
    explicit RowCountSegmentPolicy(std::size_t row_count) : max_row_count_(row_count) {}

    bool operator()(AggregationStats &stats) const {
        ARCTICDB_TRACE(log::inmem(), "RowCountSegmentPolicy AggregationStats total_rows={}, max_row={}",
            stats.total_rows_, max_row_count_);
        return stats.total_rows_ >= max_row_count_;
    }

    size_t expected_row_size() const { return max_row_count_; }

  private:
    std::size_t max_row_count_ = default_max_row_count_;
};

class NeverSegmentPolicy {
  public:
    NeverSegmentPolicy() = default;
    constexpr bool operator()(AggregationStats &) const {
        return false;
    }

    [[nodiscard]] size_t expected_row_size() const { return 0; }
};

template<class Sysclock=util::SysClock>
class TimeBasedSegmentPolicy {
public:
    static constexpr std::size_t default_max_diff_ = 2 * ONE_MINUTE;

    TimeBasedSegmentPolicy() = default;
    explicit TimeBasedSegmentPolicy(timestamp diff) : max_diff_(diff) {}

    bool operator()(AggregationStats &stats) const {
        auto curr_time = Sysclock::coarse_nanos_since_epoch();
        auto diff = curr_time - stats.last_active_time_;
        ARCTICDB_DEBUG(log::inmem(), "TimeBasedSegmentPolicy AggregationStats diff={}, max_diff={}",
            diff, max_diff_);
        bool is_policy_valid = false;
        if (diff >= max_diff_) {
            is_policy_valid = true;
            stats.last_active_time_ = curr_time;
        }
        return is_policy_valid;
    }

    [[nodiscard]] size_t expected_row_size() const { return 0; }

private:
    timestamp max_diff_ = default_max_diff_;
};

using VariantPolicyType = std::variant<NeverSegmentPolicy, RowCountSegmentPolicy, TimeBasedSegmentPolicy<>>;

template <int N>
class ListOfSegmentPolicies {
public:

    ListOfSegmentPolicies() = default;
    template <typename... Policies>
    explicit ListOfSegmentPolicies(const Policies&... policies) {
        static_assert(N == sizeof...(policies));
        policies_ = {policies...};
    }

    bool operator()(AggregationStats &stats) const {
        bool is_policy_valid = false;
        compile_for_(policies_, [&is_policy_valid, &stats](const auto& variant_policy) {
            std::visit([&is_policy_valid, &stats](const auto& policy) {
                is_policy_valid = is_policy_valid || policy(stats);
            }, variant_policy);
        }, std::make_index_sequence<N>());
        return is_policy_valid;
    }

    [[nodiscard]] size_t expected_row_size() const {
        size_t val = 0;
        // TODO: Do it differently later, currently just using 0 since that's what we need for tickdata
        return val;
    }

private:
    std::array<VariantPolicyType, N> policies_ ;
};

class DenseColumnPolicy {
public:
    static constexpr Sparsity allow_sparse = Sparsity::NOT_PERMITTED;
};

class SparseColumnPolicy {
public:
    static constexpr Sparsity allow_sparse = Sparsity::PERMITTED;
};

using VariantColumnPolicy = std::variant<DenseColumnPolicy, SparseColumnPolicy>;

template<class Index, class Schema, class SegmentingPolicy = RowCountSegmentPolicy, class DensityPolicy = DenseColumnPolicy>
class Aggregator {
  public:
    using IndexType = Index;
    using SchemaPolicy = Schema;
    using SparsePolicy = DensityPolicy;
    using SegmentingPolicyType = SegmentingPolicy;
    using SelfType = Aggregator<IndexType, Schema, SegmentingPolicy, DensityPolicy>;
    using RowBuilderType = RowBuilder<IndexType, Schema, SelfType>;
    using Callback = folly::Function<void(SegmentInMemory &&)>;
    friend RowBuilderType;

    template<class C>
    Aggregator(
        SchemaPolicy &&schema,
        C &&c,
        SegmentingPolicy &&segmenting_policy = SegmentingPolicyType(),
        const std::optional<StreamDescriptor>& desc = std::nullopt,
        const std::optional<size_t>& row_count = std::nullopt) :
        schema_policy_(std::move(schema)),
        row_builder_(schema_policy_, self()),
        callback_(std::forward<Callback>(c)),
        stats_(),
        segmenting_policy_(std::move(segmenting_policy)),
        segment_(desc ? *desc : schema_policy_.default_descriptor(), row_count.value_or(segmenting_policy_.expected_row_size()), AllocationType::DYNAMIC, SparsePolicy::allow_sparse) {
            segment_.init_column_map();
            if constexpr (!(std::is_same_v<Index, EmptyIndex> || std::is_same_v<Index, RowCountIndex>)) {
                index().check(segment_.descriptor().fields());
            }
    };

    virtual ~Aggregator() = default;

    RowBuilderType &row_builder() {
        return row_builder_;
    }

    template<class ...Args>
    RowBuilderType &start_row(Args...args) {
        SCOPE_FAIL {
            row_builder_.rollback_row();
        };
        row_builder_.start_row(args...);
        return row_builder_;
    }

    void rollback_row(util::BitSet &) noexcept {
        // TODO implement rollback
    }

    virtual void commit();

    virtual void finalize();

    void clear();

    const IndexType& index() const {
        return std::get<IndexType>(schema_policy_.index());
    }

    size_t row_count() { return segment_.row_count(); }

    size_t commits_count() const { return commits_count_; }

    const AggregationStats &stats() const { return stats_; }

    const arcticdb::entity::StreamDescriptor &descriptor() const { return segment_.descriptor(); }

    arcticdb::entity::StreamDescriptor default_descriptor() const { return schema_policy_.default_descriptor(); }

    auto &segment() { return segment_; }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_external_block(std::size_t pos, T *val, size_t size) {
        segment_.set_external_block(pos, val, size);
    }

    template<class T, std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T>, int> = 0>
    void set_sparse_block(std::size_t pos, T *val, size_t rows_to_write) {
        segment_.set_sparse_block(pos, val,  rows_to_write);
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, util::BitSet&& bitset) {
        segment_.set_sparse_block(idx, std::move(buffer), std::move(bitset));
    }

    void set_sparse_block(position_t idx, ChunkedBuffer&& buffer, Buffer&& shapes, util::BitSet&& bitset) {
        segment_.set_sparse_block(idx, std::move(buffer), std::move(shapes), std::move(bitset));
    }

    void set_string_at(position_t col, position_t row, const char* val, size_t size) {
        segment_.set_string_at(col, row, val, size);
    }

    void set_offset(ssize_t offset) {
        segment_.set_offset(offset);
    }

    void end_block_write(size_t size) {
        stats_.update_rows(size);
        segment_.end_block_write(size);
    }

    template<typename T, template<typename> typename Tensor>
    requires std::integral<T> || std::floating_point<T>
    void set_array(position_t pos, Tensor<T> &val) {
        segment_.set_array(pos, val);
    }

    template<typename T>
    requires std::integral<T> || std::floating_point<T>
    void set_array(position_t pos, py::array_t<T>& val) {
        segment_.set_array(pos, val);
    }
    
    void set_string_array(position_t pos, size_t string_size, size_t num_strings, char *data) {
        segment_.set_string_array(pos, string_size, num_strings, data);
    }

    SegmentingPolicyType segmenting_policy() {
        return segmenting_policy_;
    }

    AggregationStats& stats() { return stats_; }

protected:
    void commit_impl(bool final);

private:
    template <typename T>
    requires std::integral<T> || std::floating_point<T>
    void set_scalar(std::size_t pos, T val) {
        segment_.set_scalar(pos, val);
    }

    template<typename T>
    requires std::integral<T> || std::floating_point<T>
    void set_scalar_by_name(std::string_view name, T val, DataType data_type) {
        position_t pos = schema_policy_.get_column_idx_by_name(segment_, name, make_scalar_type(data_type), segmenting_policy_.expected_row_size(), segment_.row_count());
        set_scalar(pos, val);
    }

    void set_string(position_t pos, const std::string &str) {
        segment_.set_string(pos, str);
    }

    void set_string(position_t pos, std::string_view str) {
        segment_.set_string(pos, str);
    }

    void set_string_by_name(std::string_view name, std::string_view str, DataType desc) {
        position_t pos = schema_policy_.get_column_idx_by_name(segment_, name, make_scalar_type(desc), segmenting_policy_.expected_row_size(), segment_.row_count());
        set_string(pos, str);
    }

    void set_string_list(position_t pos, const std::vector<std::string> &input) {
        segment_.set_string_list(pos, input);
    }

    void end_row();

    SelfType& self() {
        return *this;
    }

    SchemaPolicy schema_policy_;
    RowBuilderType row_builder_;
    Callback callback_;
    AggregationStats stats_;
    SegmentingPolicyType segmenting_policy_;
    SegmentInMemory segment_;
    size_t commits_count_ = 0;
};

using FixedTimestampAggregator = Aggregator<TimeseriesIndex, FixedSchema>;
using DynamicTimestampAggregator = Aggregator<TimeseriesIndex, DynamicSchema, RowCountSegmentPolicy, SparseColumnPolicy>;
}

#define ARCTICDB_AGGREGATOR_H_
#include "aggregator-inl.hpp"
