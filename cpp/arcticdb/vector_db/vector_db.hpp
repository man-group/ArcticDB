/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/processing/clause.hpp>

namespace arcticdb {

    struct NamedColumnSortedByDistance {
        double distance_;
        std::shared_ptr<Column> column_;
        std::string_view name_;

        explicit NamedColumnSortedByDistance(double distance,
                                             std::shared_ptr<Column> column,
                                             std::string_view name) :
                distance_(distance),
                column_(column),
                name_(name) {}

        NamedColumnSortedByDistance() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(NamedColumnSortedByDistance);

        bool operator<(const NamedColumnSortedByDistance &that) const {
            return distance_ < that.distance_ || (distance_ == that.distance_ && name_ < that.name_);
            // name is a tiebreaker.
        };

        bool operator==(const NamedColumnSortedByDistance &that) const {
            return distance_ == that.distance_ && column_ == that.column_ && name_ == that.name_;
        };

    };

    struct TopK {
        uint64_t k_;
        std::set<NamedColumnSortedByDistance> top_k_;
        double furthest_distance_{std::numeric_limits<double>::infinity()};

        explicit TopK(int k) : k_(k) {}

        TopK() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(TopK);

        bool try_insert(
                double distance,
                std::shared_ptr<Column> column,
                std::string_view name
        ) {
            return try_insert(NamedColumnSortedByDistance(
                    distance,
                    column,
                    name));
        }

        bool try_insert(const NamedColumnSortedByDistance &col) {
            // NB We require <= so that the vector name acts as the tiebreaker when the distance is the same.
            // This is not guaranteed to the user but is useful for testing.
            bool inserted = false;
            if (col.distance_ < furthest_distance_ || (col.distance_ == furthest_distance_ && col < *top_k_.rbegin())) {
                top_k_.insert(col);
                inserted = true;
            }
            // Obviously if col.distance_ < furthest_distance_ then we've got a closer vector so we insert it.
            // Where col.distance_ == furthest_distance_, the latter cannot be infinity, since in Python we require that
            // all vectors should be within the n-cube centred about the origin whose long diagonal has length
            // std::numeric_limits<double>::max(). Therefore, furthest_distance_ is finite. Therefore, it's been
            // altered, and so we have at least member of top_k_. So top_k_ is nonempty and we can compare
            // col < *top_k_.rbegin().
            if (top_k_.size() > k_) {
                top_k_.erase(*top_k_.rbegin());
                furthest_distance_ = top_k_.rbegin()->distance_;
            } else if (inserted && top_k_.size() == k_) {
                furthest_distance_ = top_k_.rbegin()->distance_;
            }
            // There are three cases here.
            // - top_k.size() < k. Then we don't need to erase anything or refresh the furthest distance. When there are
            //   fewer than k elements in top_k_, we want to add any new vector we see; we may remove it later of course.
            // - top_k_.size() is exactly k. Then we don't need to erase anything. But we might have just inserted
            //   something so we need to reload the furthest_distance_ if we inserted something.
            // - top_k.size() = k+1. Then we do need to erase the furthest element. We also need to refresh the largest
            //   element.
            return inserted;
        }
    };


    struct TopKClause {
        ClauseInfo clause_info_;
        std::vector<double> query_vector_;
        uint64_t k_;

        TopKClause() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(TopKClause);

        TopKClause(std::vector<double> query_vector, uint8_t k);

        [[nodiscard]] Composite<ProcessingUnit> process(
                std::shared_ptr<Store> store,
                Composite<ProcessingUnit> &&p
        ) const;

        [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
                std::vector<SliceAndKey>& slice_and_keys, ARCTICDB_UNUSED size_t) const {
            return structure_by_column_slice(slice_and_keys);
        }

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
                std::vector<Composite<ProcessingUnit>> &&c,
                const std::shared_ptr<Store>& store) const {
            auto comps = std::move(c);
            TopK top_k(k_);
            for (auto &comp : comps) {
                comp.broadcast([&store, &top_k](auto &proc) {
                    for (const auto& slice_and_key: proc.data()) {
                        const std::vector<std::shared_ptr<Column>>& columns = slice_and_key.segment(store).columns();
                        for (auto&& [idx, col]: folly::enumerate(columns)) {
                            col->type().visit_tag([&top_k, &col=col, &slice_and_key, &store, idx=idx](auto type_desc_tag) {
                                using TDT = decltype(type_desc_tag);
                                using raw_type = typename TDT::DataTypeTag::raw_type;

                                if constexpr (is_floating_point_type(TDT::DataTypeTag::data_type)) {
                                    top_k.try_insert(
                                            col->template scalar_at<raw_type>(col->last_row()).value(),
                                            col,
                                            slice_and_key.segment(store).field(idx).name()
                                    );
                                } else {
                                    internal::raise<ErrorCode::E_INVALID_ARGUMENT>(
                                            "Vectors should exclusively comprise floats; the Python layer should otherwise raise an error and prevent upsertion of vectors containing non-float components."
                                    );
                                }
                            });
                        }
                    }
                });
            }
            SegmentInMemory seg;
            seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
            seg.set_row_id(query_vector_.size() - 1 + 1);
            for (const auto &column: top_k.top_k_) {
                seg.add_column(scalar_field(DataType::FLOAT64, column.name_), column.column_);
            }
            std::vector<Composite<ProcessingUnit>> output;
            output.emplace_back(ProcessingUnit{std::move(seg)});
            return output;
        }

        [[nodiscard]] const ClauseInfo &clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig &processing_config) {}


        [[nodiscard]] std::string to_string() const;
    };

}