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

        std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
                const std::shared_ptr<Store>& store,
                std::vector<Composite<ProcessingUnit>> &&c) const;

        [[nodiscard]] const ClauseInfo &clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig &processing_config) {}


        [[nodiscard]] std::string to_string() const;
    };

}