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
        uint64_t _k;
        std::set<NamedColumnSortedByDistance> top_k_;
        double _furthest_distance{std::numeric_limits<double>::infinity()};

        explicit TopK(int k) : _k(k) {}

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
            if (col.distance_ < _furthest_distance || (col.distance_ == _furthest_distance && col < *top_k_.rbegin())) {
                top_k_.insert(col);
                inserted = true;
            }
            // Obviously if col.distance_ < _furthest_distance then we've got a closer vector so we insert it.
            // Where col.distance_ == _furthest_distance, the latter cannot be infinity, since in Python we require that
            // all vectors should be within the n-cube centred about the origin whose long diagonal has length
            // std::numeric_limits<double>::max(). Therefore, _furthest_distance is finite. Therefore, it's been
            // altered, and so we have at least member of top_k_. So top_k_ is nonempty and we can compare
            // col < *top_k_.rbegin().
            if (top_k_.size() > _k) {
                top_k_.erase(*top_k_.rbegin());
                _furthest_distance = top_k_.rbegin()->distance_;
            } else if (inserted && top_k_.size() == _k) {
                _furthest_distance = top_k_.rbegin()->distance_;
            }
            // There are three cases here.
            // - top_k.size() < k. Then we don't need to erase anything or refresh the furthest distance. When there are
            //   fewer than k elements in top_k_, we want to add any new vector we see; we may remove it later of course.
            // - top_k_.size() is exactly k. Then we don't need to erase anything. But we might have just inserted
            //   something so we need to reload the _furthest_distance if we inserted something.
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

        [[nodiscard]] Composite<ProcessingSegment> process(
                std::shared_ptr<Store> store,
                Composite<ProcessingSegment> &&p
        ) const;

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingSegment>>> repartition(
                ARCTICDB_UNUSED std::vector<Composite<ProcessingSegment>> &&) const {
            return std::nullopt;
        }

        [[nodiscard]] const ClauseInfo &clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig &processing_config) {}


        [[nodiscard]] std::string to_string() const;
    };

}