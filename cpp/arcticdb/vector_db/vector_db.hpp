/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/storage/store.hpp>
#include <storage/library.hpp>
#include <version/version_store_objects.hpp>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexFlat.h>

namespace arcticdb {
    using namespace arcticdb::version_store; // we need to refer to UpdateInfo

    struct NamedColumnSortedByDistance {
        double distance_;
        std::shared_ptr<Column> column_;
        std::string_view name_;

        explicit NamedColumnSortedByDistance(double distance, std::shared_ptr<Column> column, std::string_view name)
                : distance_(distance), column_(column), name_(name) {}

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

    struct TopKClause {
        ClauseInfo clause_info_;
        std::vector<double> query_vector_;
        uint64_t k_;

        TopKClause() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(TopKClause);

        TopKClause(std::vector<double> query_vector, uint8_t k);

        [[nodiscard]] Composite<ProcessingUnit>
        process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const;

        [[nodiscard]] std::vector<Composite<SliceAndKey>>
        structure_for_processing(std::vector<SliceAndKey> &slice_and_keys, ARCTICDB_UNUSED size_t) const {
            return structure_by_column_slice(slice_and_keys);
        }

        std::optional<std::vector<Composite<ProcessingUnit>>>
        repartition(const std::shared_ptr<Store> &store, std::vector<Composite<ProcessingUnit>> &&c) const;

        [[nodiscard]] const ClauseInfo &clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig &processing_config) {}


        [[nodiscard]] std::string to_string() const;
    };


    struct BucketiseVectorsClause {
        ClauseInfo clause_info_;
        uint64_t dimensions_;
        faiss::Index *bucketiser_;

        BucketiseVectorsClause() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(BucketiseVectorsClause);

        BucketiseVectorsClause(uint64_t dimensions, faiss::Index *bucketiser);

        [[nodiscard]] Composite<ProcessingUnit>
        process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const;

        [[nodiscard]] std::vector<Composite<SliceAndKey>>
        structure_for_processing(std::vector<SliceAndKey> &slice_and_keys, ARCTICDB_UNUSED size_t) const {
            return structure_by_column_slice(slice_and_keys);
        }

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>>
        repartition(const std::shared_ptr<Store> &store, std::vector<Composite<ProcessingUnit>> &&c) const;

        [[nodiscard]] const ClauseInfo &clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig &processing_config) {}

        [[nodiscard]] std::string to_string() const;
    };

    struct IndexSegmentClause {
        ClauseInfo clause_info_;
        StreamId stream_id_;
        std::string index_;
        std::string metric_;
        uint64_t dimensions_;
        VersionId version_id_;
        UpdateInfo update_info_;

        IndexSegmentClause() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(IndexSegmentClause);

        IndexSegmentClause(
                StreamId stream_id,
                std::string index,
                std::string metric,
                uint64_t dimensions,
                VersionId version_id,
                UpdateInfo update_info);

        [[nodiscard]] Composite<ProcessingUnit>
        process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const;

        [[nodiscard]] std::vector<Composite<SliceAndKey>>
        structure_for_processing(std::vector<SliceAndKey> &slice_and_keys, ARCTICDB_UNUSED size_t) const {
            return structure_by_column_slice(slice_and_keys);
        }

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>>
        repartition(ARCTICDB_UNUSED const std::shared_ptr<Store> &,
                    ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
            return std::nullopt;
        }

        [[nodiscard]] const ClauseInfo &clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig &processing_config) {}

        [[nodiscard]] std::string to_string() const;
    };

    struct SearchSegmentWithIndexClause {
        ClauseInfo clause_info_;
        std::vector<float> query_vectors_;
        uint16_t k_;
        std::string vector_index_;
        uint64_t dimensions_;
        StreamId stream_id_;
        VersionId version_id_;
        UpdateInfo update_info_;
        std::shared_ptr<VersionMap> version_map_;

        SearchSegmentWithIndexClause() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(SearchSegmentWithIndexClause);

        SearchSegmentWithIndexClause(std::vector<float> query_vectors, uint16_t k, std::string vector_index,
                                     uint64_t dimensions, StreamId stream_id, VersionId version_id,
                                     UpdateInfo update_info, std::shared_ptr<VersionMap> version_map);

        [[nodiscard]] Composite<ProcessingUnit>
        process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const;

        [[nodiscard]] std::vector<Composite<SliceAndKey>>
        structure_for_processing(std::vector<SliceAndKey> &slice_and_keys, ARCTICDB_UNUSED size_t) const {
            return structure_by_column_slice(slice_and_keys);
        }

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>>
        repartition(ARCTICDB_UNUSED const std::shared_ptr<Store> &,
                    ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
            return std::nullopt;
        }

        [[nodiscard]] const ClauseInfo &clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig &processing_config) {}

        [[nodiscard]] std::string to_string() const;
    };
} // namespace arcticdb

namespace arcticdb::version_store {
    void index_segment_vectors_impl(const std::shared_ptr<Store> &store,
                              const UpdateInfo update_info,
                              const StreamId &stream_id,
                              const std::string vector_index,
                              const std::string metric,
                              const uint64_t dimensions);

    std::vector<std::string>
    search_vectors_with_bucketiser_and_index_impl(
            const std::shared_ptr<Store> &store,
            const UpdateInfo update_info,
            const StreamId &stream_id,
            const std::vector<float> query_vectors,
            const uint16_t k,
            const uint64_t nprobes,
            const uint64_t dimensions);

    void train_vector_namespace_bucketiser_impl(
            const std::shared_ptr<Store> &store,
            const UpdateInfo &update_info,
            const StreamId &stream_id,
            const std::string metric,
            const uint64_t centroids,
            const std::optional<std::vector<float>> &training_vectors,
            const uint64_t dimensions
    );

    VersionedItem bucketise_vector_namespace_impl(const std::shared_ptr<Store> &store, const StreamId &stream_id,
                                             const UpdateInfo &update_info, ARCTICDB_UNUSED const uint64_t dimensions);

} // namespace arcticdb::version_store