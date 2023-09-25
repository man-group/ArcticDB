/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>
#include <faiss/Index.h>

namespace arcticdb::vector_db {
    VersionedItem initialise_bucket_index_impl(
            const std::shared_ptr<Store>& store,
            const StreamId& stream_id,
            const std::string& index,
            const std::string& metric,
            const uint64_t dimension,
            const std::optional<std::vector<float>>& vectors = std::nullopt,
            const std::optional<std::vector<faiss::Index::idx_t>>& labels = std::nullopt
    );

    VersionedItem update_bucket_index_impl(
            const std::shared_ptr<Store> store,
            const StreamId& stream_id,
            const version_store::UpdateInfo& update_info,
            std::vector<float> vectors,
            std::vector<faiss::Index::idx_t> labels
    );

    std::pair<std::vector<faiss::Index::idx_t>, std::vector<float>> search_bucket_with_index_impl(
            const std::shared_ptr<Store> store,
            const VersionedItem& version_info,
            const std::vector<float> vectors,
            const uint64_t k
    );

    struct UpdateBucketIndexClause {
        ClauseInfo clause_info_;
        std::vector<float> vectors_;
        std::vector<faiss::Index::idx_t> labels_;

        UpdateBucketIndexClause() = delete;

        UpdateBucketIndexClause(std::vector<float> &&vectors, std::vector<faiss::Index::idx_t> &&labels);

        [[nodiscard]] Composite<ProcessingUnit> process(
                std::shared_ptr<Store> store,
                Composite<ProcessingUnit> &&p
                ) const;

        [[nodiscard]] std::vector<Composite<SliceAndKey>> structure_for_processing(
                std::vector<SliceAndKey> &slice_and_keys,
                ARCTICDB_UNUSED size_t) const {
            return structure_by_column_slice(slice_and_keys);
            // NB we expect one very long column, and this is the easiest way of getting the whole thing in one go.
            // We need the whole thing in one go so that we can read the whole index before adding to it.
        }

        [[nodiscard]] std::optional<std::vector<Composite<ProcessingUnit>>> repartition(
                ARCTICDB_UNUSED std::vector<Composite<ProcessingUnit>> &&) const {
            return std::nullopt;
        }

        [[nodiscard]] const ClauseInfo& clause_info() const {
            return clause_info_;
        }

        void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig&) {};
    };

}