/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/vector_db/vector_db.hpp>
#include <faiss/impl/io.h>
#include <faiss/index_io.h>
#include <faiss/index_factory.h>
#include <faiss/utils/distances.h>
#include <folly/futures/Future.h>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/async/base_task.hpp>

/**
 * @namespace arcticdb::vector_db
 *
 * The purpose of PyVectorDB is mostly to work out how fast Arctic could be as a vector database, without much regard
 * for the niceties of where things should actually sit with regard to the Python/C++ division of labour. The principal
 * reason for putting anything in C++ is that it avoids having to transfer the data through Pybind to Python.
 *
 * The main I/O-heavy operations are those of reading and writing faiss indices. Everything else should not take much
 * time, because we rarely want to read many vectors, and when upserting vectors, they have to originate in Python.
 * Three sorts of functionality therefore are implemented in this namespace. Notably, the indices that will grow to a
 * large size are mostly for individual buckets; the bucketiser should not be very big, and in any case there is only
 * one bucketiser. Therefore, we are only concerned here with bucket indices.
 *
 * 1. We initialise bucket indices.
 * 2. We update bucket indices.
 * 3. We read from bucket indices.
 */

namespace arcticdb::vector_db {
    /**
     * Matches metric in string with supported faiss metrics.
     * @param metric the metric given as a string; must be "L2" or "IP"..
     * @return faiss::MetricType
     */
    faiss::MetricType match_metric(const std::string& metric) {
        if (metric == "L2") { return faiss::METRIC_L2; }
        else if (metric == "IP") { return faiss::METRIC_INNER_PRODUCT; }
        else { util::raise_rte("Metrics other than L2 and inner product ['IP'] are not supported."); }
    }

    /**
     * Initialises an index on a bucket.
     * @param store
     * @param stream_id the bucket name.
     * @param index must be a valid index factory string:
     * https://github.com/facebookresearch/faiss/wiki/The-index-factory
     * @param metric must be "L2" or "IP".
     * @param dimension the number of dimensions to be initialised.
     * @param vectors optionally provided training vectors. Faiss indices that require training must be
     * initialised with training vectors.
     * Returns a VersionedItem to write to the version map of the symbol.
     */
    VersionedItem initialise_bucket_index_impl(
            const std::shared_ptr<Store>& store,
            const StreamId& stream_id,
            const std::string& index,
            const std::string& metric,
            const uint64_t dimension,
            const std::optional<std::vector<float>>& vectors,
            const std::optional<std::vector<faiss::Index::idx_t>>& labels
            ) {
        using namespace arcticdb::pipelines;
        using namespace arcticdb::version_store;

        // Initialise the index.
        auto bucket_index = faiss::index_factory(dimension, index.c_str(), match_metric(metric));
        if (vectors && labels) {
            util::check(vectors.value().size() % dimension == 0, "PyVectorDB: Training vectors of wrong size.");
            util::check(vectors.value().size() / dimension == labels.value().size(), "PyVectorDB: Label-vector size mismatch.");
            if (!bucket_index->is_trained) {
                bucket_index->train(std::max<uint64_t>(vectors.value().size() / dimension,5000), vectors.value().data());
            }
            bucket_index->add_with_ids(vectors.value().size() / dimension, vectors.value().data(), labels.value().data());
        }
        util::check(
                bucket_index->is_trained,
                "The faiss index {} requires training vectors and labels, which weren't provided.", index
                );

        // Write the index to stream_id.
        // First, write the index to a column.
        faiss::VectorIOWriter bucket_index_writer;
        faiss::write_index(bucket_index, &bucket_index_writer);
        auto col = std::make_shared<Column>(
                make_scalar_type(DataType::UINT8),
                bucket_index_writer.data.size()+1,
                true,
                false
                );
        auto col_ptr = reinterpret_cast<uint8_t *>(col->ptr());
        std::memcpy(col_ptr, bucket_index_writer.data.data(), sizeof(uint8_t)*bucket_index_writer.data.size());
        col->set_row_data(bucket_index_writer.data.size()-1);
        // Second, write it to a segment.
        SegmentInMemory seg;
        seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        seg.descriptor().set_id(stream_id);
        seg.set_row_id(bucket_index_writer.data.size()-1);
        seg.add_column(scalar_field(DataType::UINT8, index), col);
        // Third, prepare to write the segment to Arctic.
        stream::StreamSink::PartialKey pk{
            KeyType::TABLE_DATA,
            0, // We should only call this method when there's nothing in the symbol.
            stream_id,
            0,
            0
        };
        // Fourth, prepare to write the segment to the version map.
        FrameSlice slice{
                ColRange{0,1},
                RowRange{0, bucket_index_writer.data.size()}
        };
        auto write_pipeline_context = std::make_shared<PipelineContext>();
        write_pipeline_context->stream_id_ = stream_id;
        write_pipeline_context->version_id_ = 0;
        auto norm_meta = std::make_shared<arcticc::pb2::descriptors_pb2::NormalizationMetadata>();
        write_pipeline_context->norm_meta_ = norm_meta;
        write_pipeline_context->incompletes_after_ = std::nullopt;
        StreamDescriptor write_stream_descriptor;
        write_stream_descriptor.set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        write_stream_descriptor.fields().add_field(seg.field(0).ref());
        write_pipeline_context->set_descriptor(write_stream_descriptor);
        // Fifth, write the segment to Arctic, and return a versioned item to write to the version map.
        auto key = std::get<AtomKey>(std::move(store->write(pk, std::move(seg))).get());
        return collate_and_write(
                store,
                write_pipeline_context,
                {slice},
                {key},
                0u,
                std::nullopt
                );
    }

    /**
     * Updates a bucket index on vectors given.
     * @param store
     * @param version_info
     * @param update_info
     * @param vectors
     */
    VersionedItem update_bucket_index_impl(
            const std::shared_ptr<Store> store,
            const StreamId& stream_id,
            const version_store::UpdateInfo& update_info,
            std::vector<float> vectors,
            std::vector<faiss::Index::idx_t> labels
    ) {
        using namespace arcticdb::pipelines;
        using namespace arcticdb::version_store;
        auto read_pipeline_context = std::make_shared<PipelineContext>();

        read_pipeline_context->stream_id_ = stream_id;

        ReadQuery read_query;
        ReadOptions read_options;
        read_indexed_keys_to_pipeline(store, read_pipeline_context, *(update_info.previous_index_key_), read_query, read_options);

        read_query.clauses_.emplace_back(std::make_shared<Clause>(
                    UpdateBucketIndexClause{std::move(vectors), std::move(labels)})
                );

        auto sks = read_and_process(store, read_pipeline_context, read_query, read_options, 0u);
        util::check(sks.size() == 1, " VectorDB: Indexing a bucket should give exactly one segment, but didn't.");

        auto sk = sks[0];
        sk.segment(store).descriptor().set_id(stream_id);
        stream::StreamSink::PartialKey pk{
                KeyType::TABLE_DATA,
                update_info.next_version_id_,
                stream_id,
                0,
                0
        };
        std::vector<folly::Future<VariantKey>> fut_vec;
        fut_vec.emplace_back(store->write(pk, std::move(sk.segment(store))));

        auto write_pipeline_context = std::make_shared<PipelineContext>();
        write_pipeline_context->stream_id_ = stream_id;
        write_pipeline_context->version_id_ = update_info.next_version_id_;
        write_pipeline_context->norm_meta_ = read_pipeline_context->norm_meta_;
        write_pipeline_context->incompletes_after_ = std::nullopt;
        StreamDescriptor write_stream_descriptor;
        write_stream_descriptor.set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        write_pipeline_context->set_descriptor(write_stream_descriptor);
        auto keys = folly::collect(fut_vec).get();
        return collate_and_write(
                store,
                write_pipeline_context,
                {sk.slice()},
                keys,
                write_pipeline_context->incompletes_after(),
                std::nullopt
                );
    }

    std::pair<std::vector<faiss::Index::idx_t>, std::vector<float>> search_bucket_without_index_impl(
            const std::shared_ptr<Store> store,
            const VersionedItem& version_info_bucket_vectors,
            const VersionedItem& version_info_bucket_label_map,
            const std::vector<float> query_vectors,
            const uint64_t k,
            const uint64_t dimensions
            ) {
        using namespace arcticdb::pipelines;
        using namespace arcticdb::version_store;
        // Put all the vectors in one big std::vector<float> for faiss to query.
        auto bucket_vectors_pipeline_context = std::make_shared<PipelineContext>();
        bucket_vectors_pipeline_context->stream_id_ = version_info_bucket_vectors.key_.id();
        ReadQuery read_query;
        ReadOptions read_options;
        read_indexed_keys_to_pipeline(store, bucket_vectors_pipeline_context, version_info_bucket_vectors, read_query, read_options);
        auto key = bucket_vectors_pipeline_context->slice_and_keys_[0].key();
        std::vector<float> bucket_vectors;

        for (auto& sk: bucket_vectors_pipeline_context->slice_and_keys_) {
            auto seg = sk.segment(store);
            util::check(seg.columns().size() == 1, "PyVectorDB: Expected bucket index in one column.");
            auto data = seg.column(0).data();
            while (auto block = data.next<TypeDescriptorTag<DataTypeTag<DataType::FLOAT32>, DimensionTag<Dimension::Dim0>>>()) {
                auto start = reinterpret_cast<const float *>(block.value().data());
                auto end = start + block.value().row_count();
                bucket_vectors.insert(bucket_vectors.end(), start, end);
            }
        }

        util::check(bucket_vectors.size() % dimensions == 0, "PyVectorDB: dimensionality off in bucket vectors in searching bucket sans index.");
        util::check(query_vectors.size() % dimensions == 0, "PyVectorDB: dimensionality off in searching bucket sans index in query vector.");
        auto num_queried = bucket_vectors.size() / dimensions;
        auto num_queries = query_vectors.size() / dimensions;
        auto *distances = new float[k*num_queries];
        auto *labels = new faiss::Index::idx_t[k*num_queries];
        faiss::knn_L2sqr(
                query_vectors.data(),
                bucket_vectors.data(),
                dimensions,
                num_queries,
                num_queried,
                k,
                distances,
                labels
                );
        // Now we have the distances and labels. But we need to get the label map.
        auto label_map_pipeline_context = std::make_shared<PipelineContext>();
        label_map_pipeline_context->stream_id_ = version_info_bucket_label_map.key_.id();
        read_indexed_keys_to_pipeline(store, label_map_pipeline_context, version_info_bucket_label_map, read_query, read_options);

        std::vector<float> label_map;

        for (auto& sk: label_map_pipeline_context->slice_and_keys_) {
            auto seg = sk.segment(store);
            util::check(seg.columns().size() == 1, "PyVectorDB: Expected bucket index in one column.");
            auto data = seg.column(0).data();
            while (auto block = data.next<TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension::Dim0>>>()) {
                auto start = reinterpret_cast<const uint64_t *>(block.value().data());
                auto end = start + block.value().row_count();
                label_map.insert(label_map.end(), start, end);
            }
        }

        auto labels_vector = std::vector<faiss::Index::idx_t>(labels, labels+(num_queries*k));
        std::vector<faiss::Index::idx_t> mapped_labels;
        for (auto unmapped_label: labels_vector) {
            mapped_labels.push_back(label_map[unmapped_label]);
        }

        return std::pair<std::vector<faiss::Index::idx_t>, std::vector<float>>(
                mapped_labels,
                std::vector<float>(distances, distances+(num_queries*k))
                );
    }

    std::pair<std::vector<faiss::Index::idx_t>, std::vector<float>> search_bucket_with_index_impl(
            const std::shared_ptr<Store> store,
            const VersionedItem& version_info,
            const std::vector<float> vectors,
            const uint64_t k
            ) {
        using namespace arcticdb::pipelines;
        using namespace arcticdb::version_store;

        // Read the faiss index.
        auto read_pipeline_context = std::make_shared<PipelineContext>();
        read_pipeline_context->stream_id_ = version_info.key_.id();
        ReadQuery read_query;
        ReadOptions read_options;
        read_indexed_keys_to_pipeline(store, read_pipeline_context, version_info, read_query, read_options);
        util::check(
                read_pipeline_context->slice_and_keys_.size() == 1,
                "PyVectorDB: Expected one slice and key from bucket index."
                );
        auto key = read_pipeline_context->slice_and_keys_[0].key();
        auto seg = store->read_sync(key).second;
        util::check(seg.columns().size() == 1, "PyVectorDB: Expected bucket index in one column.");
        auto data = seg.column(0).data();
        util::check(data.num_blocks() == 1, "PyVectorDB: Expected serialised index to be in one block.");
        auto index_ptr = reinterpret_cast<const uint8_t *>(data.next<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>().value().data());
        faiss::VectorIOReader index_reader;
        index_reader.data.insert(index_reader.data.end(), index_ptr, index_ptr + seg.row_count());
        // todo: Can we unset the segment to avoid using too much memory here?
        auto index = faiss::read_index(&index_reader);

        // Query.
        util::check(vectors.size() % index->d == 0, "PyVectorDB: Dimensionality off in query vectors.");
        auto num_queries = vectors.size() / index->d;
        auto *indices = new faiss::Index::idx_t[k*num_queries];
        auto *distances = new float[k*num_queries];
        index->search(num_queries, vectors.data(), k, distances, indices);

        return std::pair<std::vector<faiss::Index::idx_t>, std::vector<float>>(
                std::vector<faiss::Index::idx_t>(indices, indices+(num_queries*k)),
                std::vector<float>(distances, distances+(num_queries*k))
                );
    }

    UpdateBucketIndexClause::UpdateBucketIndexClause(
            std::vector<float> &&vectors,
            std::vector<faiss::Index::idx_t> &&labels) :
            vectors_(std::move(vectors)),
            labels_(std::move(labels)) {
    }

    Composite<ProcessingUnit> UpdateBucketIndexClause::process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const {
        auto procs = std::move(p);

        // First, get a pointer to the block containing the serialised index.
        util::check(
                procs.values_.size() == 1
                    && std::holds_alternative<ProcessingUnit>(procs.values_[0]),
                "PyVectorDB: Expected serialised index to be in a single processing unit."
                );
        auto proc = std::get<ProcessingUnit>(procs.values_[0]);
        util::check(proc.data().size() == 1, "PyVectorDB: Expected serialised index to be in one slice "
                                             "and key.");
        auto sk = proc.data()[0];
        util::check(sk.segment(store).columns().size() == 1, "PyVectorDB: Expected serialised index to be in one "
                                                             "column.");
        auto data = sk.segment(store).column(0).data();
        util::check(data.num_blocks() == 1, "PyVectorDB: Expected serialised index to be in one block.");
        auto index_ptr = reinterpret_cast<const uint8_t *>(data.next<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>().value().data());

        // Second, initialise a faiss index.
        faiss::VectorIOReader index_reader;
        index_reader.data.insert(index_reader.data.end(), index_ptr, index_ptr + sk.slice().row_range.diff());
        // todo: Can we unset the segment to avoid using too much memory here?
        auto index = faiss::read_index(&index_reader);

        // Third, insert the vectors into the index.
        util::check(vectors_.size() % index->d == 0, "PyVectorDB: upserted vectors dimensionally wrong.");
        index->add_with_ids(vectors_.size() / index->d, vectors_.data(), labels_.data());

        // Fourth, return the index.
        faiss::VectorIOWriter index_writer;
        faiss::write_index(index, &index_writer);
        auto write_col = std::make_shared<Column>(
                make_scalar_type(DataType::UINT8),
                index_writer.data.size()+1,
                true,
                false);
        auto write_ptr = reinterpret_cast<uint8_t *>(write_col->ptr());
        std::memcpy(write_ptr, index_writer.data.data(), sizeof(uint8_t) * index_writer.data.size());
        write_col->set_row_data(index_writer.data.size()-1);
        SegmentInMemory seg;
        seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        seg.set_row_id(index_writer.data.size()-1);
        seg.add_column(scalar_field(DataType::UINT8, "serialised_index"), write_col);

        return Composite{ProcessingUnit{std::move(seg)}};
    }

    struct SearchBucketWithoutIndexTask : async::BaseTask {
        std::shared_ptr<Store> store_;
        VersionedItem vectors_version_info_;
        VersionedItem label_map_version_info_;
        std::vector<float> vectors_;
        uint64_t k_;
        uint64_t dimensions_;

        SearchBucketWithoutIndexTask(
                std::shared_ptr<Store> store,
                VersionedItem vectors_version_info,
                VersionedItem label_map_version_info,
                std::vector<float> vectors,
                uint64_t k,
                uint64_t dimensions
        ) :
                store_(store),
                vectors_version_info_(std::move(vectors_version_info)),
                label_map_version_info_(std::move(label_map_version_info)),
                vectors_(std::move(vectors)),
                k_(k),
                dimensions_(dimensions) {
            ARCTICDB_DEBUG(log::schedule(), "Creating task to search bucket without vectors.");
        }

        std::pair<std::vector<faiss::Index::idx_t>, std::vector<float>> operator()() {
            return vector_db::search_bucket_without_index_impl(
                    store_,
                    vectors_version_info_,
                    label_map_version_info_,
                    vectors_,
                    k_,
                    dimensions_
            );
        }
    };

    folly::Future<std::pair<std::vector<faiss::Index::idx_t>, std::vector<float>>> async_search_bucket_without_index_impl(
            const std::shared_ptr<Store> store,
            const VersionedItem& version_info_bucket_vectors,
            const VersionedItem& version_info_bucket_label_map,
            const std::vector<float> query_vectors,
            const uint64_t k,
            const uint64_t dimensions
    ) {
        return async::submit_cpu_task(SearchBucketWithoutIndexTask{
                store,
                version_info_bucket_vectors,
                version_info_bucket_label_map,
                query_vectors,
                k,
                dimensions
        });
    }

} // namespace arcticdb