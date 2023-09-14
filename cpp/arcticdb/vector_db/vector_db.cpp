/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/vector_db/vector_db.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/version/version_core.hpp>
#include "pipeline/frame_slice.hpp"
#include "pipeline/pipeline_context.hpp"

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/impl/io.h>
#include <faiss/index_io.h>
#include <faiss/IndexHNSW.h>


namespace arcticdb {
    using namespace arcticdb::version_store;

    stream::StreamSink::PartialKey atom_key_to_partial_key(const AtomKey &atom_key) {
        return stream::StreamSink::PartialKey{
                atom_key.type(),
                atom_key.version_id(),
                atom_key.id(),
                atom_key.start_index(),
                atom_key.end_index()
        };
    }

    AtomKey generate_query_bucket_to_insertion_bucket_key(const StreamId &stream_id) {
        return AtomKeyBuilder()
                .string_index("query_bucket_to_insertion_bucket")
                .build(stream_id, KeyType::NAMESPACE_INSERTION_BUCKETISER);
    }


    AtomKey generate_segment_vector_index_key(AtomKey atom_key) {
        // NB we need to identify segments with their respective centroids.
        // This is done by reusing start_index.
        // We could have assumed everything was in order but some segments will be empty.
        return AtomKeyBuilder()
                .version_id(atom_key.version_id())
                .creation_ts(atom_key.creation_ts())
                .start_index(atom_key.start_index())
                .end_index(atom_key.end_index())
                .content_hash(atom_key.content_hash())
                .build(atom_key.id(), KeyType::SEGMENT_VECTOR_INDEX);
    }

    AtomKey generate_vector_namespace_insertion_bucketiser_key(StreamId stream_id) {
        return atom_key_builder().build(stream_id, KeyType::NAMESPACE_INSERTION_BUCKETISER);
    } // completely unversioned; todo: version somehow? possibly via a training segment or something.

    AtomKey generate_vector_namespace_query_bucketiser_key(StreamId stream_id) {
        return atom_key_builder().build(stream_id, KeyType::NAMESPACE_QUERY_BUCKETISER);
    }

    SliceAndKey get_vector_sk_from_composite(Composite<ProcessingUnit> &comp) {
        // Reused substantially; this just centralises the check.
        // A similar check could be performed on the shape of processing units when vectors are stored in long columns.
        util::check(comp.values_.size() == 1 && std::holds_alternative<ProcessingUnit>(comp.values_[0]),
                    "In reading vectors from a vector namespace to a processing clause, expected a composite containing just one processing unit.");
        auto proc = std::get<ProcessingUnit>(comp.values_[0]);
        util::check(proc.data().size() == 1, "In reading vectors from a vector namespace to a processing clause, expected a composite containing just one processing unit containing just one slice and key.");
        return proc.data()[0];
    }

    struct TopK {
        uint64_t k_;
        std::set<NamedColumnSortedByDistance> top_k_;
        double furthest_distance_{std::numeric_limits<double>::infinity()};

        explicit TopK(uint64_t k) : k_(k) {}

        TopK() = delete;

        ARCTICDB_MOVE_COPY_DEFAULT(TopK);

        bool try_insert(double distance, std::shared_ptr<Column> column, std::string_view name) {
            return try_insert(NamedColumnSortedByDistance(distance, column, name));
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

    TopKClause::TopKClause(std::vector<double> query_vector, uint8_t k) : query_vector_(std::move(query_vector)),
                                                                          k_(k) {
        clause_info_.modifies_output_descriptor_ = true;
        clause_info_.requires_repartition_ = true;
        clause_info_.follows_original_columns_order_ = false;
        // There is a check to reorder the columns based on the original schema that we must disable.
        clause_info_.can_combine_with_column_selection_ = false;
    }

    std::optional<std::vector<Composite<ProcessingUnit>>>
    TopKClause::repartition(const std::shared_ptr<Store> &store, std::vector<Composite<ProcessingUnit>> &&c) const {
        auto comps = std::move(c);
        // This is a repartition method; so we expect to be given the top-k from each segment and want the overall top-k
        // which mostly involves the same logic.

        // Record, in top_k, the
        // - contents,
        // - column name, and
        // - distance from the query vector
        // of each vector in the segment. First, some boilerplate:
        TopK top_k(k_);
        for (auto &comp: comps) {
            auto slice_and_key = get_vector_sk_from_composite(comp);
            const std::vector<std::shared_ptr<Column>> &columns = slice_and_key.segment(store).columns();
            for (auto &&[idx, col]: folly::enumerate(columns)) {
                col->type().visit_tag(
                        [&top_k, &col = col, &slice_and_key, &store, idx = idx](auto type_desc_tag) {
                            using TDT = decltype(type_desc_tag);
                            using raw_type = typename TDT::DataTypeTag::raw_type;
                            // Second, check that we have a floating point type.
                            // Exact TopK shouldn't work otherwise. We raise an internal error on non-floats.
                            if constexpr (is_floating_point_type(TDT::DataTypeTag::data_type)) {
                                // TopK (as initialised) maintains the invariant that it contains the top k
                                // from the vectors already read, hence try_insert.
                                top_k.try_insert(col->template scalar_at<raw_type>(col->last_row()).value(),
                                                 col, slice_and_key.segment(store).field(idx).name());
                                // In each column, we store the distance from the query vector in the last row.
                                // So col->template scalar_at<raw_type>(col->last_row()).value() gets the
                                // distance.
                                //
                                // We have the top-k from each segment and we want the top-k in the entire
                                // symbol. We therefore run top-k over the top-k from each segment again to
                                // obtain the top-k overall.
                            } else {
                                internal::raise<ErrorCode::E_INVALID_ARGUMENT>(
                                        "Vectors should exclusively comprise floats; the Python layer should otherwise raise an error and prevent upsertion of vectors containing non-float components.");
                            }
                        });
            }
        }
        // Third, construct a segment and insert the results from top_k.
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


    Composite<ProcessingUnit> TopKClause::process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const {
        auto procs = std::move(p);
        TopK top_k(k_);
        // We expect a vector in each column.
        // top_k_ is updated as we read more vectors. We want to efficiently remove the 'worst' (least similar) vector
        // as we go along to ensure we have the top k, not the top k+1.
        auto lp = 2;
        // Let's pretend that only the lp-norms exist. We'll use p=2 for now as a default.

        // Read each vector and update top_k.
        // First, some boilerplate...
        auto slice_and_key = get_vector_sk_from_composite(procs);
        const std::vector<std::shared_ptr<Column>> &columns = slice_and_key.segment(store).columns();
        for (auto &&[idx, col]: folly::enumerate(columns)) {
            // We may as well check that the vectors read in aren't the wrong size.
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    static_cast<long unsigned>(col->row_count()) == this->query_vector_.size(),
                    "Expected vector of length {}, got vector of length {}.", col->row_count(),
                    this->query_vector_.size());
            col->type().visit_tag(
                    [&top_k, &slice_and_key, &store, &col = col, idx = idx, lp, this](auto type_desc_tag) {
                        using TypeDescriptorTag = decltype(type_desc_tag);
                        using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;
                        // If we don't have a floating point type we raise an internal error. Python should have
                        // checked already.
                        if constexpr (is_floating_point_type(TypeDescriptorTag::DataTypeTag::data_type)) {
                            ColumnData col_data = col->data();
                            // The lp norm is given by the sum the result of exponentiating each component
                            // with exponent p, all in turn exponentiated by 1/p. Since exponentiation is
                            // monotonic (for the values of p by which we are concerned), we do not need to
                            // perform the latter operation to sort. This allows us only to raise to the power
                            // of 1/p in the case of the top k, which saves on some computation.
                            double sum_differences_exponentiated = 0;
                            // We compare the jth components of the query vector and the vector being read.
                            // This has to be initialised outside the loop since we might have to read more than
                            // one block to read the whole vector.
                            auto j = 0u;
                            // We compare the jth element of the query vector with the jth element of the vector
                            // we are reading. We must initialise j outside the loop because we are reading the
                            // vector from the segment block-wise but query vector is unblocked. The segments'
                            // vectors may be long enough to fit in more than one block.
                            while (auto block = col_data.next<TypeDescriptorTag>()) {
                                auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                                for (auto progress_in_block = 0u; progress_in_block <
                                                                  block.value().row_count(); ++progress_in_block, ++j, ++ptr) {
                                    sum_differences_exponentiated += pow(abs(*ptr - this->query_vector_[j]),
                                                                         lp);
                                }
                            }
                            // See TopK for how this is implemented. We try to insert; if the distance is
                            // further than everything already in top_k we fail to insert.
                            top_k.try_insert(NamedColumnSortedByDistance(sum_differences_exponentiated, col,
                                                                         slice_and_key.segment(store).field(
                                                                                 idx).name()));
                        } else {
                            internal::raise<ErrorCode::E_INVALID_ARGUMENT>(
                                    "Vectors should exclusively comprise floats; the Python layer should otherwise raise an error and prevent upsertion of vectors containing non-float components.");
                        }
                    });
        }

        // By this time we've read each vector, keeping a running count of the top-k vectors closest to the query vector
        // of the vectors read so far. Now we write to a new segment.
        SegmentInMemory seg;
        seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        seg.set_row_id(query_vector_.size() - 1 + 1);
        // -1 because of zero-indexing, +1 because we want to add the distance on at the end.
        for (const auto &column: top_k.top_k_) {
            // todo: replace (mem)cpying of columns' contents with holding of pointers in top_k_ and addition of pointers to seg
            // this doesn't work presently because the columns can have >1 block.
            column.column_->type().visit_tag([&column, &seg, lp, this](auto type_desc_tag) {
                using TypeDescriptorTag = decltype(type_desc_tag);
                using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;
                // Another check; also perhaps useful for the compiler.
                if constexpr (is_floating_point_type(TypeDescriptorTag::DataTypeTag::data_type)) {
                    // We presize to query_vector_.size()+1 because we append the distance. (It has to go somewhere.)
                    auto write_col = std::make_shared<Column>(column.column_->type(), this->query_vector_.size() + 1,
                                                              true, false);
                    auto col_data = column.column_->data();
                    auto write_ptr = reinterpret_cast<RawType *>(write_col->ptr());
                    // We memcpy each component into the new column. This is quite inelegant but necessary because
                    // something breaks later if the column contains more than one block.
                    while (auto block = col_data.next<TypeDescriptorTag>()) {
                        auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                        std::memcpy(write_ptr, ptr, block.value().row_count() * sizeof(RawType));
                        write_ptr += block.value().row_count();
                    }
                    *write_ptr = RawType(pow(column.distance_, 1 / static_cast<double>(lp)));
                    write_col->set_row_data(this->query_vector_.size());
                    seg.add_column(scalar_field(DataType::FLOAT64, column.name_), write_col);
                } else {
                    internal::raise<ErrorCode::E_INVALID_ARGUMENT>(
                            "Vectors should exclusively comprise floats; the Python layer should otherwise raise an error and prevent upsertion of vectors containing non-float components.");
                }
            });
        }
        return Composite{ProcessingUnit{std::move(seg)}};
    }

    BucketiseVectorsClause::BucketiseVectorsClause(uint64_t dimensions, faiss::Index *bucketiser)
            : dimensions_(dimensions), bucketiser_(bucketiser) {
        clause_info_.modifies_output_descriptor_ = true;
        clause_info_.requires_repartition_ = true;
        clause_info_.follows_original_columns_order_ = false;
        clause_info_.can_combine_with_column_selection_ = false;
    }

    Composite<ProcessingUnit>
    BucketiseVectorsClause::process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const {
        // We take vectors in p and partition them into Voronoi cells.

        // Some initialisation.
        auto procs = std::move(p);
        // Each cell has its own SegmentInMemory:
        std::vector<SegmentInMemory> cells;
        // We may as well prepare the segments.
        for (auto &cell: cells) {
            cell.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
            cell.set_row_id(dimensions_ - 1); // zero-indexing.
        }
        // The bucketiser is another faiss index - in this case an exact one - on some centroids.
        // If the kth centroid is closest, we assign a vector to the kth Voronoi cell.
        // faiss indices give the number of vectors indexed through Index.ntotal.
        // We therefore have bucketiser_->ntotal cells.
        cells.resize(bucketiser_->ntotal);

        // We run a large faiss query on the whole segment.
        // First, we read the whole segment into a big vector.
        auto slice_and_key = get_vector_sk_from_composite(procs);
        std::vector<float> vectors;
        const auto &seg = slice_and_key.segment(store);
        const auto &columns = seg.columns();
        for (auto &&[idx, col]: folly::enumerate(columns)) {
            col->type().visit_tag([&vectors, &col = col](auto type_desc_tag) {
                using TDT = decltype(type_desc_tag);
                using RawType = typename TDT::DataTypeTag::raw_type;
                auto col_data = col->data();
                while (auto block = col_data.next<TDT>()) {
                    // todo: add type check for floats.
                    auto start = reinterpret_cast<const RawType *>(block.value().data());
                    auto end = start + block.value().row_count();
                    vectors.insert(vectors.end(), start, end);
                }
            });
        }
        // Second, we initialise some arrays for the faiss search to write to.
        // Bucketisation requires locating one centroid, so k = 1.
        // This is the most convenient time to partition the columns:
        // we have access to the column pointers.
        auto num_queries = vectors.size() / dimensions_;
        faiss::Index::idx_t *cells_to_move_to_arr = new faiss::Index::idx_t[num_queries];
        float *distances_arr = new float[num_queries];
        // Third, we query the index. k = 1 is hardcoded here.
        bucketiser_->search(num_queries, vectors.data(), 1, distances_arr, cells_to_move_to_arr);
        // Fourth, for convenience, we construct a vector from the arrays.
        std::vector<uint64_t> cells_to_move_to(cells_to_move_to_arr, cells_to_move_to_arr + num_queries);
        // Fifth, we iterate over the column pointers and put them in the right cell.
        for (auto &&[idx, col]: folly::enumerate(columns)) {
            auto f_r = seg.field(idx).ref();
            cells[cells_to_move_to[idx]].add_column(f_r, col);
        }

        // We construct a Composite<ProcessingUnit> containing each cell in order.
        // The composite is a singleton, and the ProcessingUnit contains SlicesAndKeys.
        std::vector<SliceAndKey> results_sk;
        for (auto &cell: cells) {
            results_sk.emplace_back(SliceAndKey(std::move(cell)));
        }
        return Composite{ProcessingUnit{std::move(results_sk)}};
    }

    std::optional<std::vector<Composite<ProcessingUnit>>>
    BucketiseVectorsClause::repartition(const std::shared_ptr<Store> &store,
                                      std::vector<Composite<ProcessingUnit>> &&c) const {
        auto comps = std::move(c);

        // Each Composite contains a ProcessingUnit containing k SliceAndKeys for k Voronoi cells.
        // Now we construct a vector of SegmentsInMemory merging each SliceAndKey corresponding to a particular cell.
        std::vector<SegmentInMemory> merged_cells;
        merged_cells.resize(bucketiser_->ntotal);
        for (auto &comp: comps) {
            // todo: check we have a singleton ProcUnit
            for (auto &&[cell_number, unmerged_cell]: folly::enumerate(
                    std::get<ProcessingUnit>(comp.values_[0]).data())) {
                auto unmerged_seg = unmerged_cell.segment(store);
                for (auto &&[col_number, col]: folly::enumerate(unmerged_seg.columns())) {
                    merged_cells[cell_number].add_column(unmerged_seg.field(col_number).ref(), col);
                }
            }
        }
        std::vector<SliceAndKey> results_sk;
        for (auto &merged_cell: merged_cells) {
            merged_cell.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
            merged_cell.set_row_id(dimensions_);
            results_sk.emplace_back(std::move(merged_cell));
        }
        std::vector<Composite<ProcessingUnit>> out;
        out.emplace_back(ProcessingUnit{std::move(results_sk)});
        return out;
    }

    IndexSegmentClause::IndexSegmentClause(
            StreamId stream_id,
            std::string index,
            std::string metric,
            uint64_t dimensions,
            VersionId version_id,
            UpdateInfo update_info) :
                                                                            stream_id_(stream_id),
                                                                            index_(index),
                                                                            metric_(metric),
                                                                            dimensions_(dimensions),
                                                                            version_id_(version_id),
                                                                            update_info_(update_info) {
        clause_info_.modifies_output_descriptor_ = true;
        clause_info_.follows_original_columns_order_ = false;
        clause_info_.can_combine_with_column_selection_ = false;
    }

    Composite<ProcessingUnit>
    IndexSegmentClause::process(std::shared_ptr<Store> store, Composite<ProcessingUnit> &&p) const {
        // We construct an index on each individual segment. By default it is an HNSW index.
        // todo: allow arbitrary indexing at this level.
        auto procs = std::move(p);
        faiss::IndexHNSWFlat index(dimensions_, 32);
        if (metric_ == "IP") {
            index.metric_arg = faiss::MetricType::METRIC_INNER_PRODUCT;
        }
        auto slice_and_key = get_vector_sk_from_composite(procs);
        // We need the AtomKey of (what should be) the sole SliceAndKey so we know where to write the index.
        AtomKey key_of_slice_and_key = slice_and_key.key();
        // Read the vectors; then
        uint64_t num_vecs = 0;
        std::vector<float> vectors;
        // todo: add check that we only have one slice and key.
        key_of_slice_and_key = slice_and_key.key();
        const std::vector<std::shared_ptr<Column>> &columns = slice_and_key.segment(store).columns();
        for (auto &&[idx, col]: folly::enumerate(columns)) {
            num_vecs += 1;
            col->type().visit_tag([&vectors, &col = col](auto type_desc_tag) {
                using TypeDescriptorTag = decltype(type_desc_tag);
                using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;

                if constexpr (is_floating_point_type(TypeDescriptorTag::DataTypeTag::data_type)) {
                    ColumnData col_data = col->data();
                    while (auto block = col_data.next<TypeDescriptorTag>()) {
                        auto start = reinterpret_cast<const RawType *>(block.value().data());
                        auto end = start + block.value().row_count();
                        vectors.insert(vectors.end(), start, end);
                    }
                }
            });
        }
        // second, add them to the index.
        index.add(num_vecs, vectors.data());

        // Write the index to Arctic.
        // First, initialise a VectorIOWriter, and serialise the index to writer.data.
        faiss::VectorIOWriter writer;
        faiss::write_index(&index, &writer);
        // Second, initialise a write column, and get a pointer to allow us to write to it.
        auto write_col = std::make_shared<Column>(make_scalar_type(DataType::UINT8), writer.data.size() + 1, true,
                                                  false);
        auto write_ptr = reinterpret_cast<uint8_t *>(write_col->ptr());
        // Third, memcpy from writer.data.data() (a pointer to the array underlying the vector containing the serialised index) to the column.
        std::memcpy(write_ptr, writer.data.data(), sizeof(uint8_t) * writer.data.size());
        // Fourth, provide some details about the column and segment containing it.
        SegmentInMemory seg;
        write_col->set_row_data(writer.data.size());
        seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        seg.descriptor().set_id(stream_id_);
        seg.set_row_id(writer.data.size());
        seg.add_column(scalar_field(DataType::UINT8, "serialised_index"), write_col);
        // Fifth, write.
        store->write_sync(generate_segment_vector_index_key(key_of_slice_and_key), std::move(seg));

        // Sixth, since we're shamelessly hijacking the code path for QueryBuilder, return an empty composite.
        // It will be ignored.
        return Composite{ProcessingUnit()};
    }
}

namespace arcticdb::version_store {
    void train_vector_namespace_bucketiser_impl(
            const std::shared_ptr<Store> &store,
            const UpdateInfo &update_info,
            const StreamId &stream_id,
            ARCTICDB_UNUSED const std::string metric,
            const uint64_t centroids,
            const std::optional<std::vector<float>> &training_vectors,
            const uint64_t dimensions
            ) {
        // We train a insertion_bucketiser for use in BucketiseVectorsClause (see comments there for explanation.)
        // todo: version.

        faiss::IndexFlat insertion_bucketiser;

        if (metric == "L2") {
            insertion_bucketiser = faiss::IndexFlatL2(dimensions);
        } else if (metric == "IP") {
            insertion_bucketiser = faiss::IndexFlatIP(dimensions);
        } else {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("VectorDB: metric other than IP or L2 given.");
        }
        if (training_vectors) {
            auto &vectors = training_vectors.value();
            if (vectors.size() % dimensions != 0) {
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("The number of floats in the training vectors provided does not match the dimension expected, and the Python internal checks failed to pick this up.");
            }
            auto num_vecs = vectors.size() / dimensions;
            faiss::Clustering cluster(dimensions, centroids);
            faiss::IndexFlatL2 cluster_index(dimensions);
            cluster.train(num_vecs, vectors.data(), cluster_index);
            insertion_bucketiser.add(centroids, cluster.centroids.data());
        }
            // When no training vectors are provided, we'll have to go and read some. Ideally the training set is random.
            // For now, let's just take the first segment.
            // todo: randomly sample.
            // todo: test this; I don't think it actually works.
        else {
            std::vector<float> vectors;
            // Read the first segment.
            auto read_pipeline_context = std::make_shared<PipelineContext>();
            read_pipeline_context->stream_id_ = stream_id;
            ReadQuery read_query;
            if (update_info.previous_index_key_.has_value()) {
                read_indexed_keys_to_pipeline(store, read_pipeline_context, *(update_info.previous_index_key_),
                                              read_query, ReadOptions{});
            }
            read_incompletes_to_pipeline(store, read_pipeline_context, read_query, ReadOptions{}, false, false, false);
            auto sk = read_pipeline_context->slice_and_keys_[0];
            sk.ensure_segment(store);
            // Put the first segment's contents into a big vector.
            const std::vector<std::shared_ptr<Column>> &columns = sk.segment(store).columns();
            auto num_vecs = 0;
            for (auto &col: columns) {
                num_vecs += 1;
                col->type().visit_tag([&vectors, &col = col](auto type_desc_tag) {
                    using TypeDescriptorTag = decltype(type_desc_tag);
                    using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;

                    if constexpr (is_floating_point_type(TypeDescriptorTag::DataTypeTag::data_type)) {
                        ColumnData col_data = col->data();
                        while (auto block = col_data.next<TypeDescriptorTag>()) {
                            auto start = reinterpret_cast<const RawType *>(block.value().data());
                            auto end = start + block.value().row_count();
                            vectors.insert(vectors.end(), start, end);
                        }
                    }
                });
            }
            num_vecs = vectors.size() / dimensions;
            insertion_bucketiser.train(num_vecs, vectors.data());
        }

        // Now we write the insertion_bucketiser.
        // First, get a VectorIOWriter containing a vector containing the serialised insertion_bucketiser.
        faiss::VectorIOWriter insertion_bucketiser_writer;
        faiss::write_index(&insertion_bucketiser, &insertion_bucketiser_writer);
        // Second, write it to a new Column.
        auto insertion_bucketiser_write_col = std::make_shared<Column>(make_scalar_type(DataType::UINT8), insertion_bucketiser_writer.data.size() + 1, true,
                                                                       false);
        auto insertion_bucketiser_write_ptr = reinterpret_cast<uint8_t *>(insertion_bucketiser_write_col->ptr());
        std::memcpy(insertion_bucketiser_write_ptr, insertion_bucketiser_writer.data.data(), sizeof(uint8_t) * insertion_bucketiser_writer.data.size());
        insertion_bucketiser_write_col->set_row_data(insertion_bucketiser_writer.data.size());
        // Third, put the column in a segment.
        SegmentInMemory insertion_bucketiser_seg;
        insertion_bucketiser_seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        insertion_bucketiser_seg.descriptor().set_id(stream_id);
        insertion_bucketiser_seg.set_row_id(insertion_bucketiser_writer.data.size());
        insertion_bucketiser_seg.add_column(scalar_field(DataType::UINT8, "insertion_bucketiser"), insertion_bucketiser_write_col);
        // Fourth, write inesrtion_bucketiser to Arctic.
        auto insertion_bucketiser_write_key = generate_vector_namespace_insertion_bucketiser_key(stream_id);
        storage::UpdateOpts update_opts;
        update_opts.upsert_ = true;
        store->update(insertion_bucketiser_write_key, std::move(insertion_bucketiser_seg), update_opts).get();

        // For reasons that wil
        faiss::IndexFlatL2 query_bucketiser(dimensions);
        faiss::VectorIOWriter query_bucketiser_writer;

        auto query_bucketiser_write_key = generate_vector_namespace_query_bucketiser_key(stream_id);
        faiss::write_index(&query_bucketiser, &query_bucketiser_writer);
        auto query_bucketiser_write_col = std::make_shared<Column>(make_scalar_type(DataType::UINT8), query_bucketiser_writer.data.size() + 1, true, false);
        auto query_bucketiser_write_ptr = reinterpret_cast<uint8_t *>(query_bucketiser_write_col->ptr());
        std::memcpy(query_bucketiser_write_ptr, query_bucketiser_writer.data.data(), sizeof(uint8_t) * query_bucketiser_writer.data.size());
        query_bucketiser_write_col->set_row_data(query_bucketiser_writer.data.size());
        SegmentInMemory query_bucketiser_seg;
        query_bucketiser_seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        query_bucketiser_seg.descriptor().set_id(stream_id);
        query_bucketiser_seg.set_row_id(query_bucketiser_writer.data.size());
        query_bucketiser_seg.add_column(scalar_field(DataType::UINT8, "query_bucketiser"), query_bucketiser_write_col);
        store->update(query_bucketiser_write_key, std::move(query_bucketiser_seg), update_opts).get();
    }


    VersionedItem bucketise_vector_namespace_impl(const std::shared_ptr<Store> &store, const StreamId &stream_id,
                                             const UpdateInfo &update_info, ARCTICDB_UNUSED const uint64_t dimensions) {
        // Read the insertion bucketiser from Arctic; cf. search_vectors_with_bucketiser_and_index_impl.
        // todo: separate reading of bucketiser into new function.
        auto insertion_bucketiser_seg = store->read_sync(generate_vector_namespace_insertion_bucketiser_key(stream_id)).second;
        util::check(insertion_bucketiser_seg.columns().size() == 1,
                    "Vector indices are stored as one insertion_bucketiser_block in one column. The segment read contained more than one column. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");
        util::check(insertion_bucketiser_seg.column(0).data().num_blocks() == 1,
                    "Vector indices are stored as one insertion_bucketiser_block in one column. The column read contained more than one insertion_bucketiser_block. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");
        auto insertion_bucketiser_block = insertion_bucketiser_seg.column(
        0).data().next<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>();
        auto insertion_bucketiser_ptr = reinterpret_cast<const uint8_t *>(insertion_bucketiser_block.value().data());
        faiss::VectorIOReader insertion_bucketiser_reader;
        insertion_bucketiser_reader.data.reserve(insertion_bucketiser_seg.row_count());
        insertion_bucketiser_reader.data.insert(insertion_bucketiser_reader.data.end(), insertion_bucketiser_ptr, insertion_bucketiser_ptr + insertion_bucketiser_seg.row_count());
        auto insertion_bucketiser = faiss::read_index(&insertion_bucketiser_reader);

        // Read the query bucketiser from Arctic. The query bucketiser maps to non-empty buckets from the insertion bucketiser.
        auto query_bucketiser_seg = store->read_sync(generate_vector_namespace_query_bucketiser_key(stream_id)).second;
        util::check(query_bucketiser_seg.columns().size() == 1,
                    "Vector indices are stored as one query_bucketiser_block in one column. The segment read contained more than one column. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");
        util::check(query_bucketiser_seg.column(0).data().num_blocks() == 1,
                    "Vector indices are stored as one query_bucketiser_block in one column. The column read contained more than one query_bucketiser_block. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");
        auto query_bucketiser_block = query_bucketiser_seg.column(
        0).data().next<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>();
        auto query_bucketiser_ptr = reinterpret_cast<const uint8_t *>(query_bucketiser_block.value().data());
        faiss::VectorIOReader query_bucketiser_reader;
        query_bucketiser_reader.data.reserve(query_bucketiser_seg.row_count());
        query_bucketiser_reader.data.insert(query_bucketiser_reader.data.end(), query_bucketiser_ptr, query_bucketiser_ptr + query_bucketiser_seg.row_count());
        auto query_bucketiser = faiss::read_index(&query_bucketiser_reader);

        // Use BucketiseVectorsClause to index the namespace.
        // First, generate a ReadQuery.
        auto read_pipeline_context = std::make_shared<PipelineContext>();
        read_pipeline_context->stream_id_ = stream_id;
        ReadQuery read_query;

        read_indexed_keys_to_pipeline(store, read_pipeline_context, *(update_info.previous_index_key_), read_query,
                                      ReadOptions{});
        read_query.clauses_.emplace_back(std::make_shared<Clause>(
                BucketiseVectorsClause(
                        dimensions,
                        insertion_bucketiser
                )
                ));
        // Second, process the query.
        auto segments = read_and_process(store, read_pipeline_context, read_query, ReadOptions{}, 0u);
        // todo: refactor when it is possible to spill intermediate results of computations to disk.

//        std::vector<std::pair<stream::StreamSink::PartialKey, SegmentInMemory>> key_segs;
        std::vector<FrameSlice> slices;

        auto write_pipeline_context = std::make_shared<PipelineContext>();
        write_pipeline_context->stream_id_ = stream_id;
        write_pipeline_context->version_id_ = update_info.next_version_id_;
        write_pipeline_context->norm_meta_ = read_pipeline_context->norm_meta_;
        write_pipeline_context->incompletes_after_ = std::nullopt;
        StreamDescriptor write_stream_descriptor;
        write_stream_descriptor.set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        std::shared_ptr<DeDupMap> de_dup_map;

        auto begin_col = 0;
        auto centroid_counter = 0;
        std::vector<uint64_t> query_bucket_to_insertion_bucket;
        query_bucket_to_insertion_bucket.reserve(segments.size());
        std::vector<folly::Future<VariantKey>> fut_vec;

        for (auto sk=segments.begin(); sk != segments.end(); ++sk, ++centroid_counter) {
            if (sk->segment(store).columns().size() > 0) {
                for (const Field& field: sk->segment(store).descriptor().fields()) {
                    write_stream_descriptor.fields().add_field(field.ref());
                }

                // Now we want to add to the query bucketiser since we have a non-empty segment.
                std::vector<float> centroid;
                centroid.reserve(dimensions);
                insertion_bucketiser->reconstruct(centroid_counter, centroid.data());
                query_bucketiser->add(1, centroid.data());
                query_bucket_to_insertion_bucket.emplace_back(centroid_counter);

                sk->slice().col_range.first = begin_col;
                sk->slice().col_range.second = begin_col + sk->segment(store).columns().size();
                begin_col += sk->segment(store).columns().size();
                slices.emplace_back(sk->slice());

                auto segment = std::move(sk->segment(store));
                segment.descriptor().set_id(stream_id);
                stream::StreamSink::PartialKey pk{
                        KeyType::TABLE_DATA,
                        write_pipeline_context->version_id_,
                        stream_id,
                        centroid_counter,
                        0
                };
//                key_segs.emplace_back(pk, std::move(segment));
                fut_vec.emplace_back(store->write(pk, std::move(segment)));
            }
        }
        write_pipeline_context->set_descriptor(write_stream_descriptor);

        // Write the query bucketiser back to Arctic.
        // First, get a VectorIOWriter containing a vector containing the serialised bucketiser.
        faiss::VectorIOWriter query_bucketiser_writer;
        faiss::write_index(query_bucketiser, &query_bucketiser_writer);
        // Second, write it to a new Column.
        auto query_bucketiser_write_col = std::make_shared<Column>(make_scalar_type(DataType::UINT8), query_bucketiser_writer.data.size() + 1, true,
                                                                   false);
        auto query_bucketiser_write_ptr = reinterpret_cast<uint8_t *>(query_bucketiser_write_col->ptr());
        std::memcpy(query_bucketiser_write_ptr, query_bucketiser_writer.data.data(), sizeof(uint8_t) * query_bucketiser_writer.data.size());
        query_bucketiser_write_col->set_row_data(query_bucketiser_writer.data.size());
        // Third, put the column in a segment.
        SegmentInMemory seg;
        seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        seg.descriptor().set_id(stream_id);
        seg.set_row_id(query_bucketiser_writer.data.size());
        seg.add_column(scalar_field(DataType::UINT8, "serialised_index"), query_bucketiser_write_col);

        auto query_bucketiser_write_pipeline_context = std::make_shared<pipelines::PipelineContext>();
        query_bucketiser_write_pipeline_context->stream_id_ = stream_id;
        query_bucketiser_write_pipeline_context->version_id_ = update_info.next_version_id_;
        query_bucketiser_write_pipeline_context->incompletes_after_ = std::nullopt;
        query_bucketiser_write_pipeline_context->rows_ = query_bucketiser_writer.data.size();
        auto norm_meta = std::make_shared<arcticc::pb2::descriptors_pb2::NormalizationMetadata>();
        query_bucketiser_write_pipeline_context->norm_meta_ = norm_meta;

        StreamDescriptor query_bucketiser_write_stream_descriptor;
        query_bucketiser_write_pipeline_context->set_descriptor(query_bucketiser_write_stream_descriptor);
        query_bucketiser_write_stream_descriptor.set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        query_bucketiser_write_stream_descriptor.add_scalar_field(DataType::UINT8, "serialised_index");

        auto query_bucketiser_write_key = generate_vector_namespace_query_bucketiser_key(stream_id);

        storage::UpdateOpts update_opts;
        update_opts.upsert_ = true;
        store->update(query_bucketiser_write_key, std::move(seg), update_opts).get();
        // The query bucketiser's results are with respect to the non-empty buckets, but the keys of buckets are numbered inclusive of the empty buckets, so we need to write the vector query_bucket_to_insertion_bucket.
        auto query_bucket_to_insertion_bucket_write_col = std::make_shared<Column>(make_scalar_type(DataType::UINT64), query_bucket_to_insertion_bucket.size()+1, true, false);
        auto query_bucket_to_insertion_bucket_write_ptr = reinterpret_cast<uint64_t *>(query_bucket_to_insertion_bucket_write_col->ptr());
        std::memcpy(query_bucket_to_insertion_bucket_write_ptr, query_bucket_to_insertion_bucket.data(), sizeof(uint64_t) * query_bucket_to_insertion_bucket.size());
        query_bucket_to_insertion_bucket_write_col->set_row_data(query_bucket_to_insertion_bucket.size());
        SegmentInMemory query_bucket_to_insertion_bucket_write_seg;
        query_bucket_to_insertion_bucket_write_seg.descriptor().set_index(IndexDescriptor(0, IndexDescriptor::ROWCOUNT));
        query_bucket_to_insertion_bucket_write_seg.descriptor().set_id(stream_id);
        query_bucket_to_insertion_bucket_write_seg.set_row_id(query_bucket_to_insertion_bucket.size());
        query_bucket_to_insertion_bucket_write_seg.add_column(scalar_field(DataType::UINT64, "query_bucket_to_insertion_bucket"), query_bucket_to_insertion_bucket_write_col);

        auto query_bucket_to_insertion_bucket_write_key = generate_query_bucket_to_insertion_bucket_key(stream_id);
        // todo: change keytype as misleading.
        store->update(query_bucket_to_insertion_bucket_write_key, std::move(query_bucket_to_insertion_bucket_write_seg), update_opts).get();

        // Write the segments back to Arctic.
        auto keys = folly::collect(fut_vec).get();
        auto vit = collate_and_write(
                store,
                write_pipeline_context,
                slices,
                keys,
                write_pipeline_context->incompletes_after(),
                std::nullopt
                );
        return vit;
    };

    void index_segment_vectors_impl(const std::shared_ptr<Store> &store,
                                    const UpdateInfo update_info,
                                    const StreamId &stream_id,
                                    const std::string vector_index,
                                    const std::string metric,
                                    const uint64_t dimensions) {
        auto read_pipeline_context = std::make_shared<PipelineContext>();
        read_pipeline_context->stream_id_ = stream_id;
        ReadQuery read_query;
        if (update_info.previous_index_key_.has_value()) {
            read_indexed_keys_to_pipeline(store, read_pipeline_context, *(update_info.previous_index_key_), read_query,
                                          ReadOptions{});
        }
        read_incompletes_to_pipeline(store, read_pipeline_context, read_query, ReadOptions{}, false, false, false);
        read_query.clauses_.emplace_back(std::make_shared<Clause>(IndexSegmentClause(
                stream_id, vector_index, metric, dimensions, update_info.next_version_id_, update_info)));
        arcticdb::version_store::read_and_process(store, read_pipeline_context, read_query,
                                                                  ReadOptions{}, 0u);
    }

    std::vector<std::string>
    search_vectors_with_bucketiser_and_index_impl(
            const std::shared_ptr<Store> &store,
            const UpdateInfo update_info,
            const StreamId &stream_id,
            const std::vector<float> query_vectors,
            const uint16_t k,
            const uint64_t nprobes,
            const uint64_t dimensions) {
        // todo: stop pretending we have only one query vector.

        // Use the quantiser to work out in which cells to look.
        // First, read the quantiser; cf. SearchSegmentWithIndexClause::process.
        // todo: parametrise nprobes.
        auto quantiser_key = generate_vector_namespace_query_bucketiser_key(stream_id);
        auto quantiser_seg = store->read_sync(quantiser_key).second;
        util::check(quantiser_seg.columns().size() == 1,
                    "Vector indices are stored as one block in one column. The segment read contained more than one column. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");
        util::check(quantiser_seg.column(0).data().num_blocks() == 1,
                    "Vector indices are stored as one block in one column. The column read contained more than one block. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");
        auto block = quantiser_seg.column(
                0).data().next<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>();
        // Second, construct a faiss::Index.
        auto ptr = reinterpret_cast<const uint8_t *>(block.value().data());
        faiss::VectorIOReader reader;
        reader.data.reserve(quantiser_seg.row_count());
        reader.data.insert(reader.data.end(), ptr, ptr + quantiser_seg.row_count());
        auto quantiser = faiss::read_index(&reader);
        // Third, construct the query to the index.
        auto num_queries = query_vectors.size() / dimensions;
        faiss::Index::idx_t *cells_arr = new faiss::Index::idx_t[nprobes * num_queries];
        float *distances_arr = new float[nprobes * num_queries];
        // Fourth, query.
        quantiser->search(num_queries, query_vectors.data(), nprobes, distances_arr, cells_arr);

        // To minimise IO we want to read the indices first. We need to get the keys for each segment since we can then
        // get the keys for the accompanying indices.
        // First, get the keys into read_pipeline_context.
        auto read_pipeline_context = std::make_shared<PipelineContext>();
        read_pipeline_context->stream_id_ = stream_id;
        ReadQuery read_query;
        util::check(update_info.previous_index_key_.has_value(), "Cannot search before vectors have been written.");
        read_indexed_keys_to_pipeline(store, read_pipeline_context, *(update_info.previous_index_key_), read_query,
                                      ReadOptions{});
        // Second, get only the keys we care about.
        std::vector<SliceAndKey> relevant_sks;
        for (uint64_t i = 0; i < nprobes * num_queries; i++) {
            relevant_sks.emplace_back(read_pipeline_context->slice_and_keys_[cells_arr[i]]);
        }
        // Third, generate the keys of the relevant segment index keys.
        std::vector<AtomKey> relevant_index_keys;
        for (auto &sk: relevant_sks) {
            relevant_index_keys.emplace_back(generate_segment_vector_index_key(sk.key()));
        }
        // Fourth, read each index.
        std::vector<faiss::Index *> relevant_vector_indices;
        std::vector<NumericIndex> indices;
        for (auto &vector_index_key: relevant_index_keys) {
            indices.emplace_back(std::get<NumericIndex>(vector_index_key.start_index()));
        }
        for (auto &vector_index_key: relevant_index_keys) {
            ARCTICDB_DEBUG(log::version(), vector_index_key);
            auto result = store->read_sync(vector_index_key);
            auto vector_index_seg = result.second;
            util::check(vector_index_seg.columns().size() == 1,
                        "Vector indices are stored as one block in one column. The segment read contained more than one column. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");
            util::check(vector_index_seg.column(0).data().num_blocks() == 1,
                        "Vector indices are stored as one block in one column. The column read contained more than one block. Manually changing the vectors in a symbol instead of using methods on VectorDB may have corrupted data.");

            auto vector_index_block = vector_index_seg.column(
                    0).data().next<TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim0>>>();
            auto vector_index_ptr = reinterpret_cast<const uint8_t *>(vector_index_block.value().data());
            faiss::VectorIOReader vector_index_reader;
            vector_index_reader.data.reserve(vector_index_seg.row_count());
            vector_index_reader.data.insert(vector_index_reader.data.begin(), vector_index_ptr, vector_index_ptr + vector_index_seg.row_count());
            relevant_vector_indices.emplace_back(faiss::read_index(&vector_index_reader));
        }

        // Now we query each index. Note that we haven't even read a segment of vectors yet! If the indices are small
        // enough, IO is minimised.
        // todo: Stop pretending we only have one query vector and make it efficient.
        // We submitted multiple query vectors to the bucketiser. This gives us a list of buckets. A bucket may fall
        // within the cells to be probed for multiple vectors. If so, we should search the bucket for each of those
        // vectors for efficiency. By the same token, we don't want to read the same cell associated with a segment
        // more than once. For now, for testing, we pretend only to have one query vector.
        std::set<std::tuple<float, int, faiss::Index::idx_t>> top_k;
            // distance, cell number, index
        auto furthest_distance = std::numeric_limits<float>::infinity();
        for (auto &&[seg_vector_index_number, seg_vector_index]: folly::enumerate(relevant_vector_indices)) {
            auto *seg_distances = new float[k*num_queries];
            auto *seg_results = new faiss::Index::idx_t[k*num_queries];
            seg_vector_index->search(num_queries, query_vectors.data(), k, seg_distances, seg_results);
            for (uint i = 0; i < k*num_queries; i++) {
                if (seg_distances[i] < furthest_distance) {
                    top_k.insert({seg_distances[i], seg_vector_index_number, seg_results[i]});
                    if (top_k.size() > k) {
                        top_k.erase(*top_k.rbegin());
                        furthest_distance = std::get<0>(*top_k.rbegin());
                    } else if (top_k.size() == k) {
                        furthest_distance = std::get<0>(*top_k.rbegin());
                    }
                }
            }
        }
        std::vector<std::string> output;
        for (auto& mem: top_k) {
            relevant_sks[std::get<1>(mem)].ensure_segment(store);
            output.emplace_back(std::string(relevant_sks[std::get<1>(mem)].segment(store).field(std::get<2>(mem)).name()));
        }
        return output;
    }

} // namespace arcticdb::version_store