/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/vector_db/vector_db.hpp>

namespace arcticdb {
    TopKClause::TopKClause(std::vector<double> query_vector, uint8_t k) :
            query_vector_(std::move(query_vector)),
            k_(k) {
        clause_info_.modifies_output_descriptor_ = true;
        clause_info_.follows_original_columns_order_ = false;
        clause_info_.can_combine_with_column_selection_ = false;
    }

    Composite<ProcessingSegment> TopKClause::process(std::shared_ptr<Store> store,
                                                     Composite<ProcessingSegment> &&p) const {
        auto procs = std::move(p);
        TopK top_k(k_);
        // We expect a vector in each column.
        // top_k_ is updated as we read more vectors. We want to efficiently remove
        // the 'worst' (least similar) vector as we go along to ensure we have the
        // top k, not the top k+1.
        auto lp = 2;
        // Let's pretend that only the lp-norms exist. We'll use p=2 for now as a default.

        procs.broadcast(
                [&store, &top_k, lp, this](const ProcessingSegment &proc) {
                    for (const auto& slice_and_key: proc.data()) {
                        const std::vector<std::shared_ptr<Column>>& columns = slice_and_key.segment(store).columns();
                        for (auto&& [idx, col]: folly::enumerate(columns)) {
                            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                                    static_cast<long unsigned>(col->row_count()) == this->query_vector_.size(),
                                    "Expected vector of length {}, got vector of length {}.",
                                    col->row_count(),
                                    this->query_vector_.size());
                            col->type().visit_tag([&top_k, &slice_and_key, &store, &col=col, idx=idx, lp, this](auto type_desc_tag) {
                                using TypeDescriptorTag = decltype(type_desc_tag);
                                using RawType = typename TypeDescriptorTag::DataTypeTag::raw_type;

                                if constexpr (is_floating_point_type(TypeDescriptorTag::DataTypeTag::data_type)) {

                                    ColumnData col_data = col->data();
                                    double sum_differences_exponentiated = 0;
                                    auto j = 0u;

                                    while (auto block = col_data.next<TypeDescriptorTag>()) {
                                        auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                                        for (auto progress_in_block = 0u; progress_in_block <
                                                                          block.value().row_count(); ++progress_in_block, ++j, ++ptr) {
                                            sum_differences_exponentiated += pow(
                                                    abs(*ptr - this->query_vector_[j]), lp);
                                        }
                                    }

                                    top_k.try_insert(NamedColumnSortedByDistance(
                                            sum_differences_exponentiated,
                                            col,
                                            slice_and_key.segment(store).field(idx).name()));
                                }
                                else {
                                    internal::raise<ErrorCode::E_INVALID_ARGUMENT>(
                                            "Vectors should exclusively comprise floats; the Python layer should otherwise raise an error and prevent upsertion of vectors containing non-float components."
                                            );
                                }
                            });
                        }
                    }
                }
        );
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

                if constexpr (is_floating_point_type(TypeDescriptorTag::DataTypeTag::data_type)) {
                    auto write_col = std::make_shared<Column>(column.column_->type(), this->query_vector_.size() + 1, true, false);

                    auto col_data = column.column_->data();
                    auto write_ptr = reinterpret_cast<RawType *>(write_col->ptr());
                    while (auto block = col_data.next<TypeDescriptorTag>()) {
                        auto ptr = reinterpret_cast<const RawType *>(block.value().data());
                        std::memcpy(write_ptr, ptr, block.value().row_count() * sizeof(RawType));
                        write_ptr += block.value().row_count();
                    }
                    *write_ptr = RawType(pow(column.distance_, 1 / static_cast<double>(lp)));
                    write_col->set_row_data(this->query_vector_.size());
                    seg.add_column(scalar_field(DataType::FLOAT64, column.name_), write_col);
                    }
                else {
                    internal::raise<ErrorCode::E_INVALID_ARGUMENT>(
                            "Vectors should exclusively comprise floats; the Python layer should otherwise raise an error and prevent upsertion of vectors containing non-float components."
                    );
                }
            });
        }
        return Composite{ProcessingSegment{std::move(seg)}};
    }
}