/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/async/tasks.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/component_manager.hpp>
#include <folly/futures/FutureSplitter.h>

using namespace arcticdb;
using namespace arcticdb::pipelines;

struct RowSliceClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;

    RowSliceClause() = default;
    ARCTICDB_MOVE_COPY_DEFAULT(RowSliceClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        log::version().warn("RowSliceClause::structure_for_processing called");
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const {
        std::mt19937_64 eng{std::random_device{}()};
        std::uniform_int_distribution<> dist{10, 100};
        auto sleep_ms = dist(eng);
        log::version().warn("RowSliceClause::process sleeping for {}ms", sleep_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds{sleep_ms});
        if (entity_ids.empty()) {
            return {};
        }
        auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
        for (const auto& segment: proc.segments_.value()) {
            auto id = std::get<int64_t>(segment->descriptor().id());
            ++id;
            segment->descriptor().set_id(id);
        }
        return push_entities(*component_manager_, std::move(proc));
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig&) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }
};

struct RestructuringClause {
    ClauseInfo clause_info_;
    std::shared_ptr<ComponentManager> component_manager_;

    RestructuringClause() {
        clause_info_.input_structure_ = ProcessingStructure::ALL;
    };
    ARCTICDB_MOVE_COPY_DEFAULT(RestructuringClause)

    [[nodiscard]] std::vector<std::vector<size_t>> structure_for_processing(std::vector<RangesAndKey>& ranges_and_keys) {
        return structure_by_row_slice(ranges_and_keys);
    }

    [[nodiscard]] std::vector<std::vector<EntityId>> structure_for_processing(std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        log::version().warn("RestructuringClause::structure_for_processing called");
        return structure_by_row_slice(*component_manager_, std::move(entity_ids_vec));
    }

    [[nodiscard]] std::vector<EntityId> process(std::vector<EntityId>&& entity_ids) const {
        std::mt19937_64 eng{std::random_device{}()};
        std::uniform_int_distribution<> dist{10, 100};
        auto sleep_ms = dist(eng);
        log::version().warn("RestructuringClause::process sleeping for {}ms", sleep_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds{sleep_ms});
        if (entity_ids.empty()) {
            return {};
        }
        auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager_, std::move(entity_ids));
        for (const auto& segment: proc.segments_.value()) {
            auto id = std::get<int64_t>(segment->descriptor().id());
            ++id;
            segment->descriptor().set_id(id);
        }
        return push_entities(*component_manager_, std::move(proc));
    }

    [[nodiscard]] const ClauseInfo& clause_info() const {
        return clause_info_;
    }

    void set_processing_config(ARCTICDB_UNUSED const ProcessingConfig&) {}

    void set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
        component_manager_ = component_manager;
    }
};

size_t generate_scheduling_iterations(const std::vector<std::shared_ptr<Clause>>& clauses) {
    size_t res{1};
    auto it = std::next(clauses.cbegin());
    while (it != clauses.cend()) {
        auto prev_it = std::prev(it);
        if ((*prev_it)->clause_info().output_structure_ != (*it)->clause_info().input_structure_) {
            ++res;
        }
        ++it;
    }
    return res;
}

folly::Future<std::vector<EntityId>> process_clauses(
        std::shared_ptr<ComponentManager> component_manager,
        std::vector<folly::Future<pipelines::SegmentAndSlice>>&& segment_and_slice_futures,
        std::vector<std::vector<size_t>>&& processing_unit_indexes,
        std::shared_ptr<std::vector<std::shared_ptr<Clause>>> clauses) {
    // All the shared pointers as arguments to this function and created within it are to ensure that resources are
    // correctly kept alive after this function returns it's future
    auto num_segments = segment_and_slice_futures.size();
    auto segment_and_slice_future_splitters = split_futures(std::move(segment_and_slice_futures));

    // Map from index in segment_and_slice_future_splitters to the number of calls to process in the first clause that
    // will require that segment
    auto segment_fetch_counts = generate_segment_fetch_counts(processing_unit_indexes, num_segments);
    // Map from position in segment_and_slice_future_splitters to entity ids
    std::vector<EntityId> pos_to_id;
    // Map from entity id to position in segment_and_slice_futures
    auto id_to_pos = std::make_shared<ankerl::unordered_dense::map<EntityId, size_t>>();
    pos_to_id.reserve(num_segments);
    auto ids = component_manager->get_new_entity_ids(num_segments);
    for (auto&& [idx, id]: folly::enumerate(ids)) {
        pos_to_id.emplace_back(id);
        id_to_pos->emplace(id, idx);
    }

    std::vector<std::vector<EntityId>> entity_ids_vec;
    entity_ids_vec.reserve(processing_unit_indexes.size());
    for (const auto& indexes: processing_unit_indexes) {
        entity_ids_vec.emplace_back();
        entity_ids_vec.back().reserve(indexes.size());
        for (auto index: indexes) {
            entity_ids_vec.back().emplace_back(pos_to_id[index]);
        }
    }

    // Used to make sure each entity is only added into the component manager once
    auto slice_added_mtx = std::make_shared<std::vector<std::mutex>>(num_segments);
    auto slice_added = std::make_shared<std::vector<bool>>(num_segments, false);
    auto futures = std::make_shared<std::vector<folly::Future<std::vector<EntityId>>>>();

    for (auto&& entity_ids: entity_ids_vec) {
        std::vector<folly::Future<pipelines::SegmentAndSlice>> local_futs;
        local_futs.reserve(entity_ids.size());
        for (auto id: entity_ids) {
            local_futs.emplace_back(segment_and_slice_future_splitters[id_to_pos->at(id)].getFuture());
        }
        futures->emplace_back(
                folly::collect(local_futs)
                        .via(&async::cpu_executor())
                        .thenValue([component_manager,
                                    segment_fetch_counts,
                                    id_to_pos,
                                    slice_added_mtx,
                                    slice_added,
                                    clauses,
                                    entity_ids = std::move(entity_ids)](std::vector<pipelines::SegmentAndSlice>&& segment_and_slices) mutable {
                            for (auto&& [idx, segment_and_slice]: folly::enumerate(segment_and_slices)) {
                                auto entity_id = entity_ids[idx];
                                auto pos = id_to_pos->at(entity_id);
                                std::lock_guard<std::mutex> lock((*slice_added_mtx)[pos]);
                                if (!(*slice_added)[pos]) {
                                    component_manager->add_entity(
                                            entity_id,
                                            std::make_shared<SegmentInMemory>(std::move(segment_and_slice.segment_in_memory_)),
                                            std::make_shared<RowRange>(std::move(segment_and_slice.ranges_and_key_.row_range_)),
                                            std::make_shared<ColRange>(std::move(segment_and_slice.ranges_and_key_.col_range_)),
                                            std::make_shared<AtomKey>(std::move(segment_and_slice.ranges_and_key_.key_)),
                                            (*segment_fetch_counts)[pos]
                                    );
                                    (*slice_added)[pos] = true;
                                }
                            }
                            return async::MemSegmentProcessingTask(*clauses, std::move(entity_ids))();
                        }));
    }

    auto entity_ids_vec_fut = folly::Future<std::vector<std::vector<EntityId>>>::makeEmpty();
    bool first_clause{true};
    // The number of iterations we need to pass through the following loop to get all the work scheduled
    auto scheduling_iterations = generate_scheduling_iterations(*clauses);

    for (size_t unused=0; unused<scheduling_iterations; ++unused) {
        folly::Future<folly::Unit> work_scheduled(folly::Unit{});
        if (!first_clause) {
            work_scheduled = entity_ids_vec_fut.via(&async::cpu_executor()).thenValue([futures, clauses](std::vector<std::vector<EntityId>>&& entity_ids_vec) {
                futures->clear();
                for (auto&& entity_ids: entity_ids_vec) {
                    futures->emplace_back(async::submit_cpu_task(async::MemSegmentProcessingTask(*clauses,std::move(entity_ids))));
                }
                return folly::Unit{};
            });
        }
        first_clause = false;

        entity_ids_vec_fut = work_scheduled.via(&async::cpu_executor()).thenValue([clauses, futures](auto&&) {
            return folly::collect(*futures).via(&async::cpu_executor()).thenValue([clauses](std::vector<std::vector<EntityId>>&& entity_ids_vec) {
                // Erase all the clauses we have already called process on
                auto it = std::next(clauses->cbegin());
                while (it != clauses->cend()) {
                    auto prev_it = std::prev(it);
                    if ((*prev_it)->clause_info().output_structure_ == (*it)->clause_info().input_structure_) {
                        ++it;
                    } else {
                        break;
                    }
                }
                clauses->erase(clauses->cbegin(), it);
                if (!clauses->empty()) {
                    return clauses->front()->structure_for_processing(std::move(entity_ids_vec));
                } else {
                    return entity_ids_vec;
                }
            });
        });
    }
    return entity_ids_vec_fut.via(&async::cpu_executor()).thenValue([](std::vector<std::vector<EntityId>>&& entity_ids_vec) {
        return flatten_entities(std::move(entity_ids_vec));
    });
}

TEST(Clause, ParallelProcessing) {
    auto num_clauses = 5;
    std::mt19937_64 eng{std::random_device{}()};
    std::uniform_int_distribution<> dist{0, 1};

    auto clauses = std::make_shared<std::vector<std::shared_ptr<Clause>>>();
    for (auto unused=0; unused<num_clauses; ++unused) {
        if (dist(eng) == 0) {
            clauses->emplace_back(std::make_shared<Clause>(RowSliceClause()));
        } else {
            clauses->emplace_back(std::make_shared<Clause>(RestructuringClause()));
        }
    }

    auto component_manager = std::make_shared<ComponentManager>();
    for (auto& clause: *clauses) {
        clause->set_component_manager(component_manager);
    }

    size_t num_segments{2};
    std::vector<folly::Promise<SegmentAndSlice>> segment_and_slice_promises(num_segments);
    std::vector<folly::Future<SegmentAndSlice>> segment_and_slice_futures;
    std::vector<std::vector<size_t>> processing_unit_indexes;
    for (size_t idx = 0; idx < num_segments; ++idx) {
        segment_and_slice_futures.emplace_back(segment_and_slice_promises[idx].getFuture());
        processing_unit_indexes.emplace_back(std::vector<size_t>{idx});
    }

    auto processed_entity_ids_fut = process_clauses(component_manager,
                                                    std::move(segment_and_slice_futures),
                                                    std::move(processing_unit_indexes),
                                                    clauses);

    for (size_t idx = 0; idx < segment_and_slice_promises.size(); ++idx) {
        SegmentInMemory segment;
        segment.descriptor().set_id(static_cast<int64_t>(idx));
        segment_and_slice_promises[idx].setValue(SegmentAndSlice(RangesAndKey({idx, idx+1}, {0, 1}, {}), std::move(segment)));
    }

    auto processed_entity_ids = std::move(processed_entity_ids_fut).get();
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(*component_manager, std::move(processed_entity_ids));
    ASSERT_EQ(proc.segments_.value().size(), num_segments);
    NumericId start_id{0};
    for (const auto& segment: proc.segments_.value()) {
        auto id = std::get<NumericId>(segment->descriptor().id());
        ASSERT_EQ(id, start_id++ + num_clauses);
    }
}
