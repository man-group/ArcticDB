/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/component_manager.hpp>
#include <arcticdb/version/version_core.hpp>

using namespace arcticdb;
using namespace arcticdb::pipelines;

struct RowSliceClause {
    // Simple clause that accepts and produces segments partitioned by row-slice, which is representative of a lot of
    // the real clauses we support. In place of doing any processing, the process method just sleeps for a random amount
    // of time and then increments the stream id of each input segment.
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
    // Simple clause that accepts non row-slice structured segments to stress the restructuring process (fan-in/fan-out)
    // process method is the same as the RowSliceClause above
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

TEST(Clause, ScheduleClauseProcessingStress) {
    // Extensible stress test of schedule_clause_processing. Useful for ensuring a lack of deadlock when running on
    // threadpools with 1 or multiple cores. Dummy clauses provided above used to stress the fan-in/fan-out behaviour.
    // Could be extended to profile and compare different scheduling algorithms and threadpool implementations if we
    // want to move away from folly.
    using namespace arcticdb::version_store;
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

    // Map from index in segment_and_slice_future_splitters to the number of calls to process in the first clause that
    // will require that segment
    auto segment_fetch_counts = generate_segment_fetch_counts(processing_unit_indexes, num_segments);

    auto processed_entity_ids_fut = schedule_clause_processing(component_manager,
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
