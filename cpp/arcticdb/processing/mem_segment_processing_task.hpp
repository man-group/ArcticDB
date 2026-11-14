/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/async/base_task.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/test/random_throw.hpp>
#include <arcticdb/util/timer.hpp>

#include <memory>
#include <vector>

namespace arcticdb::async {

struct MemSegmentProcessingTask : BaseTask {
    std::vector<std::shared_ptr<Clause>> clauses_;
    std::vector<EntityId> entity_ids_;
    timestamp creation_time_;

    explicit MemSegmentProcessingTask(
            std::vector<std::shared_ptr<Clause>> clauses, std::vector<EntityId>&& entity_ids
    ) :
        clauses_(std::move(clauses)),
        entity_ids_(std::move(entity_ids)),
        creation_time_(util::SysClock::coarse_nanos_since_epoch()) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(MemSegmentProcessingTask)

    std::vector<EntityId> operator()() {
        ARCTICDB_DEBUG_THROW(5)
        const auto nanos_start = util::SysClock::coarse_nanos_since_epoch();
        const auto time_in_queue = double(nanos_start - creation_time_) / BILLION;
        ARCTICDB_RUNTIME_DEBUG(log::inmem(), "Segment processing task running after {}s queue time", time_in_queue);
        for (auto it = clauses_.cbegin(); it != clauses_.cend(); ++it) {
            entity_ids_ = (*it)->process(std::move(entity_ids_));

            auto next_it = std::next(it);
            if (next_it != clauses_.cend() &&
                (*it)->clause_info().output_structure_ != (*next_it)->clause_info().input_structure_)
                break;
        }
        const auto nanos_end = util::SysClock::coarse_nanos_since_epoch();
        const auto time_taken = double(nanos_end - nanos_start) / BILLION;
        ARCTICDB_RUNTIME_DEBUG(log::inmem(), "Segment processing task completed after {}s run time", time_taken);
        return std::move(entity_ids_);
    }
};

} // namespace arcticdb::async
