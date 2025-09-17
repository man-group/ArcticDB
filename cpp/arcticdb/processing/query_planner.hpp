/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <variant>
#include <vector>

#include <arcticdb/processing/clause.hpp>
#include <arcticdb/processing/grouper.hpp>

namespace arcticdb {

using GroupByClause = PartitionClause<arcticdb::grouping::HashingGroupers, arcticdb::grouping::ModuloBucketizer>;
using ClauseVariant = std::variant<
        std::shared_ptr<FilterClause>, std::shared_ptr<ProjectClause>, std::shared_ptr<GroupByClause>,
        std::shared_ptr<AggregationClause>, std::shared_ptr<ResampleClause<ResampleBoundary::LEFT>>,
        std::shared_ptr<ResampleClause<ResampleBoundary::RIGHT>>, std::shared_ptr<RowRangeClause>,
        std::shared_ptr<DateRangeClause>, std::shared_ptr<ConcatClause>>;

std::vector<ClauseVariant> plan_query(std::vector<ClauseVariant>&& clauses);

} // namespace arcticdb
