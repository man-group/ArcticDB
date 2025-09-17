/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <vector>

namespace arcticdb {
struct StageResult {
    explicit StageResult(std::vector<entity::AtomKey> staged_segments) : staged_segments(std::move(staged_segments)) {}

    std::vector<entity::AtomKey> staged_segments;
};
} // namespace arcticdb