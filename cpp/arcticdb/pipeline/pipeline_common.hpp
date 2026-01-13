/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;

struct IndexPartialKey {
    StreamId id;
    VersionId version_id;
};

struct TypedStreamVersion {
    StreamId id;
    VersionId version_id;
    KeyType type;
};

} // namespace arcticdb::pipelines