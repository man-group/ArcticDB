/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb::pipelines {

using namespace arcticdb::entity;

struct IndexPartialKey {
    StreamId id;
    VersionId version_id;
};
}