/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb {

struct FrameAndDescriptor {
    SegmentInMemory frame_;
    arcticdb::proto::descriptors::TimeSeriesDescriptor desc_;
    std::vector<AtomKey> keys_;
};

} //namespace arcticdb
