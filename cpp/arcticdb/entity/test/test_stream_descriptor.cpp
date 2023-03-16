#include <gtest/gtest.h>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/stream/index.hpp>

namespace arcticdb {

TEST(StreamDescriptor, InitializerList) {
    auto desc ARCTICDB_UNUSED = stream::TimeseriesIndex::default_index().create_stream_descriptor(
        999, {
            scalar_field(DataType::UINT64, "val1"),
            scalar_field(DataType::UINT64, "val2verylongname"),
            scalar_field(DataType::UINT64, "val3")
        });
}
}