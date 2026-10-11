#include <fmt/ostream.h>
#include <arcticdb/column_store/memory_segment_pretty_print.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/test/gtest_custom_formatters.hpp>

MAKE_GTEST_FMT(arcticdb::entity::RefKey, "{}")
MAKE_GTEST_FMT(arcticdb::entity::AtomKeyImpl, "{}")
MAKE_GTEST_FMT(arcticdb::entity::VariantKey, "VariantKey({})")
MAKE_GTEST_FMT(arcticdb::VariantId, "VariantId({})")
MAKE_GTEST_FMT(arcticdb::SegmentInMemory, "{}")
MAKE_GTEST_FMT(arcticdb::pipelines::RowRange, "{}")
MAKE_GTEST_FMT(arcticdb::pipelines::ColRange, "{}")
