/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/column_map.hpp>
#include <arcticdb/util/random.h>
#include <gtest/gtest.h>

#include <arcticdb/column_store/column_map.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/util/test/rapidcheck.hpp>
#include <arcticdb/util/test/rapidcheck_generators.hpp>
#include <arcticdb/entity/types.hpp>

using namespace arcticdb;
namespace {

std::optional<size_t> test_find_column_index_by_name(const FieldCollection& frame_fields, std::string_view name) {
    auto col_in_frame_it =
            std::find_if(frame_fields.begin(), frame_fields.end(), [name](const Field& f) { return f.name() == name; });

    if (col_in_frame_it != frame_fields.end()) {
        return std::distance(frame_fields.begin(), col_in_frame_it);
    }
    ARCTICDB_DEBUG(log::version(), "Skipping: {} as it's not in frame", name);
    return std::nullopt;
}

} // namespace

RC_GTEST_PROP(ColumnMap, FromDescriptor, (const arcticdb::entity::StreamDescriptor& desc)) {
    ColumnMap column_map{desc.field_count()};
    column_map.set_from_descriptor(desc);
    for (const auto& field : desc.fields()) {
        auto col_index = column_map.column_index(field.name());
        auto check_col_index = test_find_column_index_by_name(desc.fields(), field.name());
        RC_ASSERT(col_index == check_col_index);
    }
}