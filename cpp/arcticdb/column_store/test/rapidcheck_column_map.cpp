/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/column_store/column_map.hpp>
#include <arcticdb/util/random.h>
#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>
#include <arcticdb/util/test/rapidcheck_generators.hpp>
#include <arcticdb/entity/types.hpp>

using namespace arcticdb;
namespace {

std::optional<size_t> test_find_column_index_by_name(const StreamDescriptor::FieldsCollection& frame_fields,
                                                     const std::string &name) {
    auto col_in_frame_it = std::find_if(frame_fields.begin(), frame_fields.end(),
                                        [name](FieldDescriptor::Proto f) { return f.name() == name; });

    if (col_in_frame_it != frame_fields.end()) {
        return std::distance(frame_fields.begin(), col_in_frame_it);
    }
    ARCTICDB_DEBUG(log::version(), "Skipping: {} as it's not in frame", name);
    return std::nullopt;
}

}

RC_GTEST_PROP(ColumnMap, FromDescriptor, (const arcticdb::entity::StreamDescriptor& desc)) {
    ColumnMap column_map{desc.field_count()};
    column_map.set_from_descriptor(desc);
    for (const auto& field : desc.fields()) {
        auto col_index = column_map.column_index(field.name());
        auto check_col_index = test_find_column_index_by_name(desc.fields(), field.name());
       RC_ASSERT(col_index == check_col_index);
    }
}