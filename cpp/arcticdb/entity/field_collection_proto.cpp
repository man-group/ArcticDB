/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/field_collection_proto.hpp>
#include <arcticdb/entity/types_proto.hpp>

namespace arcticdb {

FieldCollection fields_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc) {
    FieldCollection output;
    for (const auto& field : desc.fields())
        output.add_field(type_desc_from_proto(field.type_desc()), field.name());

    return output;
}

void proto_from_fields(const FieldCollection& fields, arcticdb::proto::descriptors::StreamDescriptor& desc) {
    for (const auto& field : fields) {
        auto new_field = desc.add_fields();
        new_field->MergeFrom(field_proto(field.type().data_type(), field.type().dimension(), field.name()));
    }
}

} // namespace arcticdb