/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/field_collection.hpp>
#include <descriptors.pb.h>

namespace arcticdb {

FieldCollection fields_from_proto(const arcticdb::proto::descriptors::StreamDescriptor& desc);

void proto_from_fields(const FieldCollection& fields, arcticdb::proto::descriptors::StreamDescriptor& desc);

} // namespace arcticdb
