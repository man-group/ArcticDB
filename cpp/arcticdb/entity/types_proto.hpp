/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <descriptors.pb.h>
#include <arcticdb/memory_layout.hpp>
#include <arcticdb/entity/types.hpp>

namespace arcticdb::proto {
namespace descriptors = arcticc::pb2::descriptors_pb2;
} //namespace arcticdb::proto

namespace arcticdb::entity {

using FieldProto = arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor;

bool operator==(const FieldProto &left, const FieldProto &right);
bool operator<(const FieldProto &left, const FieldProto &right);

arcticdb::proto::descriptors::SortedValue sorted_value_to_proto(SortedValue sorted);

SortedValue sorted_value_from_proto(arcticdb::proto::descriptors::SortedValue sorted_proto);

void set_data_type(DataType data_type, arcticdb::proto::descriptors::TypeDescriptor &type_desc);

DataType get_data_type(const arcticdb::proto::descriptors::TypeDescriptor &type_desc);

TypeDescriptor type_desc_from_proto(const arcticdb::proto::descriptors::TypeDescriptor &type_desc);

DataType data_type_from_proto(const arcticdb::proto::descriptors::TypeDescriptor &type_desc);

arcticdb::proto::descriptors::StreamDescriptor_FieldDescriptor field_proto(
    DataType dt,
    Dimension dim,
    std::string_view name);

} // namespace arcticdb::entity