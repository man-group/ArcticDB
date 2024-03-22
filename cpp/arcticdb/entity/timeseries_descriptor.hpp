/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/field_collection.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>

namespace arcticdb {

struct FrameDescriptorImpl : public FrameDescriptor {
    FrameDescriptorImpl() = default;

    ARCTICDB_MOVE_COPY_DEFAULT(FrameDescriptorImpl)

    [[nodiscard]] FrameDescriptorImpl clone() const {
        return *this;
    }
};

struct TimeseriesDescriptor {
  using Proto = arcticdb::proto::descriptors::FrameMetadata;

  std::shared_ptr<FrameDescriptorImpl> data_ = std::make_shared<FrameDescriptorImpl>();
  std::shared_ptr<Proto> proto_ = std::make_shared<Proto>();
  std::shared_ptr<FieldCollection> fields_ = std::make_shared<FieldCollection>();

  TimeseriesDescriptor() = default;

  TimeseriesDescriptor(
      std::shared_ptr<FrameDescriptorImpl> data,
      std::shared_ptr<Proto> proto,
      std::shared_ptr<FieldCollection> fields) :
    data_(data),
    proto_(std::move(proto)),
    fields_(std::move(fields)) {
  }

  const FrameDescriptorImpl& frame_descriptor() const {
      return *data_;
  }

  void set_stream_descriptor(const StreamDescriptor& desc) {
      data_ = desc.data_ptr();
      fields_ = desc.fields_ptr();
  }

  void set_total_rows(uint64_t rows) {
      data_->total_rows_ = rows;
  }

  [[nodiscard]] uint64_t total_rows() const {
      return data_->total_rows_;
  }


  arcticdb::proto::descriptors::UserDefinedMetadata&& detach_user_metadata() {
    return std::move(*proto_->mutable_multi_key_meta());
  }

  arcticdb::proto::descriptors::NormalizationMetadata&& detach_normalization_metadata() {
    return std::move(*proto_->mutable_normalization());
  }

  arcticdb::proto::descriptors::UserDefinedMetadata&& detach_multi_key_metadata() {
    return std::move(*proto_->mutable_multi_key_meta());
  }

  void set_user_metadata(arcticdb::proto::descriptors::UserDefinedMetadata&& user_meta) {
      *proto_->mutable_user_meta() = std::move(user_meta);
  }

  void set_normalization_metadata(arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta) {
      *proto_->mutable_normalization() = std::move(norm_meta);
  }

  void set_multi_key_metadata(arcticdb::proto::descriptors::UserDefinedMetadata&& multi_key_meta) {
      *proto_->mutable_multi_key_meta() = std::move(multi_key_meta);
  }

  [[nodiscard]] std::shared_ptr<FieldCollection> fields_ptr() const  {
      return fields_;
  }

  [[nodiscard]] std::shared_ptr<Proto> proto_ptr() const {
      return proto_;
  }

  [[nodiscard]] bool proto_is_null() const {
      return !proto_;
  }

  [[nodiscard]] const FieldCollection& fields() const {
      return *fields_;
  }

  [[nodiscard]] FieldCollection& mutable_fields() {
      return *fields_;
  }

  [[nodiscard]] Proto& mutable_proto() {
       return *proto_;
  }

  [[nodiscard]] const Proto& proto() const {
      return *proto_;
  }

  [[nodiscard]] TimeseriesDescriptor clone() const {
      auto proto = std::make_shared<Proto>();
      proto->CopyFrom(*proto_);
      auto frame_desc = std::make_shared<FrameDescriptorImpl>(data_->clone());
      return {std::move(frame_desc), std::move(proto), std::make_shared<FieldCollection>(fields_->clone())};
  }

  [[nodiscard]] bool column_groups() const {
      return data_->column_groups_;
  }

  [[nodiscard]] StreamDescriptor as_stream_descriptor() const {
      return {data_, fields_};
  }
};

}